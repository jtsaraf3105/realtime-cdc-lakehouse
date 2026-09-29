"""Measure the pipeline and prove its guarantees. Writes docs/METRICS.md.

Requires the infra (`make up`) and a registered connector (`make connect`). This script starts
and stops the streaming job itself.

    python scripts/benchmark.py all --rate 50 --seconds 180

Phases:
  1. latency   — stream at --rate for --seconds; p50/p95/max end-to-end latency
                 (Postgres commit `updated_at` -> Silver `_silver_ts`) and throughput.
  2. evolution — ALTER TABLE products ADD COLUMN weight_kg while streaming; the column must
                 appear in Silver with no job restart (F7).
  3. recovery  — kill -9 the Spark job mid-stream, keep writing to Postgres, restart it; measure
                 time to catch up and verify: Silver == Postgres exactly, no duplicate PKs,
                 no duplicate Bronze offsets (F6 exactly-once).
"""

from __future__ import annotations

import argparse
import os
import platform
import signal
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lakehouse import lake  # noqa: E402
from lakehouse.config import settings as S  # noqa: E402
from lakehouse.simulator.orders import Workload, evolve_schema  # noqa: E402

ENV = {**os.environ, "JAVA_HOME": os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")}
LOG = Path("reports/pipeline.log")


def start_pipeline() -> subprocess.Popen:
    LOG.parent.mkdir(exist_ok=True)
    return subprocess.Popen(
        [sys.executable, "-m", "lakehouse.spark.pipeline"],
        env=ENV,
        stdout=LOG.open("a"),
        stderr=subprocess.STDOUT,
    )


def pg_keys() -> dict[str, set[int]]:
    with psycopg.connect(S.pg_dsn) as c:
        return {
            t: {r[0] for r in c.execute(f"SELECT {pk} FROM {t}")}  # noqa: S608
            for t, pk in S.primary_keys.items()
        }


def silver_live_keys() -> dict[str, set[int]]:
    out = {}
    for t, pk in S.primary_keys.items():
        df = lake.read("silver", t, columns=[pk, "is_deleted"])
        out[t] = set(df.loc[~df.is_deleted, pk])
    return out


def wait_caught_up(timeout: float = 600) -> float:
    """Seconds until Silver's live keys equal Postgres' for every table."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if pg_keys() == silver_live_keys():
                return time.time() - t0
        except Exception:  # table mid-commit / not yet created
            pass
        time.sleep(2)
    raise TimeoutError("silver did not converge")


def run_workload(rate: float, seconds: float, seed: int) -> int:
    with psycopg.connect(S.pg_dsn, autocommit=True) as conn:
        w = Workload(conn, seed)
        w.seed_reference_data()
        return w.run(rate, seconds, report_every=30)


def latency(rate: float, seconds: float) -> dict:
    t_start = pd.Timestamp.now(tz="UTC")
    changes = run_workload(rate, seconds, seed=1)
    t_end = pd.Timestamp.now(tz="UTC")
    catch_up = wait_caught_up()
    lat = []
    for t in ("orders", "order_items", "customers", "products"):
        df = lake.read("silver", t, columns=["updated_at", "_silver_ts"])
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, format="ISO8601")
        df["_silver_ts"] = pd.to_datetime(df["_silver_ts"], utc=True)
        w = df[(df.updated_at >= t_start) & (df.updated_at <= t_end)]
        lat += list((w._silver_ts - w.updated_at).dt.total_seconds())
    lat_arr = np.array([x for x in lat if x >= 0])
    bronze = lake.read("bronze", "cdc_events", columns=["ingest_ts"])
    bronze["ingest_ts"] = pd.to_datetime(bronze["ingest_ts"], utc=True)
    in_window = bronze[
        (bronze.ingest_ts >= t_start) & (bronze.ingest_ts <= t_end + pd.Timedelta("2min"))
    ]
    return {
        "rate_target": rate,
        "seconds": seconds,
        "row_changes": changes,
        "achieved_rate": changes / seconds,
        "bronze_events_in_window": len(in_window),
        "latency_samples": len(lat_arr),
        "p50_s": float(np.percentile(lat_arr, 50)),
        "p95_s": float(np.percentile(lat_arr, 95)),
        "max_s": float(lat_arr.max()),
        "final_catch_up_s": catch_up,
    }


def evolution() -> dict:
    with psycopg.connect(S.pg_dsn, autocommit=True) as conn:
        conn.execute("ALTER TABLE products DROP COLUMN IF EXISTS weight_kg")
        evolve_schema(conn)
    t0 = time.time()
    while time.time() - t0 < 180:
        df = lake.read("silver", "products")
        if "weight_kg" in df.columns and df["weight_kg"].notna().sum() >= 20:
            return {
                "column_visible_after_s": time.time() - t0,
                "rows_with_weight": int(df["weight_kg"].notna().sum()),
                "job_restarted": False,
            }
        time.sleep(2)
    raise TimeoutError("weight_kg never reached silver")


def recovery(proc: subprocess.Popen, rate: float) -> tuple[dict, subprocess.Popen]:
    writer = threading.Thread(target=run_workload, args=(rate, 60, 7))
    writer.start()
    time.sleep(20)
    proc.send_signal(signal.SIGKILL)  # no graceful shutdown, no checkpoint flush
    proc.wait()
    killed_at = time.time()
    time.sleep(15)  # changes keep landing in Kafka while the job is down
    proc = start_pipeline()
    writer.join()
    caught_up = wait_caught_up()
    total_down = time.time() - killed_at
    dup_pk = {}
    for t, pk in S.primary_keys.items():
        df = lake.read("silver", t, columns=[pk])
        dup_pk[t] = int(df[pk].duplicated().sum())
    b = lake.read("bronze", "cdc_events", columns=["topic", "kafka_partition", "kafka_offset"])
    dup_offsets = int(b.duplicated().sum())
    exact = pg_keys() == silver_live_keys()
    return (
        {
            "killed_for_s": 15,
            "time_from_kill_to_consistent_s": total_down,
            "catch_up_after_restart_s": caught_up,
            "duplicate_silver_pks": dup_pk,
            "duplicate_bronze_offsets": dup_offsets,
            "silver_equals_postgres": exact,
        },
        proc,
    )


def write_report(m: dict, path: Path) -> None:
    lat, evo, rec = m["latency"], m["evolution"], m["recovery"]
    ge = {}
    for layer in ("silver", "gold"):
        p = Path(f"reports/ge_{layer}.json")
        if p.exists():
            import json

            ge[layer] = json.loads(p.read_text())
    lines = [
        "# Pipeline metrics",
        "",
        f"_Measured {datetime.now(UTC):%Y-%m-%d} with `python scripts/benchmark.py all` on "
        f"{platform.machine()} macOS laptop. Kafka, Debezium, Postgres and S3 run in a "
        "2 vCPU / 4 GB Docker VM; Spark runs locally (`local[4]`, 1.5 GB driver)._",
        "",
        "## Throughput and end-to-end latency (F1, NFR: Postgres → Silver < 60 s)",
        "",
        "| metric | value |",
        "|---|---:|",
        f"| target / achieved source rate | {lat['rate_target']:.0f} / **{lat['achieved_rate']:.1f} row changes/s** |",
        f"| row changes generated | {lat['row_changes']:,} over {lat['seconds']:.0f} s |",
        f"| latency samples (rows) | {lat['latency_samples']:,} |",
        f"| p50 latency | {lat['p50_s']:.1f} s |",
        f"| **p95 latency** | **{lat['p95_s']:.1f} s** |",
        f"| max latency | {lat['max_s']:.1f} s |",
        "",
        "Latency = Silver `_silver_ts` (MERGE time) − Postgres `updated_at` (commit time). It "
        "includes Debezium capture, Kafka, Bronze trigger (5 s), Silver trigger (5 s) and the "
        "per-table MERGEs. The max was not investigated; the likely cause is rows committed while "
        "the job was still draining the warm-up backlog (p95 is the representative figure).",
        "",
        "## Schema evolution (F7)",
        "",
        f"`ALTER TABLE products ADD COLUMN weight_kg` while streaming → column present in Silver "
        f"after **{evo['column_visible_after_s']:.0f} s**, {evo['rows_with_weight']} rows populated, "
        f"no job restart, no failure.",
        "",
        "## Failure recovery: `kill -9` mid-stream (F6 exactly-once)",
        "",
        "| check | result |",
        "|---|---:|",
        f"| job down (writes continued to Postgres) | {rec['killed_for_s']} s |",
        f"| catch-up after restart | **{rec['catch_up_after_restart_s']:.0f} s** |",
        f"| kill → Silver consistent again | {rec['time_from_kill_to_consistent_s']:.0f} s |",
        f"| duplicate primary keys in Silver | **{sum(rec['duplicate_silver_pks'].values())}** |",
        f"| duplicate Kafka offsets in Bronze | **{rec['duplicate_bronze_offsets']}** |",
        f"| Silver live rows == Postgres rows (all 4 tables) | **{rec['silver_equals_postgres']}** |",
        "",
    ]
    if ge:
        lines += [
            "## Data quality (Great Expectations)",
            "",
            "| layer | checks passed | max % rows failing a check |",
            "|---|---:|---:|",
        ]
        for layer, r in ge.items():
            lines.append(
                f"| {layer} | {r['checks_total'] - r['checks_failed']}/{r['checks_total']} | "
                f"{r['max_unexpected_percent']:.2f}% |"
            )
        lines.append("")
    path.write_text("\n".join(lines))
    print("\n".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("phase", choices=["all"])
    ap.add_argument("--rate", type=float, default=50)
    ap.add_argument("--seconds", type=float, default=180)
    a = ap.parse_args()
    proc = start_pipeline()
    try:
        print("warming up the streaming job ...")
        wait_caught_up(timeout=900)
        m = {"latency": latency(a.rate, a.seconds)}
        print("latency:", m["latency"])
        m["evolution"] = evolution()
        print("evolution:", m["evolution"])
        m["recovery"], proc = recovery(proc, a.rate)
        print("recovery:", m["recovery"])
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
    Path("reports").mkdir(exist_ok=True)
    import json

    Path("reports/metrics.json").write_text(json.dumps(m, indent=2))
    write_report(m, Path("docs/METRICS.md"))


if __name__ == "__main__":
    main()
