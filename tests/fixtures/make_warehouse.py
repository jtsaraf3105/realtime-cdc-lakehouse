"""CI helper: build a DuckDB warehouse from committed Parquet snapshots of the Silver tables,
so `dbt build` runs in CI without Kafka/Spark.

    python tests/fixtures/make_warehouse.py --export   # (maintainers) snapshot current Silver
    python tests/fixtures/make_warehouse.py            # load fixtures -> warehouse/ci.duckdb
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
FIX = Path(__file__).with_name("raw")
TABLES = ["customers", "customers_history", "products", "orders", "order_items"]


def export() -> None:
    from lakehouse import lake

    FIX.mkdir(exist_ok=True)
    for t in TABLES:
        df = lake.read("silver", t)
        df.to_parquet(FIX / f"{t}.parquet", index=False)
        print(f"exported {t}: {len(df)} rows")


def build(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    for t in TABLES:
        con.execute(
            f"CREATE OR REPLACE TABLE raw.{t} AS "
            f"SELECT * FROM read_parquet('{FIX / (t + '.parquet')}')"
        )
    con.close()
    print(f"built {path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--export", action="store_true")
    ap.add_argument("--path", default=str(ROOT / "warehouse" / "ci.duckdb"))
    a = ap.parse_args()
    export() if a.export else build(Path(a.path))
