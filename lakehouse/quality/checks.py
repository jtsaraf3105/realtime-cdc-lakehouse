"""Great Expectations checkpoints for Silver (lake) and Gold (warehouse marts).

    python -m lakehouse.quality.checks silver     # exit 1 if any expectation fails
    python -m lakehouse.quality.checks gold
    python -m lakehouse.quality.checks silver --freshness-minutes 60

Why both dbt tests AND Great Expectations? dbt tests guard the *transformations* (keys,
relationships, business rules in SQL). GX guards the *data arriving* before dbt runs (row
counts, null %, ranges, freshness) and the Gold outputs with statistical/cross-system checks —
and it runs on the lake (pandas over Delta), where dbt doesn't reach. Results (incl. the % of
rows failing each check) are written to reports/ge_<layer>.json for the metrics report.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import great_expectations as gx
import pandas as pd
from great_expectations import expectations as E

from lakehouse import lake
from lakehouse.config import settings

logging.getLogger("great_expectations").setLevel(logging.ERROR)
REPORTS = Path("reports")


def silver_expectations(freshness_minutes: int) -> dict[str, list]:
    fresh_after = pd.Timestamp(datetime.now(UTC) - timedelta(minutes=freshness_minutes))

    def fresh() -> E.ExpectColumnMaxToBeBetween:  # a new instance per suite (GX binds each to one)
        return E.ExpectColumnMaxToBeBetween(column="_silver_ts", min_value=fresh_after)

    return {
        "customers": [
            E.ExpectTableRowCountToBeBetween(min_value=1),
            E.ExpectColumnValuesToNotBeNull(column="customer_id"),
            E.ExpectColumnValuesToBeUnique(column="customer_id"),
            E.ExpectColumnValuesToNotBeNull(column="email", mostly=0.99),
            E.ExpectColumnValuesToBeInSet(column="tier", value_set=["standard", "silver", "gold"]),
            fresh(),
        ],
        "customers_history": [
            E.ExpectColumnValuesToNotBeNull(column="valid_from"),
            E.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A="valid_to",
                column_B="valid_from",
                or_equal=True,
                ignore_row_if="either_value_is_missing",
            ),
        ],
        "products": [
            E.ExpectTableRowCountToBeBetween(min_value=1),
            E.ExpectColumnValuesToBeUnique(column="product_id"),
            E.ExpectColumnValuesToBeBetween(column="price_num", min_value=0.01, max_value=10_000),
            E.ExpectColumnValuesToNotBeNull(column="category"),
        ],
        "orders": [
            E.ExpectTableRowCountToBeBetween(min_value=1),
            E.ExpectColumnValuesToBeUnique(column="order_id"),
            E.ExpectColumnValuesToNotBeNull(column="customer_id"),
            E.ExpectColumnValuesToBeInSet(
                column="status", value_set=["created", "paid", "shipped", "delivered", "cancelled"]
            ),
            E.ExpectColumnValuesToBeBetween(column="total_num", min_value=0, max_value=100_000),
            fresh(),
        ],
        "order_items": [
            E.ExpectColumnValuesToBeUnique(column="order_item_id"),
            E.ExpectColumnValuesToBeBetween(column="quantity", min_value=1, max_value=10),
            E.ExpectColumnValuesToNotBeNull(column="order_id"),
        ],
    }


def load_silver() -> dict[str, pd.DataFrame]:
    frames = {
        t: lake.read("silver", t)
        for t in ["customers", "customers_history", "products", "orders", "order_items"]
    }
    # decimals arrive as strings (decimal.handling.mode=string) — numeric views for range checks
    frames["products"]["price_num"] = pd.to_numeric(frames["products"]["price"])
    frames["orders"]["total_num"] = pd.to_numeric(frames["orders"]["total_amount"])
    for df in frames.values():
        for col in ("valid_from", "valid_to", "_silver_ts"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)
    return frames


def load_gold(path: str) -> tuple[dict[str, pd.DataFrame], dict[str, int]]:
    con = duckdb.connect(path, read_only=True)
    try:
        frames = {
            t: con.execute(f"SELECT * FROM marts.{t}").df()
            for t in ["fct_orders", "dim_customers", "dim_products", "agg_daily_revenue"]
        }
        frames["dim_customers_current"] = frames["dim_customers"][
            frames["dim_customers"].is_current
        ]
        expected = {
            "orders": int(
                con.execute("SELECT count(*) FROM raw.orders WHERE NOT is_deleted").fetchone()[0]
            )
        }
    finally:
        con.close()
    return frames, expected


def gold_expectations(expected: dict[str, int]) -> dict[str, list]:
    return {
        "fct_orders": [
            # Silver -> Gold reconciliation: every live order made it into the fact table.
            E.ExpectTableRowCountToEqual(value=expected["orders"]),
            E.ExpectColumnValuesToBeUnique(column="order_id"),
            E.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0),
            E.ExpectColumnValuesToNotBeNull(column="customer_version_lsn"),
        ],
        "dim_customers_current": [E.ExpectColumnValuesToBeUnique(column="customer_id")],
        "dim_products": [E.ExpectColumnValuesToBeUnique(column="product_id")],
        "agg_daily_revenue": [
            E.ExpectColumnValuesToBeBetween(column="revenue", min_value=0),
            E.ExpectColumnValuesToBeBetween(column="avg_order_value", min_value=0, max_value=5_000),
        ],
    }


def run_checkpoint(
    layer: str, frames: dict[str, pd.DataFrame], suites: dict[str, list]
) -> dict[str, Any]:
    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.data_sources.add_pandas(layer)
    report: dict[str, Any] = {"layer": layer, "at": datetime.now(UTC).isoformat(), "tables": {}}
    ok = True
    for table, exps in suites.items():
        bd = ds.add_dataframe_asset(table).add_batch_definition_whole_dataframe("all")
        suite = ctx.suites.add(gx.ExpectationSuite(name=f"{layer}_{table}"))
        for e in exps:
            suite.add_expectation(e)
        vd = ctx.validation_definitions.add(
            gx.ValidationDefinition(name=f"{layer}_{table}", data=bd, suite=suite)
        )
        cp = ctx.checkpoints.add(
            gx.Checkpoint(name=f"{layer}_{table}_cp", validation_definitions=[vd])
        )
        res = cp.run(batch_parameters={"dataframe": frames[table]})
        rows = []
        for vr in res.run_results.values():
            for r in vr.results:
                rows.append(
                    {
                        "expectation": r.expectation_config.type,
                        "column": r.expectation_config.kwargs.get("column"),
                        "success": bool(r.success),
                        "unexpected_percent": r.result.get("unexpected_percent"),
                        "observed": str(r.result.get("observed_value"))
                        if "observed_value" in r.result
                        else None,
                    }
                )
        report["tables"][table] = {
            "rows": len(frames[table]),
            "success": bool(res.success),
            "checks": rows,
        }
        ok &= bool(res.success)
    report["success"] = ok
    pct = [
        c["unexpected_percent"]
        for t in report["tables"].values()
        for c in t["checks"]
        if c["unexpected_percent"] is not None
    ]
    report["max_unexpected_percent"] = max(pct) if pct else 0.0
    report["checks_total"] = sum(len(t["checks"]) for t in report["tables"].values())
    report["checks_failed"] = sum(
        not c["success"] for t in report["tables"].values() for c in t["checks"]
    )
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("layer", choices=["silver", "gold"])
    ap.add_argument("--freshness-minutes", type=int, default=15)
    a = ap.parse_args()
    if a.layer == "silver":
        report = run_checkpoint("silver", load_silver(), silver_expectations(a.freshness_minutes))
    else:
        frames, expected = load_gold(settings.warehouse_path)
        report = run_checkpoint("gold", frames, gold_expectations(expected))
    REPORTS.mkdir(exist_ok=True)
    (REPORTS / f"ge_{a.layer}.json").write_text(json.dumps(report, indent=2, default=str))
    for table, t in report["tables"].items():
        failed = [f"{c['expectation']}({c['column']})" for c in t["checks"] if not c["success"]]
        print(
            f"{a.layer}.{table:22s} rows={t['rows']:>7,}  {'PASS' if t['success'] else 'FAIL'}"
            + (f"  failed: {failed}" if failed else "")
        )
    print(
        f"{report['checks_total'] - report['checks_failed']}/{report['checks_total']} checks passed"
    )
    sys.exit(0 if report["success"] else 1)


if __name__ == "__main__":
    main()
