"""Load Silver (Delta) into the warehouse RAW schema: DuckDB by default, Snowflake optional.

Full-refresh per table in one transaction (CREATE OR REPLACE), so every run is idempotent:
re-running the hourly load after a failure yields exactly the same warehouse state.

    python -m lakehouse.warehouse.load                 # WAREHOUSE_TARGET=duckdb (default)
    WAREHOUSE_TARGET=snowflake python -m lakehouse.warehouse.load   # needs SNOWFLAKE_* env
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

import duckdb
import pandas as pd

from lakehouse import lake
from lakehouse.config import Settings, settings

SILVER_TABLES = ["customers", "customers_history", "products", "orders", "order_items"]


def load_duckdb(frames: dict[str, pd.DataFrame], path: str) -> dict[str, int]:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(path)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("BEGIN")
        for name, df in frames.items():
            con.register("_df", df)
            con.execute(f"CREATE OR REPLACE TABLE raw.{name} AS SELECT * FROM _df")
            con.unregister("_df")
        con.execute(
            "CREATE OR REPLACE TABLE raw._load_audit AS SELECT now() AS loaded_at, "
            + " , ".join(f"{len(df)} AS {name}_rows" for name, df in frames.items())
        )
        con.execute("COMMIT")
    finally:
        con.close()
    return {k: len(v) for k, v in frames.items()}


def load_snowflake(frames: dict[str, pd.DataFrame]) -> dict[str, int]:  # pragma: no cover
    """Same contract on Snowflake (RAW schema, overwrite). Requires `pip install
    snowflake-connector-python[pandas]` and SNOWFLAKE_ACCOUNT/USER/PASSWORD/WAREHOUSE/DATABASE."""
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="RAW",
    )
    try:
        for name, df in frames.items():
            write_pandas(
                con,
                df,
                name.upper(),
                auto_create_table=True,
                overwrite=True,
                quote_identifiers=False,
            )
    finally:
        con.close()
    return {k: len(v) for k, v in frames.items()}


def run(s: Settings = settings) -> dict[str, int]:
    t0 = time.perf_counter()
    frames = {
        t: lake.read("silver", t, s=s) for t in SILVER_TABLES if lake.exists("silver", t, s=s)
    }
    out = (
        load_snowflake(frames)
        if s.warehouse_target == "snowflake"
        else load_duckdb(frames, s.warehouse_path)
    )
    print(f"loaded {out} into {s.warehouse_target} in {time.perf_counter() - t0:.1f}s")
    return out


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    run()


if __name__ == "__main__":
    main()
