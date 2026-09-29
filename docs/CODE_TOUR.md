# Code Tour — follow one order from Postgres to a dashboard number

## 1. The change happens — `lakehouse/simulator/orders.py`
`Workload._new_order()` runs one transaction: `INSERT orders` → 1–3 `INSERT order_items` →
`UPDATE orders SET total_amount`. That's 3–5 row changes, each with its own LSN, all in one
commit. `step()` picks operations with realistic weights (40% new orders, 32% status changes,
8% new customers, 8% customer edits, 7% price changes, 5% purges of cancelled orders).

## 2. Captured — `lakehouse/connect/postgres-source.json`
Debezium (inside Kafka Connect) reads the replication slot `shop_cdc` via the `pgoutput`
publication and writes each row change to `shop.public.<table>`, keyed by primary key:
`{"op":"c","before":null,"after":{...},"source":{"lsn":..,"ts_ms":..},"ts_ms":..}`.
`register.py` PUTs the config (create-or-update) and waits for RUNNING.

## 3. Bronze — `pipeline.py::start_bronze` + `transforms.py::envelope_fields/split_valid`
The Kafka stream is projected to (topic, table, partition, offset, key, raw value, op, lsn,
ts_ms, ingest_date). In `foreachBatch`, valid envelopes are appended to
`s3a://lakehouse/bronze/cdc_events` (partitioned by `ingest_date`, idempotent via
`txnAppId=bronze_cdc, txnVersion=batch_id`). Invalid ones (bad JSON, unknown op, missing image)
go to Kafka `shop.dlq` and the Delta table `bronze/dead_letter`.

## 4. Silver — `pipeline.py::silver_batch`
Silver streams from the Bronze Delta table. For each table in the micro-batch:
1. `extract_rows`: pick `after` (or `before` for deletes), **infer the schema from this batch**
   (schema evolution), flatten, and keep CDC metadata (`_op`, `_lsn`, `_kafka_offset`, ...).
2. `latest_per_key`: collapse to the newest change per primary key (by LSN, then offset).
3. `with_soft_delete`: `is_deleted`, `deleted_at`, `_silver_ts`.
4. `MERGE INTO silver/<table> ON pk WHEN MATCHED AND newer THEN UPDATE * WHEN NOT MATCHED THEN
   INSERT *`. Older or duplicate changes match but fail the "newer" condition, so nothing happens.
5. For `customers`, `scd2_versions` rebuilds the version timeline of the touched customers from
   (existing history ∪ every change in the batch). It's MERGEd on (customer_id, lsn, offset):
   new versions are inserted and changed `valid_to/is_current` are updated.

## 5. Warehouse — `lakehouse/warehouse/load.py`
delta-rs reads each Silver table from S3 (no Spark needed) → DuckDB `raw.<table>` via
`CREATE OR REPLACE` in one transaction (or Snowflake `write_pandas(overwrite=True)`).

## 6. dbt — `dbt/models`
- `staging/`: type casts (decimal strings → `decimal`), filters deleted rows where appropriate.
- `intermediate/int_orders_enriched`: line totals, and the **point-in-time SCD2 join**, which picks
  the customer version whose `[valid_from, valid_to)` contains the order's `created_at`.
- `marts/`: `fct_orders` (grain: order), `dim_customers` (SCD2, surrogate key), `dim_products`,
  `agg_daily_revenue`.
- tests: generic (unique, not_null, relationships, accepted_values) + singular
  (one current version per customer, no overlapping intervals, header total = sum of lines).

## 7. Quality gates — `lakehouse/quality/checks.py`
GX checkpoints over pandas frames: Silver (row counts, uniqueness, null %, ranges, allowed
values, freshness < 15 min) and Gold (fact row count reconciles with live Silver orders, ranges,
unique current customer). Results go to `reports/ge_<layer>.json`, including the % of failing rows.

## 8. Orchestration — `airflow/dags/lakehouse_hourly.py`
`ge_silver >> load_warehouse >> dbt_run >> dbt_test >> ge_gold`, hourly, `max_active_runs=1`,
2 retries, failure callback → log + optional Slack webhook.

## 9. Proof — `scripts/benchmark.py` → `docs/METRICS.md`
Throughput and latency at 50 changes/s, schema evolution while streaming, `kill -9` +
restart with duplicate and reconciliation checks.
