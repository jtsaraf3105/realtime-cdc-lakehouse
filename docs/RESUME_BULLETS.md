# Resume bullets (every number is from docs/METRICS.md)

**Real-Time CDC Lakehouse**: PostgreSQL, Debezium, Kafka, Spark Structured Streaming, Delta Lake,
dbt, Great Expectations, Airflow, GitHub Actions · [github link]

- Built a log-based CDC pipeline (Postgres WAL → Debezium → Kafka → Spark Structured Streaming →
  Delta Lake Bronze/Silver) that processes a 50 changes/s insert/update/delete workload with
  **27.5 s p95 end-to-end latency** (target < 60 s), using LSN-ordered idempotent MERGEs, soft
  deletes and a dead-letter topic.
- Proved exactly-once behaviour: `kill -9`-ing the streaming job mid-load and restarting it caught
  up in **17 s** with **0 duplicate keys, 0 duplicate Kafka offsets**, and Silver matching Postgres
  row for row. Schema evolution (`ADD COLUMN`) reached Silver in 11 s with no restart.
- Modelled SCD Type 2 customer history (1,135 versions) with point-in-time joins into dbt marts
  (fct_orders, dim_customers, dim_products, agg_daily_revenue). **36 dbt tests** caught two real
  bugs: snapshot validity start, and time-zone mixing.
- Orchestrated an hourly Airflow 3 DAG (GE Silver → load → dbt run → dbt test → GE Gold,
  **29/29 GE checks** including freshness < 15 min), with CI for ruff, sqlfluff, Spark unit tests
  and dbt build on DuckDB.

**Before using these:**
- The warehouse ran on **DuckDB**; write "Snowflake-ready" or run `DBT_TARGET=snowflake` on a
  trial account before claiming Snowflake.
- Object storage was S3-compatible SeaweedFS (MinIO images are no longer public). Say
  "S3-compatible storage".
- Everything ran on one laptop, so don't imply cluster scale.
