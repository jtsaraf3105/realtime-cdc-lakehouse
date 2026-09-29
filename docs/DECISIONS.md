# Design Decisions

**D1. Log-based CDC (Debezium + pgoutput) instead of batch extracts.** Captures every change
including hard deletes and intermediate states, adds near-zero load (it reads the WAL, not the
tables), and gives commit order (LSN). Costs: a replication slot must be monitored (an unconsumed
slot retains WAL and fills the disk), and schema changes need a plan (D5).

**D2. `REPLICA IDENTITY FULL`.** By default Postgres only logs the primary key in `before` for
UPDATE/DELETE. FULL logs the whole old row, so deletes carry the full image (needed for soft
deletes and SCD2 closing rows). Cost: more WAL volume for wide tables.

**D3. JSON without embedded schemas, `decimal.handling.mode=string`.** Keeps messages readable and
small (no Schema Registry to run in 4 GB). Money stays exact as strings and is cast to
`decimal(12,2)` in dbt staging. The default (`precise`) encodes decimals as base64 bytes, and
`double` loses cents. Trade-off: no schema contract on the topic; a production setup would use
Avro/Protobuf + Schema Registry.

**D4. Order changes by Postgres LSN, not `ts_ms`.** Many row changes in one transaction share a
millisecond (an order insert, its items and the total update). LSN is the commit-log position,
which is strictly increasing. The Kafka offset breaks ties for duplicate deliveries. Unit-tested:
`test_latest_per_key_orders_by_lsn_not_arrival`.

**D5. Schema evolution: Bronze stores raw JSON; Silver infers each micro-batch's schema and uses
Delta `autoMerge`.** A new upstream column (`products.weight_kg`) is picked up without a code
change or restart (measured in METRICS.md). Removed or renamed columns are *not* auto-applied
(the old column stays, filled with nulls), which is the safe default for analytics.

**D6. SCD2 validity = business effective time (`updated_at`), not the CDC event time.** The first
version used `source.ts_ms`. dbt's `not_null` test on the point-in-time customer version then
failed for *all 38* orders: for the initial snapshot, `ts_ms` is when Debezium *read* the row,
after every existing order. Fix: `valid_from = updated_at`, and the first known version is
extended back to `created_at` (history before CDC started is unknowable, so we assume the snapshot
image held since creation, and document it). **The same test then exposed a second, layered
bug:** staging cast ISO strings to *naive* timestamps while `valid_from` was tz-aware. DuckDB
read the naive side in the session time zone (IST on the dev laptop), which shifted every order
5.5 h *before* its customer existed. Fix: the `to_utc()` macro (adapter-dispatched for DuckDB and
Snowflake) normalises every timestamp to naive UTC in staging. SCD2 rows are rebuilt for touched keys from
(existing ∪ new) and MERGEd on (pk, lsn, offset), which makes replays idempotent.

**D7. Soft deletes in Silver.** Deletes set `is_deleted/deleted_at` instead of removing rows, so
history and reconciliation stay possible. Marts filter them out (purged cancelled orders never
count as revenue).

**D8. Exactly-once effects, not exactly-once delivery.** Bronze: checkpointed Kafka offsets plus
Delta commits with `txnAppId/txnVersion` (a re-run batch is skipped). Silver: MERGE guarded by
`s._lsn > t._lsn`, so replays and duplicates are no-ops. Proven by `kill -9` in METRICS.md: 0
duplicate keys, 0 duplicate offsets, Silver == Postgres.

**D9. One JVM for both streaming queries, running locally.** The Docker VM is 2 CPU / 4 GB (a
hard constraint). Kafka, Connect, Postgres and S3 use ~0.9 GB there. Spark runs as `local[2]` with
a 1.5 GB driver on the host. On a cluster, the same code runs with `SPARK_MASTER=spark://...` or
on Databricks/EMR.

**D10. S3-compatible storage: SeaweedFS instead of MinIO.** MinIO no longer publishes community
images to Docker Hub/quay (pulls fail). The pipeline only needs the S3 API; SeaweedFS is
lightweight (~130 MB) and actively maintained. Swapping to MinIO, AWS S3 or R2 is an endpoint
change.

**D11. Warehouse: DuckDB by default, Snowflake as a configured target.** No Snowflake account was
available while building this. The loader and dbt profile have a `snowflake` target
(`WAREHOUSE_TARGET=snowflake`, `DBT_TARGET=snowflake`). The dbt SQL is kept portable (casts, `md5`,
standard joins). **Resume implication:** claim Snowflake only after running `make warehouse dbt`
against a trial account; otherwise say "dbt (DuckDB; Snowflake-ready)".

**D12. Full-refresh load (CREATE OR REPLACE) per hour.** Idempotent and simple at this size.
At scale: incremental loads from Silver using Delta's change data feed, or read Delta directly
from Snowflake (Iceberg/Delta external tables).

**D13. Great Expectations AND dbt tests.** dbt tests are contracts on the SQL models (unique,
not_null, relationships, accepted_values, plus singular tests for SCD2 invariants and order-total
reconciliation). GX checks the *data* on the lake before dbt runs (row counts, null %, ranges,
freshness < 15 min) and on the Gold outputs (Silver→Gold row reconciliation). Each caught
something the other wouldn't.

**D14. Partitioning and Z-ordering.** Bronze is partitioned by `ingest_date`: append-only,
time-bounded reprocessing and retention (drop old partitions). Silver tables are small and
MERGE-heavy, so partitioning would create many tiny files; instead run `OPTIMIZE ... ZORDER BY
(customer_id)` on large Silver tables to co-locate keys, which speeds up the MERGE's join and
point lookups. At this data size neither is needed; the reasoning is what matters.

**D15. Airflow calls the project venv via BashOperator.** Airflow 3 pins hundreds of packages
that conflict with Spark/GX/dbt. Isolating them keeps both sides upgradable. Each task is
idempotent, so retries and backfills are safe.

**D16. Incidents found while benchmarking (and the fixes).**
1. *S3 OOM:* SeaweedFS at 384 MB was OOM-killed under Delta's many small files at 50 changes/s,
   and the Spark job then failed its writes. Fix: 768 MB limit plus `restart: unless-stopped` on
   every infra service. Nothing was lost: Bronze/Silver resumed from checkpoints.
2. *Kafka without a volume:* adding the restart policy made compose recreate the Kafka container,
   and with no persistent log dir **every topic vanished** (CDC data *and* Connect's
   offset/config topics), while Spark's checkpoint still referenced the old partitions. In
   production that is data loss plus a stuck pipeline. Fix: a `kafkadata` volume
   (`KAFKA_LOG_DIRS`). Recovery procedure (RUNBOOK): re-register the connector with a fresh
   snapshot and reset the lake checkpoints, or rebuild from scratch.
3. *Slow Silver batches (~44 s):* redundant `count()` recomputations and an S3 existence check
   per table per batch. Fix: persist intermediate frames and cache table existence.
