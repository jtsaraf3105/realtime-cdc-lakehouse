# Interview Guide — model answers (PRD §10 talking points)

Numbers referenced here are in [METRICS.md](METRICS.md). Quote them exactly.

## 0. The 60-second story
> "I built a real-time CDC pipeline: Postgres changes are captured from the WAL by Debezium, go
> through Kafka, and Spark Structured Streaming lands them in Delta Lake as a raw Bronze layer,
> then MERGEs them into Silver current-state tables and an SCD Type 2 customer history. An hourly
> Airflow DAG runs Great Expectations on Silver, loads the warehouse, runs dbt models and tests,
> and validates Gold. At 50 changes per second, end-to-end latency is a few seconds at p95. I
> proved exactly-once behaviour by `kill -9`-ing the Spark job mid-stream: after restart, zero
> duplicates, and Silver matched Postgres row for row. A dbt test caught a real SCD2 bug where
> snapshot rows got the wrong validity start."

## 1. Why CDC instead of batch extracts?
Batch `WHERE updated_at > last_run` misses **hard deletes**, misses **intermediate states** (an
order that went created→paid→cancelled between runs looks like it was only ever cancelled), relies
on every writer maintaining `updated_at`, and scans the source tables. Log-based CDC reads the WAL
the database already writes: it's low-impact, complete, ordered by commit (LSN), and near
real-time. Costs: replication slots to monitor (unconsumed slots retain WAL until the disk fills),
more moving parts, and schema changes that need handling.

## 2. Exactly-once vs at-least-once
Delivery over networks is at-least-once at best, so exactly-once is an **end-to-end effect** built
from a replayable source plus an idempotent or transactional sink. Here:
- **Bronze:** Spark checkpoints Kafka offsets. Each micro-batch is a Delta commit tagged
  `txnAppId/txnVersion=batch_id`, so a re-run batch after a crash is recognised and skipped.
- **Silver:** MERGE applies a change only if its LSN is newer than the stored one, so replayed
  and duplicate events are no-ops.
- **Evidence:** `kill -9` mid-stream → restart → 0 duplicate primary keys, 0 duplicate Kafka
  offsets in Bronze, Silver identical to Postgres.
- **What it doesn't cover:** side effects outside Delta (e.g. sending an email from foreachBatch)
  would still be at-least-once.

## 3. Watermarking and late data
Watermarks tell Spark how long to wait for late events in **event-time windowed aggregations** or
stream-stream joins, then drop state older than the watermark. This pipeline doesn't aggregate in
the stream: it MERGEs by key, and late or out-of-order events are resolved **by LSN**, not by
waiting. A late older change simply loses to the newer row already in Silver. Where I *would* need
a watermark: a streaming "revenue per 5 minutes" aggregate (`withWatermark("order_ts", "10
minutes")` + window), trading completeness for bounded state.

## 4. SCD2 design
Columns: business key, surrogate key, `valid_from`, `valid_to` (null = open), `is_current`, plus
CDC lineage (`_lsn`). Every change is a version, including several in one micro-batch, which are
all kept, not just the latest. Implementation: for the keys touched in a batch, rebuild the
timeline from existing ∪ new versions with `LEAD(valid_from)`, then MERGE on (key, lsn), which is
idempotent. **The bug story:** `valid_from` was initially the CDC event time, and for the initial
snapshot that's when Debezium *read* the row, so no existing order found a matching customer
version. dbt's `not_null` test on the point-in-time join caught it (38/38 orders failed). Fix:
effective time = `updated_at`, and the first version reaches back to `created_at` (a documented
assumption).

## 5. Why dbt tests AND Great Expectations?
They guard different things at different places. **dbt tests** are contracts on transformations:
keys unique, foreign keys resolve, allowed statuses, SCD2 invariants, header total = sum of
lines. They run in the warehouse after models build. **Great Expectations** validates the data
itself where dbt doesn't reach (the lake), and before models build: volume, null %, value
ranges, **freshness** (Silver updated within 15 min), and cross-system reconciliation
(fct_orders row count = live Silver orders). Failing GX on Silver stops the DAG before bad data
reaches Gold.

## 6. Partitioning and Z-ordering
- **Partition** by a low-cardinality column used in filters and lifecycle: Bronze by
  `ingest_date` (append-only, reprocess or expire by day). Don't partition small or MERGE-heavy
  tables: tiny files and no pruning benefit.
- **Z-order** (`OPTIMIZE ... ZORDER BY (customer_id)`) clusters related keys across files so
  MERGE joins and point lookups read fewer files. Useful on large Silver tables, run periodically
  with file compaction. Liquid clustering is the newer alternative.

## 7. Rapid-fire
| Q | A |
|---|---|
| What's in a Structured Streaming checkpoint? | Source offsets per batch, commit log, state store (if stateful), metadata. |
| Why Bronze at all? | Replayability: rebuild Silver/Gold with fixed logic from the raw changes (we did exactly that after the SCD2 fix). |
| Debezium snapshot? | Initial consistent read (op `r`) at a known LSN, then streaming from that LSN, so no gap and no overlap. |
| How do you handle a dropped column upstream? | Silver keeps it (nulls going forward); dbt staging decides; a contract/schema registry would catch it earlier. |
| Why DuckDB? | Local, free warehouse with the same dbt code; Snowflake is a profile switch (`DBT_TARGET=snowflake`). |
| Kafka partitions and ordering? | Order holds per partition; Debezium keys by PK, so one row's changes stay ordered. |
| What would you monitor? | Replication slot lag (bytes), Kafka consumer lag, batch duration vs trigger, DLQ rate, GE failures, end-to-end latency. |

## 8. Honest limitations
Single-node Spark (a 4 GB VM constraint), DuckDB instead of Snowflake until an account is used,
SeaweedFS instead of MinIO (images unavailable), JSON without Schema Registry, full-refresh
warehouse loads.
