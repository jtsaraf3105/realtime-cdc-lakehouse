# Topics to Study — realtime-cdc-lakehouse

Ordered checklist: **explain**, **search terms**, **where in repo**, **self-check**. About 25–30
hours over two weeks.

## Level 1 — Foundations
### 1.1 OLTP vs OLAP; batch vs streaming
- **Explain:** why analytics shouldn't query the production DB; batch extract (full/incremental by
  `updated_at`) vs CDC; what each misses (hard deletes, intermediate updates).
- **Search:** `CDC vs batch ETL`, `incremental load updated_at pitfalls`
- **Self-check:** *Name two changes a nightly `updated_at > last_run` extract can't see.*

### 1.2 Postgres WAL and logical replication
- **Explain:** write-ahead log; LSN; `wal_level=logical`; replication slots (and the disk-bloat risk
  if a slot isn't consumed); publications; `REPLICA IDENTITY FULL`.
- **Search:** `postgres logical replication slots`, `pgoutput`, `replica identity full`
- **Repo:** `docker-compose.yml` (postgres command), `infra/postgres/init.sql`
- **Self-check:** *What happens to Postgres disk if Debezium is down for a day? How do you monitor it?*

### 1.3 Kafka essentials
- **Explain:** topics, partitions, offsets, consumer groups, retention, ordering guarantees (per
  partition), keys; KRaft (no ZooKeeper).
- **Search:** `kafka partitions ordering guarantee`, `kafka KRaft mode`
- **Self-check:** *Why does Debezium key messages by primary key?*

## Level 2 — Capture and streaming
### 2.1 Debezium
- **Explain:** connector config; snapshot modes; the envelope (`op` c/u/d/r, `before`, `after`,
  `source.lsn`, `ts_ms`); tombstones; `decimal.handling.mode`; Kafka Connect (distributed mode,
  offset/config/status topics).
- **Search:** `debezium postgres connector properties`, `debezium event envelope`
- **Repo:** `lakehouse/connect/postgres-source.json`
- **Self-check:** *Why `decimal.handling.mode=string` instead of the default?*

### 2.2 Spark Structured Streaming
- **Explain:** micro-batches, triggers (processingTime, availableNow), checkpoints (offsets +
  commits), `foreachBatch`, output modes, `maxOffsetsPerTrigger`; watermarks and late data (and why
  a MERGE keyed on LSN doesn't need one here).
- **Search:** `structured streaming programming guide`, `foreachBatch exactly once`, `spark watermark late data`
- **Repo:** `lakehouse/spark/pipeline.py`
- **Self-check:** *What exactly is stored in the checkpoint directory?*

### 2.3 Exactly-once vs at-least-once
- **Explain:** source replayability + idempotent/transactional sink = exactly-once *effect*; Delta
  `txnAppId/txnVersion`; idempotent MERGE with ordering guard.
- **Search:** `exactly once semantics spark delta`, `delta idempotent writes txnAppId`
- **Repo:** `pipeline.py` docstring, [METRICS.md](METRICS.md) recovery section
- **Self-check:** *The job crashes after the Delta commit but before the checkpoint commit. What happens on restart?*

## Level 3 — Lakehouse modelling
### 3.1 Delta Lake
- **Explain:** transaction log (`_delta_log`), ACID on object storage, MERGE, schema enforcement
  vs evolution (`autoMerge`), time travel, OPTIMIZE / Z-ORDER, VACUUM, partitioning choices.
- **Search:** `delta lake transaction log explained`, `delta merge schema evolution`, `delta z-order`
- **Repo:** `session.py`, `pipeline.py`
- **Self-check:** *Why is Bronze partitioned by ingest date but Silver not partitioned?*

### 3.2 Medallion architecture
- **Search:** `medallion architecture bronze silver gold`
- **Self-check:** *What would you lose if you skipped Bronze and merged Kafka straight into Silver?*

### 3.3 Slowly Changing Dimensions
- **Explain:** SCD 0/1/2/3/4/6; SCD2 columns (valid_from/valid_to/is_current, surrogate key);
  point-in-time joins; effective time vs commit time.
- **Search:** `SCD type 2 explained`, `SCD2 delta merge`, `point in time join`
- **Repo:** `transforms.py::scd2_versions`, `dbt/models/intermediate/int_orders_enriched.sql`, [DECISIONS.md](DECISIONS.md) D6
- **Self-check:** *Why did `valid_from = CDC event time` break the point-in-time join for snapshot rows?*

### 3.4 Dimensional modelling
- **Explain:** facts vs dimensions, grain, conformed dimensions, star schema, aggregates.
- **Search:** `Kimball dimensional modeling fact grain`

## Level 4 — Transformation, quality, orchestration
| Topic | Explain | Search | Repo |
|---|---|---|---|
| dbt | models, `ref`/`source`, materialisations, layers, generic vs singular tests, profiles/targets | `dbt best practices project structure` | `dbt/` |
| dbt tests vs Great Expectations | transformation contracts vs data expectations; where each runs | `dbt tests vs great expectations` | `lakehouse/quality/checks.py` |
| Great Expectations 1.x | data sources, batch definitions, suites, validation definitions, checkpoints | `great expectations 1.0 checkpoint` | `checks.py` |
| Airflow 3 | DAGs, operators, retries, callbacks, idempotent tasks, catchup/backfill | `airflow 3 dag authoring`, `airflow idempotent tasks` | `airflow/dags/` |
| Snowflake basics | warehouses, stages, `COPY INTO`, `write_pandas`, roles | `snowflake write_pandas`, `snowflake copy into` | `warehouse/load.py` |
| CI for data | sqlfluff, dbt build on DuckDB, unit tests for Spark | `sqlfluff dbt`, `dbt duckdb ci` | `.github/workflows/ci.yml` |

## Level 5 — Talking points (PRD §10)
CDC vs batch · exactly-once vs at-least-once · watermarking and late data · SCD2 design ·
dbt tests AND Great Expectations · partitioning and Z-ordering. Model answers:
[LEARNING_GUIDE.md](LEARNING_GUIDE.md).

## Plan
| Days | Do |
|---|---|
| 1–2 | Level 1; `make up`, watch topics fill with `make topics`. |
| 3–5 | Level 2; run `make stream`, kill it, restart, check `reports/`. |
| 6–8 | Level 3; read `transforms.py` and the Spark tests; draw the SCD2 timeline for one customer. |
| 9–11 | Level 4; `make warehouse dbt quality`; trigger the DAG with `make airflow-test`. |
| 12–14 | LEARNING_GUIDE out loud; explain METRICS.md numbers. |
