# Runbook

## Prerequisites
- Docker with **≥ 4 GB** for the VM (the infra stack uses ~0.9 GB). On macOS with colima:
  `colima start --cpu 2 --memory 4`.
- Python 3.12, **Java 17** for Spark (`brew install openjdk@17`, `export JAVA_HOME=/opt/homebrew/opt/openjdk@17`).
- `make install` creates `.venv` (Spark, dbt, GX, …) and `.venv-airflow` (Airflow 3.3).
- The first Spark run downloads the Kafka, Delta and hadoop-aws jars (~600 MB, cached in `~/.ivy2`).

## Run it end to end
```bash
make up          # Postgres, Kafka (KRaft), Debezium Connect, S3 (SeaweedFS)
make connect     # register the CDC connector
make simulate &  # 50 changes/s for 2 min (in another terminal)
make stream      # continuous Kafka -> Bronze -> Silver; Ctrl-C and re-run to resume exactly
make evolve      # while streaming: ALTER TABLE products ADD COLUMN weight_kg
make warehouse   # Silver -> DuckDB raw
make dbt         # dbt build (models + tests)
make quality     # Great Expectations on silver and gold
make airflow-test  # run the whole hourly DAG once, in-process
make benchmark   # regenerate docs/METRICS.md (latency, schema evolution, kill -9 recovery)
```
Batch-style shortcut: `make demo`.

## Useful checks
```bash
make topics                                                  # list topics, peek at an event
curl -s localhost:8083/connectors/shop-postgres-cdc/status | jq
docker-compose exec postgres psql -U shop -c \
  "select slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag from pg_replication_slots"
.venv/bin/python -c "from lakehouse import lake; print(lake.read('silver','orders').tail())"
.venv/bin/python -c "import duckdb; print(duckdb.connect('warehouse/lakehouse.duckdb').sql('select * from marts.agg_daily_revenue'))"
```

## Operations
| Situation | Action |
|---|---|
| Rebuild Silver/Gold after a logic change | `make reset` (wipes lake + checkpoints; Kafka and Postgres untouched), then `make stream-once` — Bronze is rebuilt from Kafka (retention 72 h), Silver from Bronze. |
| Replication slot lag growing | The connector is down or stuck: check `connectors/.../status`, restart the task. Unconsumed slots retain WAL. |
| Messages in `shop.dlq` | Inspect `bronze/dead_letter`; fix the producer or parser; replay from Bronze. |
| Spark job killed | Just restart it: checkpoints + idempotent MERGE guarantee no duplicates and no gaps. |
| Kafka topics lost (broker rebuilt without its volume) | Spark fails with `UnknownTopicOrPartition`. Re-register the connector (`make connect`; it re-snapshots because its offsets are gone), then `make reset` and `make stream`. Silver converges to Postgres because the snapshot is complete and MERGE is idempotent. Prevent it with the `kafkadata` volume. |
| S3 container OOM / down | Writes fail and the stream stops. Restart S3 (`restart: unless-stopped` does it) and the stream; it resumes from the last committed batch. |
| GE freshness failing | Silver hasn't been updated for 15 min: is the stream running? Is the source idle? |
| Use Snowflake | `pip install "snowflake-connector-python[pandas]" dbt-snowflake`, set `SNOWFLAKE_*`, then `WAREHOUSE_TARGET=snowflake make warehouse` and `DBT_TARGET=snowflake make dbt`. |

## Troubleshooting
| Symptom | Fix |
|---|---|
| Spark `BlockManagerId ... is null` on macOS | `export SPARK_LOCAL_IP=127.0.0.1` (the Makefile sets it). |
| `Unable to locate a Java Runtime` | Install Java 17 and set `JAVA_HOME`. |
| `pull access denied for minio/minio` | Expected — MinIO images are no longer public; this repo uses SeaweedFS. |
| Connector FAILED: replication slot exists | A previous stack left the slot: `make down` (drops volumes) or `select pg_drop_replication_slot('shop_cdc')`. |
