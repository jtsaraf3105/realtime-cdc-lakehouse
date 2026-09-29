# Start Here — the CDC lakehouse, explained plainly

## 1. What problem does this solve?
An online shop runs on a normal database (Postgres): every order, customer and product lives there
and changes all day. Analysts want those numbers too, fresh, historical and reliable, without
slowing down the shop's database with heavy queries. The old way was to copy the whole database
every night ("batch extract"). This project does it the modern way: **capture every individual
change as it happens (CDC)**, stream it into cheap storage (a **lakehouse**), keep an exact
up-to-date copy plus full history, then model it for reporting, **with quality gates and tests at
every step**.

## 2. The picture

```
 Shop app (simulator) ──► Postgres ──WAL──► Debezium ──► Kafka (one topic per table)
                                                              │
                                        Spark Structured Streaming
                                                              │
                         ┌────────────── Bronze (Delta on S3): every change, raw, append-only
                         │                    │
                         │              Spark MERGE (idempotent, ordered by LSN)
                         │                    │
                         │             Silver (Delta): current state + soft deletes
                         │                     + customers_history (SCD Type 2)
                         │                    │
                         │   Airflow hourly:  GE(silver) → load → dbt run → dbt test → GE(gold)
                         │                    │
                         └─ dead-letter topic  Warehouse (DuckDB | Snowflake): stg → int → marts
```

## 3. The eight ideas
1. **CDC from the write-ahead log.** Postgres records every change in its WAL for crash recovery.
   Debezium reads that log (logical replication), so capture costs the database almost nothing,
   and it sees deletes and every intermediate update, which nightly copies miss.
2. **Kafka as the buffer.** Changes land in durable, ordered topics. If Spark is down, nothing is
   lost; it resumes from its last offset.
3. **Bronze/Silver/Gold (medallion).** Bronze = raw events, never modified (you can always replay).
   Silver = cleaned current state. Gold = business models (facts, dimensions, aggregates).
4. **Delta Lake = ACID tables on files.** Parquet files plus a transaction log give atomic writes,
   MERGE, schema evolution and time travel on plain object storage.
5. **Exactly-once effects.** Kafka offsets are checkpointed together with each Delta commit, and
   Silver MERGEs only apply *newer* changes (by Postgres LSN). Kill the job at any moment, restart
   it, and you get no duplicates and no gaps. `docs/METRICS.md` proves it with `kill -9`.
6. **SCD Type 2.** When a customer moves from Pune to Berlin, we don't overwrite: we close the old
   row (`valid_to`) and add a new one. Orders placed before the move still report Pune.
7. **Two kinds of tests.** dbt tests check the transformations (unique keys, relationships,
   allowed values). Great Expectations checks the data itself (row counts, null %, ranges,
   freshness) before and after dbt.
8. **Orchestration.** Airflow runs the hourly batch layer in order, retries failures, and alerts.

## 4. Reading order
1. [STUDY_TOPICS.md](STUDY_TOPICS.md): what to learn, in order.
2. `infra/postgres/init.sql` → `lakehouse/connect/postgres-source.json`: the source and capture.
3. `lakehouse/spark/transforms.py` → `pipeline.py`: the streaming heart.
4. `dbt/models/`: staging → intermediate → marts.
5. [CODE_TOUR.md](CODE_TOUR.md), [DECISIONS.md](DECISIONS.md), [METRICS.md](METRICS.md).
6. [LEARNING_GUIDE.md](LEARNING_GUIDE.md): the interview questions.
