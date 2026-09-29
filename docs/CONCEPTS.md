# Concepts & Vocabulary

**OLTP / OLAP.** OLTP = the shop's transactional database (many small reads and writes). OLAP =
analytics (big scans and aggregates). Mixing them slows the shop down, so we copy data out.

**CDC (change data capture).** Recording every insert, update and delete as an event. *Log-based*
CDC reads the database's own change log instead of querying tables. *Analogy:* reading a bank's
transaction journal instead of re-counting every account nightly.

**WAL / LSN.** Postgres writes every change to the write-ahead log before applying it. The Log
Sequence Number is a change's position in that log, strictly increasing in commit order. We use it
to order changes.

**Logical replication slot / publication.** A slot remembers how far a consumer has read, so the
WAL is kept until it's consumed (monitor it!). A publication lists which tables are published.
`pgoutput` is Postgres's built-in decoding plugin.

**REPLICA IDENTITY FULL.** Makes UPDATE and DELETE events carry the complete old row (`before`).

**Debezium / Kafka Connect.** Debezium is a set of CDC connectors running inside Kafka Connect
(a framework for moving data in and out of Kafka). It produces the *envelope*: `op` (c=create,
u=update, d=delete, r=snapshot read), `before`, `after`, `source` (lsn, ts_ms, table...), `ts_ms`.

**Snapshot.** When a connector starts, it first reads existing rows consistently (`op=r`), then
streams changes from that point, with no gap and no overlap.

**Kafka topic / partition / offset / consumer group.** An append-only log (topic), split into
ordered shards (partitions). Each record has a position (offset). Consumers track offsets; a group
shares partitions. Ordering is guaranteed within a partition only.

**KRaft.** Kafka's built-in consensus (no ZooKeeper).

**Dead-letter queue (DLQ).** Where unprocessable messages go, so one bad record doesn't stop the
stream. Here: Kafka topic `shop.dlq` + Delta table `bronze/dead_letter`.

**Spark Structured Streaming.** Treats a stream as an ever-growing table processed in
*micro-batches*. **Trigger** = how often. **Checkpoint** = offsets + commit log, so a restart
resumes exactly. **foreachBatch** = run arbitrary batch code (e.g. MERGE) on each micro-batch.
**availableNow** = process everything available, then stop.

**Watermark.** For event-time aggregations: how late an event may be and still be counted, which
bounds state. Not needed for key-based MERGE, where ordering is by LSN.

**Delta Lake.** Parquet files plus a transaction log (`_delta_log/`), giving ACID commits, MERGE,
schema enforcement/evolution, time travel and OPTIMIZE. *Analogy:* a git history for a folder of
data files.

**Lakehouse / medallion.** Warehouse-like reliability on cheap object storage. Bronze (raw),
Silver (clean current state), Gold (business models).

**Exactly-once (effect).** Each change affects the output exactly once despite retries. It needs a
replayable source (Kafka offsets), checkpointing, and an idempotent or transactional sink (Delta
txn ids, guarded MERGE).

**Idempotent.** Running it twice = running it once. Our MERGE, load and dbt steps are all
idempotent, so retries are safe.

**Soft delete.** Mark `is_deleted` instead of removing the row, so history stays queryable.

**SCD Type 1 / 2.** Type 1 overwrites; Type 2 keeps every version with `valid_from / valid_to /
is_current`. **Point-in-time join:** attach the version that was valid when the fact happened.

**Surrogate key.** A warehouse-generated key for each dimension row/version (here md5 of
customer_id + version LSN).

**dbt.** SQL-first transformation framework: models (`select` statements) with `ref()`
dependencies, materialisations (view/table), and tests (generic: unique, not_null,
relationships, accepted_values; singular: custom SQL that must return zero rows).

**Great Expectations.** Declarative data checks (expectations) grouped in suites and run by
checkpoints against batches of data; reports the % of rows failing each.

**Airflow DAG.** A scheduled graph of tasks with dependencies, retries, and callbacks.

**Partitioning / Z-ordering.** Partitioning splits a table into folders by a column (prune by
filter; manage lifecycle). Z-ordering clusters rows with similar key values into the same files
(fewer files to read for lookups and MERGE joins).

**Latency (end-to-end).** Time from the Postgres commit to the row being visible in Silver.
