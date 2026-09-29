# Pipeline metrics

_Measured 2026-09-29 with `python scripts/benchmark.py all` on arm64 macOS laptop. Kafka, Debezium, Postgres and S3 run in a 2 vCPU / 4 GB Docker VM; Spark runs locally (`local[4]`, 1.5 GB driver)._

## Throughput and end-to-end latency (F1, NFR: Postgres → Silver < 60 s)

| metric | value |
|---|---:|
| target / achieved source rate | 50 / **50.0 row changes/s** |
| row changes generated | 9,000 over 180 s |
| latency samples (rows) | 5,459 |
| p50 latency | 17.4 s |
| **p95 latency** | **27.5 s** |
| max latency | 148.2 s |

Latency = Silver `_silver_ts` (MERGE time) − Postgres `updated_at` (commit time). It includes Debezium capture, Kafka, Bronze trigger (5 s), Silver trigger (5 s) and the per-table MERGEs. The max was not investigated; the likely cause is rows committed while the job was still draining the warm-up backlog (p95 is the representative figure).

## Schema evolution (F7)

`ALTER TABLE products ADD COLUMN weight_kg` while streaming → column present in Silver after **11 s**, 20 rows populated, no job restart, no failure.

## Failure recovery: `kill -9` mid-stream (F6 exactly-once)

| check | result |
|---|---:|
| job down (writes continued to Postgres) | 15 s |
| catch-up after restart | **17 s** |
| kill → Silver consistent again | 57 s |
| duplicate primary keys in Silver | **0** |
| duplicate Kafka offsets in Bronze | **0** |
| Silver live rows == Postgres rows (all 4 tables) | **True** |

## Data quality (Great Expectations)

| layer | checks passed | max % rows failing a check |
|---|---:|---:|
| silver | 21/21 | 0.00% |
| gold | 8/8 | 0.00% |
