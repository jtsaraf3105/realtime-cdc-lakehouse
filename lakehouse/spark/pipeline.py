"""Structured Streaming jobs: Kafka -> Bronze (Delta, raw) -> Silver (Delta, current state + SCD2).

    python -m lakehouse.spark.pipeline               # both queries in one JVM (default)
    python -m lakehouse.spark.pipeline --only bronze
    python -m lakehouse.spark.pipeline --only silver
    python -m lakehouse.spark.pipeline --available-now   # process what's there, then stop

Exactly-once:
* Bronze — Kafka offsets live in the checkpoint and are committed together with the Delta
  transaction, so a killed job resumes exactly after the last committed batch.
* Silver — foreachBatch MERGEs are idempotent (applied only if the change is newer, by LSN), and
  the Delta streaming source checkpoints which Bronze versions were consumed. Replaying a batch
  after a crash changes nothing.
"""

from __future__ import annotations

import argparse
import json
import logging
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import Settings, settings
from lakehouse.spark.session import build_spark
from lakehouse.spark.transforms import (
    envelope_fields,
    extract_rows,
    latest_per_key,
    merge_condition_newer,
    scd2_versions,
    split_valid,
    with_soft_delete,
)

log = logging.getLogger("lakehouse.pipeline")


# ------------------------------------------------------------------------------------ bronze


def start_bronze(spark: SparkSession, s: Settings, available_now: bool = False):  # type: ignore[no-untyped-def]
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", ",".join(s.topics()))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50_000)
        .load()
    )
    events = envelope_fields(kafka)

    def write_batch(df: DataFrame, batch_id: int) -> None:
        df.persist()
        valid, invalid = split_valid(df)
        # Idempotent append: (txnAppId, txnVersion) makes Delta skip a batch it already wrote,
        # so a restart that re-runs the last batch can't duplicate Bronze rows.
        (
            valid.write.format("delta")
            .mode("append")
            .partitionBy("ingest_date")
            .option("txnAppId", "bronze_cdc")
            .option("txnVersion", batch_id)
            .save(s.path("bronze", "cdc_events"))
        )
        n_bad = invalid.count()
        if n_bad:
            # F11 dead-letter: unparseable/invalid messages go to a Kafka DLQ topic (for
            # alerting/replay) and a Delta table (for inspection); the stream keeps flowing.
            (
                invalid.select(
                    F.col("key"),
                    F.to_json(
                        F.struct(
                            "topic",
                            "kafka_partition",
                            "kafka_offset",
                            "value",
                            F.lit("invalid CDC envelope").alias("reason"),
                        )
                    ).alias("value"),
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", s.kafka_bootstrap)
                .option("topic", s.dlq_topic)
                .save()
            )
            (
                invalid.withColumn("dlq_reason", F.lit("invalid CDC envelope"))
                .write.format("delta")
                .mode("append")
                .option("txnAppId", "bronze_dlq")
                .option("txnVersion", batch_id)
                .save(s.path("bronze", "dead_letter"))
            )
            log.warning("batch %s: %s invalid message(s) -> %s", batch_id, n_bad, s.dlq_topic)
        df.unpersist()

    w = events.writeStream.foreachBatch(write_batch).option(
        "checkpointLocation", s.path("_checkpoints", "bronze")
    )
    w = (
        w.trigger(availableNow=True)
        if available_now
        else w.trigger(processingTime=f"{s.trigger_seconds} seconds")
    )
    return w.queryName("bronze").start()


# ------------------------------------------------------------------------------------ silver


_KNOWN_TABLES: set[str] = set()  # Delta paths known to exist (avoids an S3 listing per batch)


def _table_exists(spark: SparkSession, path: str) -> bool:
    if path in _KNOWN_TABLES:
        return True
    if DeltaTable.isDeltaTable(spark, path):
        _KNOWN_TABLES.add(path)
        return True
    return False


def _merge_current_state(
    spark: SparkSession, rows: DataFrame, table: str, pk: str, s: Settings
) -> int:
    latest = with_soft_delete(latest_per_key(rows, pk)).persist()
    n = latest.count()  # materialises once; the MERGE below reuses the cached rows
    path = s.path("silver", table)
    if not _table_exists(spark, path):
        latest.write.format("delta").save(path)
    else:
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(latest.alias("s"), f"t.{pk} = s.{pk}")
            .whenMatchedUpdateAll(condition=merge_condition_newer())
            .whenNotMatchedInsertAll()
            .execute()
        )
    latest.unpersist()
    return n


def _merge_scd2(spark: SparkSession, rows: DataFrame, table: str, pk: str, s: Settings) -> int:
    path = s.path("silver", f"{table}_history")
    existing = None
    if _table_exists(spark, path):
        keys = rows.select(pk).distinct()
        existing = spark.read.format("delta").load(path).join(keys, pk, "left_semi")
    versions = scd2_versions(rows, existing, pk).persist()
    n = versions.count()
    if existing is None:
        versions.write.format("delta").save(path)
    else:
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(
                versions.alias("s"),
                f"t.{pk} = s.{pk} AND t._lsn = s._lsn AND t._kafka_offset = s._kafka_offset",
            )
            .whenMatchedUpdate(
                set={
                    "valid_to": "s.valid_to",
                    "is_current": "s.is_current",
                    "_silver_ts": "s._silver_ts",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    versions.unpersist()
    return n


def silver_batch(spark: SparkSession, s: Settings):  # type: ignore[no-untyped-def]
    def process(batch: DataFrame, batch_id: int) -> None:
        t0 = time.perf_counter()
        batch.persist()
        stats: dict[str, int] = {}
        for table in [r.table for r in batch.select("table").distinct().collect()]:
            pk = s.primary_keys.get(table)
            if pk is None:
                continue
            rows = extract_rows(spark, batch.filter(F.col("table") == table)).persist()
            stats[table] = _merge_current_state(spark, rows, table, pk, s)
            if table == "customers":  # F5: full change history as SCD Type 2
                stats["customers_history"] = _merge_scd2(spark, rows, table, pk, s)
            rows.unpersist()
        batch.unpersist()
        if stats:
            log.warning(
                "silver batch %s (%.1fs): %s", batch_id, time.perf_counter() - t0, json.dumps(stats)
            )

    return process


def start_silver(spark: SparkSession, s: Settings, available_now: bool = False):  # type: ignore[no-untyped-def]
    bronze = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load(s.path("bronze", "cdc_events"))
    )
    w = bronze.writeStream.foreachBatch(silver_batch(spark, s)).option(
        "checkpointLocation", s.path("_checkpoints", "silver")
    )
    w = (
        w.trigger(availableNow=True)
        if available_now
        else w.trigger(processingTime=f"{s.trigger_seconds} seconds")
    )
    return w.queryName("silver").start()


def ensure_bronze_table(spark: SparkSession, s: Settings) -> None:
    """Silver streams from Bronze, so Bronze must exist before the Silver query starts."""
    path = s.path("bronze", "cdc_events")
    if not DeltaTable.isDeltaTable(spark, path):
        empty = envelope_fields(
            spark.createDataFrame(
                [],
                "topic string, partition int, offset long, "
                "timestamp timestamp, key binary, value binary",
            )
        )
        empty.write.format("delta").partitionBy("ingest_date").save(path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["bronze", "silver"])
    ap.add_argument(
        "--available-now",
        action="store_true",
        help="process everything available, then stop (batch-style run)",
    )
    a = ap.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(message)s")
    spark = build_spark("cdc-pipeline")
    ensure_bronze_table(spark, settings)
    queries = []
    if a.only in (None, "bronze"):
        queries.append(start_bronze(spark, settings, a.available_now))
    if a.available_now and a.only is None:
        queries[0].awaitTermination()  # silver must see everything bronze just wrote
    if a.only in (None, "silver"):
        queries.append(start_silver(spark, settings, a.available_now))
    if a.available_now:
        for q in queries:
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
