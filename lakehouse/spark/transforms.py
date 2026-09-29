"""Pure DataFrame transformations for the CDC pipeline (unit-tested in Spark local mode).

Naming: an *envelope* is one Debezium change event:
    {"op": "c|u|d|r", "before": {...}|null, "after": {...}|null,
     "source": {"lsn": ..., "ts_ms": ..., "table": ...}, "ts_ms": ...}
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

VALID_OPS = ["c", "u", "d", "r"]
CDC_COLS = ["_op", "_lsn", "_source_ts_ms", "_kafka_partition", "_kafka_offset", "_bronze_ts"]


# ------------------------------------------------------------------------------------ bronze


def envelope_fields(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows -> Bronze rows: raw value kept verbatim plus the few fields we route/order on.

    Bronze is append-only and schema-agnostic (the payload stays a JSON string), so upstream
    schema changes can never break ingestion.
    """
    v = F.col("value").cast("string")
    return (
        kafka_df.select(
            F.col("topic"),
            F.element_at(F.split("topic", r"\."), -1).alias("table"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("key").cast("string").alias("key"),
            v.alias("value"),
            F.get_json_object(v, "$.op").alias("op"),
            F.get_json_object(v, "$.source.lsn").cast("long").alias("lsn"),
            F.get_json_object(v, "$.source.ts_ms").cast("long").alias("source_ts_ms"),
            F.get_json_object(v, "$.ts_ms").cast("long").alias("envelope_ts_ms"),
        )
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
    )


def is_valid_envelope() -> Column:
    """A record is valid if it's JSON with a known op and the image that op requires."""
    v = F.col("value")
    has_after = F.get_json_object(v, "$.after").isNotNull()
    has_before = F.get_json_object(v, "$.before").isNotNull()
    return (
        F.col("value").isNotNull()
        & F.col("op").isin(VALID_OPS)
        & F.when(F.col("op") == "d", has_before).otherwise(has_after)
    )


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = is_valid_envelope()
    return df.filter(valid), df.filter(~valid | valid.isNull())


# ------------------------------------------------------------------------------------ silver


def infer_row_schema(spark: SparkSession, json_rows: DataFrame, column: str) -> T.StructType:
    """Infer the row schema from this micro-batch's JSON images.

    Inferring per batch (instead of a fixed schema) is what lets a new upstream column
    (F7 schema evolution) appear in Silver without a code change or a job restart.
    """
    sample = json_rows.select(F.col(column).alias("value")).filter(F.col("value").isNotNull())
    return spark.read.json(sample.rdd.map(lambda r: r.value)).schema


def extract_rows(spark: SparkSession, bronze: DataFrame) -> DataFrame:
    """Envelope -> one flat row per change: the row image (after, or before for deletes)
    plus CDC metadata columns."""
    df = bronze.select(
        "op",
        "lsn",
        "source_ts_ms",
        "kafka_partition",
        "kafka_offset",
        "ingest_ts",
        F.when(F.col("op") == "d", F.get_json_object("value", "$.before"))
        .otherwise(F.get_json_object("value", "$.after"))
        .alias("image"),
    )
    schema = infer_row_schema(spark, df, "image")
    return df.select(
        F.from_json("image", schema).alias("row"),
        F.col("op").alias("_op"),
        F.col("lsn").alias("_lsn"),
        F.col("source_ts_ms").alias("_source_ts_ms"),
        F.col("kafka_partition").alias("_kafka_partition"),
        F.col("kafka_offset").alias("_kafka_offset"),
        F.col("ingest_ts").alias("_bronze_ts"),
    ).select("row.*", *CDC_COLS)


def order_key(prefix: str = "") -> list[Column]:
    """Change order: commit LSN first (true commit order), then Kafka offset as a tie-breaker.
    ts_ms alone is not safe — many changes in one transaction share a millisecond."""
    return [F.col(f"{prefix}_lsn"), F.col(f"{prefix}_kafka_offset")]


def latest_per_key(df: DataFrame, pk: str) -> DataFrame:
    """Collapse duplicates and multiple changes to the same row within a micro-batch to the
    newest one (F4 dedupe of late/duplicate events)."""
    w = Window.partitionBy(pk).orderBy(*[c.desc() for c in order_key()])
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def with_soft_delete(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("is_deleted", F.col("_op") == "d")
        .withColumn(
            "deleted_at",
            F.when(F.col("_op") == "d", F.timestamp_millis(F.col("_source_ts_ms"))),
        )
        .withColumn("_silver_ts", F.current_timestamp())
    )


def merge_condition_newer() -> str:
    """Only apply a change if it's newer than what Silver already has — makes replays and
    out-of-order duplicates no-ops (idempotent MERGE => exactly-once effect)."""
    return "s._lsn > t._lsn OR (s._lsn = t._lsn AND s._kafka_offset > t._kafka_offset)"


# ------------------------------------------------------------------------------------ SCD2


def scd2_versions(
    changes: DataFrame,
    existing: DataFrame | None,
    pk: str,
    effective_col: str = "updated_at",
    created_col: str = "created_at",
) -> DataFrame:
    """Rebuild the SCD Type 2 timeline for the keys touched in this batch.

    Every change (not just the latest) becomes a version.
    * valid_from = the row's business effective time (`updated_at`), falling back to the CDC
      commit time. NOT the Debezium event time: for the initial snapshot that's the moment the
      row was *read*, which would leave every pre-existing order without a matching version.
    * The first known version of a key is extended back to `created_at` — when CDC starts from a
      snapshot, earlier history is unknown, so we assume the snapshot image held since creation.
    * valid_to = next version's valid_from; is_current = newest version and not deleted.
    Recomputing from (existing ∪ new) and MERGE-ing on (pk, _lsn, _kafka_offset) is idempotent:
    replaying a batch produces exactly the same rows.
    """
    commit_ts = F.timestamp_millis(F.col("_source_ts_ms"))
    eff = (
        F.coalesce(F.to_timestamp(F.col(effective_col)), commit_ts)
        if effective_col in changes.columns
        else commit_ts
    )
    new = changes.withColumn("valid_from", eff)
    if existing is not None:
        cols = existing.columns
        keep = existing.select(*cols)
        new = new.select(
            *[c for c in new.columns if c in cols] + [c for c in new.columns if c not in cols]
        )
        both = keep.unionByName(new, allowMissingColumns=True)
    else:
        both = new
    w_dup = Window.partitionBy(pk, "_lsn", "_kafka_offset").orderBy(F.lit(1))
    both = both.withColumn("_d", F.row_number().over(w_dup)).filter("_d = 1").drop("_d")
    w = Window.partitionBy(pk).orderBy(*order_key())
    if created_col in both.columns:
        first = F.row_number().over(w) == 1
        both = both.withColumn(
            "valid_from",
            F.when(
                first, F.least(F.col("valid_from"), F.to_timestamp(F.col(created_col)))
            ).otherwise(F.col("valid_from")),
        )
    nxt = F.lead("valid_from").over(w)
    return (
        both.withColumn("valid_to", nxt)
        .withColumn("is_current", nxt.isNull() & (F.col("_op") != "d"))
        .withColumn("is_deleted", F.col("_op") == "d")
        .withColumn("_silver_ts", F.current_timestamp())
    )
