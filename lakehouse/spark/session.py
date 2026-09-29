"""SparkSession factory: Delta Lake + Kafka source + S3A (any S3-compatible store)."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from lakehouse.config import Settings, settings


def _packages() -> list[str]:
    from pyspark import __version__ as spark_version

    scala = "2.13"
    return [
        f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{spark_version}",
        # must match the Hadoop version bundled with pyspark (3.5.0 for Spark 4.2)
        f"org.apache.hadoop:hadoop-aws:{os.environ.get('HADOOP_AWS_VERSION', '3.5.0')}",
    ]


def build_spark(
    app: str,
    s: Settings = settings,
    local_threads: int = 4,
    driver_memory: str = "1536m",
    with_kafka_s3: bool = True,
) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    b = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", f"local[{local_threads}]"))
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        # F7 schema evolution: new source columns are added to Delta tables on MERGE/append.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", s.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s.s3_key)
        .config("spark.hadoop.fs.s3a.secret.key", s.s3_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    extra = _packages() if with_kafka_s3 else []
    spark = configure_spark_with_delta_pip(b, extra_packages=extra).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
