"""Unit tests for the CDC transformations (Spark local mode, no Kafka/S3 needed)."""

import json
import os

import pytest

os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")

from lakehouse.spark.transforms import (  # noqa: E402
    envelope_fields,
    extract_rows,
    latest_per_key,
    scd2_versions,
    split_valid,
    with_soft_delete,
)


@pytest.fixture(scope="module")
def spark():
    from lakehouse.spark.session import build_spark

    s = build_spark("tests", local_threads=1, driver_memory="1g", with_kafka_s3=False)
    yield s
    s.stop()


def env(op, after=None, before=None, lsn=100, ts=1_700_000_000_000, table="customers"):
    return json.dumps(
        {
            "op": op,
            "after": after,
            "before": before,
            "ts_ms": ts,
            "source": {"lsn": lsn, "ts_ms": ts, "table": table},
        }
    )


def kafka_rows(spark, values, table="customers"):
    rows = [
        (f"shop.public.{table}", 0, i, None, None, v.encode() if v is not None else None)
        for i, v in enumerate(values)
    ]
    df = spark.createDataFrame(
        rows,
        "topic string, partition int, offset long, timestamp timestamp, key binary, value binary",
    )
    return envelope_fields(df)


def test_envelope_fields_and_dlq_split(spark):
    good = env("c", after={"customer_id": 1, "city": "Pune"})
    bad_json = "{not json"
    bad_op = json.dumps({"op": "x", "after": {"customer_id": 2}})
    delete_without_before = env("d", after=None, before=None)
    df = kafka_rows(spark, [good, bad_json, bad_op, delete_without_before, None])
    assert df.first()["table"] == "customers"
    valid, invalid = split_valid(df)
    assert valid.count() == 1 and invalid.count() == 4


def test_extract_uses_before_image_for_deletes(spark):
    df = kafka_rows(
        spark,
        [
            env("c", after={"customer_id": 1, "city": "Pune"}, lsn=1),
            env("d", before={"customer_id": 1, "city": "Pune"}, lsn=2),
        ],
    )
    rows = extract_rows(spark, df).orderBy("_lsn").collect()
    assert [(r.customer_id, r._op) for r in rows] == [(1, "c"), (1, "d")]


def test_latest_per_key_orders_by_lsn_not_arrival(spark):
    # arrival order (offset) is 0,1,2 but commit order (lsn) says "Delhi" is the newest
    df = kafka_rows(
        spark,
        [
            env("u", after={"customer_id": 1, "city": "Delhi"}, lsn=30),
            env("u", after={"customer_id": 1, "city": "Pune"}, lsn=10),
            env("u", after={"customer_id": 1, "city": "Pune"}, lsn=10),  # duplicate delivery
        ],
    )
    out = latest_per_key(extract_rows(spark, df), "customer_id").collect()
    assert len(out) == 1 and out[0].city == "Delhi"


def test_soft_delete_flags(spark):
    df = kafka_rows(spark, [env("d", before={"customer_id": 5, "city": "X"}, lsn=9)])
    r = with_soft_delete(extract_rows(spark, df)).first()
    assert r.is_deleted is True and r.deleted_at is not None


def test_schema_evolution_new_column_is_picked_up(spark):
    df = kafka_rows(
        spark,
        [
            env("u", after={"product_id": 1, "price": "9.99"}, lsn=1, table="products"),
            env(
                "u",
                after={"product_id": 2, "price": "5.00", "weight_kg": "1.20"},
                lsn=2,
                table="products",
            ),
        ],
        table="products",
    )
    rows = extract_rows(spark, df)
    assert "weight_kg" in rows.columns
    assert rows.filter("product_id = 2").first().weight_kg == "1.20"


def _versions(spark, changes):
    df = kafka_rows(
        spark,
        [
            env(op, after=a if op != "d" else None, before=a if op == "d" else None, lsn=lsn, ts=ts)
            for op, a, lsn, ts in changes
        ],
    )
    return extract_rows(spark, df)


def test_scd2_builds_timeline_and_is_idempotent(spark):
    base = {"customer_id": 1, "created_at": "2026-01-01T00:00:00Z"}
    changes = _versions(
        spark,
        [
            (
                "r",
                {**base, "city": "Pune", "updated_at": "2026-01-05T00:00:00Z"},
                10,
                1_800_000_000_000,
            ),
            (
                "u",
                {**base, "city": "Berlin", "updated_at": "2026-02-01T00:00:00Z"},
                20,
                1_800_000_100_000,
            ),
            (
                "u",
                {**base, "city": "Austin", "updated_at": "2026-03-01T00:00:00Z"},
                30,
                1_800_000_200_000,
            ),
        ],
    )
    v = scd2_versions(changes, None, "customer_id").orderBy("_lsn").collect()
    assert [r.city for r in v] == ["Pune", "Berlin", "Austin"]
    assert [r.is_current for r in v] == [False, False, True]
    # first known version extends back to created_at (snapshot-start assumption)
    assert str(v[0].valid_from).startswith("2026-01-01")
    # valid_from is the business effective time, and versions tile time without gaps
    assert v[0].valid_to == v[1].valid_from and v[1].valid_to == v[2].valid_from
    assert v[2].valid_to is None
    # replaying the same batch against the result changes nothing
    again = scd2_versions(changes, scd2_versions(changes, None, "customer_id"), "customer_id")
    assert again.count() == 3 and again.filter("is_current").count() == 1


def test_scd2_delete_closes_current_version(spark):
    base = {
        "customer_id": 2,
        "city": "Pune",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    v = (
        scd2_versions(
            _versions(
                spark, [("c", base, 1, 1_800_000_000_000), ("d", base, 2, 1_800_000_500_000)]
            ),
            None,
            "customer_id",
        )
        .orderBy("_lsn")
        .collect()
    )
    assert [r.is_current for r in v] == [False, False]
    assert v[1].is_deleted is True
