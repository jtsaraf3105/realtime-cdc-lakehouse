"""Read Delta tables from the lake without Spark (delta-rs), for loading, QA and metrics."""

from __future__ import annotations

import pandas as pd
from deltalake import DeltaTable

from lakehouse.config import Settings, settings


def storage_options(s: Settings = settings) -> dict[str, str]:
    return {
        "AWS_ENDPOINT_URL": s.s3_endpoint,
        "AWS_ACCESS_KEY_ID": s.s3_key,
        "AWS_SECRET_ACCESS_KEY": s.s3_secret,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def uri(*parts: str, s: Settings = settings) -> str:
    return s.path(*parts).replace("s3a://", "s3://")


def read(*parts: str, s: Settings = settings, columns: list[str] | None = None) -> pd.DataFrame:
    target = uri(*parts, s=s)
    opts = storage_options(s) if target.startswith("s3://") else None
    return DeltaTable(target, storage_options=opts).to_pandas(columns=columns)


def exists(*parts: str, s: Settings = settings) -> bool:
    try:
        read(*parts, s=s, columns=[])
        return True
    except Exception:
        return False
