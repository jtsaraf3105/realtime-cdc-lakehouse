"""Settings from environment variables (defaults match docker-compose.yml for local runs)."""

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    pg_dsn: str = _env("PG_DSN", "postgresql://shop:shop@localhost:5462/shop")
    kafka_bootstrap: str = _env("KAFKA_BOOTSTRAP", "localhost:29092")
    connect_url: str = _env("CONNECT_URL", "http://localhost:8083")
    topic_prefix: str = _env("TOPIC_PREFIX", "shop")
    dlq_topic: str = _env("DLQ_TOPIC", "shop.dlq")
    tables: tuple[str, ...] = ("customers", "products", "orders", "order_items")

    s3_endpoint: str = _env("S3_ENDPOINT", "http://localhost:8333")
    s3_key: str = _env("S3_ACCESS_KEY", "lakehouse")
    s3_secret: str = _env("S3_SECRET_KEY", "lakehouse12345")
    lake_root: str = _env("LAKE_ROOT", "s3a://lakehouse")  # file:///... also works

    trigger_seconds: int = int(_env("TRIGGER_SECONDS", "5"))
    warehouse_path: str = _env("WAREHOUSE_PATH", "warehouse/lakehouse.duckdb")
    warehouse_target: str = _env("WAREHOUSE_TARGET", "duckdb")  # duckdb | snowflake
    slack_webhook: str | None = os.environ.get("SLACK_WEBHOOK_URL")
    primary_keys: dict[str, str] = field(
        default_factory=lambda: {
            "customers": "customer_id",
            "products": "product_id",
            "orders": "order_id",
            "order_items": "order_item_id",
        }
    )

    def path(self, *parts: str) -> str:
        return "/".join([self.lake_root.rstrip("/"), *parts])

    def topics(self) -> list[str]:
        return [f"{self.topic_prefix}.public.{t}" for t in self.tables]


settings = Settings()
