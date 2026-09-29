-- Operational e-commerce schema (the CDC source).
-- REPLICA IDENTITY FULL makes Debezium include the complete old row ("before") on UPDATE and
-- DELETE, not just the primary key — needed for auditability and soft deletes downstream.

CREATE TABLE customers (
    customer_id  BIGSERIAL PRIMARY KEY,
    email        TEXT        NOT NULL,
    full_name    TEXT        NOT NULL,
    city         TEXT        NOT NULL,
    country      TEXT        NOT NULL,
    tier         TEXT        NOT NULL DEFAULT 'standard',  -- standard | silver | gold
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id   BIGSERIAL PRIMARY KEY,
    sku          TEXT          NOT NULL UNIQUE,
    name         TEXT          NOT NULL,
    category     TEXT          NOT NULL,
    price        NUMERIC(10,2) NOT NULL CHECK (price > 0),
    active       BOOLEAN       NOT NULL DEFAULT true,
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id     BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT      NOT NULL REFERENCES customers (customer_id),
    status       TEXT        NOT NULL,  -- created | paid | shipped | delivered | cancelled
    total_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id      BIGINT        NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    product_id    BIGINT        NOT NULL REFERENCES products (product_id),
    quantity      INT           NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(10,2) NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
