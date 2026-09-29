"""E-commerce workload generator for the CDC source database.

Produces a realistic mix of row changes at a target rate (default 50 row-changes/second):
new orders with items, order status progressions (created → paid → shipped → delivered, or
cancelled), new customers, customer profile changes (drives SCD2), product price changes, and
deletes (cancelled orders are purged, which also cascades to their items).

    python -m lakehouse.simulator.orders --rate 50 --seconds 120
    python -m lakehouse.simulator.orders --evolve-schema   # ALTER TABLE products ADD weight_kg
"""

from __future__ import annotations

import argparse
import random
import time
from collections import Counter

import psycopg

from lakehouse.config import settings

CITIES = [
    ("Mumbai", "IN"),
    ("Bengaluru", "IN"),
    ("Delhi", "IN"),
    ("Pune", "IN"),
    ("London", "GB"),
    ("Berlin", "DE"),
    ("New York", "US"),
    ("Austin", "US"),
    ("Singapore", "SG"),
    ("Toronto", "CA"),
]
CATEGORIES = ["Electronics", "Books", "Home", "Apparel", "Beauty", "Sports", "Toys"]
NEXT_STATUS = {"created": "paid", "paid": "shipped", "shipped": "delivered"}


class Workload:
    def __init__(self, conn: psycopg.Connection, seed: int = 42) -> None:
        self.c = conn
        self.rng = random.Random(seed)
        self.counts: Counter[str] = Counter()

    # ------------------------------------------------------------------ bootstrap

    def seed_reference_data(self, customers: int = 300, products: int = 80) -> None:
        with self.c.transaction():
            n = self.c.execute("SELECT count(*) FROM products").fetchone()[0]
            if n:
                return
            for i in range(products):
                self.c.execute(
                    "INSERT INTO products (sku, name, category, price) VALUES (%s, %s, %s, %s)",
                    (
                        f"SKU-{i:05d}",
                        f"Product {i}",
                        self.rng.choice(CATEGORIES),
                        round(self.rng.uniform(3, 250), 2),
                    ),
                )
            for _ in range(customers):
                self._insert_customer()

    # ------------------------------------------------------------------ operations

    def _insert_customer(self) -> None:
        city, country = self.rng.choice(CITIES)
        k = self.rng.randint(1, 10**9)
        self.c.execute(
            "INSERT INTO customers (email, full_name, city, country) VALUES (%s, %s, %s, %s)",
            (f"user{k}@example.com", f"Customer {k}", city, country),
        )
        self.counts["customer_insert"] += 1

    def _update_customer(self) -> None:
        city, country = self.rng.choice(CITIES)
        field_choice = self.rng.random()
        if field_choice < 0.5:  # moved house
            sql, args = "city = %s, country = %s", (city, country)
        elif field_choice < 0.8:
            sql, args = "tier = %s", (self.rng.choice(["standard", "silver", "gold"]),)
        else:
            sql, args = "email = %s", (f"new{self.rng.randint(1, 10**9)}@example.com",)
        self.c.execute(
            f"UPDATE customers SET {sql}, updated_at = clock_timestamp() "  # noqa: S608
            "WHERE customer_id = (SELECT customer_id FROM customers ORDER BY random() LIMIT 1)",
            args,
        )
        self.counts["customer_update"] += 1

    def _update_price(self) -> None:
        self.c.execute(
            "UPDATE products SET price = round((price * (0.85 + random() * 0.3))::numeric, 2), "
            "updated_at = clock_timestamp() "
            "WHERE product_id = (SELECT product_id FROM products ORDER BY random() LIMIT 1)"
        )
        self.counts["product_update"] += 1

    def _new_order(self) -> int:
        """1 order insert + 1-3 item inserts + 1 total update = 3-5 row changes."""
        with self.c.transaction():
            cid = self.c.execute(
                "SELECT customer_id FROM customers ORDER BY random() LIMIT 1"
            ).fetchone()[0]
            oid = self.c.execute(
                "INSERT INTO orders (customer_id, status) VALUES (%s, 'created') "
                "RETURNING order_id",
                (cid,),
            ).fetchone()[0]
            n_items = self.rng.randint(1, 3)
            for pid, price in self.c.execute(
                "SELECT product_id, price FROM products WHERE active ORDER BY random() LIMIT %s",
                (n_items,),
            ).fetchall():
                self.c.execute(
                    "INSERT INTO order_items (order_id, product_id, quantity, unit_price) "
                    "VALUES (%s, %s, %s, %s)",
                    (oid, pid, self.rng.randint(1, 4), price),
                )
            self.c.execute(
                "UPDATE orders SET total_amount = (SELECT sum(quantity * unit_price) "
                "FROM order_items WHERE order_id = %s), updated_at = clock_timestamp() "
                "WHERE order_id = %s",
                (oid, oid),
            )
        self.counts["order_insert"] += 1
        self.counts["order_item_insert"] += n_items
        return 2 + n_items

    def _advance_order(self) -> None:
        row = self.c.execute(
            "SELECT order_id, status FROM orders WHERE status IN ('created','paid','shipped') "
            "ORDER BY random() LIMIT 1"
        ).fetchone()
        if row is None:
            return
        oid, status = row
        new = (
            "cancelled" if status == "created" and self.rng.random() < 0.12 else NEXT_STATUS[status]
        )
        self.c.execute(
            "UPDATE orders SET status = %s, updated_at = clock_timestamp() WHERE order_id = %s",
            (new, oid),
        )
        self.counts[f"order_{new}"] += 1

    def _purge_cancelled(self) -> int:
        row = self.c.execute(
            "DELETE FROM orders WHERE order_id = (SELECT order_id FROM orders "
            "WHERE status = 'cancelled' ORDER BY updated_at LIMIT 1) "
            "RETURNING (SELECT count(*) FROM order_items i WHERE i.order_id = orders.order_id)"
        ).fetchone()
        if row is None:
            return 0
        self.counts["order_delete"] += 1
        return 1 + int(row[0])

    def step(self) -> int:
        """Perform one weighted operation; return the number of row changes it produced."""
        r = self.rng.random()
        if r < 0.40:
            return self._new_order()
        if r < 0.72:
            self._advance_order()
            return 1
        if r < 0.80:
            self._insert_customer()
            return 1
        if r < 0.88:
            self._update_customer()
            return 1
        if r < 0.95:
            self._update_price()
            return 1
        return self._purge_cancelled() or 0

    def run(self, rate: float, seconds: float, report_every: float = 10) -> int:
        """Open-loop at `rate` row changes/second (sleeps to hold the rate)."""
        start = last = time.perf_counter()
        changes = 0
        while (now := time.perf_counter()) - start < seconds:
            changes += self.step()
            ahead = changes / rate - (time.perf_counter() - start)
            if ahead > 0:
                time.sleep(ahead)
            if now - last >= report_every:
                print(f"  {changes / (now - start):6.1f} changes/s  total={changes:,}", flush=True)
                last = now
        return changes


def evolve_schema(conn: psycopg.Connection) -> None:
    """F7: add a column upstream while the pipeline runs; it must flow through to Silver."""
    conn.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS weight_kg NUMERIC(6,2)")
    conn.execute(
        "UPDATE products SET weight_kg = round((0.1 + random() * 5)::numeric, 2), "
        "updated_at = clock_timestamp() WHERE product_id <= 20"
    )
    print("products.weight_kg added and populated for 20 products")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=50)
    ap.add_argument("--seconds", type=float, default=60)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--evolve-schema", action="store_true")
    a = ap.parse_args()
    with psycopg.connect(settings.pg_dsn, autocommit=True) as conn:
        if a.evolve_schema:
            evolve_schema(conn)
            return
        w = Workload(conn, a.seed)
        w.seed_reference_data()
        n = w.run(a.rate, a.seconds)
        print(f"done: {n:,} row changes in {a.seconds:.0f}s -> {dict(w.counts)}")


if __name__ == "__main__":
    main()
