"""Register (or update) the Debezium Postgres connector with Kafka Connect — idempotent.

python -m lakehouse.connect.register            # create/update and wait until RUNNING
python -m lakehouse.connect.register --status
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import requests

from lakehouse.config import settings

CONFIG = Path(__file__).with_name("postgres-source.json")


def register(wait: float = 60) -> dict:
    spec = json.loads(CONFIG.read_text())
    name = spec["name"]
    # PUT /connectors/{name}/config is create-or-update, so re-running is safe.
    r = requests.put(
        f"{settings.connect_url}/connectors/{name}/config", json=spec["config"], timeout=30
    )
    r.raise_for_status()
    deadline = time.time() + wait
    while time.time() < deadline:
        st = status(name)
        tasks = st.get("tasks", [])
        if (
            st.get("connector", {}).get("state") == "RUNNING"
            and tasks
            and all(t["state"] == "RUNNING" for t in tasks)
        ):
            return st
        if any(t["state"] == "FAILED" for t in tasks):
            raise RuntimeError(json.dumps(st, indent=2))
        time.sleep(2)
    raise TimeoutError(f"connector {name} not RUNNING after {wait}s: {status(name)}")


def status(name: str | None = None) -> dict:
    name = name or json.loads(CONFIG.read_text())["name"]
    r = requests.get(f"{settings.connect_url}/connectors/{name}/status", timeout=10)
    return r.json() if r.ok else {"error": r.status_code}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", action="store_true")
    a = ap.parse_args()
    print(json.dumps(status() if a.status else register(), indent=2))


if __name__ == "__main__":
    main()
