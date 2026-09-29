"""Hourly batch layer on top of the streaming Silver tables (F10).

    ge_silver ─► load_warehouse ─► dbt_run ─► dbt_test ─► ge_gold

* Every task shells out to the project's own venv (PROJECT_DIR / PROJECT_PYTHON), so Airflow's
  dependency pins never collide with Spark / dbt / GX.
* Every step is idempotent (GX is read-only, the load is a full-refresh CREATE OR REPLACE, dbt
  rebuilds its models), so retries and backfills are safe.
* Failures are logged and, if SLACK_WEBHOOK_URL is set, posted to Slack.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator

    from airflow import DAG

PROJECT_DIR = os.environ.get(
    "PROJECT_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)
PY = os.environ.get("PROJECT_PYTHON", os.path.join(PROJECT_DIR, ".venv", "bin", "python"))
DBT = os.path.join(os.path.dirname(PY), "dbt")
FRESHNESS = os.environ.get("LAKEHOUSE_FRESHNESS_MINUTES", "15")  # PRD F9: < 15 min lag
ENV = {**os.environ, "JAVA_HOME": os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")}
log = logging.getLogger(__name__)


def alert_on_failure(context: dict) -> None:
    ti = context.get("task_instance")
    msg = (
        f":rotating_light: lakehouse_hourly failed — task `{getattr(ti, 'task_id', '?')}` "
        f"run `{context.get('run_id')}`: {context.get('exception')}"
    )
    log.error(msg)
    hook = os.environ.get("SLACK_WEBHOOK_URL")
    if hook:
        req = urllib.request.Request(
            hook,
            data=json.dumps({"text": msg}).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)  # noqa: S310 (operator-configured URL)


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="lakehouse_hourly",
    description="Silver -> warehouse -> dbt marts, with quality gates",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "dbt", "great_expectations"],
) as dag:
    cd = f"cd {PROJECT_DIR} && "
    ge_silver = BashOperator(
        task_id="ge_silver",
        env=ENV,
        bash_command=cd
        + f"{PY} -m lakehouse.quality.checks silver --freshness-minutes {FRESHNESS}",
    )
    load = BashOperator(
        task_id="load_warehouse", env=ENV, bash_command=cd + f"{PY} -m lakehouse.warehouse.load"
    )
    dbt_run = BashOperator(
        task_id="dbt_run", env=ENV, bash_command=cd + f"cd dbt && {DBT} run --profiles-dir ."
    )
    dbt_test = BashOperator(
        task_id="dbt_test", env=ENV, bash_command=cd + f"cd dbt && {DBT} test --profiles-dir ."
    )
    ge_gold = BashOperator(
        task_id="ge_gold", env=ENV, bash_command=cd + f"{PY} -m lakehouse.quality.checks gold"
    )

    ge_silver >> load >> dbt_run >> dbt_test >> ge_gold
