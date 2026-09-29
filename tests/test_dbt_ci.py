"""dbt build (models + all tests) against the committed Silver fixtures — what CI runs."""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_dbt_build_on_fixtures(tmp_path):
    db = tmp_path / "ci.duckdb"
    subprocess.run(
        [sys.executable, str(ROOT / "tests/fixtures/make_warehouse.py"), "--path", str(db)],
        check=True,
    )
    dbt = Path(sys.executable).with_name("dbt")
    env = {**os.environ, "DBT_TARGET": "ci", "WAREHOUSE_PATH": str(db)}
    r = subprocess.run(
        [
            str(dbt),
            "build",
            "--profiles-dir",
            ".",
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
        ],
        cwd=ROOT / "dbt",
        env=env,
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stdout[-3000:]
    assert "ERROR=0" in r.stdout
