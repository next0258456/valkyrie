"""Test 06: CLI Interface (valkyrie run & valkyrie to-sql)."""

import subprocess
import sys
import pytest


def test_cli_run_success(tmp_path):
    """Running valkyrie run via CLI returns exit code 0 on success."""
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
policies:
  max_error_rate: 0.5
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
""")
    clean_out = tmp_path / "clean_cli.parquet"

    cmd = [
        sys.executable,
        "-m",
        "valkyrie.cli",
        "run",
        "--sql",
        "SELECT 1 AS order_id",
        "--contract",
        str(contract_file),
        "--write-clean",
        str(clean_out),
    ]

    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    assert res.returncode == 0
    assert "VALKYRIE VALIDATION REPORT" in res.stdout
    assert "PASSED" in res.stdout
    assert clean_out.exists()


def test_cli_run_breach_exit_code_1(tmp_path):
    """Running valkyrie run returns exit code 1 on breach and prints alert banner."""
    contract_file = tmp_path / "strict_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
policies:
  max_error_rate: 0.01
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
""")

    cmd = [
        sys.executable,
        "-m",
        "valkyrie.cli",
        "run",
        "--sql",
        "SELECT NULL AS order_id",
        "--contract",
        str(contract_file),
    ]

    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 1
    assert "DATA CONTRACT BREACH DETECTED" in res.stderr or "DATA CONTRACT BREACH DETECTED" in res.stdout
    assert "finance@yggdrasil.local" in res.stderr or "finance@yggdrasil.local" in res.stdout


def test_cli_to_sql(tmp_path):
    """Running valkyrie to-sql outputs the compiled SQL query string."""
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
""")

    cmd = [
        sys.executable,
        "-m",
        "valkyrie.cli",
        "to-sql",
        "--contract",
        str(contract_file),
        "--sql",
        "SELECT * FROM raw_table",
    ]

    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0
    assert "WITH raw AS (" in res.stdout
    assert "ERR_NULL_VIOLATION" in res.stdout
