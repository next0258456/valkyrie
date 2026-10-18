"""Comprehensive test suite for Valkyrie Data Contract Engine."""

import os
import pytest
import duckdb
import pyarrow.parquet as pq
from pathlib import Path
from valkyrie import run, execute, to_sql, load_contract, ValkyrieContract
from valkyrie.exceptions import ContractBreachException, SchemaConformanceException


@pytest.fixture
def valid_contract_yaml(tmp_path):
    path = tmp_path / "valid_contract.yaml"
    path.write_text("""
version: "v1"
dataset: "orders"
owner: "data-eng@valkyrie.local"
policies:
  strict_schema: true
  max_error_rate: 0.1
  quarantine_action: "DLQ"
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
    unique: true
  - name: "customer_id"
    type: "VARCHAR"
    not_null: true
    pattern: "^CUST-[0-9]{3}$"
  - name: "status"
    type: "VARCHAR"
    enum: ["NEW", "SHIPPED", "DELIVERED"]
  - name: "amount"
    type: "DOUBLE"
    range: [0.0, 1000.0]
custom_rules:
  - name: "status_amount_check"
    expression: "status != 'DELIVERED' OR amount > 0"
    error_code: "ERR_FREE_DELIVERY"
""")
    return str(path)


# 1. CONTRACT PARSER TESTS
def test_contract_loader_valid(valid_contract_yaml):
    c = load_contract(valid_contract_yaml)
    assert c.dataset == "orders"
    assert c.owner == "data-eng@valkyrie.local"
    assert len(c.schema_) == 4
    assert len(c.custom_rules) == 1


def test_contract_loader_rejects_fluff(tmp_path):
    p = tmp_path / "fluff.yaml"
    p.write_text("""
dataset: "orders"
owner: "me"
sla: "99.9%"
schema:
  - name: "id"
    type: "INT"
""")
    with pytest.raises(ValueError, match="Prohibited.*sla"):
        load_contract(p)


def test_contract_loader_requires_owner_and_dataset(tmp_path):
    p = tmp_path / "no_owner.yaml"
    p.write_text("dataset: 'orders'\nschema: []")
    with pytest.raises(ValueError, match="owner"):
        load_contract(p)


# 2. PRE-FLIGHT SCHEMA TESTS
def test_preflight_missing_column(valid_contract_yaml):
    sql = "SELECT 1 AS order_id, 'CUST-001' AS customer_id"  # missing status and amount
    with pytest.raises(SchemaConformanceException, match="ERR_SCHEMA_MISSING_COLUMN"):
        run(sql=sql, contract=valid_contract_yaml)


def test_preflight_extra_column_strict(valid_contract_yaml):
    sql = """
    SELECT 1 AS order_id, 'CUST-001' AS customer_id, 'NEW' AS status, 10.0 AS amount, 'unexpected' AS extra_col
    """
    with pytest.raises(SchemaConformanceException, match="ERR_SCHEMA_EXTRA_COLUMN"):
        run(sql=sql, contract=valid_contract_yaml)


def test_preflight_type_mismatch(valid_contract_yaml):
    # amount declared as DOUBLE (NUMERIC), provided as VARCHAR
    sql = "SELECT 1 AS order_id, 'CUST-001' AS customer_id, 'NEW' AS status, 'invalid_text' AS amount"
    with pytest.raises(SchemaConformanceException, match="ERR_SCHEMA_TYPE_MISMATCH"):
        run(sql=sql, contract=valid_contract_yaml)


# 3. SINGLE-PASS CONSTRAINTS & COMPILER TESTS
def test_to_sql_compiles_cte(valid_contract_yaml):
    sql = to_sql(valid_contract_yaml, "SELECT 1 AS order_id")
    assert "WITH raw AS" in sql
    assert "_error_code" in sql
    assert "_error_column" in sql
    assert "_raw_value" in sql


def test_run_clean_and_dlq_split(valid_contract_yaml):
    sql = """
    SELECT * FROM (VALUES
        (1, 'CUST-001', 'NEW', 50.0),
        (2, 'CUST-002', 'SHIPPED', 100.0),
        (3, 'INVALID', 'NEW', 50.0),        -- pattern fail
        (4, 'CUST-004', 'UNKNOWN', 50.0),    -- enum fail
        (5, 'CUST-005', 'NEW', 1500.0),      -- range fail
        (NULL, 'CUST-006', 'NEW', 50.0)      -- null fail
    ) AS t(order_id, customer_id, status, amount)
    """
    result = run(sql=sql, contract=valid_contract_yaml, raise_on_breach=False)
    clean, dlq = result

    assert clean.num_rows == 2
    assert dlq.num_rows == 4
    assert result.metrics["total_rows"] == 6
    assert result.metrics["clean_rows"] == 2
    assert result.metrics["error_rows"] == 4
    assert "duration_ms" in result.metrics
    assert result.is_passed is False  # 4/6 = 66% error rate > 10%


def test_custom_rules_with_nested_list():
    contract = {
        "dataset": "receipts",
        "owner": "audit@valkyrie.local",
        "schema": [{"name": "items", "type": "STRUCT(price DOUBLE)[]"}],
        "custom_rules": [
            {
                "name": "min_total",
                "expression": "list_sum(items.price) >= 10.0",
                "error_code": "ERR_TOTAL_TOO_LOW"
            }
        ]
    }
    sql = """
    SELECT [{'price': 5.0}, {'price': 10.0}] AS items
    UNION ALL
    SELECT [{'price': 2.0}] AS items
    """
    clean, dlq = run(sql=sql, contract=contract, raise_on_breach=False)
    assert clean.num_rows == 1
    assert dlq.num_rows == 1
    assert dlq.column("_error_code").to_pylist() == ["ERR_TOTAL_TOO_LOW"]


# 4. CIRCUIT BREAKER & ALERT BANNER TESTS
def test_circuit_breaker_raises_on_breach(valid_contract_yaml):
    sql = "SELECT NULL AS order_id, 'CUST-001' AS customer_id, 'NEW' AS status, 10.0 AS amount"
    with pytest.raises(ContractBreachException) as exc_info:
        run(sql=sql, contract=valid_contract_yaml, raise_on_breach=True)

    err_str = str(exc_info.value)
    assert "DATA CONTRACT BREACH DETECTED" in err_str
    assert "data-eng@valkyrie.local" in err_str
    assert "ERR_NULL_VIOLATION" in err_str
    assert exc_info.value.dlq_df is not None


# 5. DUCKDB NATIVE EXPORT & PERSISTENCE
def test_execute_native_parquet_persistence(tmp_path, valid_contract_yaml):
    clean_path = tmp_path / "clean.parquet"
    dlq_path = tmp_path / "dlq.parquet"

    sql = """
    SELECT * FROM (VALUES
        (1, 'CUST-001', 'NEW', 50.0),
        (2, 'CUST-002', 'SHIPPED', 100.0)
    ) AS t(order_id, customer_id, status, amount)
    """
    result = execute(
        sql=sql,
        contract=valid_contract_yaml,
        write_clean=str(clean_path),
        write_dlq=str(dlq_path),
    )
    assert result.is_passed is True
    assert clean_path.exists()
    assert pq.read_table(clean_path).num_rows == 2


def test_execute_persists_dlq_on_breach_without_rescan(tmp_path, valid_contract_yaml, monkeypatch):
    import valkyrie.engine as eng
    run_count = 0
    orig_run = eng.run

    def tracked_run(*args, **kwargs):
        nonlocal run_count
        run_count += 1
        return orig_run(*args, **kwargs)

    monkeypatch.setattr(eng, "run", tracked_run)

    clean_path = tmp_path / "clean_fail.parquet"
    dlq_path = tmp_path / "dlq_fail.parquet"
    sql = "SELECT NULL AS order_id, 'CUST-001' AS customer_id, 'NEW' AS status, 10.0 AS amount"

    with pytest.raises(ContractBreachException):
        execute(
            sql=sql,
            contract=valid_contract_yaml,
            write_clean=str(clean_path),
            write_dlq=str(dlq_path),
        )

    assert run_count == 1
    assert not clean_path.exists()
    assert dlq_path.exists()
    assert pq.read_table(dlq_path).num_rows == 1


# 6. CLI TESTS
def test_cli_execution(valid_contract_yaml, monkeypatch, capsys):
    from valkyrie.cli import main
    import sys

    # CLI compile (spec primary)
    monkeypatch.setattr(sys, "argv", ["valkyrie", "compile", "--contract", valid_contract_yaml, "--sql", "SELECT 1"])
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code == 0
    assert "WITH raw AS" in capsys.readouterr().out

    # CLI to-sql (spec alias)
    monkeypatch.setattr(sys, "argv", ["valkyrie", "to-sql", "--contract", valid_contract_yaml, "--sql", "SELECT 1"])
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code == 0
    assert "WITH raw AS" in capsys.readouterr().out

    # CLI run success
    sql = "SELECT 1 AS order_id, 'CUST-001' AS customer_id, 'NEW' AS status, 10.0 AS amount"
    monkeypatch.setattr(sys, "argv", ["valkyrie", "run", "--contract", valid_contract_yaml, "--sql", sql])
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code == 0
    assert "PASSED ✅" in capsys.readouterr().out


def test_execute_zero_oom_clean_arrow_is_none(tmp_path, valid_contract_yaml):
    """execute() streams directly to disk without loading full Arrow table into Python RAM."""
    clean_path = tmp_path / "clean_oom.parquet"
    sql = "SELECT 1 AS order_id, 'CUST-001' AS customer_id, 'NEW' AS status, 10.0 AS amount"
    result = execute(
        sql=sql,
        contract=valid_contract_yaml,
        write_clean=str(clean_path),
    )
    assert result.is_passed is True
    # Zero-OOM guarantee: clean_arrow is not materialized in memory during disk-to-disk execute
    assert result.clean_arrow is None
    assert clean_path.exists()
    assert pq.read_table(clean_path).num_rows == 1


def test_preflight_complex_type_mismatch():
    """Verify preflight rejects complex type mismatch (e.g. STRUCT declared but INT given)."""
    contract = {
        "dataset": "nested",
        "owner": "eng@valkyrie.local",
        "schema": [{"name": "meta", "type": "STRUCT(id INT)"}],
    }
    sql = "SELECT 123 AS meta"  # meta is INTEGER, not STRUCT!
    with pytest.raises(SchemaConformanceException, match="ERR_SCHEMA_TYPE_MISMATCH"):
        run(sql=sql, contract=contract)
