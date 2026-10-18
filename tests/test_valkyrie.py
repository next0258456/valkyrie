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
    with pytest.raises(ValueError, match="Unknown or prohibited key 'sla'"):
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


# 7. RESTORED CRITICAL EDGE CASES (Ticket 04)
def test_unique_duplicate_key_violation():
    """Verify unique constraint trips ERR_DUPLICATE_KEY via DuckDB window function."""
    contract = {
        "dataset": "users",
        "owner": "security@valkyrie.local",
        "schema": [{"name": "user_id", "type": "INT", "unique": True}],
    }
    sql = "SELECT * FROM (VALUES (1), (2), (1)) AS t(user_id)"
    clean, dlq = run(sql=sql, contract=contract, raise_on_breach=False)
    assert clean.num_rows == 1
    assert dlq.num_rows == 2
    assert all(code == "ERR_DUPLICATE_KEY" for code in dlq.column("_error_code").to_pylist())


def test_quarantine_action_fail_fast():
    """quarantine_action: FAIL_FAST trips on any invalid row regardless of max_error_rate."""
    contract = {
        "dataset": "transactions",
        "owner": "fraud@valkyrie.local",
        "policies": {"max_error_rate": 0.99, "quarantine_action": "FAIL_FAST"},
        "schema": [{"name": "tx_id", "type": "INT", "not_null": True}],
    }
    sql = "SELECT * FROM (VALUES (1), (2), (NULL)) AS t(tx_id)"
    with pytest.raises(ContractBreachException, match="Quarantine policy is FAIL_FAST"):
        run(sql=sql, contract=contract, raise_on_breach=True)


def test_first_violation_precedence():
    """First-violation short-circuit: null check fires before range check."""
    contract = {
        "dataset": "orders",
        "owner": "ops@valkyrie.local",
        "schema": [{"name": "price", "type": "DOUBLE", "not_null": True, "range": [0, 100]}],
    }
    sql = "SELECT NULL AS price"
    clean, dlq = run(sql=sql, contract=contract, raise_on_breach=False)
    assert dlq.column("_error_code").to_pylist() == ["ERR_NULL_VIOLATION"]


def test_range_dict_syntax():
    """Range supports dict syntax {min: x, max: y} in addition to list."""
    contract = {
        "dataset": "sensor",
        "owner": "iot@valkyrie.local",
        "schema": [{"name": "temp", "type": "DOUBLE", "range": {"min": -20.0, "max": 50.0}}],
    }
    sql = "SELECT * FROM (VALUES (-25.0), (25.0), (60.0)) AS t(temp)"
    clean, dlq = run(sql=sql, contract=contract, raise_on_breach=False)
    assert clean.num_rows == 1
    assert dlq.num_rows == 2


def test_configure_cloud_storage_secret(monkeypatch):
    """Auto-detects S3 environment variables and builds secret config."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test_secret")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_USE_SSL", "false")
    monkeypatch.setenv("S3_URL_STYLE", "path")

    from valkyrie.engine import configure_cloud_storage
    con = duckdb.connect()
    # Must execute without error
    configure_cloud_storage(con, "s3://my-bucket/orders.parquet")


def test_execute_zero_oom_on_breach(tmp_path, valid_contract_yaml):
    """When execute encounters a breach on a large dataset, DLQ is persisted and dlq_df is None (Zero-OOM)."""
    clean_path = tmp_path / "clean_oom_fail.parquet"
    dlq_path = tmp_path / "dlq_oom_fail.parquet"
    # Breach query: 100% error rate
    sql = "SELECT NULL AS order_id, 'CUST-001' AS customer_id, 'NEW' AS status, 10.0 AS amount"

    with pytest.raises(ContractBreachException) as exc_info:
        execute(
            sql=sql,
            contract=valid_contract_yaml,
            write_clean=str(clean_path),
            write_dlq=str(dlq_path),
        )

    # DLQ Parquet was safely streamed to disk
    assert dlq_path.exists()
    assert pq.read_table(dlq_path).num_rows == 1
    # Zero-OOM guarantee on breach: exception holds None for dlq_df when called from execute
    assert exc_info.value.dlq_df is None


def test_contract_strict_root_and_column_whitelist(tmp_path):
    """Arbitrary root keys or unknown column attributes are strictly rejected."""
    p1 = tmp_path / "unknown_root.yaml"
    p1.write_text("dataset: 'd'\nowner: 'o'\nunknown_meta: 123\nschema:\n  - name: 'a'\n")
    with pytest.raises(ValueError, match="Unknown or prohibited key 'unknown_meta'"):
        load_contract(p1)

    p2 = tmp_path / "unknown_col.yaml"
    p2.write_text("dataset: 'd'\nowner: 'o'\nschema:\n  - name: 'a'\n    not_nul: true\n")
    with pytest.raises(ValueError, match="Unknown column attribute 'not_nul'"):
        load_contract(p2)


def test_contract_rejects_empty_schema_and_empty_expression(tmp_path):
    """Empty schema list and empty custom rule expression are rejected."""
    p1 = tmp_path / "empty_schema.yaml"
    p1.write_text("dataset: 'd'\nowner: 'o'\nschema: []\n")
    with pytest.raises(ValueError, match="at least one column"):
        load_contract(p1)

    p2 = tmp_path / "empty_expr.yaml"
    p2.write_text("dataset: 'd'\nowner: 'o'\nschema:\n  - name: 'a'\ncustom_rules:\n  - name: 'r'\n    expression: '  '\n")
    with pytest.raises(ValueError, match="non-empty 'expression'"):
        load_contract(p2)


def test_range_dict_invalid_keys(tmp_path):
    """Range dict with unknown keys or missing min/max raises ValueError."""
    # Unknown key 'minimum' instead of 'min'
    p1 = tmp_path / "typo_range.yaml"
    p1.write_text("dataset: 'd'\nowner: 'o'\nschema:\n  - name: 'price'\n    range:\n      minimum: 0\n      max: 10\n")
    with pytest.raises(ValueError, match="Unknown key.*range constraint"):
        load_contract(p1)

    # Empty range dict
    p2 = tmp_path / "empty_range.yaml"
    p2.write_text("dataset: 'd'\nowner: 'o'\nschema:\n  - name: 'price'\n    range: {}\n")
    with pytest.raises(ValueError, match="at least 'min' or 'max'"):
        load_contract(p2)


def test_validation_result_dataframe_aliases(valid_contract_yaml):
    """ValidationResult provides clean_df and dlq_df property aliases matching GLOSSARY."""
    sql = "SELECT 1 AS order_id, 'CUST-001' AS customer_id, 'NEW' AS status, 10.0 AS amount"
    result = run(sql=sql, contract=valid_contract_yaml)
    assert result.clean_df is not None
    assert result.clean_df.num_rows == 1
    assert result.dlq_df is not None
    assert result.dlq_df.num_rows == 0


