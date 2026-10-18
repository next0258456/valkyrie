"""Test 01: Core Single-Pass Contract Engine (not_null, range)."""

import pytest
import pyarrow as pa
import duckdb
from valkyrie import run, to_sql, load_contract, ValkyrieContract
from valkyrie.exceptions import ContractBreachException


def test_contract_parsing_requires_owner_and_dataset(tmp_path):
    """Contract parser requires owner and dataset, and rejects non-executable text prose."""
    # 1. Missing owner
    bad_yaml_no_owner = tmp_path / "no_owner.yaml"
    bad_yaml_no_owner.write_text("""
version: "v1"
dataset: "orders"
schema:
  - name: "order_id"
    type: "BIGINT"
""")
    with pytest.raises(ValueError, match="owner"):
        load_contract(str(bad_yaml_no_owner))

    # 2. Non-executable prose rule (type: text)
    bad_yaml_prose = tmp_path / "prose.yaml"
    bad_yaml_prose.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
schema:
  - name: "order_id"
    type: "BIGINT"
    quality:
      - type: "text"
        description: "Must be a positive integer"
""")
    with pytest.raises(ValueError, match="Non-executable prose"):
        load_contract(str(bad_yaml_prose))


def test_single_pass_sql_generation(tmp_path):
    """Compiler generates single-pass CASE WHEN query with First-Violation Short-Circuit."""
    contract_file = tmp_path / "test_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
  - name: "unit_price"
    type: "DOUBLE"
    not_null: true
    range:
      min: 0.01
      max: 1000.0
""")
    contract = load_contract(str(contract_file))
    sql = to_sql(contract, source_sql="SELECT * FROM raw_orders")
    
    # Assert query structure
    assert "WITH raw AS (" in sql
    assert "annotated AS (" in sql
    assert "_error_code" in sql
    assert "_error_column" in sql
    assert "_raw_value" in sql
    assert "_ingestion_timestamp" in sql
    assert "ERR_NULL_VIOLATION" in sql
    assert "ERR_OUT_OF_RANGE" in sql


def test_core_engine_run_end_to_end(tmp_path):
    """Calling valkyrie.run separates clean and DLQ records accurately in single pass."""
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
policies:
  max_error_rate: 0.8
  quarantine_action: "DLQ"
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
  - name: "amount"
    type: "DOUBLE"
    not_null: true
    range:
      min: 10.0
      max: 100.0
""")
    # Source dataset with:
    # Row 1: Valid (order_id=1, amount=50.0) -> Clean
    # Row 2: order_id is NULL -> DLQ (ERR_NULL_VIOLATION on order_id)
    # Row 3: amount = 5.0 (< min 10.0) -> DLQ (ERR_OUT_OF_RANGE on amount)
    # Row 4: Valid (order_id=4, amount=100.0) -> Clean
    source_query = """
    SELECT * FROM (
        VALUES 
            (1, 50.0),
            (NULL, 60.0),
            (3, 5.0),
            (4, 100.0)
    ) AS t(order_id, amount)
    """

    result = run(sql=source_query, contract=str(contract_file))

    # Test tuple unpacking
    clean_arrow, dlq_arrow = result

    # Verify Clean stream
    assert clean_arrow.num_rows == 2
    clean_ids = clean_arrow.column("order_id").to_pylist()
    assert clean_ids == [1, 4]
    # Clean records must NOT contain diagnostic metadata columns
    assert "_error_code" not in clean_arrow.column_names
    assert "_error_column" not in clean_arrow.column_names

    # Verify DLQ stream
    assert dlq_arrow.num_rows == 2
    assert "_error_code" in dlq_arrow.column_names
    assert "_error_column" in dlq_arrow.column_names
    assert "_raw_value" in dlq_arrow.column_names
    assert "_ingestion_timestamp" in dlq_arrow.column_names

    dlq_rows = dlq_arrow.to_pylist()
    # Row 2 check
    assert dlq_rows[0]["order_id"] is None
    assert dlq_rows[0]["_error_code"] == "ERR_NULL_VIOLATION"
    assert dlq_rows[0]["_error_column"] == "order_id"

    # Row 3 check
    assert dlq_rows[1]["order_id"] == 3
    assert dlq_rows[1]["amount"] == 5.0
    assert dlq_rows[1]["_error_code"] == "ERR_OUT_OF_RANGE"
    assert dlq_rows[1]["_error_column"] == "amount"
