"""Test 03: Cross-Column & Nested List Rules (custom_rules)."""

import pytest
import pyarrow as pa
from valkyrie import run


def test_custom_rules_cross_column_arithmetic(tmp_path):
    """Custom rules evaluate multi-column arithmetic, routing failures to DLQ with custom error code."""
    contract_file = tmp_path / "custom_rules_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "sales"
owner: "finance@yggdrasil.local"
policies:
  max_error_rate: 1.0
schema:
  - name: "unit_price"
    type: "DOUBLE"
    not_null: true
  - name: "quantity"
    type: "INTEGER"
    not_null: true
  - name: "discount"
    type: "DOUBLE"
    not_null: true
custom_rules:
  - name: "valid_discount"
    expression: "discount <= (unit_price * quantity)"
    error_code: "ERR_DISCOUNT_EXCEEDS_TOTAL"
""")
    # Row 1: Valid: 100 * 2 = 200 >= discount 20 -> Clean
    # Row 2: Invalid: 50 * 1 = 50 < discount 60 -> DLQ (ERR_DISCOUNT_EXCEEDS_TOTAL)
    source_query = """
    SELECT * FROM (
        VALUES 
            (100.0, 2, 20.0),
            (50.0, 1, 60.0)
    ) AS t(unit_price, quantity, discount)
    """

    clean_arrow, dlq_arrow = run(sql=source_query, contract=str(contract_file))

    assert clean_arrow.num_rows == 1
    assert dlq_arrow.num_rows == 1

    dlq_row = dlq_arrow.to_pylist()[0]
    assert dlq_row["unit_price"] == 50.0
    assert dlq_row["_error_code"] == "ERR_DISCOUNT_EXCEEDS_TOTAL"
    assert dlq_row["_error_column"] == "valid_discount"


def test_custom_rules_nested_list_aggregation(tmp_path):
    """Custom rules evaluate DuckDB list functions on nested structs."""
    contract_file = tmp_path / "nested_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
policies:
  max_error_rate: 1.0
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
  - name: "order_total"
    type: "DOUBLE"
    not_null: true
  - name: "line_items"
    type: "STRUCT(price DOUBLE)[]"
custom_rules:
  - name: "order_total_matches_items"
    expression: "order_total == list_sum(line_items.price)"
    error_code: "ERR_TOTAL_MISMATCH"
""")
    # DuckDB nested struct array
    source_query = """
    SELECT 1 AS order_id, 30.0 AS order_total, [{'price': 10.0}, {'price': 20.0}] AS line_items
    UNION ALL
    SELECT 2 AS order_id, 99.0 AS order_total, [{'price': 10.0}, {'price': 20.0}] AS line_items
    """

    clean_arrow, dlq_arrow = run(sql=source_query, contract=str(contract_file))

    # Order 1 matches 10 + 20 = 30 -> Clean
    # Order 2 has total 99 != 30 -> DLQ (ERR_TOTAL_MISMATCH)
    assert clean_arrow.num_rows == 1
    assert clean_arrow.column("order_id").to_pylist() == [1]

    assert dlq_arrow.num_rows == 1
    assert dlq_arrow.column("order_id").to_pylist() == [2]
    assert dlq_arrow.column("_error_code").to_pylist() == ["ERR_TOTAL_MISMATCH"]
    assert dlq_arrow.column("_error_column").to_pylist() == ["order_total_matches_items"]
