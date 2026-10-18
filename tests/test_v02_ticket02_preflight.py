"""Test v0.2 Ticket 02: Pre-Flight Schema & Type Conformance Check."""

import pytest
from valkyrie import run
from valkyrie.exceptions import SchemaConformanceException


def test_missing_mandatory_column_raises_preflight(tmp_path):
    """Missing not_null column in query schema raises SchemaConformanceException."""
    contract_file = tmp_path / "contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
  - name: "order_total"
    type: "DOUBLE"
    not_null: true
""")
    # Query only provides order_id, missing mandatory order_total!
    source_query = "SELECT 1 AS order_id"

    with pytest.raises(SchemaConformanceException, match="Missing mandatory column.*order_total"):
        run(sql=source_query, contract=str(contract_file))


def test_strict_schema_unexpected_columns(tmp_path):
    """Extra unexpected columns raise SchemaConformanceException when strict_schema is True."""
    contract_file = tmp_path / "strict_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
policies:
  strict_schema: true
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
""")
    # Query provides unexpected extra column 'rogue_col'
    source_query = "SELECT 1 AS order_id, 'malicious' AS rogue_col"

    with pytest.raises(SchemaConformanceException, match="Unexpected columns found under strict_schema"):
        run(sql=source_query, contract=str(contract_file))


def test_lenient_schema_allows_unexpected_columns(tmp_path):
    """Extra columns are permitted when strict_schema is False."""
    contract_file = tmp_path / "lenient_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
policies:
  strict_schema: false
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
""")
    source_query = "SELECT 1 AS order_id, 'ok_extra' AS extra_col"
    clean_arrow, dlq_arrow = run(sql=source_query, contract=str(contract_file))

    assert clean_arrow.num_rows == 1
    assert "extra_col" in clean_arrow.column_names
