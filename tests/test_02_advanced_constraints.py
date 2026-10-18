"""Test 02: Advanced Column Constraints (enum, pattern, unique)."""

import pytest
import pyarrow as pa
from valkyrie import run


def test_advanced_constraints_end_to_end(tmp_path):
    """Calling valkyrie.run validates enum, regex pattern, and duplicate keys via window function."""
    contract_file = tmp_path / "advanced_contract.yaml"
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
    unique: true
  - name: "status"
    type: "VARCHAR"
    not_null: true
    enum: ["PENDING", "PAID", "CANCELLED"]
  - name: "email"
    type: "VARCHAR"
    pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+$"
""")

    # Rows:
    # 1. Valid: (1, 'PAID', 'alice@example.com') -> Clean
    # 2. Duplicate order_id: (1, 'PENDING', 'bob@example.com') -> DLQ (ERR_DUPLICATE_KEY on order_id)
    # 3. Invalid status: (2, 'UNKNOWN', 'carol@example.com') -> DLQ (ERR_INVALID_ENUM on status)
    # 4. Invalid email regex: (3, 'PAID', 'bad-email-no-at') -> DLQ (ERR_REGEX_MISMATCH on email)
    # 5. Multi-violation: (1, 'INVALID_STATUS', 'bad_email') -> DLQ (First violation is ERR_DUPLICATE_KEY on order_id)
    source_query = """
    SELECT * FROM (
        VALUES 
            (1, 'PAID', 'alice@example.com'),
            (1, 'PENDING', 'bob@example.com'),
            (2, 'UNKNOWN', 'carol@example.com'),
            (3, 'PAID', 'bad-email-no-at'),
            (1, 'INVALID_STATUS', 'bad_email')
    ) AS t(order_id, status, email)
    """

    clean_arrow, dlq_arrow = run(sql=source_query, contract=str(contract_file))

    # Assert Clean: only alice would be clean EXCEPT that order_id 1 is duplicated!
    # Wait, order_id 1 appears 3 times in the batch, so ALL three rows with order_id=1 are duplicates!
    # And row 3 (order_id 2) has invalid status -> DLQ
    # And row 4 (order_id 3) has invalid email -> DLQ
    # Therefore, 0 clean rows, 5 DLQ rows!
    assert clean_arrow.num_rows == 0
    assert dlq_arrow.num_rows == 5

    dlq_rows = dlq_arrow.to_pylist()
    codes = [r["_error_code"] for r in dlq_rows]
    assert "ERR_DUPLICATE_KEY" in codes
    assert "ERR_INVALID_ENUM" in codes
    assert "ERR_REGEX_MISMATCH" in codes


def test_unique_preserves_distinct_clean_rows(tmp_path):
    """Distinct unique keys remain in clean table."""
    contract_file = tmp_path / "unique_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "users"
owner: "data@yggdrasil.local"
policies:
  max_error_rate: 0.5
schema:
  - name: "user_id"
    type: "BIGINT"
    unique: true
  - name: "status"
    type: "VARCHAR"
    enum: ["ACTIVE", "INACTIVE"]
""")
    source_query = """
    SELECT * FROM (
        VALUES 
            (10, 'ACTIVE'),
            (20, 'ACTIVE'),
            (30, 'INACTIVE'),
            (30, 'ACTIVE')
    ) AS t(user_id, status)
    """
    clean_arrow, dlq_arrow = run(sql=source_query, contract=str(contract_file))

    # user_id 10 and 20 are clean (2 rows)
    # user_id 30 is duplicated (2 rows in DLQ)
    assert clean_arrow.num_rows == 2
    assert clean_arrow.column("user_id").to_pylist() == [10, 20]

    assert dlq_arrow.num_rows == 2
    for r in dlq_arrow.to_pylist():
        assert r["user_id"] == 30
        assert r["_error_code"] == "ERR_DUPLICATE_KEY"
        assert r["_error_column"] == "user_id"
