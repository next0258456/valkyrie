"""Test 05: Zero-Boilerplate Storage & Persistence (valkyrie.execute)."""

import os
import pytest
import pyarrow.parquet as pq
from valkyrie import execute, ContractBreachException


def test_valkyrie_execute_file_persistence(tmp_path):
    """Calling valkyrie.execute persists clean and dlq Parquet datasets directly to paths."""
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
    clean_path = tmp_path / "clean.parquet"
    dlq_path = tmp_path / "dlq.parquet"

    source_query = """
    SELECT * FROM (VALUES (1), (NULL), (3)) AS t(order_id)
    """

    result = execute(
        sql=source_query,
        contract=str(contract_file),
        write_clean=str(clean_path),
        write_dlq=str(dlq_path),
    )

    # Verify return object
    assert result.is_passed is True

    # Verify clean.parquet exists and has 2 rows
    assert clean_path.exists()
    clean_tbl = pq.read_table(clean_path)
    assert clean_tbl.num_rows == 2
    assert clean_tbl.column("order_id").to_pylist() == [1, 3]

    # Verify dlq.parquet exists and has 1 row
    assert dlq_path.exists()
    dlq_tbl = pq.read_table(dlq_path)
    assert dlq_tbl.num_rows == 1
    assert dlq_tbl.column("_error_code").to_pylist() == ["ERR_NULL_VIOLATION"]


def test_valkyrie_execute_persists_dlq_on_breach(tmp_path):
    """Even when circuit breaker trips, DLQ is persisted to write_dlq for audit before raising."""
    contract_file = tmp_path / "strict_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
policies:
  max_error_rate: 0.01  # 1%
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
""")
    clean_path = tmp_path / "clean_breach.parquet"
    dlq_path = tmp_path / "dlq_breach.parquet"

    source_query = "SELECT NULL AS order_id"

    with pytest.raises(ContractBreachException):
        execute(
            sql=source_query,
            contract=str(contract_file),
            write_clean=str(clean_path),
            write_dlq=str(dlq_path),
        )

    # Clean file was NOT written, but DLQ file WAS safely preserved!
    assert not clean_path.exists()
    assert dlq_path.exists()
    dlq_tbl = pq.read_table(dlq_path)
    assert dlq_tbl.num_rows == 1


def test_s3_secret_configuration_helper(monkeypatch):
    """Auto-detects S3 environment variables and builds secret config."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test_secret")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_USE_SSL", "false")
    monkeypatch.setenv("S3_URL_STYLE", "path")

    from valkyrie.engine import configure_cloud_storage
    import duckdb

    con = duckdb.connect()
    # Should configure secret without error
    configure_cloud_storage(con, "s3://my-bucket/orders.parquet")
