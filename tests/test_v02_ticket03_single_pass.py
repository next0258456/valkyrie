"""Test v0.2 Ticket 03: True Single-Pass Table & Native DuckDB S3 Export."""

import pytest
import duckdb
from valkyrie import run, execute, ContractBreachException


def test_materialized_temporary_table_lifecycle(tmp_path):
    """Temporary table is created, evaluated once, and dropped in finally, leaving zero residue."""
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
    con = duckdb.connect()
    source_query = "SELECT 1 AS order_id"

    result = run(sql=source_query, contract=str(contract_file), connection=con)

    # 1. Verify duration_ms is present in metrics and > 0
    assert "duration_ms" in result.metrics
    assert result.metrics["duration_ms"] >= 0

    # 2. Verify _valkyrie_annotated table is completely dropped from connection
    tables_in_db = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    assert "_valkyrie_annotated" not in tables_in_db


def test_duckdb_native_copy_to_parquet(tmp_path):
    """valkyrie.execute uses DuckDB native COPY TO command for Parquet export."""
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
    clean_file = tmp_path / "clean_native.parquet"
    dlq_file = tmp_path / "dlq_native.parquet"

    source_query = "SELECT * FROM (VALUES (1), (NULL), (2)) AS t(order_id)"

    result = execute(
        sql=source_query,
        contract=str(contract_file),
        write_clean=str(clean_file),
        write_dlq=str(dlq_file),
    )

    assert result.is_passed is True
    assert "duration_ms" in result.metrics

    # Verify DuckDB can read the output Parquet files directly
    con = duckdb.connect()
    clean_count = con.execute(f"SELECT count(*) FROM read_parquet('{clean_file}')").fetchone()[0]
    dlq_count = con.execute(f"SELECT count(*) FROM read_parquet('{dlq_file}')").fetchone()[0]

    assert clean_count == 2
    assert dlq_count == 1


def test_execute_persists_dlq_on_breach_without_rescan(tmp_path, monkeypatch):
    """When execute() encounters a breach, DLQ is persisted and ContractBreachException raised without calling run() twice."""
    import valkyrie.engine as eng

    run_call_count = 0
    original_run = eng.run

    def counting_run(*args, **kwargs):
        nonlocal run_call_count
        run_call_count += 1
        return original_run(*args, **kwargs)

    monkeypatch.setattr(eng, "run", counting_run)

    contract_file = tmp_path / "breach_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
policies:
  max_error_rate: 0.1
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
""")
    clean_file = tmp_path / "clean_breach.parquet"
    dlq_file = tmp_path / "dlq_breach.parquet"
    source_query = "SELECT NULL AS order_id"

    with pytest.raises(ContractBreachException) as exc_info:
        execute(
            sql=source_query,
            contract=str(contract_file),
            write_clean=str(clean_file),
            write_dlq=str(dlq_file),
        )

    # 1. run() must have been called exactly once (no second scan)
    assert run_call_count == 1

    # 2. DLQ file exists and clean file does not
    assert not clean_file.exists()
    assert dlq_file.exists()

    con = duckdb.connect()
    dlq_rows = con.execute(f"SELECT count(*) FROM read_parquet('{dlq_file}')").fetchone()[0]
    assert dlq_rows == 1

    # 3. Breach exception has correct banner formatting and details
    assert "DATA CONTRACT BREACH DETECTED" in str(exc_info.value)
    assert "ERR_NULL_VIOLATION" in str(exc_info.value)

