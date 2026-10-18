"""Test 04: Circuit Breaker & Actionable Breach Alerting."""

import pytest
import pyarrow as pa
from valkyrie import run, ContractBreachException


def test_circuit_breaker_max_error_rate(tmp_path):
    """Exceeding max_error_rate raises ContractBreachException with formatted alert banner."""
    contract_file = tmp_path / "strict_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance-team@yggdrasil.local"
policies:
  max_error_rate: 0.10  # 10%
  quarantine_action: "DLQ"
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
""")
    # 4 rows: 2 nulls (50% error rate > 10%)
    source_query = """
    SELECT * FROM (VALUES (1), (NULL), (3), (NULL)) AS t(order_id)
    """

    with pytest.raises(ContractBreachException) as exc_info:
        run(sql=source_query, contract=str(contract_file))

    e = exc_info.value
    # Check alert banner contents
    msg = str(e)
    assert "DATA CONTRACT BREACH DETECTED" in msg
    assert "finance-team@yggdrasil.local" in msg
    assert "orders" in msg
    assert "50.00%" in msg
    assert "10.00%" in msg
    assert "ERR_NULL_VIOLATION" in msg

    # Check preserved attributes
    assert e.metrics["total_rows"] == 4
    assert e.metrics["error_rows"] == 2
    assert e.metrics["error_rate"] == 0.5
    assert e.dlq_df is not None
    assert e.dlq_df.num_rows == 2


def test_quarantine_action_fail_fast(tmp_path):
    """FAIL_FAST trips on any invalid row even if error rate is below max_error_rate."""
    contract_file = tmp_path / "fail_fast_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "platform@yggdrasil.local"
policies:
  max_error_rate: 0.90
  quarantine_action: "FAIL_FAST"
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
""")
    # 10 rows: 1 null (10% < 90%), but quarantine_action is FAIL_FAST!
    source_query = """
    SELECT * FROM (
        VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)
    ) AS t(order_id)
    """
    with pytest.raises(ContractBreachException) as exc_info:
        run(sql=source_query, contract=str(contract_file))

    assert "FAIL_FAST" in str(exc_info.value)
    assert exc_info.value.metrics["error_rows"] == 1


def test_raise_on_breach_false(tmp_path):
    """Passing raise_on_breach=False suppresses exception and returns failed result."""
    contract_file = tmp_path / "soft_contract.yaml"
    contract_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
policies:
  max_error_rate: 0.05
schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
""")
    source_query = "SELECT NULL AS order_id"
    result = run(sql=source_query, contract=str(contract_file), raise_on_breach=False)

    assert result.is_passed is False
    assert result.owner == "finance@yggdrasil.local"
    assert result.dlq_arrow.num_rows == 1
    assert result.clean_arrow.num_rows == 0
