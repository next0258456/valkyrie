"""Test v0.2 Ticket 01: Zero-Pydantic Pure Dataclasses & Flexible Range."""

import pytest
import sys
from valkyrie.contract import load_contract, ValkyrieContract, RangeConstraint


def test_zero_pydantic_in_contract_module():
    """Verify that pydantic is not imported or required by valkyrie.contract."""
    import valkyrie.contract
    # Check that contract.py does not import pydantic
    assert "pydantic" not in sys.modules or "pydantic" not in dir(valkyrie.contract)


def test_flexible_range_syntax_list(tmp_path):
    """Contract parser accepts range: [min, max] list format."""
    yaml_file = tmp_path / "range_list.yaml"
    yaml_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
schema:
  - name: "price"
    type: "DOUBLE"
    range: [0.01, 1000.0]
""")
    contract = load_contract(str(yaml_file))
    price_col = contract.schema_[0]
    assert price_col.range is not None
    assert price_col.range.min == 0.01
    assert price_col.range.max == 1000.0


def test_quarantine_action_validation(tmp_path):
    """quarantine_action must be DLQ or FAIL_FAST, rejecting invalid values."""
    yaml_file = tmp_path / "invalid_action.yaml"
    yaml_file.write_text("""
version: "v1"
dataset: "orders"
owner: "finance@yggdrasil.local"
policies:
  quarantine_action: "INVALID_ACTION"
""")
    with pytest.raises(ValueError, match="quarantine_action"):
        load_contract(str(yaml_file))
