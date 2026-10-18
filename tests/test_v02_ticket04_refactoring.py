"""Test v0.2 Ticket 04: Anti-Smell Refactoring & Alert Banner Formatting."""

import pytest
import pyarrow as pa
from valkyrie.compiler import build_case_when
from valkyrie.exceptions import format_breach_banner, extract_top_violations


def test_build_case_when_helper():
    """Verify build_case_when correctly formats SQL CASE statements."""
    # Empty branches returns default
    assert build_case_when([]) == "NULL"
    assert build_case_when([], default="0") == "0"

    # Multi branches
    branches = [
        ('"price" < 0', "'ERR_NEGATIVE'"),
        ('"price" > 1000', "'ERR_TOO_EXPENSIVE'"),
    ]
    sql = build_case_when(branches)
    assert "CASE" in sql
    assert 'WHEN "price" < 0 THEN \'ERR_NEGATIVE\'' in sql
    assert 'WHEN "price" > 1000 THEN \'ERR_TOO_EXPENSIVE\'' in sql
    assert "ELSE NULL" in sql
    assert "END" in sql


def test_format_breach_banner_and_top_violations():
    """Verify format_breach_banner and extract_top_violations helpers."""
    table = pa.Table.from_arrays(
        [
            pa.array(["ERR_NULL", "ERR_NULL", "ERR_RANGE"]),
            pa.array([1, 2, 3]),
        ],
        names=["_error_code", "id"],
    )

    top_v = extract_top_violations(table, limit=2)
    assert len(top_v) == 2
    assert "[ERR_NULL]: 2 rows" in top_v[0]
    assert "[ERR_RANGE]: 1 rows" in top_v[1]

    banner = format_breach_banner(
        dataset="orders",
        owner="team@valkyrie.local",
        max_error_rate=0.05,
        error_rate=0.15,
        error_rows=3,
        total_rows=10,
        breach_reason="Error rate 15.00% exceeded threshold 5.00%.",
        top_violations=top_v,
    )

    assert "DATA CONTRACT BREACH DETECTED" in banner
    assert "orders" in banner
    assert "team@valkyrie.local" in banner
    assert "15.00%" in banner
    assert "5.00%" in banner
    assert "[ERR_NULL]: 2 rows" in banner
    assert "Pipeline HALTED" in banner


def test_zero_pydantic_in_source_and_dependencies():
    """Ensure pydantic is not imported in src/ or listed in pyproject.toml."""
    from pathlib import Path
    root = Path(__file__).parent.parent
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    assert "pydantic" not in pyproject.lower()

    # Search all python files in src/
    for py_file in (root / "src").rglob("*.py"):
        content = py_file.read_text(encoding="utf-8")
        assert "pydantic" not in content.lower(), f"Pydantic found in {py_file}"
