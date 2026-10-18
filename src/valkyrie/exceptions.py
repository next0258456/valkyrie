"""Valkyrie exception classes."""

from typing import Any, Dict, Optional


def format_breach_banner(
    dataset: str,
    owner: str,
    max_error_rate: float,
    error_rate: float,
    error_rows: int,
    total_rows: int,
    breach_reason: str,
    top_violations: Optional[list] = None,
) -> str:
    """Format standard ANSI-styled Valkyrie breach alert banner."""
    violations_str = "\n".join(top_violations) if top_violations else "  - None"
    return (
        f"\n{'=' * 80}\n"
        f"🔥 [VALKYRIE] DATA CONTRACT BREACH DETECTED!\n"
        f"Dataset     : {dataset}\n"
        f"Owner       : {owner}\n"
        f"Threshold   : max_error_rate = {max_error_rate:.2%}\n"
        f"Actual Error: {error_rate:.2%} ({error_rows:,} / {total_rows:,} rows)\n"
        f"Top Violations:\n{violations_str}\n"
        f"Reason      : {breach_reason}\n"
        f"Action Taken: Pipeline HALTED. DLQ preserved. Contact owner immediately!\n"
        f"{'=' * 80}"
    )


def extract_top_violations(dlq_arrow: Any, limit: int = 3) -> list:
    """Extract top error violation codes from DLQ Arrow table."""
    if dlq_arrow is None or getattr(dlq_arrow, "num_rows", 0) == 0:
        return []
    if hasattr(dlq_arrow, "column_names") and "_error_code" in dlq_arrow.column_names:
        import collections
        code_counts = collections.Counter(dlq_arrow.column("_error_code").to_pylist())
        return [f"  - [{code}]: {count:,} rows" for code, count in code_counts.most_common(limit)]
    return []


class SchemaConformanceException(Exception):
    """Exception raised when raw data schema does not conform to the data contract."""
    pass


class ContractBreachException(Exception):
    """Exception raised when a data batch breaches the contract threshold."""

    def __init__(
        self,
        message: str,
        metrics: Optional[Dict[str, Any]] = None,
        dlq_df: Optional[Any] = None,
        owner: Optional[str] = None,
    ):
        super().__init__(message)
        self.metrics = metrics or {}
        self.dlq_df = dlq_df
        self.owner = owner
