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
