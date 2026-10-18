"""Valkyrie exception classes."""

from typing import Any, Dict, Optional


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
