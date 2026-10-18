"""Contract schema definition and parser for Valkyrie."""

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, ConfigDict, Field, model_validator
import yaml
from pathlib import Path


class RangeConstraint(BaseModel):
    min: Optional[float] = None
    max: Optional[float] = None


class ColumnContract(BaseModel):
    name: str
    type: str = "VARCHAR"
    not_null: bool = False
    unique: bool = False
    range: Optional[RangeConstraint] = None
    enum: Optional[List[Any]] = None
    pattern: Optional[str] = None
    quality: Optional[List[Dict[str, Any]]] = None

    @model_validator(mode="after")
    def reject_prose_rules(self) -> "ColumnContract":
        if self.quality:
            for rule in self.quality:
                rule_type = rule.get("type", "").lower()
                if rule_type in ("text", "description", "prose"):
                    raise ValueError(
                        f"Non-executable prose rule detected in column '{self.name}'. "
                        "Valkyrie only accepts machine-verifiable rules."
                    )
        return self


class PolicyConfig(BaseModel):
    strict_schema: bool = True
    max_error_rate: float = 0.05
    quarantine_action: str = "DLQ"  # "DLQ" or "FAIL_FAST"


class CustomRule(BaseModel):
    name: str
    expression: str
    error_code: Optional[str] = None


class ValkyrieContract(BaseModel):
    version: str = "v1"
    dataset: str = Field(..., min_length=1, description="Dataset name is required")
    owner: str = Field(..., min_length=1, description="Contract owner is required for accountability")
    policies: PolicyConfig = Field(default_factory=PolicyConfig)
    schema_: List[ColumnContract] = Field(default_factory=list, alias="schema")
    custom_rules: List[CustomRule] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)


def load_contract(source: Union[str, Path, Dict[str, Any]]) -> ValkyrieContract:
    """Load and validate a Valkyrie YAML contract file or dictionary."""
    if isinstance(source, (str, Path)):
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Contract file not found: {source}")
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    elif isinstance(source, dict):
        data = source
    else:
        raise TypeError(f"Invalid contract source type: {type(source)}")

    if not isinstance(data, dict):
        raise ValueError("Contract YAML must be a mapping dictionary")

    return ValkyrieContract.model_validate(data)
