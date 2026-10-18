"""Contract schema definition and parser for Valkyrie."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import yaml

PROHIBITED_FLUFF_KEYS = {"sla", "team", "pricing", "description", "business_logic"}


@dataclass
class RangeConstraint:
    min: Optional[float] = None
    max: Optional[float] = None


@dataclass
class ColumnContract:
    name: str
    type: str = "VARCHAR"
    not_null: bool = False
    unique: bool = False
    range: Optional[RangeConstraint] = None
    enum: Optional[List[Any]] = None
    pattern: Optional[str] = None


@dataclass
class PolicyConfig:
    strict_schema: bool = True
    max_error_rate: float = 0.05
    quarantine_action: str = "DLQ"


@dataclass
class CustomRule:
    name: str
    expression: str
    error_code: Optional[str] = None


@dataclass
class ValkyrieContract:
    dataset: str
    owner: str
    version: str = "v1"
    policies: PolicyConfig = field(default_factory=PolicyConfig)
    schema_: List[ColumnContract] = field(default_factory=list)
    custom_rules: List[CustomRule] = field(default_factory=list)


def parse_range(r_val: Any) -> Optional[RangeConstraint]:
    """Parse range from [min, max] or {min: x, max: y} into RangeConstraint."""
    if r_val is None:
        return None
    if isinstance(r_val, (list, tuple)):
        if len(r_val) != 2:
            raise ValueError(f"Range list must contain exactly 2 numbers [min, max], got: {r_val}")
        return RangeConstraint(min=float(r_val[0]) if r_val[0] is not None else None, max=float(r_val[1]) if r_val[1] is not None else None)
    if isinstance(r_val, dict):
        min_v = float(r_val["min"]) if r_val.get("min") is not None else None
        max_v = float(r_val["max"]) if r_val.get("max") is not None else None
        return RangeConstraint(min=min_v, max=max_v)
    raise TypeError(f"Invalid range type: {type(r_val)}. Expected list [min, max] or dict {{min, max}}")


def load_contract(source: Union[str, Path, Dict[str, Any]]) -> ValkyrieContract:
    """Load and validate a Valkyrie YAML contract file or dictionary."""
    if isinstance(source, (str, Path)):
        with open(source, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    elif isinstance(source, dict):
        data = source
    else:
        raise TypeError(f"Invalid contract source type: {type(source)}")

    if not isinstance(data, dict):
        raise ValueError("Contract YAML must be a mapping dictionary")

    # Reject prohibited fluff
    for key in data:
        if key.lower() in PROHIBITED_FLUFF_KEYS:
            raise ValueError(f"Prohibited non-executable prose/fluff key '{key}' detected in contract.")

    dataset = str(data.get("dataset", "")).strip()
    owner = str(data.get("owner", "")).strip()
    if not dataset:
        raise ValueError("Contract must declare a non-empty 'dataset' name")
    if not owner:
        raise ValueError("Contract must declare a non-empty 'owner' for accountability")

    pol_data = data.get("policies") or {}
    quarantine_action = pol_data.get("quarantine_action", "DLQ")
    if quarantine_action not in ("DLQ", "FAIL_FAST"):
        raise ValueError(f"Invalid quarantine_action '{quarantine_action}'. Must be 'DLQ' or 'FAIL_FAST'")

    policies = PolicyConfig(
        strict_schema=bool(pol_data.get("strict_schema", True)),
        max_error_rate=float(pol_data.get("max_error_rate", 0.05)),
        quarantine_action=quarantine_action,
    )

    schema_cols = []
    for col_def in (data.get("schema") or []):
        name = col_def.get("name")
        if not name:
            raise ValueError("Each column in schema must have a 'name'")
        # Reject prose quality rules
        for q in (col_def.get("quality") or []):
            if str(q.get("type", "")).lower() in ("text", "description", "prose"):
                raise ValueError(f"Non-executable prose rule detected in column '{name}'.")

        schema_cols.append(
            ColumnContract(
                name=name,
                type=str(col_def.get("type", "VARCHAR")),
                not_null=bool(col_def.get("not_null", False)),
                unique=bool(col_def.get("unique", False)),
                range=parse_range(col_def.get("range")),
                enum=col_def.get("enum"),
                pattern=col_def.get("pattern"),
            )
        )

    custom_rules = [
        CustomRule(name=r.get("name", "custom_rule"), expression=r.get("expression", ""), error_code=r.get("error_code"))
        for r in (data.get("custom_rules") or [])
    ]

    return ValkyrieContract(
        dataset=dataset,
        owner=owner,
        version=str(data.get("version", "v1")),
        policies=policies,
        schema_=schema_cols,
        custom_rules=custom_rules,
    )
