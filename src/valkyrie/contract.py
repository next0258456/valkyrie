"""Contract schema definition and parser for Valkyrie using Python standard library dataclasses."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union
import yaml


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
    quality: Optional[List[Dict[str, Any]]] = None


@dataclass
class PolicyConfig:
    strict_schema: bool = True
    max_error_rate: float = 0.05
    quarantine_action: Literal["DLQ", "FAIL_FAST"] = "DLQ"


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
    """Parse range constraint from either list/tuple [min, max] or dict {min, max}."""
    if r_val is None:
        return None
    if isinstance(r_val, (list, tuple)):
        if len(r_val) != 2:
            raise ValueError(f"Range list must contain exactly 2 numbers [min, max], got: {r_val}")
        return RangeConstraint(min=float(r_val[0]), max=float(r_val[1]))
    if isinstance(r_val, dict):
        min_v = float(r_val["min"]) if r_val.get("min") is not None else None
        max_v = float(r_val["max"]) if r_val.get("max") is not None else None
        return RangeConstraint(min=min_v, max=max_v)
    raise TypeError(f"Invalid range type: {type(r_val)}. Expected list [min, max] or dict {{min, max}}")


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

    # Validate dataset
    dataset = data.get("dataset")
    if not dataset or not str(dataset).strip():
        raise ValueError("Contract must declare a non-empty 'dataset' name")

    # Validate owner
    owner = data.get("owner")
    if not owner or not str(owner).strip():
        raise ValueError("Contract must declare a non-empty 'owner' for accountability")

    version = data.get("version", "v1")

    # Policies
    pol_data = data.get("policies") or {}
    quarantine_action = pol_data.get("quarantine_action", "DLQ")
    if quarantine_action not in ("DLQ", "FAIL_FAST"):
        raise ValueError(
            f"Invalid quarantine_action '{quarantine_action}'. Must be either 'DLQ' or 'FAIL_FAST'"
        )

    policies = PolicyConfig(
        strict_schema=bool(pol_data.get("strict_schema", True)),
        max_error_rate=float(pol_data.get("max_error_rate", 0.05)),
        quarantine_action=quarantine_action,
    )

    # Columns
    schema_cols: List[ColumnContract] = []
    raw_schema = data.get("schema") or []
    for col_def in raw_schema:
        col_name = col_def.get("name")
        if not col_name:
            raise ValueError("Each column in schema must have a 'name'")

        quality_rules = col_def.get("quality")
        if quality_rules:
            for rule in quality_rules:
                rule_type = rule.get("type", "").lower()
                if rule_type in ("text", "description", "prose"):
                    raise ValueError(
                        f"Non-executable prose rule detected in column '{col_name}'. "
                        "Valkyrie only accepts machine-verifiable rules."
                    )

        range_constraint = parse_range(col_def.get("range"))

        col = ColumnContract(
            name=col_name,
            type=str(col_def.get("type", "VARCHAR")),
            not_null=bool(col_def.get("not_null", False)),
            unique=bool(col_def.get("unique", False)),
            range=range_constraint,
            enum=col_def.get("enum"),
            pattern=col_def.get("pattern"),
            quality=quality_rules,
        )
        schema_cols.append(col)

    # Custom rules
    custom_rules: List[CustomRule] = []
    raw_rules = data.get("custom_rules") or []
    for r_def in raw_rules:
        r_name = r_def.get("name", "custom_rule")
        r_expr = r_def.get("expression", "")
        r_code = r_def.get("error_code")
        custom_rules.append(CustomRule(name=r_name, expression=r_expr, error_code=r_code))

    return ValkyrieContract(
        dataset=str(dataset),
        owner=str(owner),
        version=str(version),
        policies=policies,
        schema_=schema_cols,
        custom_rules=custom_rules,
    )
