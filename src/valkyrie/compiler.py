"""Valkyrie SQL compiler: compiles YAML contracts to DuckDB SQL CTEs."""

import re
from typing import List, Tuple
from valkyrie.contract import ValkyrieContract


def build_case_when(branches: List[Tuple[str, str]], default: str = "NULL") -> str:
    """Build a SQL CASE WHEN statement from a list of (condition, value_expr) pairs."""
    if not branches:
        return default
    lines = ["CASE"]
    for cond, val in branches:
        lines.append(f"            WHEN {cond} THEN {val}")
    lines.append(f"            ELSE {default}\n        END")
    return "\n".join(lines)


def compile_to_sql(contract: ValkyrieContract, source_sql: str) -> str:
    """Compile contract validation rules into a single-pass DuckDB SQL query."""
    clean_source = source_sql.strip().rstrip(";")

    # Collect rule tuples: (condition, error_code, error_column, raw_value_expr)
    rules: List[Tuple[str, str, str, str]] = []

    for col in contract.schema_:
        # 1. NOT NULL constraint
        if col.not_null:
            cond = f'"{col.name}" IS NULL'
            code = "ERR_NULL_VIOLATION"
            rules.append((cond, code, col.name, f'CAST("{col.name}" AS VARCHAR)'))

        # 2. UNIQUE constraint (via DuckDB Window Function)
        if col.unique:
            cond = f'count(*) OVER (PARTITION BY "{col.name}") > 1'
            code = "ERR_DUPLICATE_KEY"
            rules.append((cond, code, col.name, f'CAST("{col.name}" AS VARCHAR)'))

        # 3. RANGE constraint
        if col.range:
            parts = []
            if col.range.min is not None:
                parts.append(f'"{col.name}" < {col.range.min}')
            if col.range.max is not None:
                parts.append(f'"{col.name}" > {col.range.max}')
            if parts:
                cond = " OR ".join(parts)
                code = "ERR_OUT_OF_RANGE"
                rules.append((cond, code, col.name, f'CAST("{col.name}" AS VARCHAR)'))

        # 4. ENUM whitelist constraint
        if col.enum:
            enum_items = []
            for item in col.enum:
                if isinstance(item, str):
                    escaped_item = item.replace("'", "''")
                    enum_items.append(f"'{escaped_item}'")
                else:
                    enum_items.append(str(item))
            enum_list_str = ", ".join(enum_items)
            cond = f'"{col.name}" NOT IN ({enum_list_str})'
            code = "ERR_INVALID_ENUM"
            rules.append((cond, code, col.name, f'CAST("{col.name}" AS VARCHAR)'))

        # 5. REGEX PATTERN constraint
        if col.pattern:
            escaped_pattern = col.pattern.replace("'", "''")
            cond = f"NOT regexp_matches(CAST(\"{col.name}\" AS VARCHAR), '{escaped_pattern}')"
            code = "ERR_REGEX_MISMATCH"
            rules.append((cond, code, col.name, f'CAST("{col.name}" AS VARCHAR)'))

    # 6. CUSTOM RULES (Cross-column & Nested List expressions)
    for rule in contract.custom_rules:
        expr_clean = rule.expression.strip()
        # Automatically rewrite shorthand list_sum(col.field) to DuckDB lambda list_sum(list_transform(col, x -> x.field))
        expr_clean = re.sub(
            r'(list_[a-zA-Z0-9_]+)\(\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*\)',
            r'\1(list_transform(\2, x -> x.\3))',
            expr_clean
        )
        cond = f"(NOT ({expr_clean}) OR ({expr_clean}) IS NULL)"
        code = rule.error_code or "ERR_CUSTOM_RULE_FAILED"
        rules.append((cond, code, rule.name, f"'{rule.name}'"))

    # If no rules declared, provide a dummy always-false condition
    if not rules:
        code_cases = "NULL"
        col_cases = "NULL"
        raw_cases = "NULL"
    else:
        code_cases = build_case_when([(cond, f"'{code}'") for cond, code, _, _ in rules])
        col_cases = build_case_when([(cond, f"'{col_name}'") for cond, _, col_name, _ in rules])
        raw_cases = build_case_when([(cond, raw_expr) for cond, _, _, raw_expr in rules])

    compiled_sql = f"""WITH raw AS (
    {clean_source}
),
annotated AS (
    SELECT
        *,
        {code_cases} AS _error_code,
        {col_cases} AS _error_column,
        {raw_cases} AS _raw_value,
        CURRENT_TIMESTAMP AS _ingestion_timestamp
    FROM raw
)
SELECT * FROM annotated"""

    return compiled_sql
