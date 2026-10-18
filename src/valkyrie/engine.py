"""Valkyrie runtime engine powered by DuckDB."""

from typing import Any, Dict, Iterator, Optional, Union
from pathlib import Path
import time
import duckdb
import pyarrow as pa

from valkyrie.contract import ValkyrieContract, load_contract
from valkyrie.compiler import compile_to_sql
from valkyrie.exceptions import (
    ContractBreachException,
    SchemaConformanceException,
    format_breach_banner,
)


class ValidationResult:
    """Validation outcome holding clean Arrow data, DLQ Arrow data, and metrics."""

    def __init__(
        self,
        clean_arrow: pa.Table,
        dlq_arrow: pa.Table,
        metrics: Dict[str, Any],
        owner: str,
        is_passed: bool,
    ):
        self.clean_arrow = clean_arrow
        self.dlq_arrow = dlq_arrow
        self.metrics = metrics
        self.owner = owner
        self.is_passed = is_passed

    def __iter__(self) -> Iterator[pa.Table]:
        """Support unpacking like a tuple: clean_arrow, dlq_arrow = result."""
        yield self.clean_arrow
        yield self.dlq_arrow

    @property
    def clean_df(self):
        try:
            import polars as pl
            return pl.from_arrow(self.clean_arrow)
        except ImportError:
            return self.clean_arrow.to_pandas()

    @property
    def dlq_df(self):
        try:
            import polars as pl
            return pl.from_arrow(self.dlq_arrow)
        except ImportError:
            return self.dlq_arrow.to_pandas()


def to_sql(contract: Union[str, Path, ValkyrieContract], source_sql: str) -> str:
    """Compile a contract and source SQL query to DuckDB SQL without executing."""
    if not isinstance(contract, ValkyrieContract):
        contract = load_contract(contract)
    return compile_to_sql(contract, source_sql)


def validate_preflight_schema(
    con: duckdb.DuckDBPyConnection,
    source_sql: str,
    contract: ValkyrieContract,
) -> None:
    """Inspect query schema metadata prior to query execution."""
    clean_sql = source_sql.strip().rstrip(";")
    rel = con.sql(clean_sql)
    source_cols = {col.lower(): col for col in rel.columns}
    source_types = {col.lower(): str(t).upper() for col, t in zip(rel.columns, rel.types)}

    # 1. Missing contracted columns
    missing_cols = [c.name for c in contract.schema_ if c.name.lower() not in source_cols]
    if missing_cols:
        raise SchemaConformanceException(f"[ERR_SCHEMA_MISSING_COLUMN] Missing mandatory column(s): {missing_cols}")

    # 2. Extra columns under strict_schema
    if contract.policies.strict_schema:
        contract_keys = {c.name.lower() for c in contract.schema_}
        extra_cols = [orig for low, orig in source_cols.items() if low not in contract_keys]
        if extra_cols:
            raise SchemaConformanceException(
                f"[ERR_SCHEMA_EXTRA_COLUMN] Unexpected columns found under strict_schema: {extra_cols}"
            )

    # 3. Physical type compatibility
    type_categories = {
        "BIGINT": "NUMERIC", "INTEGER": "NUMERIC", "INT": "NUMERIC", "SMALLINT": "NUMERIC", "TINYINT": "NUMERIC",
        "DOUBLE": "NUMERIC", "FLOAT": "NUMERIC", "REAL": "NUMERIC", "DECIMAL": "NUMERIC", "NUMERIC": "NUMERIC",
        "VARCHAR": "STRING", "TEXT": "STRING", "STRING": "STRING",
        "BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN",
        "DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "TIME": "TIME",
    }
    for col in contract.schema_:
        col_type = col.type.split("(")[0].strip().upper()
        col_cat = type_categories.get(col_type)
        actual_raw_type = source_types.get(col.name.lower(), "")
        actual_type = actual_raw_type.split("(")[0].strip().upper()
        actual_cat = type_categories.get(actual_type)

        if col_cat and actual_cat and col_cat != actual_cat:
            raise SchemaConformanceException(
                f"[ERR_SCHEMA_TYPE_MISMATCH] Column '{col.name}' declared as {col.type} but source query produces incompatible type {actual_raw_type}"
            )


def configure_cloud_storage(con: duckdb.DuckDBPyConnection, *targets: str) -> None:
    """Auto-detect S3/MinIO paths or queries and configure DuckDB S3 secret from environment."""
    import os
    needs_s3 = any("s3://" in t.lower() or "minio://" in t.lower() for t in targets if t)
    if not needs_s3:
        return

    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception:
        pass

    key_id = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    endpoint = os.getenv("AWS_ENDPOINT_URL") or os.getenv("S3_ENDPOINT") or ""
    use_ssl = os.getenv("S3_USE_SSL", os.getenv("AWS_USE_SSL", "true")).lower() == "true"
    url_style = os.getenv("S3_URL_STYLE", "vhost")

    if key_id and secret:
        params = ["TYPE S3", f"KEY_ID '{key_id}'", f"SECRET '{secret}'", f"REGION '{region}'"]
        if endpoint:
            clean_endpoint = endpoint.replace("http://", "").replace("https://", "").rstrip("/")
            params.append(f"ENDPOINT '{clean_endpoint}'")
        params.append(f"USE_SSL {'true' if use_ssl else 'false'}")
        params.append(f"URL_STYLE '{url_style}'")
        con.execute(f"CREATE OR REPLACE SECRET s3_valkyrie ({', '.join(params)})")


def _format_target_path(path: str) -> str:
    """Ensure path format is valid for DuckDB COPY TO (forward slashes on Windows)."""
    is_cloud = any(path.startswith(prefix) for prefix in ("s3://", "minio://", "http://", "https://"))
    if not is_cloud:
        p = Path(path).resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        return str(p).replace("\\", "/")
    return path


def run(
    sql: str,
    contract: Union[str, Path, ValkyrieContract],
    write_clean: Optional[str] = None,
    write_dlq: Optional[str] = None,
    raise_on_breach: bool = True,
    connection: Optional[duckdb.DuckDBPyConnection] = None,
) -> ValidationResult:
    """Execute single-pass validation on a source SQL query against a Valkyrie contract."""
    if not isinstance(contract, ValkyrieContract):
        contract = load_contract(contract)

    con = connection or duckdb.connect()
    configure_cloud_storage(con, sql, write_clean or "", write_dlq or "")

    validate_preflight_schema(con, sql, contract)
    annotated_sql = compile_to_sql(contract, sql)

    start_time = time.perf_counter()
    try:
        con.execute(f"CREATE TEMPORARY TABLE _valkyrie_annotated AS {annotated_sql}")

        # Compute metrics directly in DuckDB SQL
        metrics_row = con.execute(
            "SELECT count(*), count(_error_code) FROM _valkyrie_annotated"
        ).fetchone()
        total_rows = metrics_row[0]
        error_rows = metrics_row[1]
        clean_rows = total_rows - error_rows
        error_rate = (error_rows / total_rows) if total_rows > 0 else 0.0

        # Check circuit breaker
        is_breached = False
        breach_reason = ""
        if contract.policies.quarantine_action.upper() == "FAIL_FAST" and error_rows > 0:
            is_breached = True
            breach_reason = f"Quarantine policy is FAIL_FAST and {error_rows} invalid row(s) detected."
        elif error_rate > contract.policies.max_error_rate:
            is_breached = True
            breach_reason = f"Error rate {error_rate:.2%} exceeded threshold {contract.policies.max_error_rate:.2%}."

        # Fetch top violations directly in DuckDB SQL if needed
        top_violations = []
        if error_rows > 0:
            top_rows = con.execute(
                "SELECT _error_code, count(*) FROM _valkyrie_annotated "
                "WHERE _error_code IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3"
            ).fetchall()
            top_violations = [f"  - [{code}]: {count:,} rows" for code, count in top_rows]

        # Direct DuckDB native COPY TO for DLQ
        if write_dlq and error_rows > 0:
            dest_dlq = _format_target_path(write_dlq)
            con.execute(
                f"COPY (SELECT * FROM _valkyrie_annotated WHERE _error_code IS NOT NULL) TO '{dest_dlq}' (FORMAT PARQUET)"
            )

        # Handle breach
        if is_breached and raise_on_breach:
            dlq_arrow = con.execute("SELECT * FROM _valkyrie_annotated WHERE _error_code IS NOT NULL").to_arrow_table()
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
            metrics = {
                "dataset": contract.dataset,
                "owner": contract.owner,
                "total_rows": total_rows,
                "clean_rows": clean_rows,
                "error_rows": error_rows,
                "error_rate": error_rate,
                "duration_ms": duration_ms,
                "max_error_rate": contract.policies.max_error_rate,
                "quarantine_action": contract.policies.quarantine_action,
                "breach_reason": breach_reason,
            }
            msg = format_breach_banner(
                dataset=contract.dataset,
                owner=contract.owner,
                max_error_rate=contract.policies.max_error_rate,
                error_rate=error_rate,
                error_rows=error_rows,
                total_rows=total_rows,
                breach_reason=breach_reason,
                top_violations=top_violations,
            )
            raise ContractBreachException(message=msg, metrics=metrics, dlq_df=dlq_arrow, owner=contract.owner)

        # Direct DuckDB native COPY TO for Clean data if passed
        if write_clean and not is_breached:
            dest_clean = _format_target_path(write_clean)
            con.execute(
                "COPY (SELECT * EXCLUDE (_error_code, _error_column, _raw_value, _ingestion_timestamp) "
                f"FROM _valkyrie_annotated WHERE _error_code IS NULL) TO '{dest_clean}' (FORMAT PARQUET)"
            )

        clean_arrow = con.execute(
            "SELECT * EXCLUDE (_error_code, _error_column, _raw_value, _ingestion_timestamp) "
            "FROM _valkyrie_annotated WHERE _error_code IS NULL"
        ).to_arrow_table()
        dlq_arrow = con.execute("SELECT * FROM _valkyrie_annotated WHERE _error_code IS NOT NULL").to_arrow_table()

    finally:
        con.execute("DROP TABLE IF EXISTS _valkyrie_annotated")

    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
    metrics = {
        "dataset": contract.dataset,
        "owner": contract.owner,
        "total_rows": total_rows,
        "clean_rows": clean_rows,
        "error_rows": error_rows,
        "error_rate": error_rate,
        "duration_ms": duration_ms,
        "max_error_rate": contract.policies.max_error_rate,
        "quarantine_action": contract.policies.quarantine_action,
        "breach_reason": breach_reason if is_breached else "",
    }

    return ValidationResult(
        clean_arrow=clean_arrow,
        dlq_arrow=dlq_arrow,
        metrics=metrics,
        owner=contract.owner,
        is_passed=not is_breached,
    )


def execute(
    sql: str,
    contract: Union[str, Path, ValkyrieContract],
    write_clean: Optional[str] = None,
    write_dlq: Optional[str] = None,
    connection: Optional[duckdb.DuckDBPyConnection] = None,
) -> ValidationResult:
    """Execute validation and safely persist outputs directly using DuckDB COPY."""
    return run(
        sql=sql,
        contract=contract,
        write_clean=write_clean,
        write_dlq=write_dlq,
        raise_on_breach=True,
        connection=connection,
    )
