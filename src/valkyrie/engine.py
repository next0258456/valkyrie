"""Valkyrie runtime engine powered by DuckDB."""

from typing import Any, Dict, Iterator, Optional, Tuple, Union
from pathlib import Path
import duckdb
import pyarrow as pa

from valkyrie.contract import ValkyrieContract, load_contract
from valkyrie.compiler import compile_to_sql
from valkyrie.exceptions import (
    ContractBreachException,
    SchemaConformanceException,
    format_breach_banner,
    extract_top_violations,
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
        """Convenience property converting clean table to Polars DataFrame if available."""
        try:
            import polars as pl
            return pl.from_arrow(self.clean_arrow)
        except ImportError:
            return self.clean_arrow.to_pandas()

    @property
    def dlq_df(self):
        """Convenience property converting DLQ table to Polars DataFrame if available."""
        try:
            import polars as pl
            return pl.from_arrow(self.dlq_arrow)
        except ImportError:
            return self.dlq_arrow.to_pandas()

    def __repr__(self) -> str:
        status = "PASSED ✅" if self.is_passed else "FAILED ❌"
        return (
            f"<ValidationResult status={status} total={self.metrics['total_rows']} "
            f"clean={self.metrics['clean_rows']} dlq={self.metrics['error_rows']} "
            f"error_rate={self.metrics['error_rate']:.2%}>"
        )


def to_sql(contract: Union[str, Path, ValkyrieContract], source_sql: str) -> str:
    """Compile contract to a single-pass DuckDB SQL query string."""
    if not isinstance(contract, ValkyrieContract):
        contract = load_contract(contract)
    return compile_to_sql(contract, source_sql)


def validate_preflight_schema(
    con: duckdb.DuckDBPyConnection,
    source_sql: str,
    contract: ValkyrieContract,
) -> None:
    """Inspect query schema metadata prior to heavy query execution."""
    clean_sql = source_sql.strip().rstrip(";")
    rel = con.sql(clean_sql)
    source_cols = {col.lower(): col for col in rel.columns}

    # 1. Check missing mandatory columns
    missing_cols = []
    for col in contract.schema_:
        if col.not_null and col.name.lower() not in source_cols:
            missing_cols.append(col.name)
    if missing_cols:
        raise SchemaConformanceException(f"Missing mandatory column(s): {missing_cols}")

    # 2. Check unexpected columns if strict_schema is True
    if contract.policies.strict_schema:
        contract_keys = {c.name.lower() for c in contract.schema_}
        extra_cols = [orig for low, orig in source_cols.items() if low not in contract_keys]
        if extra_cols:
            raise SchemaConformanceException(
                f"Unexpected columns found under strict_schema: {extra_cols}"
            )


def run(
    sql: str,
    contract: Union[str, Path, ValkyrieContract],
    raise_on_breach: bool = True,
    connection: Optional[duckdb.DuckDBPyConnection] = None,
) -> ValidationResult:
    """Execute single-pass validation on a source SQL query against a Valkyrie contract."""
    if not isinstance(contract, ValkyrieContract):
        contract = load_contract(contract)

    con = connection or duckdb.connect()

    # Pre-flight schema validation
    validate_preflight_schema(con, sql, contract)

    annotated_sql = compile_to_sql(contract, sql)

    import time
    start_time = time.perf_counter()

    try:
        # Create materialized temporary table (Single-Pass execution)
        con.execute(f"CREATE TEMPORARY TABLE _valkyrie_annotated AS {annotated_sql}")

        # 1. Clean Data: select all columns except internal diagnostic columns where _error_code IS NULL
        clean_sql = (
            "SELECT * EXCLUDE (_error_code, _error_column, _raw_value, _ingestion_timestamp) "
            "FROM _valkyrie_annotated WHERE _error_code IS NULL"
        )
        clean_arrow = con.execute(clean_sql).to_arrow_table()

        # 2. DLQ Data: select all columns where _error_code IS NOT NULL
        dlq_sql = "SELECT * FROM _valkyrie_annotated WHERE _error_code IS NOT NULL"
        dlq_arrow = con.execute(dlq_sql).to_arrow_table()
    finally:
        con.execute("DROP TABLE IF EXISTS _valkyrie_annotated")

    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

    # Metrics
    clean_rows = clean_arrow.num_rows
    error_rows = dlq_arrow.num_rows
    total_rows = clean_rows + error_rows
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

    is_passed = not is_breached

    if is_breached and raise_on_breach:
        top_violations = extract_top_violations(dlq_arrow)
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
        raise ContractBreachException(
            message=msg,
            metrics=metrics,
            dlq_df=dlq_arrow,
            owner=contract.owner,
        )

    return ValidationResult(
        clean_arrow=clean_arrow,
        dlq_arrow=dlq_arrow,
        metrics=metrics,
        owner=contract.owner,
        is_passed=is_passed,
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
        pass  # httpfs might already be loaded or built-in

    key_id = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    endpoint = os.getenv("AWS_ENDPOINT_URL") or os.getenv("S3_ENDPOINT") or ""
    use_ssl = os.getenv("S3_USE_SSL", os.getenv("AWS_USE_SSL", "true")).lower() == "true"
    url_style = os.getenv("S3_URL_STYLE", "vhost")

    if key_id and secret:
        params = [
            "TYPE S3",
            f"KEY_ID '{key_id}'",
            f"SECRET '{secret}'",
            f"REGION '{region}'",
        ]
        if endpoint:
            clean_endpoint = endpoint.replace("http://", "").replace("https://", "").rstrip("/")
            params.append(f"ENDPOINT '{clean_endpoint}'")
        params.append(f"USE_SSL {'true' if use_ssl else 'false'}")
        params.append(f"URL_STYLE '{url_style}'")

        con.execute(f"CREATE OR REPLACE SECRET s3_valkyrie ({', '.join(params)})")


def _duckdb_export_parquet(con: duckdb.DuckDBPyConnection, table: pa.Table, path: str) -> None:
    """Persist Arrow table to Parquet file natively through DuckDB."""
    is_cloud = (
        path.startswith("s3://")
        or path.startswith("minio://")
        or path.startswith("http://")
        or path.startswith("https://")
    )
    if not is_cloud:
        target_path = str(Path(path).resolve()).replace("\\", "/")
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    else:
        target_path = path

    temp_view_name = f"_valkyrie_export_{abs(id(table))}"
    con.register(temp_view_name, table)
    try:
        con.execute(f"COPY {temp_view_name} TO '{target_path}' (FORMAT PARQUET)")
    finally:
        try:
            con.unregister(temp_view_name)
        except Exception:
            pass


def execute(
    sql: str,
    contract: Union[str, Path, ValkyrieContract],
    write_clean: Optional[str] = None,
    write_dlq: Optional[str] = None,
    connection: Optional[duckdb.DuckDBPyConnection] = None,
) -> ValidationResult:
    """Execute validation and safely persist outputs, preserving DLQ even when a breach occurs."""
    con = connection or duckdb.connect()

    # Auto-configure S3/MinIO if needed
    configure_cloud_storage(con, sql, write_clean or "", write_dlq or "")

    # Execute without raising immediately so we can persist DLQ first
    result = run(sql=sql, contract=contract, raise_on_breach=False, connection=con)

    # Persist DLQ if requested and records failed
    if write_dlq and result.dlq_arrow.num_rows > 0:
        _duckdb_export_parquet(con, result.dlq_arrow, write_dlq)

    # If breach occurred, raise ContractBreachException directly without rescanning
    if not result.is_passed:
        top_violations = extract_top_violations(result.dlq_arrow)
        breach_reason = result.metrics.get("breach_reason") or "Data contract breach threshold exceeded."
        msg = format_breach_banner(
            dataset=result.metrics["dataset"],
            owner=result.owner,
            max_error_rate=result.metrics["max_error_rate"],
            error_rate=result.metrics["error_rate"],
            error_rows=result.metrics["error_rows"],
            total_rows=result.metrics["total_rows"],
            breach_reason=breach_reason,
            top_violations=top_violations,
        )
        raise ContractBreachException(
            message=msg,
            metrics=result.metrics,
            dlq_df=result.dlq_arrow,
            owner=result.owner,
        )

    # Persist Clean data if passed
    if write_clean:
        _duckdb_export_parquet(con, result.clean_arrow, write_clean)

    return result

