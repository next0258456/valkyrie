"""Valkyrie runtime engine powered by DuckDB."""

from typing import Any, Dict, Iterator, Optional, Tuple, Union
from pathlib import Path
import duckdb
import pyarrow as pa

from valkyrie.contract import ValkyrieContract, load_contract
from valkyrie.compiler import compile_to_sql
from valkyrie.exceptions import ContractBreachException


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

    # Compile SQL with single-pass annotation
    annotated_sql = compile_to_sql(contract, sql)

    # Register annotated relation
    con.execute(f"CREATE OR REPLACE TEMPORARY VIEW _valkyrie_annotated AS {annotated_sql}")

    # 1. Clean Data: select all columns except internal diagnostic columns where _error_code IS NULL
    clean_sql = (
        "SELECT * EXCLUDE (_error_code, _error_column, _raw_value, _ingestion_timestamp) "
        "FROM _valkyrie_annotated WHERE _error_code IS NULL"
    )
    clean_arrow = con.execute(clean_sql).to_arrow_table()

    # 2. DLQ Data: select all columns where _error_code IS NOT NULL
    dlq_sql = "SELECT * FROM _valkyrie_annotated WHERE _error_code IS NOT NULL"
    dlq_arrow = con.execute(dlq_sql).to_arrow_table()

    # Metrics
    clean_rows = clean_arrow.num_rows
    error_rows = dlq_arrow.num_rows
    total_rows = clean_rows + error_rows
    error_rate = (error_rows / total_rows) if total_rows > 0 else 0.0

    metrics = {
        "dataset": contract.dataset,
        "owner": contract.owner,
        "total_rows": total_rows,
        "clean_rows": clean_rows,
        "error_rows": error_rows,
        "error_rate": error_rate,
        "max_error_rate": contract.policies.max_error_rate,
        "quarantine_action": contract.policies.quarantine_action,
    }

    # Check circuit breaker
    is_breached = False
    breach_reason = ""

    if contract.policies.quarantine_action.upper() == "FAIL_FAST" and error_rows > 0:
        is_breached = True
        breach_reason = f"Quarantine policy is FAIL_FAST and {error_rows} invalid row(s) detected."
    elif error_rate > contract.policies.max_error_rate:
        is_breached = True
        breach_reason = f"Error rate {error_rate:.2%} exceeded threshold {contract.policies.max_error_rate:.2%}."

    is_passed = not is_breached

    if is_breached and raise_on_breach:
        top_violations = []
        if error_rows > 0 and "_error_code" in dlq_arrow.column_names:
            import collections
            code_counts = collections.Counter(dlq_arrow.column("_error_code").to_pylist())
            for code, count in code_counts.most_common(3):
                top_violations.append(f"  - [{code}]: {count:,} rows")
        violations_str = "\n".join(top_violations) if top_violations else "  - None"

        msg = (
            f"\n{'=' * 80}\n"
            f"🔥 [VALKYRIE] DATA CONTRACT BREACH DETECTED!\n"
            f"Dataset     : {contract.dataset}\n"
            f"Owner       : {contract.owner}\n"
            f"Threshold   : max_error_rate = {contract.policies.max_error_rate:.2%}\n"
            f"Actual Error: {error_rate:.2%} ({error_rows:,} / {total_rows:,} rows)\n"
            f"Top Violations:\n{violations_str}\n"
            f"Reason      : {breach_reason}\n"
            f"Action Taken: Pipeline HALTED. DLQ preserved. Contact owner immediately!\n"
            f"{'=' * 80}"
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
        import pyarrow.parquet as pq
        pq.write_table(result.dlq_arrow, write_dlq)

    # If breach occurred, re-trigger the breach exception with alert banner
    if not result.is_passed:
        # Calling run with raise_on_breach=True triggers the exact exception
        run(sql=sql, contract=contract, raise_on_breach=True, connection=con)

    # Persist Clean data if passed
    if write_clean:
        import pyarrow.parquet as pq
        pq.write_table(result.clean_arrow, write_clean)

    return result

