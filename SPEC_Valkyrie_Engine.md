# Valkyrie: Minimal DuckDB SQL Contract Compiler & Wrapper

*Labels: ready-for-agent*

## Problem Statement

Data Engineers building Bronze-to-Silver pipelines in the Yggdrasil Lakehouse need a fast, zero-overhead way to validate datasets against a declarative YAML Data Contract (`contract.yaml`), separate clean records from Dead Letter Queue (DLQ) records, and trigger a circuit breaker when error rates exceed policy thresholds.

However, the current Valkyrie repository has suffered from severe scope creep and over-engineering (~2,500 lines across 34 files, with 741 lines in `src/valkyrie/`):
1. **Doing DuckDB's Work in Python:** Instead of letting DuckDB execute SQL end-to-end, the engine pulls `clean` and `dlq` datasets into Python Arrow memory (`.to_arrow_table()`), drops the DuckDB temporary table, and then re-registers the Arrow tables back into DuckDB views (`_valkyrie_export_*`) just to run `COPY ... TO` Parquet. This breaks DuckDB's Out-of-Core streaming and wastes memory and code.
2. **In-Python Aggregations:** Row counts (`.num_rows`) and top violation tallies (`collections.Counter` over `.to_pylist()`) are computed in Python instead of a single native SQL `COUNT` / `GROUP BY` query inside DuckDB.
3. **Bloated OOP Contract Boilerplate:** `contract.py` uses 156 lines and 6 nested `@dataclass` classes with hand-rolled validation loops just to pass a YAML dictionary into a 100-line SQL compiler.
4. **Duplicated Execution Flows & Speculative Wrappers:** `run()` and `execute()` duplicate breach detection and banner formatting, while `ValidationResult` includes speculative Polars/Pandas conversion properties (`.clean_df`, `.dlq_df`).
5. **Incomplete Pre-Flight Schema Check:** Pre-flight inspection ignores missing nullable contract columns (causing raw DuckDB `BinderException` later) and does not verify physical type compatibility (`rel.types`) against declared contract column types.
6. **Repository & Test Sprawl:** Three overlapping spec files, 10 `.scratch` ticket files, and 10 fragmented test files (945 lines) repeating identical YAML fixtures make the project look like a complex framework rather than a thin ~200-line DuckDB SQL wrapper.

---

## Solution

Radically slim down **Valkyrie** to its true architectural identity: a **~200-line Pure DuckDB SQL Compiler & Execution Wrapper** (`duckdb` + `pyyaml` + `pyarrow`).

1. **Pure SQL Compiler (`to_sql`)**: Takes a validated YAML contract and a source SQL query, and compiles all column constraints (`not_null`, `unique`, `range`, `enum`, `pattern`) and `custom_rules` into a single-pass DuckDB CTE query (`WITH raw AS (...), annotated AS (...)`) using First-Violation Short-Circuit `CASE WHEN` expressions.
2. **100% DuckDB-Native Execution (`run` & `execute`)**:
   - **Pre-Flight Check**: Uses `con.sql(source_sql)` metadata (`rel.columns`, `rel.types`) to verify all contracted columns exist, physical types are compatible, and no undeclared columns exist when `strict_schema: true`.
   - **Single-Pass Materialization & SQL Metrics**: Materializes `TEMP TABLE _valkyrie_annotated` once. Computes `total_rows`, `error_rows`, and top 3 `_error_code` counts directly via DuckDB SQL (`COUNT` + `GROUP BY`).
   - **Direct SQL Export (`COPY TO`)**: When `write_dlq` or `write_clean` paths are provided, executes `COPY (SELECT ... FROM _valkyrie_annotated WHERE ...) TO '{path}' (FORMAT PARQUET)` directly inside DuckDB before dropping the temporary table—zero Python/Arrow round-trip.
   - **Unified Circuit Breaker**: Single unified flow for both `run()` and `execute()`. Persists DLQ first (if `write_dlq` is set), then raises `ContractBreachException` with the owner accountability banner if breached.
3. **Lean Contract Loader**: Validates `contract.yaml` directly into a clean normalized structure in ~40 lines without nested OOP ceremony.
4. **Consolidated Repo & Test Suite**: One authoritative spec (`SPEC_Valkyrie_Engine.md`), one ADR (`ADR-0001`), `GLOSSARY.md`, lean `src/valkyrie/` (~200–250 lines total), and a single consolidated test suite evaluating the public seam.

---

## User Stories

1. As a Data Engineer, I want to call `valkyrie.to_sql(sql, contract)` to get the exact DuckDB SQL CTE string compiled from my YAML contract, so that I can inspect, debug, or execute the SQL anywhere.
2. As a Data Engineer, I want `contract.yaml` to strictly allow only machine-executable keys (`version`, `dataset`, `owner`, `policies`, `schema`, `custom_rules`) and reject prose/fluff keys (`sla`, `team`, `pricing`, `description`, `quality` prose), so that contracts remain 100% deterministic.
3. As a Platform Engineer, I want `owner` to be mandatory at the root of `contract.yaml`, so that every contract breach alert identifies the accountable person or team.
4. As a Data Engineer, I want pre-flight schema validation to check that every column declared in `schema` exists in the source query before execution, so that missing columns (nullable or not) raise `SchemaConformanceException` with `ERR_SCHEMA_MISSING_COLUMN`.
5. As a Data Engineer, I want pre-flight schema validation to reject extra undeclared columns when `policies.strict_schema: true` with `SchemaConformanceException` (`ERR_SCHEMA_EXTRA_COLUMN`), so that unexpected upstream schema drift is caught at Gate 1.
6. As a Data Engineer, I want pre-flight schema validation to verify physical type compatibility (`rel.types`) against declared contract column types (e.g. numeric vs string vs boolean vs struct/list) and raise `SchemaConformanceException` (`ERR_SCHEMA_TYPE_MISMATCH`) on incompatible types.
7. As a Data Engineer, I want `range` constraints to support both `[min, max]` list syntax and `{min: x, max: y}` dict syntax, so that writing range rules in YAML is ergonomic.
8. As a Data Engineer, I want `not_null`, `unique` (via `count(*) OVER (PARTITION BY col) > 1`), `range`, `enum`, `pattern` (via `regexp_matches`), and `custom_rules` (including nested list shorthand `list_sum(items.price)`) to compile into a single `CASE WHEN` pass.
9. As a Data Engineer, I want `valkyrie.run(sql, contract)` to evaluate the dataset in a single pass inside DuckDB and return a `ValidationResult` unpacking into `(clean_arrow, dlq_arrow)` along with `metrics` (`total_rows`, `clean_rows`, `error_rows`, `error_rate`, `duration_ms`).
10. As a Data Engineer, I want `valkyrie.execute(sql, contract, write_clean=..., write_dlq=...)` to stream `clean` and `dlq` directly from DuckDB `_valkyrie_annotated` to Parquet/S3 via DuckDB's native `COPY (...) TO '{path}' (FORMAT PARQUET)` without re-registering Arrow tables.
11. As a Data Engineer, I want S3/MinIO paths (`s3://`, `minio://`) to automatically configure DuckDB S3 Secrets from standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT_URL`, `S3_USE_SSL`, `S3_URL_STYLE`).
12. As a Platform Engineer, I want the circuit breaker to trip when `error_rate > max_error_rate` or when `quarantine_action == "FAIL_FAST"` and `error_rows > 0`, safely writing `write_dlq` via DuckDB `COPY` before raising `ContractBreachException`.
13. As a Data Engineer, I want a lightweight CLI (`valkyrie run` and `valkyrie compile`) that wraps the Python API with exit codes `0` (pass), `1` (contract breach), and `2` (schema/contract error).

---

## Implementation Decisions

### 1. Target Codebase Size & Module Structure (~220 Lines Total)
- **`contract.py` (~45 lines)**:
  - Pure YAML loader (`load_contract`) returning a minimal normalized contract object/dict (`dataset`, `owner`, `version`, `policies`, `schema`, `custom_rules`).
  - Rejects prohibited fluff keys at root and column levels.
  - Normalizes `range: [min, max]` and `range: {min, max}` into `(min_val, max_val)`.
  - Zero Pydantic; no deep 6-class OOP boilerplate.
- **`compiler.py` (~75 lines)**:
  - `build_case_when(branches)` helper for SQL `CASE WHEN ... ELSE NULL END`.
  - `compile_to_sql(contract, source_sql)` generating the `WITH raw AS (...), annotated AS (...) SELECT * FROM annotated` query.
  - Rewrites `list_<agg>(col.field)` shorthand into DuckDB lambda `list_<agg>(list_transform(col, x -> x.field))`.
- **`engine.py` (~95 lines)**:
  - **Pre-Flight Check (`validate_preflight_schema`)**: Runs `rel = con.sql(clean_sql)`. Checks:
    1. Every column in `contract.schema` exists in `rel.columns` (`ERR_SCHEMA_MISSING_COLUMN`).
    2. If `strict_schema` is enabled, no extra columns exist in `rel.columns` (`ERR_SCHEMA_EXTRA_COLUMN`).
    3. Physical type family compatibility between `rel.types` and declared column `type` (`ERR_SCHEMA_TYPE_MISMATCH`).
  - **Unified Single-Pass Execution**:
    - Creates `TEMP TABLE _valkyrie_annotated AS {compiled_sql}`.
    - Queries metrics and top 3 violations directly in DuckDB SQL:
      - `SELECT count(*), count(_error_code) FROM _valkyrie_annotated`
      - `SELECT _error_code, count(*) FROM _valkyrie_annotated WHERE _error_code IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3`
    - If `write_dlq` is provided and `error_rows > 0`: runs `COPY (SELECT * FROM _valkyrie_annotated WHERE _error_code IS NOT NULL) TO '{dlq_path}' (FORMAT PARQUET)` directly inside DuckDB.
    - If circuit breaker is breached and `raise_on_breach=True`: fetches `dlq_arrow` for the exception payload and raises `ContractBreachException`.
    - If `write_clean` is provided and not breached: runs `COPY (SELECT * EXCLUDE (_error_code, _error_column, _raw_value, _ingestion_timestamp) FROM _valkyrie_annotated WHERE _error_code IS NULL) TO '{clean_path}' (FORMAT PARQUET)` directly inside DuckDB.
    - Fetches `clean_arrow` and `dlq_arrow` from `_valkyrie_annotated` and drops `_valkyrie_annotated` in `finally`.
  - **`ValidationResult`**: Minimal container holding `(clean_arrow, dlq_arrow, metrics, owner, is_passed)` with tuple unpacking `__iter__`. Removes speculative Polars/Pandas `.clean_df` / `.dlq_df` methods.
- **`exceptions.py` (~30 lines)**:
  - `SchemaConformanceException`, `ContractBreachException`, and `format_breach_banner`. Removes Python `collections.Counter` helper since DuckDB computes top violations via SQL `GROUP BY`.
- **`cli.py` (~45 lines)**:
  - Minimal `argparse` wrapper around `to_sql` and `execute`/`run`.

### 2. Repository Cleanup
- Remove redundant spec files (`SPEC_Valkyrie_v0.1_Implementation.md`, `SPEC_Valkyrie_v0.2_Refinement.md`) and `.scratch/valkyrie-v0.1/`, `.scratch/valkyrie-v0.2/`.
- Consolidate the 10 fragmented test files into a clean, high-signal test suite (`tests/test_valkyrie.py`).

---

## Testing Decisions

- **Single Public Seam**: All tests interact exclusively through `valkyrie.to_sql`, `valkyrie.run`, `valkyrie.execute`, `valkyrie.load_contract`, and the `valkyrie` CLI.
- **Consolidated Test Suite**: Replace the 10 repetitive test files (945 lines) with a single concise `tests/test_valkyrie.py` (~200 lines) using shared fixtures covering:
  1. Contract validation (missing owner, fluff rejection, range list/dict normalization).
  2. Pre-flight schema conformance (missing column, extra column under `strict_schema`, and physical type mismatch `ERR_SCHEMA_TYPE_MISMATCH`).
  3. Single-pass constraints (`not_null`, `unique`, `range`, `enum`, `pattern`, `custom_rules` + nested list lambda, first-violation short-circuit).
  4. Circuit breaker (`max_error_rate`, `FAIL_FAST`, alert banner with SQL-aggregated top violations).
  5. Direct DuckDB `COPY TO` Parquet persistence (`write_clean`, `write_dlq` preserved on breach, S3 secret configuration, temporary table cleanup).
  6. CLI `compile` and `run` exit codes (`0`, `1`, `2`).

---

## Out of Scope

- Rust crates, LMAX Disruptor, or background daemon services.
- Pydantic or any validation library outside Python standard library + `pyyaml`.
- Built-in Polars/Pandas conversion wrappers inside Valkyrie (callers can call `pl.from_arrow(clean_arrow)` directly if needed).
