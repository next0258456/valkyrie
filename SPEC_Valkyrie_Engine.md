# Valkyrie Engine v0.4: Production Hardening & Out-of-Core Resilience

*Labels: ready-for-agent*

## Problem Statement

Following the initial slim-down refactoring, Valkyrie successfully eliminated ~1,800 lines of dead documentation, redundant scratch files, and copy-pasted test suites. However, the latest two-axis code review highlighted four concrete architectural vulnerabilities and behavioral gaps:

1. **Broken Out-of-Core Streaming in `execute()`**: In disk-to-disk pipelines (`valkyrie.execute` or `valkyrie run --write-clean ... --write-dlq ...`), the engine streams Parquet directly via DuckDB's native `COPY TO`, but then unconditionally pulls the entire dataset into Python RAM via `.to_arrow_table()` inside `run()`. For multi-gigabyte or 100M+ row Lakehouse datasets, this causes Out-Of-Memory (OOM) crashes, defeating DuckDB's streaming capability.
2. **Dead Code & Incomplete Type Model**: `exceptions.py` retains `extract_top_violations()` as dead code after migrating violation aggregation to DuckDB SQL. In `contract.py`, `ColumnContract.range` is typed as `Optional[tuple]` while `parse_range()` returns a `RangeConstraint` dataclass instance, forcing defensive duck-typing in `compiler.py`.
3. **Pre-Flight Type Checking Skips Complex / Nested Types**: The pre-flight `type_categories` map in `engine.py` omits complex types (`STRUCT`, `LIST`) and timestamp variants (`TIMESTAMP_TZ`, `TIMESTAMPTZ`), allowing schema drift on nested types to slip through undetected.
4. **CLI Subcommand Inconsistency & Retained Bloat**: The spec designates `valkyrie compile` as the command to preview compiled SQL, but the CLI registers `to-sql`. Furthermore, speculative `.clean_df` / `.dlq_df` properties in `ValidationResult` remain in the core engine.

*(Note: The user explicitly exempted the strict line-count target of ~220 lines for `src/valkyrie/`; the focus is strictly on correctness, zero-OOM Out-of-Core execution, clean interfaces, and robust error handling).*

---

## Solution

Address all flagged code review defects while preserving existing public seams:

1. **Zero-OOM Out-of-Core Execution (`execute` vs `run`)**:
   - `valkyrie.execute()` executes disk-to-disk pipelines in true streaming mode: runs preflight check, compiles CTE, executes `COPY ... TO` for clean and DLQ datasets natively in DuckDB, queries row metrics and top violations purely via SQL, and drops the temporary table **without calling `.to_arrow_table()`**.
   - `ValidationResult` returned by `execute()` holds Arrow tables as `None` (or empty tables) with full populated `metrics` (`total_rows`, `clean_rows`, `error_rows`, `duration_ms`, etc.), while `valkyrie.run()` continues to populate `clean_arrow` and `dlq_arrow` for in-memory callers.
2. **Clean Type Model & Dead Code Removal**:
   - Standardize `RangeConstraint` across `contract.py` and `compiler.py` with consistent type annotations.
   - Delete unused `extract_top_violations` in `exceptions.py`.
   - Remove speculative `.clean_df` / `.dlq_df` properties from `ValidationResult`.
3. **Comprehensive Physical Type Conformance**:
   - Expand `validate_preflight_schema()` to categorize and validate complex/nested types (`STRUCT`, `LIST`/`ARRAY`), temporal types (`TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, `TIME`), and numeric/string/boolean types.
4. **CLI Alignment**:
   - Support both `compile` (primary, as per spec) and `to-sql` (alias/backward compatibility) in the CLI parser.

---

## User Stories

1. As a Data Engineer processing a 50GB Parquet batch, I want `valkyrie.execute(sql, contract, write_clean=..., write_dlq=...)` to stream directly to disk without loading Arrow tables into Python RAM, so that my pipeline never crashes with Out-Of-Memory (OOM).
2. As a Data Engineer doing interactive analysis, I want `valkyrie.run(sql, contract)` to return full `clean_arrow` and `dlq_arrow` tables in memory, so that I can directly inspect validation results in my notebook or script.
3. As a Data Engineer with nested Lakehouse schemas (e.g. order line items), I want pre-flight validation to verify that `STRUCT` or `LIST` columns in the source query match the contract's declared types, so that `ERR_SCHEMA_TYPE_MISMATCH` is caught before execution begins.
4. As a DevOps Engineer, I want the CLI to support `valkyrie compile --sql "..." --contract "..."` as documented in the specification, while maintaining `valkyrie to-sql` for backward compatibility.
5. As a Code Maintainer, I want no dead or unused functions in `exceptions.py` and a consistent `RangeConstraint` model across `contract.py` and `compiler.py`, so that the codebase remains clean, maintainable, and free of code smells.

---

## Implementation Decisions

### 1. Engine Out-of-Core Decoupling (`engine.py`)
- In `run()`:
  - Add an internal parameter `materialize_arrow: bool = True` (default `True` for `run`, `False` when called from `execute` with write paths).
  - When `materialize_arrow=False`:
    - Perform pre-flight validation, create temporary table, compute metrics via SQL `COUNT`, extract top violations via SQL `GROUP BY`, and execute native DuckDB `COPY TO` commands.
    - If breached and `raise_on_breach=True`: fetch only the DLQ sample/arrow table needed for the exception banner and payload, persist DLQ to `write_dlq`, and raise `ContractBreachException`.
    - If passed: drop temporary table immediately without converting the full clean dataset to Arrow in memory.
- `execute()` invokes `run(..., materialize_arrow=False)`.

### 2. Enhanced Pre-Flight Schema Inspection (`engine.py`)
- Update `type_categories` in `validate_preflight_schema` to include:
  - `STRUCT` -> `"STRUCT"`
  - `LIST`, `ARRAY` -> `"LIST"`
  - `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE` -> `"TIMESTAMP"`
  - `JSON` -> `"STRING"` (or `"JSON"`)
- Ensure case-insensitive prefix matching so types like `STRUCT(a INT)` or `INTEGER[]` are cleanly classified.

### 3. Cleanup & Consistency (`contract.py`, `compiler.py`, `exceptions.py`, `cli.py`)
- In `contract.py`: Annotate `range: Optional[RangeConstraint]` in `ColumnContract`.
- In `compiler.py`: Access `col.range.min` and `col.range.max` directly without defensive type-checking.
- In `exceptions.py`: Delete `extract_top_violations()`.
- In `engine.py`: Remove `.clean_df` and `.dlq_df` properties from `ValidationResult`.
- In `cli.py`: Register `compile` as the primary subcommand, with `to-sql` as an alias.

---

## Testing Decisions

- Test across the single public seam: `valkyrie.run`, `valkyrie.execute`, `valkyrie.to_sql`, `valkyrie.load_contract`, and the CLI.
- New test cases in `tests/test_valkyrie.py`:
  1. `test_execute_zero_oom_out_of_core`: Verify that `execute()` with `write_clean` writes the Parquet file correctly while `result.clean_arrow` is `None` (preserving zero-RAM consumption).
  2. `test_preflight_complex_type_mismatch`: Verify that mismatching `STRUCT` vs `VARCHAR` or `LIST` vs `INT` raises `SchemaConformanceException` (`ERR_SCHEMA_TYPE_MISMATCH`).
  3. `test_cli_compile_subcommand`: Verify both `valkyrie compile` and `valkyrie to-sql` output compiled SQL with exit code 0.
  4. `test_range_constraint_model_consistency`: Verify `col.range.min` and `col.range.max` work seamlessly across contract and compiler.

---

## Out of Scope

- Strict line count reduction down to 220 lines (explicitly exempted by user).
- Pydantic or external schema validator dependencies.
