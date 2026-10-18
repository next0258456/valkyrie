# Valkyrie Engine: Bug-Fix & Production Hardening Specification

*Labels: ready-for-agent*

## Problem Statement

During an adversarial review of the Valkyrie codebase, five concrete bugs, shortcuts, and omissions were uncovered that undermine data contract integrity and out-of-core stability:

1. **Catastrophic OOM on Breach during Out-of-Core `execute()`**:
   - In `src/valkyrie/engine.py:206`, when `is_breached=True`, `run()` unconditionally calls `dlq_arrow = con.execute("SELECT * FROM _valkyrie_annotated WHERE _error_code IS NOT NULL").to_arrow_table()` regardless of `materialize_arrow=False`. If an upstream change causes 50 million invalid rows on a 50GB dataset, `execute()` attempts to dump the entire multi-gigabyte DLQ into Python memory, causing an instant Out-Of-Memory process crash instead of safely exiting after persisting `write_dlq`.
2. **Type Classification (`_classify_type`) Regex/Prefix Bugs**:
   - `INTERVAL` starts with `"INT"` and is falsely classified as `NUMERIC`.
   - Fixed-size arrays (e.g. `INTEGER[3]`) in DuckDB do not end with `[]` and start with `"INT"`, falsely categorizing array types as `NUMERIC` instead of `LIST`.
   - DuckDB unsigned integer types (`UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`, `UHUGEINT`) start with `"U"` and fail to match `"INT"`/`"BIGINT"`, raising false-positive `ERR_SCHEMA_TYPE_MISMATCH` exceptions.
3. **Blacklist Bypass and Unvalidated Schemas in `contract.py`**:
   - `load_contract()` only checks a 5-word blacklist (`sla`, `team`, `pricing`, `description`, `business_logic`) at root instead of enforcing a strict Whitelist of executable keys (`version`, `dataset`, `owner`, `policies`, `schema`, `custom_rules`). Arbitrary prose keys (e.g., `notes`, `author`, `wiki`) slip through.
   - Column attributes also lack a whitelist; typos like `not_nul: true` are silently ignored.
   - If `schema` is omitted or empty (`schema: []`), `load_contract()` does not raise `ValueError`, violating Ticket 01 acceptance criteria.
   - If `custom_rules` contains an empty `expression: ""`, it passes validation and compiles to invalid SQL: `(NOT () OR () IS NULL)`.
4. **Duplicated `metrics` Dictionary Construction in `engine.py`**:
   - Lines 208-219 and lines 254-265 duplicate the exact 12-line dictionary creation logic.
5. **Dropped Edge-Case Tests**:
   - The test consolidation silently dropped tests for:
     - `unique: true` (`ERR_DUPLICATE_KEY`)
     - `quarantine_action: "FAIL_FAST"`
     - First-Violation Short-Circuit precedence
     - `range: {min: x, max: y}` dictionary syntax
     - S3/MinIO secret auto-configuration (`configure_cloud_storage`)

---

## Solution

Fix all 5 identified defects while preserving the minimal DuckDB SQL wrapper architecture:

1. **True Zero-OOM on Breach in `execute()`**:
   - If `materialize_arrow=False`, `dlq_arrow` in `ContractBreachException` must NOT materialize the entire table into Python RAM. Either fetch an empty table / top sample (e.g., `LIMIT 100`) or set `dlq_df=None` and reference the persisted `write_dlq` path in the breach exception.
2. **Robust Type Classification**:
   - Rewrite `_classify_type()` to use exact token matching or robust regex:
     - Detect `STRUCT(...)` -> `STRUCT`.
     - Detect array brackets `[...]` or `LIST(...)` -> `LIST`.
     - Detect `INTERVAL` -> `INTERVAL`.
     - Categorize both signed and unsigned integers (`UINTEGER`, `UBIGINT`, etc.), floats, and decimals -> `NUMERIC`.
3. **Strict Whitelisting in `contract.py`**:
   - Enforce allowed root keys: `{"version", "dataset", "owner", "policies", "schema", "custom_rules"}`. Reject any unknown/prose key with `ValueError`.
   - Enforce non-empty `schema`: missing or empty list raises `ValueError("Contract must declare at least one column in 'schema'")`.
   - Enforce allowed column keys: `{"name", "type", "not_null", "unique", "range", "enum", "pattern"}`.
   - Enforce non-empty `expression` on every `custom_rule`.
4. **DRY Metrics Computation in `engine.py`**:
   - Assemble the `metrics` dict once before checking breach conditions.
5. **Restore Comprehensive Edge-Case Tests**:
   - Add dedicated test cases in `tests/test_valkyrie.py` verifying:
     - `test_unique_duplicate_key_violation`
     - `test_quarantine_action_fail_fast`
     - `test_first_violation_precedence`
     - `test_range_dict_syntax`
     - `test_configure_cloud_storage_secret`
     - `test_execute_zero_oom_on_breach`
     - `test_contract_strict_root_and_column_whitelist`
     - `test_contract_rejects_empty_schema_and_empty_expression`

---

## User Stories

1. As a Data Engineer processing a 50GB Parquet batch, I want `valkyrie.execute(sql, contract, write_dlq=...)` to stream corrupt records to `dlq.parquet` without loading millions of rows into Python memory when a contract breach trips, so that the crash handler never runs out of memory.
2. As a Data Engineer writing contract YAML, I want typos in column definitions (like `not_nul: true`) or arbitrary root keys (like `notes: "TODO"`) to be rejected with `ValueError`, so that I never have silent contract misconfigurations.
3. As a Data Engineer using unsigned integers (`UBIGINT`) or intervals (`INTERVAL`), I want preflight type checking to correctly identify their types without false-positive type mismatch errors.
4. As a Data Platform Auditor, I want a contract with zero columns (`schema: []`) or empty custom rule expressions (`expression: ""`) to fail validation immediately at load time.
5. As a QA Engineer, I want the test suite to continuously verify `unique`, `FAIL_FAST`, range dict syntax, first-violation short-circuiting, and S3 credentials alongside core tests.

---

## Implementation Decisions

### 1. `contract.py`
- Root Whitelist: `ALLOWED_ROOT_KEYS = {"version", "dataset", "owner", "policies", "schema", "custom_rules"}`.
- Column Whitelist: `ALLOWED_COLUMN_KEYS = {"name", "type", "not_null", "unique", "range", "enum", "pattern", "quality"}`.
- Check `if not raw_schema: raise ValueError("Contract must declare at least one column in 'schema'")`.
- In `custom_rules`, check `if not str(r_expr).strip(): raise ValueError(...)`.

### 2. `engine.py`
- `_classify_type(type_str)`:
  - If `re.search(r'\[\s*\d*\s*\]', t)` or `t.startswith("LIST")` -> `"LIST"`.
  - If `t.startswith("STRUCT")` -> `"STRUCT"`.
  - If `t.startswith("INTERVAL")` -> `"INTERVAL"`.
  - Match signed and unsigned numeric types cleanly: `r'^(U?(BIGINT|INTEGER|INT|SMALLINT|TINYINT|HUGEINT)|DOUBLE|FLOAT|REAL|DECIMAL|NUMERIC)'` -> `"NUMERIC"`.
- `run()`:
  - Construct `metrics` dict immediately after calculating `clean_rows`, `error_rows`, `duration_ms`.
  - On breach: if `materialize_arrow=False`, set `dlq_arrow = None` (or sample `LIMIT 100`) rather than loading the full table.
  - Return `metrics` and raise `ContractBreachException(dlq_df=dlq_arrow)`.

### 3. `tests/test_valkyrie.py`
- Restore all 5 missing edge cases plus tests for whitelist enforcement and zero-OOM breach.

---

## Testing Decisions

- Test exclusively via public seams: `load_contract()`, `run()`, `execute()`, and CLI.
- Ensure all restored edge cases run against real DuckDB in-memory executions with no mocks.

---

## Out of Scope

- Rewriting engine into Rust or introducing external dependencies.
- Changes to the single-pass CTE compiler logic (other than type compatibility adjustments).
