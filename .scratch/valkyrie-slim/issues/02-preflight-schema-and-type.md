# 02: Full Pre-Flight Schema & Type Conformance Check

**What to build:** In `engine.py`, inspect DuckDB SQL relation metadata (`rel.columns`, `rel.types`) before execution. Raise `SchemaConformanceException` for: (1) any missing contracted column (both not_null and nullable, with `ERR_SCHEMA_MISSING_COLUMN`), (2) unexpected extra columns when `policies.strict_schema: true` (`ERR_SCHEMA_EXTRA_COLUMN`), and (3) physical type family mismatch (`ERR_SCHEMA_TYPE_MISMATCH`).

**Blocked by:** 01: Lean Dictionary-Based Contract Loader

**Status:** completed

- [x] Inspects `con.sql(clean_sql)` metadata directly.
- [x] Raises `SchemaConformanceException` (`ERR_SCHEMA_MISSING_COLUMN`) if ANY column declared in contract schema is missing.
- [x] Raises `SchemaConformanceException` (`ERR_SCHEMA_EXTRA_COLUMN`) if undeclared columns exist under `strict_schema: true`.
- [x] Raises `SchemaConformanceException` (`ERR_SCHEMA_TYPE_MISMATCH`) if column physical type is incompatible with contract type.
- [x] Runs in sub-millisecond time at metadata level without fetching data into Python.
