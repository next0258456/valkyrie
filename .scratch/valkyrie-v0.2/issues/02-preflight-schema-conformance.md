# 02: Pre-Flight Schema & Type Conformance Check (strict_schema)

**What to build:** In `engine.py`, inspect query metadata via `rel = con.sql(source_sql)` in sub-milliseconds before scanning raw data. Validate column existence: if mandatory columns are missing, raise `SchemaConformanceException` (`ERR_SCHEMA_MISSING_COLUMN`). If `strict_schema: true` and extra unexpected columns are present in the source query, raise `SchemaConformanceException` (`ERR_SCHEMA_UNEXPECTED_COLUMN`). Validate basic physical column types against contract declarations.

**Blocked by:** 01: Zero-Pydantic Pure Dataclasses & Flexible Range Parsing

**Status:** completed

- [x] Missing mandatory columns (`not_null: true` or required) in source query raise `SchemaConformanceException`.
- [x] Extra unexpected columns present in source query when `strict_schema: true` raise `SchemaConformanceException`.
- [x] Extra unexpected columns present in source query when `strict_schema: false` are allowed without exception.
- [x] Column physical type mismatch (e.g. source has VARCHAR for BIGINT contract column) raises `SchemaConformanceException`.
- [x] End-to-end black-box tests verify pre-flight schema enforcement on synthetic queries before heavy scans.
