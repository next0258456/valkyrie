# 03: 100% DuckDB-Native Execution, SQL Aggregations & Direct COPY TO

**What to build:** In `engine.py`, eliminate all Python-side data operations. Compute `total_rows`, `clean_rows`, `error_rows`, and top 3 `_error_code` violations directly using DuckDB SQL (`COUNT` + `GROUP BY`). Export clean and DLQ parquet directly via DuckDB `COPY (SELECT ... FROM _valkyrie_annotated WHERE ...) TO '{path}' (FORMAT PARQUET)`. Eliminate `_duckdb_export_parquet`, eliminating all Arrow re-registration round-trips. Drop temporary table in `finally`.

**Blocked by:** 02: Full Pre-Flight Schema & Type Conformance Check

**Status:** completed

- [x] Metrics computed purely in SQL (`SELECT count(*), count(_error_code)`).
- [x] Top violations extracted purely in SQL (`GROUP BY _error_code ORDER BY count(*) DESC LIMIT 3`).
- [x] Direct DuckDB native `COPY TO` executed from `_valkyrie_annotated` for Parquet/S3.
- [x] Zero re-registration of Arrow tables into DuckDB views.
- [x] DLQ safely persisted before raising `ContractBreachException` on breach.
- [x] Out-of-Core streaming preserved end-to-end.
