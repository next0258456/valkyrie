# 03: True Single-Pass Table & Native DuckDB S3 Export (COPY TO)

**What to build:** In `valkyrie.run()`, switch from `TEMPORARY VIEW` to `TEMPORARY TABLE` with guaranteed `DROP TABLE IF EXISTS` in a `finally` block, ensuring raw data is physically scanned only once. In `valkyrie.execute()`, replace PyArrow writes with DuckDB's native `COPY ... TO '{path}' (FORMAT PARQUET)` so that DuckDB S3 Secrets are honored when writing to S3/MinIO. Measure execution latency and include `duration_ms` in `metrics`. Ensure DLQ is persisted on breach without re-scanning.

**Blocked by:** 01: Zero-Pydantic Pure Dataclasses & Flexible Range Parsing, 02: Pre-Flight Schema & Type Conformance Check (strict_schema)

**Status:** completed

- [x] `valkyrie.run()` materializes single-pass evaluation into `TEMPORARY TABLE _valkyrie_annotated`, ensuring underlying source query is scanned exactly once.
- [x] `_valkyrie_annotated` is deterministically dropped in a `finally` block, leaving zero temporary table residue.
- [x] `valkyrie.execute()` writes Parquet files using DuckDB's native `COPY (...) TO '{path}' (FORMAT PARQUET)`.
- [x] Exporting to S3/MinIO endpoints with custom credentials works seamlessly without PyArrow dependency or authentication failure.
- [x] `ValidationResult.metrics` includes `duration_ms` which is a positive float.
- [x] When a circuit breach occurs in `execute()`, DLQ data is safely persisted to `write_dlq` before raising `ContractBreachException` without calling `run()` a second time.

