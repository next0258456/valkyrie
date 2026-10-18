# 01: Tracer Bullet - Core Single-Pass Contract Engine (not_null, range)

**What to build:** An end-to-end working pipeline that accepts a physical contract with basic constraints (`not_null`, `range`), compiles it into a single-pass DuckDB SQL query, executes it against an in-memory SQL source, and produces two clean output streams: clean records and isolated DLQ records enriched with diagnostic metadata (`_error_code`, `_error_column`, `_raw_value`, `_ingestion_timestamp`).

**Blocked by:** None (can start immediately)

**Status:** completed

- [x] Parsing a contract YAML file requires the mandatory `owner` and `dataset` fields, rejecting files that omit them.
- [x] Non-executable prose rules (such as `type: text`) are strictly rejected during contract parsing.
- [x] The SQL compiler translates `not_null: true` into `CASE WHEN col IS NULL THEN 'ERR_NULL_VIOLATION'` in the single-pass query.
- [x] The SQL compiler translates numeric `range: { min: ..., max: ... }` into `CASE WHEN col < min OR col > max THEN 'ERR_OUT_OF_RANGE'`.
- [x] Calling `valkyrie.run(sql=..., contract=...)` returns a tuple/result containing clean Arrow records and DLQ Arrow records.
- [x] DLQ records retain all original raw columns plus four diagnostic metadata columns: `_error_column`, `_error_code`, `_raw_value`, `_ingestion_timestamp`.
- [x] Clean records exclude all diagnostic metadata columns.
- [x] End-to-end black-box test verifies correct partitioning of clean vs DLQ records on synthetic test batches.
