# 02: Advanced Column Constraints (enum, pattern, unique)

**What to build:** Extend the single-pass compiler and engine to support categorical whitelists (`enum`), regular expression matching (`pattern`), and business key uniqueness (`unique`). Invalid records are tagged with accurate error codes, maintaining the First-Violation Short-Circuit guarantee when multiple constraints fail simultaneously.

**Blocked by:** 01: Tracer Bullet - Core Single-Pass Contract Engine (not_null, range)

**Status:** completed

- [x] The SQL compiler translates `enum: [...]` into `CASE WHEN col NOT IN (...) THEN 'ERR_INVALID_ENUM'`.
- [x] The SQL compiler translates `pattern: "..."` into `CASE WHEN NOT regexp_matches(col, '...') THEN 'ERR_REGEX_MISMATCH'`.
- [x] The SQL compiler translates `unique: true` into `CASE WHEN count(*) OVER (PARTITION BY col) > 1 THEN 'ERR_DUPLICATE_KEY'`.
- [x] When a record violates multiple constraints simultaneously across different columns, only the first encountered violation is recorded in `_error_code` and `_error_column` (First-Violation Short-Circuit).
- [x] End-to-end black-box test verifies duplicate key detection, regex validation, and enum validation across diverse string and numeric datasets.
