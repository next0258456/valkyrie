# 04: Anti-Smell Refactoring & Alert Banner Formatting

**What to build:** Refactor `compiler.py` to eliminate duplicated SQL CASE-building code via a single helper. Move ANSI breach alert banner rendering out of `engine.py` into `format_breach_banner()` in `exceptions.py`. Update `ADR-0001` documentation to record the pure `duckdb` + `pyyaml` zero-pydantic dependency model. Verify the entire test suite passes cleanly with 100% green tests.

**Blocked by:** 02: Pre-Flight Schema & Type Conformance Check (strict_schema), 03: True Single-Pass Table & Native DuckDB S3 Export (COPY TO)

**Status:** completed

- [x] `compiler.py` extracts a shared `build_case_when(...)` helper, eliminating triplicated CASE-statement generator blocks.
- [x] ANSI alert banner formatting is moved to `format_breach_banner()` in `valkyrie.exceptions`.
- [x] `docs/adr/0001-valkyrie-duckdb-sql-contract-engine.md` is updated to confirm standard library `dataclasses` and zero-pydantic architecture.
- [x] All test suites (existing v0.1 + new v0.2 tests) pass 100% cleanly.

