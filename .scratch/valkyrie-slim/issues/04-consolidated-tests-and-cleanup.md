# 04: Consolidated Testing Suite & Final Repo Slim-Down

**What to build:** Consolidate the 10 repetitive test files (945 lines) into a single concise `tests/test_valkyrie.py` (~200 lines) testing the single public seam (`to_sql`, `run`, `execute`, CLI). Verify total codebase under `src/valkyrie/` is ~200-250 lines and 100% of tests pass cleanly.

**Blocked by:** 03: 100% DuckDB-Native Execution, SQL Aggregations & Direct COPY TO

**Status:** completed

- [x] Consolidate 10 test files into `tests/test_valkyrie.py`.
- [x] Delete redundant test files.
- [x] 100% tests pass via `python -m pytest`.
- [x] Total lines in `src/valkyrie/` is <= 250 lines.
- [x] Clean, lean, professional repository structure.
