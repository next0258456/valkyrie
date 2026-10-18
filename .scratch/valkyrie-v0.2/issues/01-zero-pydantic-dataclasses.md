# 01: Zero-Pydantic Pure Dataclasses & Flexible Range Parsing

**What to build:** Remove `pydantic` entirely from `pyproject.toml` and codebase. Rewrite `src/valkyrie/contract.py` using Python's standard library `dataclasses`. Add support for flexible range syntax (`range: [min, max]` list as well as dict `{min, max}`). Enforce mandatory `owner` and `dataset`, strictly reject non-executable prose rules (`type: text`), and validate `quarantine_action` as `"DLQ"` or `"FAIL_FAST"`.

**Blocked by:** None (can start immediately)

**Status:** completed

- [x] `pyproject.toml` no longer lists `pydantic` in `dependencies`.
- [x] `src/valkyrie/contract.py` uses `@dataclass` from standard library with zero third-party validation dependencies.
- [x] Parsing YAML raises `ValueError` if `owner` or `dataset` is missing or empty.
- [x] Parsing YAML raises `ValueError` if non-executable prose rules (`type: text`) are present in column `quality`.
- [x] Parsing YAML supports both list syntax `range: [0.01, 100.0]` and dict syntax `range: {min: 0.01, max: 100.0}`.
- [x] `quarantine_action` only accepts `"DLQ"` or `"FAIL_FAST"`, raising `ValueError` on invalid values.
- [x] All existing and new contract parsing tests pass cleanly.
