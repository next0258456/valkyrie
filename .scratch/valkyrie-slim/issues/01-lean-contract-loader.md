# 01: Lean Dictionary-Based Contract Loader

**What to build:** Replace the 156-line, 6-dataclass hierarchy in `contract.py` with a lightweight, clean contract loader (~45 lines). Strictly validate mandatory fields (`dataset`, `owner`, `schema`), normalize `range` ([min, max] or {min, max}), and reject all prohibited prose/fluff keys (`sla`, `team`, `pricing`, `description`, `quality`).

**Blocked by:** None (can start immediately)

**Status:** completed

- [x] `load_contract()` parses YAML directly into a lean, normalized structure.
- [x] Missing `owner`, `dataset`, or `schema` raises ValueError.
- [x] Prohibited fluff keys are strictly rejected.
- [x] `range` constraints are normalized cleanly without complex class hierarchies.
- [x] File size of `contract.py` drops to ~40-50 lines with 0 Pydantic.
