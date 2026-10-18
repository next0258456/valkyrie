# 02: Robust Type Classification & Pre-Flight Schema Verification

**What to build:** In `engine.py`, rewrite `_classify_type` using robust regex / token boundary matching. Handle fixed-size arrays (`INT[3]`), intervals (`INTERVAL`), and signed/unsigned numbers (`UINTEGER`, `UBIGINT`, etc.) cleanly. Verify pre-flight checks against these types.

**Blocked by:** 01: Lean Contract Loader with Strict Whitelisting & Schema Enforcement

**Status:** completed

- [x] `INTERVAL` is classified as `INTERVAL` (not `NUMERIC`).
- [x] `INT[3]` and `LIST` types are classified as `LIST` (not `NUMERIC`).
- [x] Unsigned integers (`UBIGINT`, `UINTEGER`, etc.) are recognized as `NUMERIC`.
- [x] Pre-flight checks pass for matching unsigned/signed numerics and fail with `ERR_SCHEMA_TYPE_MISMATCH` for actual mismatches.
