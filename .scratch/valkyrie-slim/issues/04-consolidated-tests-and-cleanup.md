# 04: Restore Dropped Edge-Case Tests & Verify 100% Green

**What to build:** In `tests/test_valkyrie.py`, add back comprehensive test coverage for: `unique` duplicate key violations, `FAIL_FAST` policy, first-violation short-circuit precedence, range dict syntax, S3 secret auto-configuration, whitelist validation, and zero-OOM breach execution.

**Blocked by:** 03: True Zero-OOM on Breach & DRY Metrics Construction

**Status:** completed

- [x] Add test for `unique` duplicate key violation (`ERR_DUPLICATE_KEY`).
- [x] Add test for `quarantine_action: FAIL_FAST`.
- [x] Add test for First-Violation Short-Circuit precedence.
- [x] Add test for `range: {min: x, max: y}` dictionary syntax.
- [x] Add test for `configure_cloud_storage` S3 credentials.
- [x] Add test for zero-OOM breach in `execute()` (`e.dlq_df is None`).
- [x] Add test for strict root and column whitelisting.
- [x] 100% of tests pass cleanly.
