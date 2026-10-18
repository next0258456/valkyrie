# 04: Circuit Breaker & Actionable Breach Alerting

**What to build:** Enforce contract operational policies (`policies.max_error_rate`, `policies.quarantine_action`). When an unrecoverable breach occurs, halt the pipeline immediately by raising `ContractBreachException` and print an actionable alert banner displaying the dataset, owner, actual vs allowed error rate, and top violation codes. Preserve DLQ data on the exception object for safe recovery.

**Blocked by:** 01: Tracer Bullet - Core Single-Pass Contract Engine (not_null, range), 02: Advanced Column Constraints (enum, pattern, unique)

**Status:** completed

- [x] Engine calculates metrics after single-pass execution: `total_rows`, `clean_rows`, `error_rows`, `error_rate`, `duration_ms`.
- [x] If `quarantine_action == "FAIL_FAST"` and any invalid records exist, raise `ContractBreachException`.
- [x] If `error_rate > max_error_rate`, raise `ContractBreachException`.
- [x] `ContractBreachException` renders a high-visibility terminal banner with ANSI formatting clearly highlighting `owner`, `dataset`, `max_error_rate`, actual `error_rate`, and the top violation codes.
- [x] `ContractBreachException` attaches `.dlq_df` and `.metrics` properties so callers can inspect or archive bad records within an exception handler.
- [x] Passing `raise_on_breach=False` to `valkyrie.run()` bypasses raising the exception and allows the caller to check `result.is_passed`.
- [x] End-to-end black-box test verifies circuit breaker tripping on batches exceeding error rate thresholds.
