# 03: True Zero-OOM on Breach & DRY Metrics Construction

**What to build:** In `engine.py`, eliminate the OOM risk on breach when `materialize_arrow=False`. If `materialize_arrow=False` and a breach occurs, do NOT pull the entire multi-gigabyte DLQ into Python memory; provide `dlq_df=None` (or top sample). Eliminate the duplicated 12-line `metrics` dict creation.

**Blocked by:** 02: Robust Type Classification & Pre-Flight Schema Verification

**Status:** completed

- [x] On breach with `materialize_arrow=False` (e.g. from `execute`), `dlq_arrow` is not loaded into memory (`None` or small sample).
- [x] Large breach datasets stream directly to `write_dlq` file without OOM.
- [x] `metrics` dictionary is assembled once before breach checks (DRY).
