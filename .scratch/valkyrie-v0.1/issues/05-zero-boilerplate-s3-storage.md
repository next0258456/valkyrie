# 05: Zero-Boilerplate Storage (S3/MinIO) & Pipeline Persistence (valkyrie.execute)

**What to build:** Automatic configuration of DuckDB Cloud Storage (S3/MinIO) using standard AWS environment variables, plus an all-in-one execution function `valkyrie.execute(sql=..., contract=..., write_clean=..., write_dlq=...)` that executes validation and streams the clean and DLQ datasets directly to target Parquet storage paths.

**Blocked by:** 01: Tracer Bullet - Core Single-Pass Contract Engine (not_null, range), 04: Circuit Breaker & Actionable Breach Alerting

**Status:** completed

- [x] Engine detects `s3://` or `minio://` prefixes in input SQL or output write paths and automatically installs/loads the DuckDB `httpfs` extension.
- [x] Engine automatically reads standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT_URL`, `S3_USE_SSL`, `S3_URL_STYLE`) and configures a DuckDB S3 secret without manual Python configuration.
- [x] `valkyrie.execute()` writes the clean dataset to `write_clean` (if specified) in Parquet format.
- [x] `valkyrie.execute()` writes the DLQ dataset to `write_dlq` (if specified) in Parquet format whenever invalid records exist.
- [x] If a circuit breach occurs, `valkyrie.execute()` still writes the DLQ to `write_dlq` before raising `ContractBreachException`.
- [x] End-to-end black-box test verifies zero-boilerplate reading from and writing to local/simulated S3 directory endpoints.
