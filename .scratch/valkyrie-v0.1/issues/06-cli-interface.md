# 06: CLI Interface (valkyrie run & valkyrie to-sql)

**What to build:** Command-line interface allowing data engineers and CI/CD pipelines to run contract validations and inspect compiled SQL directly from the terminal without writing Python code.

**Blocked by:** 04: Circuit Breaker & Actionable Breach Alerting, 05: Zero-Boilerplate Storage (S3/MinIO) & Pipeline Persistence (valkyrie.execute)

**Status:** completed

- [x] CLI command `valkyrie run --sql "..." --contract "..."` executes the contract validation.
- [x] CLI command accepts optional `--write-clean` and `--write-dlq` arguments to persist output Parquet datasets.
- [x] Process exits with code 0 on successful validation.
- [x] Process exits with code 1 and prints the breach alert banner when Circuit Breaker trips.
- [x] CLI command `valkyrie to-sql --contract "..." --sql "..."` compiles and prints the formatted DuckDB SQL query to stdout for external inspection/debugging.
- [x] End-to-end black-box test runs CLI subprocesses and asserts expected exit codes and stdout outputs.
