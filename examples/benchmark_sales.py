"""Load test and benchmark script for Valkyrie Data Contract Engine."""

import os
import sys
import time
import psutil
import duckdb
from valkyrie import execute, load_contract

def run_benchmark():
    contract_path = "sales_contract.yaml"
    data_path = "sales_300m.parquet"
    clean_path = "sales_clean.parquet"
    dlq_path = "sales_dlq.parquet"

    print("=" * 60)
    print("VALKYRIE LOAD TESTING BENCHMARK (50x DUPLICATED DATASET)")
    print("=" * 60)

    if not os.path.exists(data_path):
        print(f"File {data_path} not found!")
        return

    file_size_gb = os.path.getsize(data_path) / (1024 ** 3)
    print(f"Data file: {data_path} ({file_size_gb:.2f} GB)")

    # Count rows
    con = duckdb.connect()
    total_rows = con.execute(f"SELECT count(*) FROM '{data_path}'").fetchone()[0]
    print(f"Total Rows: {total_rows:,}")
    print("Contract: sales_contract.yaml")
    print("-" * 60)

    # Clean up previous benchmark outputs if any
    for p in [clean_path, dlq_path]:
        if os.path.exists(p):
            os.remove(p)

    proc = psutil.Process(os.getpid())
    mem_before_mb = proc.memory_info().rss / (1024 * 1024)

    print("Executing Valkyrie out-of-core pipeline with streaming parquet sinks...")
    t0 = time.perf_counter()

    result = execute(
        sql=f"SELECT * FROM '{data_path}'",
        contract=contract_path,
        write_clean=clean_path,
        write_dlq=dlq_path,
    )

    t1 = time.perf_counter()
    duration_sec = t1 - t0
    mem_after_mb = proc.memory_info().rss / (1024 * 1024)
    m = result.metrics
    throughput = m["total_rows"] / duration_sec if duration_sec > 0 else 0

    print("=" * 60)
    print("BENCHMARK RESULTS")
    print("=" * 60)
    print(f"Total Processed Rows : {m['total_rows']:,}")
    print(f"Clean Rows           : {m['clean_rows']:,}")
    print(f"Quarantined (DLQ)    : {m['error_rows']:,}")
    print(f"Error Rate           : {m['error_rate'] * 100:.4f}%")
    print(f"DuckDB Query Time    : {m['duration_ms']:.1f}ms ({m['duration_ms']/1000:.2f}s)")
    print(f"Total Wall Time      : {duration_sec:.2f}s")
    print(f"Throughput           : {throughput:,.0f} rows/sec ({throughput / 1e6:.2f} M rows/sec)")
    print(f"Python RAM Delta     : {mem_after_mb - mem_before_mb:.2f} MB (Current: {mem_after_mb:.2f} MB)")
    print("=" * 60)

    # Validate output files exist
    if os.path.exists(clean_path):
        clean_size_gb = os.path.getsize(clean_path) / (1024 ** 3)
        print(f"Clean parquet created: {clean_path} ({clean_size_gb:.2f} GB)")
    if os.path.exists(dlq_path):
        dlq_size_kb = os.path.getsize(dlq_path) / 1024
        print(f"DLQ parquet created: {dlq_path} ({dlq_size_kb:.2f} KB)")

if __name__ == "__main__":
    run_benchmark()
