# Valkyrie v0.1 Implementation Specification

*Labels: ready-for-agent*

## Problem Statement

Trong kiến trúc Data Lakehouse hiện đại (Yggdrasil), các Data Engineer gặp phải nhiều rào cản khi bảo vệ tính toàn vẹn của dữ liệu tại Gate 1 và Gate 2:
1. **Sự cồng kềnh và bất khả thi của chuẩn ODCS đầy đủ**: Các file hợp đồng dữ liệu theo chuẩn Open Data Contract Standard (ODCS) chứa quá nhiều trường phi kỹ thuật (Pricing, SLA, Team, Roles, Infrastructure) cùng các đoạn văn xuôi định tính (`type: text`) mà máy tính không thể thực thi tự động.
2. **Chi phí I/O khổng lồ từ cơ chế Anti-Join truyền thống**: Ý tưởng quét bảng dữ liệu nguồn để bắt lỗi sang Dead Letter Queue (DLQ) rồi sau đó thực hiện phép `ANTI JOIN` với bảng nguồn lần thứ 2 làm tăng gấp đôi dung lượng đọc đĩa (100 triệu dòng phải đọc 2 lần), gây nghẽn băng thông NVMe/S3 và có nguy cơ làm mất mát dữ liệu sạch nếu dữ liệu nguồn thiếu khóa chính hoặc khóa chính bị `NULL`.
3. **Cấu hình phức tạp và xung đột môi trường**: Các công cụ truyền thống (Spark, Great Expectations) quá nặng nề, khởi động chậm và đòi hỏi cấu hình hạ tầng phức tạp, trong khi việc tự dựng runtime Rust FFI/C++ lại gây khó khăn cho việc triển khai trên các Airflow Worker thông thường.
4. **Thiếu vắng cơ chế truy vết trách nhiệm tức thì**: Khi dữ liệu bẩn tràn vào làm sập pipeline, kỹ sư vận hành không biết liên hệ ai vì thông tin chủ sở hữu (`owner`) bị phân tán hoặc không được tích hợp vào thông báo lỗi.

## Solution

**Valkyrie v0.1** là một động cơ thực thi hợp đồng dữ liệu All-in-One được đóng gói dưới dạng thư viện Python nội bộ siêu nhẹ, nhúng trực tiếp **DuckDB** làm lõi tính toán vector hóa và quản lý I/O:
1. **Pure Executable Contract (No Fluff)**: Hợp đồng chỉ chứa 100% các quy tắc kỹ thuật có thể chạy bằng máy (Schema, Physical Constraints, Custom Rules) và trường `owner` bắt buộc.
2. **Single-Pass SQL Compiler**: Biên dịch hợp đồng thành câu truy vấn SQL CTEs tối ưu cho DuckDB với cấu trúc `CASE WHEN` phân nhánh lỗi trong một lượt đọc duy nhất (Single-Pass), tiết kiệm 50% dung lượng I/O đọc đĩa so với Anti-Join và bảo toàn tương quan 1:1 giữa dòng lỗi và bản ghi DLQ (First-Violation Short-Circuit).
3. **Zero-Boilerplate Cloud Storage**: Tự động nhận diện tiền tố `s3://` hoặc `minio://` để nạp extension `httpfs` và ánh xạ các biến môi trường AWS/MinIO chuẩn vào DuckDB Secret mà không cần cấu hình thủ công trong code.
4. **Trải nghiệm 1 Dòng Lệnh (1-Line DX)**: Cung cấp API trực diện cho Data Engineer (`valkyrie.run(sql=..., contract=...)`), tự động phân tách dữ liệu sạch (sẵn sàng nạp vào Iceberg qua Bifrost Proxy) và dữ liệu rác (ghi vào DLQ S3).
5. **Cầu dao Tự ngắt (Circuit Breaker)**: Tự động ngắt pipeline và in khung cảnh báo trực quan nêu rõ danh tính `owner`, tỷ lệ lỗi thực tế và top các mã lỗi vi phạm khi vượt ngưỡng `max_error_rate`.

## User Stories

### Nhóm Quản Trị Hợp Đồng & Cấu Hình (Contract & Configuration)
1. As a Data Engineer, I want to declare physical data contracts in a concise `contract.yaml` file, so that I can validate datasets without writing repetitive boilerplate code.
2. As a Data Platform Engineer, I want the contract parser to strictly reject non-executable prose (such as `type: text` quality checks), so that all rules in the repository are guaranteed to be machine-verifiable.
3. As a Data Engineer, I want a mandatory `owner` field at the root of `contract.yaml`, so that accountability is clear when data quality is compromised.
4. As a Data Engineer, I want to specify operational policies (`strict_schema`, `max_error_rate`, `quarantine_action`), so that pipeline behavior during errors is declaratively controlled.
5. As a Data Analyst, I want to declare data types for columns (`BIGINT`, `DOUBLE`, `VARCHAR`, `TIMESTAMP_TZ`, `BOOLEAN`, `STRUCT[]`), so that incoming records with incompatible types are caught early.
6. As a Data Engineer, I want to enforce `not_null: true` constraints on critical fields, so that null values in primary or mandatory attributes are immediately quarantined.
7. As a Data Engineer, I want to declare numeric boundary constraints (`min`, `max`, `range: [min, max]`), so that negative prices, zero quantities, or out-of-bound percentages are trapped.
8. As a Data Analyst, I want to define static whitelist constraints (`enum: [...]`), so that unexpected categorical codes or statuses are flagged.
9. As a Data Engineer, I want to define regex pattern constraints (`pattern: "..."`), so that malformed emails, phone numbers, or UUIDs are identified.
10. As a Data Engineer, I want to enforce uniqueness constraints (`unique: true`), so that duplicate business keys are flagged using DuckDB vectorized window functions without external bitset overhead.
11. As a Data Engineer, I want to define cross-column and nested mathematical rules (`custom_rules[].expression`), so that complex rules like `order_total == list_sum(line_items.price)` can be evaluated inside the row.

### Nhóm Thực Thi & Single-Pass Isolation (Execution & Single-Pass Isolation)
12. As a Data Engineer, I want Valkyrie to compile the contract into DuckDB SQL expressions, so that validation runs at line-rate NVMe speed using DuckDB's vectorized streaming engine.
13. As a Platform Administrator, I want Valkyrie to evaluate all rules in a Single-Pass scan over the raw data, so that disk I/O is reduced by 50% compared to two-pass Anti-Join workflows.
14. As a Data Engineer, I want invalid records to be assigned technical error codes (`ERR_NULL_VIOLATION`, `ERR_OUT_OF_RANGE`, etc.) based on First-Violation Short-Circuiting, so that each bad record generates exactly one entry in the DLQ.
15. As a Data Auditor, I want every DLQ record to preserve all original raw columns and attach standard audit columns (`_error_column`, `_error_code`, `_raw_value`, `_ingestion_timestamp`), so that upstream producers can troubleshoot issues easily.
16. As a Data Engineer, I want clean records (`_error_code IS NULL`) to automatically omit all diagnostic metadata columns, so that the clean dataset is pure and ready for downstream tables.
17. As an Airflow Operator, I want Valkyrie to process large datasets Out-of-Core without memory blowups, so that 50GB–100GB batches can be processed reliably on resource-constrained worker nodes.

### Nhóm Tích Hợp Đám Mây & Hệ Sinh Thái Yggdrasil (Cloud Storage & Lakehouse Integration)
18. As a DevOps Engineer, I want Valkyrie to automatically read standard environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, etc.), so that connecting to S3 or MinIO requires zero code changes.
19. As an S3 Administrator, I want Valkyrie to automatically load DuckDB's `httpfs` extension and configure DuckDB S3 Secrets, so that users do not have to execute low-level database configurations.
20. As a Data Lakehouse Architect, I want Valkyrie to support reading directly from DuckLake or Parquet sources, so that ingestion from the Bronze layer is seamless.
21. As a Data Engineer, I want Valkyrie to return clean data as an Apache Arrow Table, so that it can be passed directly to Bifrost Proxy for ACID append into Apache Iceberg tables.
22. As a Data Engineer, I want Valkyrie to write DLQ data directly to S3/MinIO Parquet paths, so that bad records are preserved in object storage for auditing.

### Nhóm Trải Nghiệm Lập Trình & Cảnh Báo (Developer Experience & Alerting)
23. As a Python Developer, I want to run contract enforcement with a single function call `valkyrie.run(sql=..., contract=...)`, so that the learning curve is nearly zero.
24. As an Airflow Developer, I want an execution helper `valkyrie.execute(sql=..., contract=..., write_clean=..., write_dlq=...)`, so that an entire validation step can be executed in one line within a PythonOperator.
25. As a Terminal User, I want a CLI command `valkyrie run --sql "..." --contract "..."`, so that contracts can be tested and executed from bash scripts and CI pipelines.
26. As a Data Engineer, I want to inspect the generated SQL via `valkyrie.to_sql(contract=..., source_sql=...)`, so that I can debug and inspect the execution logic in DBeaver or DuckDB CLI.
27. As an On-Call Engineer, I want a high-visibility terminal alert banner when a contract breach occurs, displaying the dataset, owner, threshold, actual error rate, and top violation codes.
28. As an Airflow Operator, I want a `ContractBreachException` to be raised when `error_rate > max_error_rate` or on `FAIL_FAST`, so that the Airflow task immediately fails and prevents corrupt data from propagating.
29. As a Developer handling breaches gracefully, I want `ContractBreachException` to include `.dlq_df` and `.metrics` attributes, so that I can archive the DLQ even within an exception handler block.

## Implementation Decisions

### 1. Kiến Trúc Gói & Module Hệ Thống
Dự án được xây dựng dưới dạng thư viện Pure Python chuẩn mực:
- **`valkyrie.contract`**: Phân tích cú pháp `contract.yaml` bằng Pydantic v2. Định nghĩa cấu trúc `ValkyrieContract`, `ColumnContract`, `PolicyConfig`, `CustomRule`. Tự động từ chối các quy tắc không thể thực thi bằng máy và ép buộc có trường `owner`.
- **`valkyrie.compiler`**: Chịu trách nhiệm dịch `ValkyrieContract` thành câu truy vấn SQL CTEs của DuckDB. Sinh các mệnh đề `CASE WHEN` tuần tự theo thứ tự cột cho `_error_code`, `_error_column`, và `_raw_value`.
- **`valkyrie.engine`**: Quản trị vòng đời DuckDB in-memory connection, tự động cấu hình extension `httpfs` và S3 Secrets từ môi trường hệ thống, thực thi câu lệnh SQL, đo lường metrics thời gian và số lượng dòng, kiểm tra Circuit Breaker, và trả về đối tượng `ValidationResult`.
- **`valkyrie.exceptions`**: Định nghĩa `ContractBreachException` với phương thức render bảng cảnh báo trực quan ANSI color to rõ.
- **`valkyrie.cli`**: Điểm vào dòng lệnh sử dụng `argparse` hỗ trợ chạy trực tiếp từ terminal.

### 2. Cấu Trúc Bản Đặc Tả Kỹ Thuật (Contract Schema Shape)
```yaml
version: "v1"
dataset: "orders"
owner: "finance-team@yggdrasil.local"

policies:
  strict_schema: true
  max_error_rate: 0.05
  quarantine_action: "DLQ"

schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
    unique: true

  - name: "order_total"
    type: "DOUBLE"
    not_null: true
    range: { min: 0.01, max: 1000000.0 }

  - name: "order_status"
    type: "VARCHAR"
    not_null: true
    enum: ["PENDING", "PAID", "CANCELLED"]

  - name: "customer_email"
    type: "VARCHAR"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

custom_rules:
  - name: "order_total_matches_items"
    expression: "order_total == list_sum(line_items.price)"
    error_code: "ERR_TOTAL_MISMATCH"
```

### 3. Cấu Trúc Câu Lệnh SQL Single-Pass Biên Dịch (Compiled SQL Blueprint)
```sql
WITH raw AS (
    {source_sql}
),
annotated AS (
    SELECT 
        *,
        CASE 
            WHEN {cond_1} THEN '{code_1}'
            WHEN {cond_2} THEN '{code_2}'
            ...
            ELSE NULL 
        END AS _error_code,
        CASE 
            WHEN {cond_1} THEN '{col_1}'
            WHEN {cond_2} THEN '{col_2}'
            ...
            ELSE NULL 
        END AS _error_column,
        CASE 
            WHEN {cond_1} THEN CAST({col_1} AS VARCHAR)
            WHEN {cond_2} THEN CAST({col_2} AS VARCHAR)
            ...
            ELSE NULL 
        END AS _raw_value,
        CURRENT_TIMESTAMP AS _ingestion_timestamp
    FROM raw
)
```

### 4. Tự Động Hóa Quản Trị Bí Mật Lưu Trữ (S3 Storage Automation)
Khi phát hiện `s3://` hoặc `minio://`:
Valkyrie tự động nạp `INSTALL httpfs; LOAD httpfs;` và sinh lệnh `CREATE SECRET` của DuckDB với các biến:
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT_URL`, `S3_USE_SSL`, `S3_URL_STYLE`.

## Testing Decisions

### 1. Triết Lý Kiểm Thử (Black-Box Testing at Highest Seam)
Tuân thủ nguyên tắc kiểm thử hộp đen tại ranh giới cao nhất có thể:
- Không viết test can thiệp vào các hàm phụ trợ bên trong trình tạo chuỗi SQL.
- Mọi bài kiểm thử đều kích hoạt qua **Public API Seam duy nhất**: gọi `valkyrie.run()` hoặc `valkyrie.execute()` với file `contract.yaml` và câu truy vấn SQL đầu vào, sau đó assert đối tượng kết quả trả về (`ValidationResult`) hoặc file Parquet được sinh ra trên đĩa.

### 2. Các Mod-un Được Kiểm Thử (Tested Behaviors)
- **Contract Schema Parsing**: Xác minh hợp đồng hợp lệ nạp thành công; hợp đồng thiếu `owner` hoặc chứa văn xuôi định tính lập tức bị từ chối với lỗi rõ ràng.
- **Physical Column Constraints**: Kiểm tra đầy đủ các loại ràng buộc:
  - `not_null`: Bắt chính xác dòng null và gán `ERR_NULL_VIOLATION`.
  - `range`: Bắt chính xác giá trị nhỏ hơn `min` hoặc lớn hơn `max` với `ERR_OUT_OF_RANGE`.
  - `enum`: Bắt chính xác chuỗi ngoài whitelist với `ERR_INVALID_ENUM`.
  - `pattern`: Bắt chính xác chuỗi sai định dạng regex với `ERR_REGEX_MISMATCH`.
  - `unique`: Bắt chính xác các bản ghi trùng lặp khóa với `ERR_DUPLICATE_KEY`.
- **Custom Mathematical Rules**: Kiểm tra biểu thức đa cột và list functions (như `list_sum`).
- **First-Violation Short-Circuit**: Kiểm tra dòng vi phạm nhiều lỗi cùng lúc chỉ xuất hiện đúng 1 lần trong DLQ với mã lỗi đầu tiên.
- **Circuit Breaker Mechanics**: Xác minh ngưỡng `max_error_rate` tự ngắt pipeline và ném `ContractBreachException` khi tỷ lệ lỗi vượt ngưỡng; xác minh chế độ `FAIL_FAST`.
- **Storage & End-to-End Execution**: Tạo dữ liệu mẫu trong DuckDB in-memory, chạy qua pipeline và kiểm tra tính toàn vẹn của Arrow table sạch và DLQ table.

### 3. Tiền Lệ Kiểm Thử (Prior Art)
Kế thừa mô hình kiểm thử Black-Box End-to-End đã thành công tại `test_e2e_pyiceberg_ducklake.py` của dự án Bifrost Proxy (gửi dữ liệu qua public seam và verify tính toàn vẹn của kết quả đầu ra).

## Out of Scope

1. **Không xây dựng Query Engine phân tán**: Valkyrie không thay thế Trino, Spark hay BigQuery; nó hoạt động in-process bên trong Python runtime.
2. **Không tự can thiệp sửa đổi giá trị dữ liệu (No Data Mutation)**: Valkyrie tuân thủ nghiêm ngặt nguyên tắc của Yggdrasil: *No Reverse Data Flow*. Nó chỉ phán xét và phân loại (Sạch thì cho đi tiếp, Lỗi thì ném sang DLQ), tuyệt đối không tự ý sửa giá trị dòng dữ liệu.
3. **Không tự quản lý Transaction Commit cho Iceberg**: Dữ liệu sạch được trả về dưới dạng Arrow Table để **Bifrost Proxy** chịu trách nhiệm thực hiện commit ACID vào Apache Iceberg.
4. **Không viết mã nguồn Rust cấp thấp trong v0.1**: Tận dụng 100% C++ vectorized engine của DuckDB.

## Further Notes

- **Tiến trình nâng cấp Phase 2**: Khi hệ thống mở rộng sang các kịch bản streaming thời gian thực có yêu cầu độ trễ P99 < 5 micro-giây trực tiếp từ Redpanda, bản Container LMAX Disruptor (`valkyrie-daemon` viết bằng Rust) sẽ được kích hoạt dựa trên các giao thức hợp đồng đã chuẩn hóa ở Phase 1.
