# 0001. Kiến Trúc Valkyrie Data Contract Engine: All-in-One DuckDB SQL Single-Pass Compiler

- **Trạng thái**: Accepted (Đã chấp thuận)
- **Ngày**: 2026-10-18
- **Tác giả**: Data Platform Engineering Team

---

## 1. Bối cảnh (Context)

Trong hệ sinh thái Data Lakehouse (Yggdrasil), Gate 1 và Gate 2 bảo vệ chất lượng dữ liệu từ tầng Bronze lên Silver. Sau các vòng phản biện kiến trúc chuyên sâu:
1. **Loại bỏ sự cồng kềnh**: Chuẩn ODCS v3 đầy đủ chứa nhiều phần văn xuôi (Pricing, SLA, Team, text descriptions) không thể thực thi được bằng máy.
2. **Nghẽn I/O từ chiến thuật Anti-Join truyền thống**: Ý tưởng đọc bảng nguồn để sinh DLQ rồi sau đó thực hiện `ANTI JOIN` với bảng nguồn lần 2 gây lãng phí 50% dung lượng I/O đọc đĩa (100M dòng phải đọc 2 lần), đồng thời phá vỡ tính đúng đắn khi dữ liệu không có primary key hoặc key bị `NULL`.
3. **Đơn giản hóa hạ tầng**: Việc nhồi nhét nhiều công cụ (Polars + DuckDB + Rust FFI) làm tăng kích thước cài đặt và rủi ro xung đột dependency trong các pipeline Airflow.

---

## 2. Quyết định Kiến trúc (Decisions)

### 2.1. Đóng gói All-in-One trên nền tảng Pure DuckDB Engine & Zero-Pydantic Dataclasses
- Valkyrie được đóng gói thành một thư viện Python nội bộ thuần túy (`pip install -e .`), chỉ phụ thuộc vào `duckdb`, `pyarrow`, và `pyyaml`.
- **Zero-Pydantic Architecture**: Hợp đồng contract được parse trực tiếp bằng Python Standard Library `@dataclass`, loại bỏ 100% Pydantic để triệt tiêu hoàn toàn rủi ro xung đột version (Pydantic v1 vs v2) trong các container Airflow/Data Platform.
- Nhúng DuckDB làm động cơ tính toán và quản lý I/O cốt lõi:
  - Tận dụng streaming engine đa luồng, SIMD và khả năng xử lý Out-of-Core (chạy file hàng chục GB không tràn RAM).
  - Khả năng đọc trực tiếp từ S3, Iceberg, Delta Lake, Parquet, hoặc in-memory DataFrame (Polars/Pandas).
  - Khả năng xuất file Parquet song song ở tốc độ giới hạn của ổ cứng NVMe thông qua lệnh gốc `COPY ... TO` của DuckDB.

### 2.2. Hợp đồng Kỹ thuật Thuần túy (No Fluff) & Bắt buộc `owner`
- `contract.yaml` chỉ chứa 100% các quy tắc máy kiểm chứng được (`schema`, `policies`, `custom_rules`). Nghiêm cấm các trường văn xuôi định tính.
- Trường `owner` là bắt buộc ở cấp root của contract để đảm bảo trách nhiệm giải trình khi pipeline bị vi phạm.

### 2.3. Trình biên dịch SQL Vectorized Single-Pass & Materialized Temporary Table
- Valkyrie dịch `contract.yaml` thành các biểu thức SQL CTEs với `CASE WHEN`:
  - Đánh dấu `_error_code`, `_error_column`, `_raw_value` theo chính sách **First-Violation Short-Circuit** (1 dòng lỗi = 1 bản ghi DLQ).
  - Kết quả được lưu tạm bằng `CREATE TEMPORARY TABLE _valkyrie_annotated` trong RAM và giải phóng dứt điểm trong khối `finally`. Nhờ vậy, câu query nguồn chỉ được quét vật lý đúng một lần (True Single-Pass), triệt tiêu hiện tượng quét 2 lần của `TEMPORARY VIEW`.
  - Phân tách dứt khoát:
    - `clean`: các dòng có `_error_code IS NULL`.
    - `dlq`: các dòng có `_error_code IS NOT NULL`.
  - Tiết kiệm 50% dung lượng I/O đọc đĩa so với chiến thuật Anti-Join.

### 2.4. Trải nghiệm Data Engineer Tối giản & Tích Hợp Hệ Sinh Thái Yggdrasil
- **Đầu vào (Reading)**: DuckDB đọc trực tiếp từ Parquet, DuckLake, hoặc Iceberg tables tầng Bronze.
- **Phán xét (Gatekeeper)**: Valkyrie lọc Single-Pass, phân tách tập sạch và tập lỗi (DLQ).
- **Đầu ra Ghi dữ liệu (Write Path)**:
  - **Dữ liệu sạch (`clean_df`)**: Được xuất ra định dạng Apache Arrow RecordBatch / PyArrow Table chuẩn, sẵn sàng bàn giao cho **Bifrost Proxy** (qua PyIceberg REST Catalog) để thực hiện commit ACID vào bảng Apache Iceberg tầng Silver.
  - **Dữ liệu lỗi (`dlq_df`)**: Tự động ghi thẳng ra file `dlq.parquet` trên S3/MinIO để phục vụ kiểm toán và chẩn đoán.
- **Zero-Boilerplate Cloud Storage**: Khi phát hiện tiền tố `s3://` trong câu lệnh SQL hoặc đường dẫn xuất file, Valkyrie tự động:
  - Nạp extension `httpfs` của DuckDB.
  - Tự động map các biến môi trường chuẩn của container/Airflow vào DuckDB Secret:
    - `AWS_ACCESS_KEY_ID`
    - `AWS_SECRET_ACCESS_KEY`
    - `AWS_REGION` / `AWS_DEFAULT_REGION`
    - `AWS_ENDPOINT_URL` (hỗ trợ MinIO local: `minio:9000`)
    - `AWS_USE_SSL` / `S3_USE_SSL` (`false` nếu chạy MinIO dev)
    - `S3_URL_STYLE` (`path` cho MinIO hoặc `vhost` cho AWS S3 chuẩn)
- Hỗ trợ cả 3 hình thái sử dụng:
  1. **Python API trả về Arrow/DataFrame**: `clean_df, dlq_df = valkyrie.run(sql="...", contract="...")`
  2. **Pipeline tự động xuất ra đĩa/S3**: `valkyrie.execute(sql="...", contract="...", write_clean="...", write_dlq="...")`
  3. **CLI One-Liner**: `valkyrie run --sql "..." --contract "..."`
- **Lộ trình tương lai (Roadmap v0.2+)**: Hỗ trợ tùy chọn ghi trực tiếp vào Iceberg REST Catalog (`write_iceberg_table="silver.orders"`, `catalog="bifrost"`) thông qua Lazy Import để bảo vệ tính Zero-Bloat của core engine trong khi mang lại trải nghiệm 1-line pipeline hoàn chỉnh cho Data Engineer.

### 2.5. Cơ chế Cầu dao Tự ngắt (Circuit Breaker) & Actionable Alert Banner
- Nếu `error_rate > max_error_rate` hoặc gặp `quarantine_action == "FAIL_FAST"`, ném `ContractBreachException`.
- Render khung cảnh báo trực quan to rõ: Tên dataset, `owner`, tỷ lệ lỗi, top vi phạm. Lưu kèm thuộc tính `e.dlq_df` để pipeline kịp lưu trữ trước khi thoát.

---

## 3. Hệ quả (Consequences)

### Tích cực (Positive)
- **Tốc độ tối thượng**: Single-pass đọc đĩa 1 lần, nhanh gấp đôi Anti-Join, stream trực tiếp Out-of-Core không lo OOM.
- **Dependency siêu gọn nhẹ**: Chỉ cần `duckdb` và `pyyaml` (< 50MB), cài đặt chạy ngay ở mọi môi trường.
- **Khả năng Debug tuyệt vời**: DE có thể in câu lệnh SQL đã biên dịch (`valkyrie.to_sql(...)`) để kiểm tra hoặc chạy thử trong DBeaver/DuckDB CLI.

### Đánh đổi (Trade-offs)
- Phụ thuộc vào cú pháp SQL và hàm của DuckDB (DuckDB hiện là chuẩn mực SQL phân tích in-process mạnh nhất).
