# Cẩm Nang Kỹ Sư Dữ Liệu: Hướng Dẫn Sử Dụng Valkyrie Data Contract Engine

Tài liệu này dành cho Data Engineers (DE) để thiết kế, viết Hợp đồng Dữ liệu (Data Contract) kỹ thuật và vận hành Valkyrie trên toàn bộ các môi trường từ phân tích tương tác (Jupyter) đến Pipeline tự động hóa (Airflow/Dagster) và Cloud Storage (S3/MinIO).

---

## 1. Hướng Dẫn Viết Hợp Đồng Dữ Liệu (`contract.yaml`)

Valkyrie tuân thủ triệt để triết lý **"Zero-Fluff" (Không văn xuôi sáo rỗng)**. Hợp đồng chỉ chứa 100% các chỉ thị kỹ thuật máy có thể biên dịch thành SQL và thực thi được. Nếu xuất hiện các trường phi kỹ thuật (`sla`, `pricing`, `team`, `description`, v.v.), engine sẽ **từ chối nạp ngay lập tức**.

### Cấu Trúc Tổng Thể

```yaml
version: "v1"
dataset: "orders"
owner: "checkout-team@company.com"   # BẮT BUỘC: Không được để trống

policies:
  strict_schema: true                # Whitelist cột: cấm cột lạ, bắt buộc đủ cột
  max_error_rate: 0.05               # Ngưỡng lỗi tối đa cho phép (0.05 = 5%)
  quarantine_action: "DLQ"           # "DLQ" (cách ly vào Dead-Letter Queue) hoặc "FAIL_FAST" (ngắt ngay khi có 1 lỗi)

schema:
  - name: "order_id"
    type: "BIGINT"
    not_null: true
    unique: true                     # Chống trùng lặp khóa chính

  - name: "customer_id"
    type: "VARCHAR"
    not_null: true
    pattern: "^CUST-[0-9]{4}$"       # Kiểm tra biểu thức chính quy (Regex)

  - name: "status"
    type: "VARCHAR"
    enum: ["PENDING", "PROCESSING", "COMPLETED", "CANCELLED"]  # Danh sách giá trị hợp lệ

  - name: "quantity"
    type: "INTEGER"
    not_null: true
    range:
      min: 1
      max: 1000                      # Khoảng giá trị: [1, 1000]

  - name: "discount"
    type: "DOUBLE"
    range: [0.0, 0.5]               # Cú pháp viết tắt dạng danh sách [min, max]

  - name: "order_date"
    type: "TIMESTAMP"
    not_null: true

custom_rules:
  # Ràng buộc liên cột (Cross-column)
  - name: "completed_order_must_have_quantity"
    expression: "status != 'COMPLETED' OR quantity > 0"
    error_code: "ERR_INVALID_COMPLETED_ORDER"

  # Ràng buộc trên mảng đối tượng lồng nhau (Nested List Shorthand)
  - name: "order_total_matches_items"
    expression: "order_total == list_sum(line_items.price)"
    error_code: "ERR_TOTAL_MISMATCH"
```

---

## 2. Chi Tiết Danh Sách Các Trường & Ràng Buộc

### 2.1. Cấp Gốc (Root Level)

| Trường | Kiểu | Bắt buộc | Mô tả |
| :--- | :--- | :---: | :--- |
| `version` | `str` | Không | Phiên bản contract (Mặc định: `"v1"`). |
| `dataset` | `str` | Có | Tên tập dữ liệu hoặc bảng (ví dụ: `orders`, `sales`). |
| `owner` | `str` | **BẮT BUỘC** | Email hoặc handle đội phụ trách. Nếu rỗng/thiếu sẽ ném `ContractConfigException`. |
| `policies` | `dict` | Không | Các chính sách vận hành và dung sai lỗi. |
| `schema` | `list` | **BẮT BUỘC** | Danh sách định nghĩa các cột kỹ thuật. |
| `custom_rules` | `list` | Không | Các luật kiểm tra logic nghiệp vụ phức tạp bằng SQL DuckDB. |

> [!WARNING]
> **Quy tắc Zero-Fluff**: Tuyệt đối không thêm các trường: `sla`, `pricing`, `team`, `description`, `tags`, `author`, `contact`. Engine sẽ ném `ContractConfigException: Fluff detected!` nếu phát hiện.

---

### 2.2. Khối Chính Sách (`policies`)

| Thuộc tính | Kiểu | Mặc định | Ý nghĩa kỹ thuật |
| :--- | :--- | :---: | :--- |
| `strict_schema` | `bool` | `true` | **Whitelist cột**: Nếu dữ liệu nguồn xuất hiện cột thừa không khai báo trong contract, hoặc thiếu cột bắt buộc, engine sẽ ném `SchemaConformanceException` ngay tại bước pre-flight trước khi quét dữ liệu. |
| `max_error_rate` | `float` | `0.0` | Ngưỡng tỷ lệ lỗi tối đa (từ `0.0` đến `1.0`). Nếu `error_rows / total_rows > max_error_rate`, Circuit Breaker sẽ nổ và ngắt pipeline. |
| `quarantine_action` | `str` | `"DLQ"` | - `"DLQ"`: Cách ly các dòng vi phạm sang tập dữ liệu lỗi, dòng sạch đi tiếp.<br>- `"FAIL_FAST"`: Ngắt pipeline ngay lập tức nếu xuất hiện dù chỉ 1 dòng lỗi. |

---

### 2.3. Khối Cột (`schema`) & Ràng Buộc (Constraints)

| Ràng buộc | Kiểu | Mã lỗi sinh ra khi vi phạm | Ví dụ & Cú pháp |
| :--- | :--- | :--- | :--- |
| `name` | `str` | - | Tên cột trong bảng nguồn (`order_id`, `sales_date`). |
| `type` | `str` | `SchemaConformanceException` | Kiểu dữ liệu vật lý: `BIGINT`, `INTEGER`, `VARCHAR`, `DOUBLE`, `TIMESTAMP`, `DATE`, `BOOLEAN`, v.v. |
| `not_null` | `bool` | `ERR_NULL_VIOLATION` | `not_null: true` $\rightarrow$ kiểm tra giá trị khác `NULL`. |
| `unique` | `bool` | `ERR_DUPLICATE_KEY` | `unique: true` $\rightarrow$ Sử dụng DuckDB window partition `count(*) OVER (PARTITION BY col) > 1` để phát hiện trùng lặp. |
| `range` | `dict` hoặc `list` | `ERR_OUT_OF_RANGE` | - Dạng dict: `range: {min: 1, max: 100}` *(Lưu ý: chỉ dùng key `min`, `max`; cấm dùng `minimum`, `maximum`)*.<br>- Dạng list: `range: [1, 100]`. Có thể để `null` 1 đầu: `[0.0, null]`. |
| `enum` | `list` | `ERR_INVALID_ENUM` | Danh sách giá trị cho phép: `enum: ["A", "B", "C"]`. |
| `pattern` | `str` | `ERR_REGEX_MISMATCH` | Biểu thức Regex kiểm tra định dạng chuỗi: `pattern: "^[A-Z0-9]{20}$"`. |

---

### 2.4. Khối Luật Nghiệp Vụ (`custom_rules`)

Dùng để biểu diễn các luật liên cột hoặc trên cấu trúc mảng lồng nhau (Nested List/Struct):

```yaml
custom_rules:
  - name: "discount_applied_only_for_vip"
    expression: "discount == 0.0 OR customer_type == 'VIP'"
    error_code: "ERR_UNAUTHORIZED_DISCOUNT"

  # Cú pháp mảng lồng: tự động dịch list_sum(items.price) thành Lambda DuckDB
  - name: "total_matches_item_sum"
    expression: "total_amount == list_sum(items.price)"
    error_code: "ERR_SUM_MISMATCH"
```

* **Cơ chế Lambda Translation**: Biểu thức `list_sum(items.price)` sẽ được trình biên dịch Valkyrie tự động viết lại thành cú pháp chuẩn của DuckDB:
  `list_sum(list_transform(items, x -> x.price))`.

---

## 3. Hướng Dẫn Sử Dụng Trong Từng Môi Trường

Valkyrie hỗ trợ cơ chế **Tự Động Chọn Chiến Lược (Adaptive Execution Strategy)** giữa `TEMPORARY VIEW` và `TEMPORARY TABLE` tùy theo mục đích và quy mô dữ liệu.

```
                  ┌─────────────────────────────────────┐
                  │    Yêu cầu gọi từ Data Engineer     │
                  └──────────────────┬──────────────────┘
                                     │
                 Có ghi ra đĩa/S3 và materialize_arrow=False?
                                    ╱ ╲
                                   ╱   ╲
                              Có  ╱     ╲  Không (Interactive)
                                 ▼       ▼
                     ┌───────────────┐  Dữ liệu > 5 triệu dòng?
                     │ Dùng   VIEW   │         ╱ ╲
                     │ (0-byte RAM)  │    Có  ╱   ╲  Không (Nhỏ/Vừa)
                     └───────────────┘       ▼     ▼
                                     ┌──────────┐ ┌──────────┐
                                     │   VIEW   │ │  TABLE   │
                                     └──────────┘ └──────────┘
```

---

### Môi Trường 1: Jupyter Notebook & Khám Phá Dữ Liệu (Interactive)

Phù hợp cho quá trình phân tích ad-hoc, dev thử nghiệm, cần lấy ngay Arrow Table hoặc DataFrame về bộ nhớ để trực quan hóa.

```python
import valkyrie

# Thực thi kiểm định trực tiếp
result = valkyrie.run(
    sql="SELECT * FROM 'sales_sample.parquet'",
    contract="sales_contract.yaml"
)

# 1. Kiểm tra trạng thái
print("Trạng thái:", "PASS" if result.is_passed else "BREACH")
print("Metrics:", result.metrics)
print("Chiến lược tự chọn:", result.metrics["execution_strategy"]) # -> TABLE

# 2. Lấy dữ liệu sạch và dữ liệu lỗi dạng PyArrow Table
clean_table = result.clean_df   # hoặc result.clean_arrow
dlq_table = result.dlq_df       # hoặc result.dlq_arrow

# 3. Chuyển sang Pandas / Polars nếu cần
df_clean = clean_table.to_pandas()
df_dlq = dlq_table.to_pandas()

# 4. Hỗ trợ cú pháp unpacking nhanh như một tuple:
clean_df, dlq_df = valkyrie.run(sql="...", contract="...")
```

> [!TIP]
> Trong môi trường này, Valkyrie tự động dùng `TEMPORARY TABLE` trong RAM để quét dữ liệu đúng 1 lần duy nhất, giúp bóc tách `clean_df` và `dlq_df` cực nhanh.

---

### Môi Trường 2: Production Pipeline (Airflow, Dagster, ETL Lớn)

Khi chạy xử lý tập dữ liệu lớn hàng chục đến hàng trăm triệu dòng (Out-of-Core), tuyệt đối không kéo toàn bộ bảng về RAM của Python Worker. Sử dụng hàm **`valkyrie.execute()`** để stream trực tiếp ra đĩa:

```python
import valkyrie

# Chạy Out-of-Core Streaming: 0-Byte Python RAM, an toàn tuyệt đối
result = valkyrie.execute(
    sql="SELECT * FROM 's3://bronze/orders/date=2026-10-18/*.parquet'",
    contract="contracts/orders_contract.yaml",
    write_clean="s3://silver/orders/date=2026-10-18/clean.parquet",
    write_dlq="s3://dlq/orders/date=2026-10-18/dlq.parquet"
)

print(f"Tổng dòng xử lý : {result.metrics['total_rows']:,}")
print(f"Dòng sạch đã ghi: {result.metrics['clean_rows']:,}")
print(f"Dòng lỗi cách ly: {result.metrics['error_rows']:,}")
print(f"Tốc độ xử lý    : {result.metrics['duration_ms']} ms")
```

**Đặc điểm kiến trúc:**
* `execute()` tự động kích hoạt chế độ `materialize_arrow=False` và chọn **`CREATE TEMPORARY VIEW`**.
* Không lưu bảng tạm trong bộ đệm của DuckDB, dữ liệu được stream thẳng từ nguồn sang sink Parquet.
* Python RAM giữ ổn định ở mức vài trăm MB dù tập dữ liệu đầu vào lên tới hàng trăm triệu dòng (> 300M rows).

---

### Môi Trường 3: Cloud Storage & Data Lake (AWS S3, MinIO, Ceph)

Valkyrie trang bị tính năng **Zero-Boilerplate S3/MinIO**. Bạn không cần viết code cấu hình credentials trong Python; chỉ cần đặt các biến môi trường chuẩn trong Docker/K8s/Airflow:

#### Các biến môi trường hỗ trợ:
```bash
# AWS S3 chuẩn
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_REGION="us-east-1"

# Hoặc MinIO Local / On-premise
export AWS_ENDPOINT_URL="http://minio:9000"
export S3_USE_SSL="false"
export S3_URL_STYLE="path"
```

#### Sử dụng trực tiếp trong code hoặc SQL:
Khi phát hiện đường dẫn chứa tiền tố `s3://` hoặc `minio://`, Valkyrie sẽ tự động:
1. Nạp extension `httpfs` của DuckDB.
2. Khởi tạo `CREATE OR REPLACE SECRET s3_valkyrie (...)`.

```python
result = valkyrie.execute(
    sql="SELECT * FROM 's3://lakehouse-bronze/events/*.parquet'",
    contract="event_contract.yaml",
    write_clean="s3://lakehouse-silver/events/clean.parquet",
    write_dlq="s3://lakehouse-dlq/events/dlq.parquet"
)
```

---

### Môi Trường 4: CI/CD & Giao Diện Dòng Lệnh (CLI)

Valkyrie cung cấp CLI không có dependency cồng kềnh (xây dựng bằng `argparse` chuẩn), tiện lợi để nhúng vào Git Pre-commit Hooks hoặc GitLab CI / GitHub Actions.

```bash
# 1. Kiểm tra cú pháp contract có hợp lệ và sạch fluff không (trước khi commit)
valkyrie validate-contract --contract "contracts/sales.yaml"

# 2. Biên dịch xem trước câu lệnh SQL CTEs mà Valkyrie sẽ thực thi (để debug)
valkyrie compile --sql "SELECT * FROM sales" --contract "contracts/sales.yaml"

# 3. Chạy kiểm định thực tế từ Terminal / Bash Script
valkyrie run \
  --sql "SELECT * FROM 'data/sales.parquet'" \
  --contract "contracts/sales.yaml" \
  --clean "data/sales_clean.parquet" \
  --dlq "data/sales_dlq.parquet"
```

* **Exit Code**: Trả về `0` nếu dữ liệu đạt chuẩn; trả về `1` nếu phát hiện vi phạm contract hoặc schema không khớp.

---

### Môi Trường 5: Xử Lý Lỗi & Giám Sát Cầu Dao (Circuit Breaker)

Khi dữ liệu vượt ngưỡng `max_error_rate` hoặc gặp chính sách `quarantine_action: "FAIL_FAST"`, Valkyrie sẽ ném ngoại lệ `ContractBreachException` và in khung cảnh báo trực quan:

```python
from valkyrie import execute, ContractBreachException

try:
    execute(
        sql="SELECT * FROM 'incoming_orders.parquet'",
        contract="orders_contract.yaml",
        write_clean="clean.parquet",
        write_dlq="dlq.parquet"
    )
except ContractBreachException as e:
    # 1. In banner chẩn đoán sự cố
    print(e.message)

    # 2. Thông tin phục vụ gửi alert sang Slack / PagerDuty
    print("Dataset vi phạm:", e.metrics["dataset"])
    print("Người phụ trách:", e.owner)
    print("Tỷ lệ lỗi thực tế:", f"{e.metrics['error_rate']:.2%}")

    # 3. Dữ liệu lỗi đã được cách ly an toàn trong e.dlq_df hoặc file write_dlq
```

#### Banner cảnh báo mẫu:
```text
================================================================================
🔥 [VALKYRIE] DATA CONTRACT BREACH DETECTED!
Dataset     : orders
Owner       : checkout-team@company.com
Threshold   : max_error_rate = 5.00%
Actual Error: 12.45% (124,500 / 1,000,000 rows)
Top Violations:
  - [ERR_NULL_VIOLATION]: 80,000 rows
  - [ERR_DUPLICATE_KEY]: 34,500 rows
  - [ERR_OUT_OF_RANGE]: 10,000 rows
Reason      : Error rate 12.45% exceeded threshold 5.00%.
Action Taken: Pipeline HALTED. DLQ preserved. Contact owner immediately!
================================================================================
```

---

### Môi Trường 6: Tích Hợp Ghi Bảng Apache Iceberg Tầng Silver (Bifrost Proxy & PyIceberg)

Trong kiến trúc tổng thể **Yggdrasil Lakehouse**, Valkyrie đóng vai trò là Người gác cổng (Gatekeeper). Dữ liệu sạch sau khi được thẩm định sẽ được bàn giao dưới dạng Apache Arrow Table để **Bifrost Proxy** thực hiện ACID commit vào bảng Apache Iceberg tầng Silver.

#### Sơ Đồ Luồng Dữ Liệu Yggdrasil Lakehouse

```mermaid
flowchart TD
    Bronze["Bronze Layer (Parquet / S3 / DuckLake)"] -->|Read Path| Valkyrie["Valkyrie Engine (DuckDB Single-Pass)"]
    Valkyrie -->|Validation & Segregation| Decision{"Hợp đồng đạt chuẩn?"}
    Decision -->|Dữ liệu lỗi| DLQ["DLQ (MinIO / S3 dlq.parquet)"]
    Decision -->|Dữ liệu sạch: Arrow Table| Bifrost["Bifrost Proxy (REST Catalog / PyIceberg)"]
    Bifrost -->|ACID Commit| Silver["Apache Iceberg Table (Silver Layer)"]
```

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           YGGDRASIL DATA PIPELINE                               │
│                                                                                 │
│   1. ĐỌC DỮ LIỆU (Read Path):                                                  │
│      - DuckDB sử dụng DuckLake / httpfs extension đọc Bronze siêu tốc.         │
│                                  │                                              │
│                                  ▼                                              │
│   2. PHÁN XÉT (Gatekeeper Gate 1):                                              │
│      - VALKYRIE thẩm định hợp đồng bằng Single-Pass SQL.                        │
│      - Tách Clean Data vs DLQ.                                                  │
│                                  │                                              │
│                 ┌────────────────┴────────────────┐                             │
│                 ▼                                 ▼                             │
│   3. DỮ LIỆU SẠCH (Clean Data):     4. DỮ LIỆU RÁC (DLQ):                       │
│      - Bàn giao Arrow Table            - Tự động ghi ra dlq.parquet             │
│      - BIFROST PROXY nhận và ghi         trên S3/MinIO để kiểm toán.            │
│        ACID Append vào bảng                                                     │
│        Apache Iceberg tầng Silver!                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### Code Mẫu Vận Hành Của Data Engineer:

```python
import valkyrie
from pyiceberg.catalog import load_catalog

# 1. Valkyrie đọc Bronze và lọc sạch hợp đồng
clean_arrow, dlq_arrow = valkyrie.run(
    sql="SELECT * FROM 's3://lakehouse-bronze/orders/*.parquet'",
    contract="contracts/orders_contract.yaml",
    write_dlq="s3://lakehouse-dlq/orders/date=2026-10-18/dlq.parquet"
)

# 2. Ghi dữ liệu sạch trực tiếp vào Apache Iceberg tầng Silver qua Bifrost Proxy!
if clean_arrow and clean_arrow.num_rows > 0:
    # Kết nối Iceberg REST Catalog thông qua Bifrost Proxy
    catalog = load_catalog(
        "bifrost_catalog",
        **{
            "uri": "http://bifrost-proxy:8181",
            "warehouse": "s3://lakehouse-silver/warehouse",
            "s3.endpoint": "http://minio:9000",
        }
    )
    
    # Load bảng Silver và thực hiện ACID Append
    silver_table = catalog.load_table("silver.orders")
    silver_table.append(clean_arrow)
    print(f"Đã commit thành công {clean_arrow.num_rows:,} dòng sạch vào Iceberg Silver!")
```

---

## 4. Tóm Tắt Các Mã Lỗi Chuẩn (Standard Error Codes)

Khi các dòng vi phạm bị đẩy vào Dead-Letter Queue (DLQ), các cột metadata sau sẽ tự động được gắn vào:
* `_error_code`: Mã loại lỗi vi phạm đầu tiên được phát hiện.
* `_error_column`: Tên cột (hoặc tên custom rule) gây ra lỗi.
* `_raw_value`: Giá trị chuỗi thô của dữ liệu tại thời điểm vi phạm.
* `_ingestion_timestamp`: Thời gian kiểm định dữ liệu.

| Mã lỗi (`_error_code`) | Nguyên nhân |
| :--- | :--- |
| `ERR_NULL_VIOLATION` | Cột có khai báo `not_null: true` nhưng dữ liệu nhận về giá trị `NULL`. |
| `ERR_DUPLICATE_KEY` | Cột có khai báo `unique: true` nhưng xuất hiện nhiều hơn 1 lần trong tập dữ liệu. |
| `ERR_OUT_OF_RANGE` | Giá trị nằm ngoài cận `min` hoặc `max` đã định nghĩa. |
| `ERR_INVALID_ENUM` | Giá trị không nằm trong danh sách `enum` cho phép. |
| `ERR_REGEX_MISMATCH` | Chuỗi không khớp với mẫu biểu thức chính quy `pattern`. |
| `ERR_CUSTOM_RULE_FAILED` (hoặc custom code) | Biểu thức điều kiện trong `custom_rules` trả về `FALSE` hoặc `NULL`. |
