# Valkyrie Data Contract Engine - Specification

*Labels: ready-for-agent*

## Problem Statement

Trong kiến trúc Data Lakehouse hiện đại (như Yggdrasil), việc bảo vệ tính toàn vẹn của dữ liệu từ tầng Bronze lên Silver (Gate 1 & Gate 2) gặp phải sự bế tắc giữa hai thái cực:

1. **Sự cồng kềnh, chậm chạp và tốn kém của các công cụ truyền thống:**
   - Các công cụ kiểm định dữ liệu phổ biến như Great Expectations, Pandera hay Soda Core chạy trên runtime Python thuần rất chậm chạp, dễ bị tràn bộ nhớ (Out-Of-Memory) khi đối mặt với các tập dữ liệu từ 10 triệu đến 200 triệu dòng.
   - Khi chạy trên Cloud Data Warehouse (Snowflake, BigQuery), việc chạy hàng trăm test cases làm bùng nổ chi phí tính toán (compute credits).
   - Việc lạm dụng dbt test hoặc các framework phân tán (Spark) gây ra độ trễ khởi động quá lớn (hàng chục giây đến vài phút), không đáp ứng được yêu cầu micro-batching hoặc near-real-time ingestion.
2. **Sự đánh đổi giữa Tính Tiện Dụng (In-Process) và Hiệu Năng Tối Thượng (Low-Latency Streaming):**
   - Nếu ép buộc hệ thống phải triển khai một cụm service phức tạp với cơ chế khóa nhân CPU (Core Pinning) thì sẽ phá vỡ môi trường chạy thông thường của các Data Engineer (vốn quen dùng Python, Airflow, DuckDB trên cùng một máy chủ).
   - Ngược lại, nếu chỉ dựa vào Python bindings thông thường mà không có thiết kế phần cứng chuyên sâu, hệ thống sẽ sụp đổ hiệu năng khi dữ liệu vượt ngưỡng hàng trăm triệu dòng.

Hệ thống cần một giải pháp kiểm định Data Contract siêu tốc, cấu hình hóa 100% bằng YAML, vừa có khả năng nhúng nhẹ nhàng vào Python/Airflow (In-Process), vừa có thể bung sức thành một cỗ máy độc lập bọc thép (Container với LMAX RingBuffer) khi cần hiệu năng giới hạn của phần cứng.

---

## Solution

**Valkyrie Engine** — Một động cơ kiểm định và cưỡng chế Hợp đồng Dữ liệu (Data Contract Enforcement Engine) viết bằng **Rust**, định hướng phi tập trung và dựa trên bộ nhớ chuẩn **Apache Arrow**.

Valkyrie giải quyết bài toán thông qua **hai hình thái thực thi chuyên biệt (Dual Execution Profiles)** dùng chung một lõi logic kiểm định:

1. **Bản In-Process ("Lịch sự & Thực dụng" - `valkyrie-core` / Python Package):**
   - Đóng gói thành thư viện nhúng trực tiếp vào tiến trình gọi (Python / Airflow / Polars / DuckDB) thông qua chuẩn giao tiếp **Arrow C Data Interface (PyCapsule Zero-Copy)**.
   - Vận hành trên cơ chế đa luồng hợp tác **Rayon Work-Stealing** kết hợp véc-tơ hóa **SIMD**.
   - Cư xử hòa đồng với hệ điều hành: Tuyệt đối không chiếm dụng nhân CPU (No Core Pinning), tự động san sẻ tài nguyên và nhả CPU khi hệ thống mẹ cần.
   - Mục tiêu: Đạt tốc độ **15 – 25 triệu bản ghi / giây** (quét 100M dòng trong 4.5 – 6.5s).
2. **Bản Container ("Quái vật Bọc thép" - `valkyrie-daemon`):**
   - Đóng gói thành Container Docker / Binary CLI độc lập, vận hành theo mô hình kiến trúc sàn giao dịch **LMAX Disruptor + Thread-per-Core**.
   - Chiếm dụng tài nguyên độc quyền: Ghim nhân CPU (`core_affinity`), vòng lặp thăm dò tích cực (`Busy-Spin`), và sử dụng RingBuffer phi khóa (Lock-Free) để triệt tiêu hoàn toàn độ trễ điều phối của OS Scheduler.
   - Phân tách luồng chuyên trách: Các worker core chỉ lo tính toán SIMD trên thanh ghi; một I/O core riêng biệt lo nhiệm vụ đẩy dòng lỗi sang Dead Letter Queue (DLQ).
   - Mục tiêu: Đạt độ trễ phản hồi **P99 < 5 micro-giây (< 5µs)** cho các luồng streaming từ Redpanda/Kafka và thông lượng **35 – 50+ triệu bản ghi / giây** cho batch processing.

Cả hai hình thái đều tuân thủ nguyên lý **Asymmetric Error Isolation (Bắt lỗi trước, Anti-Join sau)**: Engine chỉ tập trung bắt các dòng vi phạm và xuất ra DLQ siêu nhẹ, phần tạo tập dữ liệu sạch (Clean Data) được giải phóng cho các công cụ chuyên trách I/O hoặc phép trừ tập hợp thực thi ở tốc độ tối đa của ổ cứng NVMe.

---

## User Stories

### Nhóm Quản Trị Cấu Hình & Hợp Đồng (Contract & Configuration)
1. As a Data Engineer, I want to declare all data contracts in a declarative `contract.yaml` file, so that I can modify validation rules without changing or recompiling any source code.
2. As a Data Architect, I want the contract format to be compatible with the Open Data Contract Standard (ODCS), so that my organization avoids vendor lock-in and adheres to open industry standards.
3. As a Data Engineer, I want to define strict schema validation (column names, types, nullability) in YAML, so that incoming data with unexpected or missing columns is immediately flagged.
4. As a Data Engineer, I want to define physical and business range constraints (`min`, `max`, `range: [min, max]`) on numeric fields, so that impossible negative prices or quantities are prevented from entering the Silver layer.
5. As a Data Analyst, I want to define static whitelist/enum constraints on categorical fields, so that invalid statuses or payment methods are quarantined before downstream analytics.
6. As a Data Engineer, I want to configure regex patterns for standardized strings (Email, Phone, UUID, Tax Code), so that syntactically malformed identifiers are filtered out early.
7. As a Platform Administrator, I want to configure the engine completely via at most 10 environment variables, so that deployment in Docker, Airflow, or Kubernetes is trivial and clean.

### Nhóm Xử Lý Lỗi & Dead Letter Queue (DLQ & Resilience)
8. As a Data Engineer, I want every invalid record to be stamped with metadata (`_error_column`, `_error_code`, `_raw_value`, `_ingestion_timestamp`), so that upstream producers can immediately identify why and where their data broke the contract.
9. As a Data Engineer, I want invalid records to be routed to an isolated Dead Letter Queue (`dlq.parquet`), so that bad data does not corrupt the clean Silver table while still being preserved for auditing.
10. As a Platform Engineer, I want to set a `max_error_rate` threshold (e.g., 0.05 / 5%), so that the engine trips a circuit breaker and halts the pipeline if a structural breaking change occurs at the source.
11. As a Data Owner, I want contract violations to preserve upstream lineage/ownership tags, so that alerts can be sent directly to the team responsible for producing the faulty batch.

### Nhóm Kiểm Tra Trùng Lặp & Quan Hệ (Deduplication & Referential Integrity)
12. As a Data Engineer, I want to enforce uniqueness on integer primary keys using an in-memory Atomic BitSet, so that 100M–200M rows can be deduplicated inside L3 cache in sub-second time without heap thrashing.
13. As a Data Engineer, I want to enforce composite uniqueness on multiple string/UUID columns, so that business keys like `(store_id, receipt_no, date)` are guaranteed unique across batches.
14. As a Data Analyst, I want to check referential integrity against a reference dimension table (`dim_customers.parquet`) via pre-warmed Bloom filters, so that orphan foreign keys are caught before data reaches the Gold layer.

### Nhóm Bản In-Process (Python / Data Ecosystem)
15. As a Python Data Engineer, I want to install Valkyrie via `pip install valkyrie`, so that I can use it directly inside Python scripts, Jupyter Notebooks, or Airflow PythonOperators without running Docker daemons.
16. As a Data Engineer using Polars or PyArrow, I want to pass in-memory Arrow tables to Valkyrie using zero-copy (Arrow C Data Interface), so that memory is not duplicated between Python and Rust runtimes.
17. As an Airflow Administrator, I want the In-Process version to use cooperative Rayon work-stealing without CPU core pinning, so that other concurrent tasks on the Airflow worker node are not starved of CPU.
18. As a Data Engineer, I want the In-Process engine to achieve at least 15M–20M records/sec throughput, so that a 10M row batch is validated in under 0.6 seconds.

### Nhóm Bản Container (LMAX & Low-Latency)
19. As a High-Throughput Streaming Engineer, I want to run Valkyrie as a standalone Docker container with LMAX Disruptor ring buffers, so that continuous streaming records from Redpanda are validated with sub-millisecond P99 latency.
20. As an Infrastructure Engineer, I want to assign dedicated CPU cores to Valkyrie container workers using `core_affinity`, so that cache-lines remain warm and OS context-switching overhead is eliminated.
21. As a Systems Developer, I want worker cores to avoid all memory allocations (`malloc`/`free`) on the hot validation path, so that GC pauses and heap allocator contention never occur.
22. As a Platform Engineer, I want a dedicated I/O core to drain the DLQ ring buffer asynchronously, so that writing error files to MinIO/S3 never blocks the primary validation ring.

---

## Implementation Decisions

### 1. Kiến Trúc Mô-đun & Phân Tầng Mã Nguồn
Hệ thống được tổ chức thành một Cargo Workspace thuần Rust gồm 3 crate:
- **`valkyrie-core` (In-Process Library):**
  - Chứa toàn bộ logic kiểm định dữ liệu, mô hình Contract AST, Arrow array evaluators, và SIMD kernels.
  - Sử dụng `rayon` cho việc song song hóa dữ liệu dựa trên chunks (64K dòng/chunk).
  - Không chứa bất kỳ mã nguồn nào liên quan đến core pinning hay busy-spin.
- **`valkyrie-py` (Python Bindings):**
  - Sử dụng `pyo3` và `arrow50+` C Data Interface (PyCapsule Protocol).
  - Nhận Arrow pointers từ Python (Polars / PyArrow), gọi trực tiếp vào `valkyrie-core` và trả về Arrow RecordBatch cho DLQ cùng Boolean Bitmask cho Clean rows.
- **`valkyrie-daemon` (Container / Standalone CLI):**
  - Bọc ngoài `valkyrie-core`, kích hoạt kiến trúc LMAX Disruptor.
  - Tích hợp crate `core_affinity` để ghim chặt các worker threads vào các CPU physical cores đã được chỉ định qua biến môi trường `WORKER_CORES`.
  - Tích hợp lock-free ring buffer (dựa trên crate `disruptor` hoặc `crossbeam-epoch`) để chuyển giao các bản ghi lỗi sang I/O thread mà không dùng Mutex.

### 2. Thiết Kế Chuẩn Hợp Đồng Dữ Liệu (Data Contract Standard Specification)
File hợp đồng dữ liệu tuân thủ chuẩn mở **Open Data Contract Standard (ODCS v3 Compatible)**, cho phép khai báo toàn bộ metadata, chính sách bảo vệ và danh sách các ràng buộc kỹ thuật. 

Dưới đây là đặc tả chuẩn của một file `contract.yaml` (Prototype Schema):

```yaml
version: "v1"
dataset: "fmcg_sales"
domain: "sales"
owner: "finance-engineering@yggdrasil.local"

# 1. Chính sách bảo vệ dữ liệu & Quản trị lỗi (Operational Policies)
policies:
  max_error_rate: 0.05                  # Ngưỡng tỷ lệ lỗi tối đa (5%) kích hoạt Circuit Breaker
  quarantine_action: "DLQ"              # Hành vi: "DLQ" (cách ly dòng lỗi) hoặc "FAIL_FAST" (dừng pipeline)
  strict_schema: true                   # Bắt buộc khớp schema: tự động từ chối nếu thiếu cột hoặc có cột lạ

# 2. Quy tắc hợp đồng chi tiết trên từng cột (Column-Level Contracts)
schema:
  - name: "order_id"
    type: "int64"                       # Kiểu dữ liệu đích: int32, int64, float64, decimal(p,s), string, timestamp
    not_null: true
    unique:
      enabled: true
      strategy: "bitset"                # "bitset" (dùng Atomic Roaring BitSet cho Int) hoặc "hash" (cho String)

  - name: "customer_id"
    type: "int64"
    not_null: true
    foreign_key:                        # Ràng buộc toàn vẹn tham chiếu
      reference_table: "s3://lakehouse/silver/dim_customers.parquet"
      reference_column: "id"

  - name: "unit_price"
    type: "float64"
    not_null: true
    range:
      min: 0.01                         # Ép điều kiện SIMD CMP: unit_price >= 0.01
      max: 1000000.0                    # Ép điều kiện SIMD CMP: unit_price <= 1,000,000.0

  - name: "quantity"
    type: "int32"
    not_null: true
    range:
      min: 1                            # Số lượng tối thiểu phải từ 1

  - name: "status"
    type: "string"
    not_null: true
    enum: ["PENDING", "PAID", "CANCELLED", "REFUNDED"] # Whitelist tĩnh

  - name: "customer_email"
    type: "string"
    not_null: false
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$" # Regex định dạng email
    length:
      max: 255                          # Độ dài chuỗi tối đa 255 ký tự

  - name: "created_at"
    type: "timestamp_tz"                # Ép chuẩn UTC Microseconds
    not_null: true

# 3. Quy tắc kiểm tra chéo đa cột (Cross-Column Business Logic)
custom_rules:
  - name: "valid_discount_amount"
    expression: "discount_amount <= (unit_price * quantity)"
    error_code: "ERR_DISCOUNT_EXCEEDS_TOTAL"

  - name: "delivery_after_order"
    expression: "delivered_at >= created_at"
    error_code: "ERR_DELIVERY_BEFORE_ORDER"
```

#### Ánh Xạ Kiểu Dữ Liệu Vật Lý (Type Mapping Matrix):
| Khai báo trong YAML | Kiểu dữ liệu Apache Arrow | Cơ chế kiểm tra tối ưu (Fast-Path) |
| :--- | :--- | :--- |
| `int32` / `int64` | `DataType::Int32` / `Int64` | SIMD Range Vector Comparator |
| `float32` / `float64` | `DataType::Float32` / `Float64` | SIMD Floating Point Comparator |
| `string` / `varchar` | `DataType::Utf8` / `LargeUtf8` | Byte Length Check / Aho-Corasick / DFA Regex |
| `boolean` | `DataType::Boolean` | Bitwise Bitmap Mask |
| `timestamp_tz` | `DataType::Timestamp(Microsecond, Some("UTC"))` | Timestamp Scaler & Bounds Validator |
| `decimal(P, S)` | `DataType::Decimal128(P, S)` | Integer Arithmetic Bound Comparator |

#### Cấu Trúc Bảng Lỗi Xuất Ra DLQ (DLQ Output Schema):
Mỗi dòng vi phạm khi được ghi vào `dlq.parquet` sẽ được Engine giữ nguyên các cột gốc và tự động đính kèm thêm 4 cột metadata chuẩn:
- `_error_column` (`Utf8`): Tên cột đầu tiên phát hiện vi phạm hợp đồng.
- `_error_code` (`Utf8`): Mã lỗi kỹ thuật (`ERR_NULL_VIOLATION`, `ERR_OUT_OF_RANGE`, `ERR_INVALID_ENUM`, `ERR_REGEX_MISMATCH`, `ERR_DUPLICATE_KEY`, `ERR_FK_NOT_FOUND`).
- `_raw_value` (`Utf8`): Giá trị nguyên bản bị đánh giá là sai phạm.
- `_ingestion_timestamp` (`Timestamp(Microsecond, "UTC")`): Thời điểm Engine thực hiện phán xét dòng dữ liệu.

### 3. Cơ Chế Biên Dịch Config (Contract Compilation Lúc Khởi Động)
- Engine tuyệt đối không parse YAML hay regex trong vòng lặp xử lý dữ liệu.
- Khi khởi động, Engine biên dịch file Contract thành một cây thực thi tĩnh trong bộ nhớ (**In-Memory Compiled Execution Plan**):
  - Ánh xạ tên cột sang chỉ số index vật lý của mảng Arrow.
  - Khởi tạo trước mảng các con trỏ hàm (Function Pointers) SIMD tương ứng với từng kiểu dữ liệu.
  - Biên dịch trước các biểu thức Regex thành automata cố định (Deterministic Finite Automata - DFA).
  - Cấp phát trước dung lượng bộ nhớ tĩnh cho mảng Atomic BitSet (25MB cho 200M keys) hoặc Bloom filter.

### 4. Chiến Lược Đột Phá Bắt Lỗi & Phép Trừ Tập Hợp (The Anti-Join Asymmetry)
- Engine không chịu trách nhiệm phân tách đồng thời 2 luồng dữ liệu đầy đủ trong bộ nhớ để tránh áp lực ghi đĩa và cấp phát RAM.
- **Quy trình chuẩn:**
  1. Valkyrie quét tập dữ liệu nguồn, thực thi toàn bộ Contract và chỉ xuất ra tập **DLQ siêu nhỏ** (`dlq.parquet`) chứa các dòng vi phạm kèm thông tin metadata lỗi.
  2. Việc tạo bảng dữ liệu sạch (**Clean Table**) được bàn giao cho cơ chế `ANTI JOIN` (hoặc `EXCEPT`) của DuckDB/DataFusion thực thi trực tiếp trên ổ cứng NVMe ở tốc độ đọc tuần tự (Line-Rate I/O):
     `SELECT r.* FROM raw r ANTI JOIN dlq d ON r.id = d.id`.

---

## Testing Decisions

### 1. Triết Lý Thiết Kế Test (Black-Box Seams)
- Tuân thủ nguyên tắc tối giản điểm can thiệp (Test Seams): **Toàn bộ hệ thống chỉ được kiểm thử ở ranh giới cao nhất (Highest Possible Seams)**.
- Tuyệt đối không viết unit test gắn chặt vào các hàm nội bộ (như kiểm tra từng hàm con trỏ SIMD hay thuật toán xoay vòng RingBuffer). Mọi bài test đều đối xử với Valkyrie như một chiếc hộp đen.

### 2. Hai Điểm Can Thiệp Kiểm Thử (The 2 Test Seams)
Hệ thống chỉ có chính xác 2 Seam tương ứng với 2 hình thái thực thi:

- **Seam 1: In-Process Python Interface Boundary (`valkyrie-py`):**
  - *Cách kiểm thử:* Gửi một bảng PyArrow/Polars DataFrame bất kỳ kèm đường dẫn file `contract.yaml` vào hàm `valkyrie.validate()`.
  - *Assert:* Xác minh trực tiếp đối tượng kết quả trả về: Danh sách các dòng lỗi trong DLQ DataFrame có đúng chính xác `_error_code`, `_error_column` không, và tỷ lệ lỗi trả về có khớp toán học không.
- **Seam 2: Container / CLI Black-Box Process Boundary (`valkyrie-daemon`):**
  - *Cách kiểm thử:* Khởi chạy tiến trình binary CLI hoặc Docker container với các cờ tham số đầu vào (`--contract`, `--input`, `--output-dlq`).
  - *Assert:* 
    - Kiểm tra Exit Code của tiến trình (Exit 0 khi thành công, Exit 1 khi vi phạm Circuit Breaker).
    - Đọc nội dung file Parquet sinh ra tại đường dẫn `--output-dlq` để đối chiếu tính toàn vẹn của dữ liệu lỗi.
    - Đo lường mức tiêu thụ bộ nhớ (RSS Memory) để đảm bảo không vượt quá quota tài nguyên cam kết.

### 3. Tiền Lệ Kiểm Thử Trong Dự Án (Prior Art)
- Kế thừa mô hình kiểm thử Black-box End-to-End đã thành công tại [test_e2e_pyiceberg_ducklake.py](file:///c:/Users/CongAnh/Desktop/Yggdrasil/Bifrost%20Proxy/test_e2e_pyiceberg_ducklake.py) của dự án Bifrost Proxy (gửi dữ liệu qua interface công khai và verify kết quả tại tầng storage đích).

---

## Out of Scope

1. **Không xây dựng Query Engine hoàn chỉnh:** Valkyrie không phải là DuckDB hay Spark. Nó không hỗ trợ các phép tính toán tổng hợp phức tạp (như `GROUP BY`, Window Functions hay đa tầng `JOIN` phân tích).
2. **Không tự viết Parquet Reader/Writer từ số 0:** Hệ thống tận dụng 100% tầng I/O của Apache Arrow / Parquet crate chính thức trong hệ sinh thái Rust.
3. **Không xử lý sửa đổi dữ liệu (Data Transformation / Mutation):** Valkyrie tuân thủ nghiêm ngặt nguyên tắc của Yggdrasil: *No Reverse Data Flow*. Nó chỉ làm nhiệm vụ **phán xét và phân loại** (Sạch thì cho đi tiếp, Lỗi thì ném sang DLQ), tuyệt đối không tự ý "chữa lành" hay can thiệp sửa giá trị của dòng dữ liệu.

---

## Further Notes

- **Định vị trong Yggdrasil:** Valkyrie đóng vai trò là **Gatekeeper tại Gate 1 và một phần Gate 2**, đứng sừng sững bên cạnh **Bifrost Proxy** để tạo nên cặp bài trùng bảo vệ toàn diện cả giao thức mạng (REST) lẫn chất lượng dữ liệu (Data Contracts).
- **Tính khả thi triển khai:** Bản In-Process (`valkyrie-core`) là ưu tiên triển khai số 1 cho các pipeline batch hiện tại của Yggdrasil để đạt mốc 15M–20M rows/s ngay trong tuần đầu tiên. Bản Container LMAX (`valkyrie-daemon`) đóng vai trò là động cơ dự bị chiến lược, sẵn sàng kích hoạt khi hệ thống mở rộng sang các kịch bản Ingestion dạng Streaming thời gian thực có yêu cầu khắt khe về độ trễ Sub-millisecond.
