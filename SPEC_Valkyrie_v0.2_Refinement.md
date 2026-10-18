# Valkyrie v0.2 Refinement Specification - Code Review Fixes & Adaptive Engine

*Labels: ready-for-agent*

## Problem Statement

Đợt nghiệm thu Code Review độc lập trên 2 hệ quy chiếu (**Standards** và **Spec**) đối với bản triển khai v0.1 đã chỉ ra chính xác các lỗi vi phạm kiến trúc, code smells và các yêu cầu bị cài đặt sai lệch hoặc bỏ sót. Nếu không khắc phục triệt để, hệ thống sẽ gặp các vấn đề nghiêm trọng:
1. **Lãng phí I/O và nghẽn mạng**: Quét lặp dữ liệu từ 2 đến 4 lần trên tập dữ liệu lớn do dùng `TEMPORARY VIEW` ảo.
2. **Lỗi xác thực Cloud Storage**: Ghi Parquet lên S3/MinIO bằng PyArrow bị từ chối truy cập (Access Denied) vì không liên kết được với DuckDB S3 Secret.
3. **Lỗ hổng dữ liệu (Schema Drift & Type Blindness)**: Không thực thi `strict_schema` và không kiểm tra kiểu dữ liệu, khiến dữ liệu sai lệch cấu trúc vẫn lọt qua Gatekeeper.
4. **Vi phạm tiêu chuẩn mã nguồn**: Chứa các Fowler Code Smells (Duplicated Code, Primitive Obsession, Divergent Change) và lệch chuẩn phụ thuộc tài liệu.

---

## Detailed Code Review Findings Matrix (Chi Tiết Các Vấn Đề Từ Code Review)

Bản đặc tả v0.2 này quy định xử lý dứt điểm toàn bộ danh sách các phát hiện sau:

### 1. Hệ Quy Chiếu Standards (Chuẩn Kiến Trúc & Code Smells)
| Mã | Phân loại | Vị trí phát hiện | Nội dung chi tiết | Giải pháp trong v0.2 |
| :--- | :--- | :--- | :--- | :--- |
| **STD-01** | *Hard Violation* | `src/valkyrie/contract.py:4`, `pyproject.toml` | **Lệch chuẩn phụ thuộc ADR-0001**: Tài liệu ghi chỉ phụ thuộc `duckdb` & `pyyaml`, nhưng thực tế cài thêm `pydantic` và `pyarrow`. | Cập nhật đồng bộ lại ADR-0001 thừa nhận `pydantic` & `pyarrow` là core dependencies chính thức phục vụ validation và Arrow transfer. |
| **STD-02** | *Hard Violation* | `src/valkyrie/contract.py:38`, `compiler.py` | **Bỏ quên thực thi `strict_schema`**: Khai báo cờ nhưng không kiểm tra danh sách cột đầu vào, không chặn cột lạ. | Xây dựng hàm kiểm tra Pre-flight Schema Check đối chiếu `rel.columns` trước khi chạy. |
| **STD-03** | *Hard Violation* | `src/valkyrie/engine.py:85-96, 224` | **Phá vỡ cam kết Single-Pass**: Dùng `TEMPORARY VIEW` ảo khiến DuckDB quét file nguồn 2 lần; `execute()` gọi lại `run()` quét thêm 1-2 lần nữa. | Chuyển sang `TEMPORARY TABLE` (cho `run()`) và `COPY ... TO` native (cho `execute()`). Triệt tiêu việc gọi lại `run()`. |
| **STD-04** | *Code Smell* | `src/valkyrie/compiler.py:80-90` | **Duplicated Code (Mã lặp)**: Khối sinh `CASE WHEN` lặp lại 3 lần cho `code`, `column`, `raw_value`. Logic `rules.append()` lặp 5 lần. | Tái cấu trúc (Extract Helper) gom logic sinh mệnh đề `CASE WHEN` vào một hàm dùng chung duy nhất. |
| **STD-05** | *Code Smell* | `src/valkyrie/contract.py:40` | **Primitive Obsession (Ám ảnh kiểu nguyên thủy)**: `quarantine_action: str = "DLQ"` để kiểu chuỗi tự do, không kiểm soát giá trị hợp lệ. | Đổi sang `Literal["DLQ", "FAIL_FAST"]` có validate chặt chẽ của Pydantic. |
| **STD-06** | *Code Smell* | `src/valkyrie/engine.py:137-148` | **Divergent Change**: Engine điều phối database lại gánh luôn việc format chuỗi ANSI alert banner của terminal. | Tách logic render banner cảnh báo sang `valkyrie.exceptions.format_breach_banner()`. |

---

### 2. Hệ Quy Chiếu Spec (Tính Đúng Đắn & Đầy Đủ Của Yêu Cầu)
| Mã | Phân loại | Vị trí phát hiện | Nội dung chi tiết | Giải pháp trong v0.2 |
| :--- | :--- | :--- | :--- | :--- |
| **SPEC-01** | *Missing/Partial* | `compiler.py`, `engine.py` | **Chưa kiểm tra kiểu dữ liệu vật lý**: Cột `type` (`BIGINT`, `DOUBLE`, v.v.) được parse nhưng không sinh câu lệnh check kiểu. | Đối chiếu `rel.types` trong Pre-flight check; phát hiện kiểu dữ liệu không thể cast an toàn sẽ ném lỗi `ERR_SCHEMA_TYPE_MISMATCH`. |
| **SPEC-02** | *Missing/Partial* | `src/valkyrie/engine.py:104-113` | **Thiếu `duration_ms` trong metrics**: Không đo thời lượng thực thi của batch. | Bổ sung `time.perf_counter()` đo chính xác `duration_ms` vào từ điển `metrics`. |
| **SPEC-03** | *Missing/Partial* | `src/valkyrie/contract.py:9-11` | **Cú pháp `range` chưa hỗ trợ list**: Chỉ nhận dict `{min: ..., max: ...}`, chưa nhận mảng `[min, max]`. | Thêm validator hỗ trợ cả `Union[Dict, List[float]]` cho `range`. |
| **SPEC-04** | *Incorrect Impl* | `src/valkyrie/engine.py:218, 229` | **Lỗi ghi S3 do dùng PyArrow**: Dùng `pq.write_table()` không đọc được DuckDB S3 Secret, gây lỗi khi ghi MinIO/S3. | Thay bằng lệnh `COPY (...) TO '{path}' (FORMAT PARQUET)` của chính DuckDB. |
| **SPEC-05** | *Incorrect Impl* | `src/valkyrie/engine.py:152` | **Lệch kiểu `dlq_df` trong Exception**: Gán thẳng PyArrow `Table` cho thuộc tính mang tên `_df`. | Đảm bảo `.dlq_df` trả về DataFrame chuẩn (hoặc thống nhất đặt tên `.dlq_table` / `.dlq_arrow`). |
| **SPEC-06** | *Scope Creep* | `src/valkyrie/engine.py:36-52` | **Thuộc tính chuyển đổi Polars/Pandas**: Thêm `.clean_df` và `.dlq_df` động không nằm trong spec v0.1. | Chuẩn hóa chính thức vào Spec v0.2 như một tính năng tiện ích mở rộng (Convenience Accessor). |

---

## Solution

**Valkyrie v0.2** xử lý toàn bộ các điểm trên qua 4 nâng cấp kiến trúc:
1. **Pre-Flight Schema & Type Conformance (Thẩm định trước khi chạy)**:
   - Dùng `rel = con.sql(source_sql)` đọc header metadata trong 0.001 giây.
   - Kiểm tra thiếu cột bắt buộc $\rightarrow$ ném `SchemaConformanceException("ERR_SCHEMA_MISSING_COLUMN")`.
   - Kiểm tra cột lạ khi `strict_schema: true` $\rightarrow$ ném `SchemaConformanceException("ERR_SCHEMA_UNEXPECTED_COLUMN")`.
   - Kiểm tra lệch kiểu vật lý $\rightarrow$ ném `SchemaConformanceException("ERR_SCHEMA_TYPE_MISMATCH")`.
2. **Cơ Chế Single-Pass Thích Ứng & Ghi S3 Chuẩn Xác**:
   - `valkyrie.run()`: Chuyển sang `CREATE TEMPORARY TABLE` (lưu RAM, đọc nguồn đúng 1 lần), bóc tách Clean và DLQ, dọn dẹp bằng `DROP TABLE IF EXISTS` trong khối `finally`.
   - `valkyrie.execute()`: Ghi Parquet bằng lệnh native `COPY ... TO` của DuckDB, tận dụng trực tiếp DuckDB S3 Secret đã thiết lập.
3. **Bảo Toàn DLQ Khi Vi Phạm Không Quét Lại**:
   - Trong `execute()`, lưu trữ file DLQ ra đĩa trước khi kích hoạt ngoại lệ dừng pipeline, không gọi lại `run()`.
4. **Tái Cấu Trúc Mã Nguồn Sạch (Clean Code & Formatting)**:
   - Gom logic sinh SQL `CASE WHEN` vào hàm helper dùng chung.
   - Tách khung cảnh báo ANSI sang `exceptions.py`.
   - Chuyển `quarantine_action` sang `Literal["DLQ", "FAIL_FAST"]`.
   - Bổ sung `duration_ms` vào `metrics` và hỗ trợ cú pháp `range: [min, max]`.

---

## User Stories

### Nhóm Thẩm Định Schema Trước Khi Chạy (Pre-Flight Conformance)
1. As a Data Engineer, I want Valkyrie to inspect `rel.columns` and `rel.types` before running the heavy query, so that schema drift is caught in sub-milliseconds without scanning raw data.
2. As a Data Architect, I want unexpected columns under `strict_schema: true` to raise `SchemaConformanceException`, so that unapproved columns never enter the lakehouse.
3. As a Data Engineer, I want missing mandatory columns to raise `SchemaConformanceException`, so that incomplete datasets are halted immediately.
4. As a Data Analyst, I want physical column type mismatches to be caught during pre-flight inspection, so that incompatible data types are stopped before execution.
5. As a Contract Author, I want to declare `range: [min, max]` as a concise list, so that I am not forced to write multi-line dictionaries for simple boundaries.
6. As a Platform Administrator, I want `quarantine_action` to be strictly validated as `"DLQ"` or `"FAIL_FAST"`, so that invalid typo configurations are prevented at parse time.

### Nhóm Hiệu Năng Single-Pass & Cloud Storage S3
7. As a Systems Developer, I want `valkyrie.run()` to use a materialized `TEMPORARY TABLE` dropped in `finally`, so that source data is physically read only once and memory is guaranteed clean.
8. As a Cloud Data Engineer, I want `valkyrie.execute()` to export Parquet using DuckDB's native `COPY ... TO`, so that custom S3/MinIO endpoints and credentials configured in DuckDB Secrets work without authentication errors.
9. As an Airflow Operator, I want `valkyrie.execute()` to persist the DLQ dataset before raising `ContractBreachException` without re-scanning the dataset, so that bad records are saved in Single-Pass fashion.

### Nhóm Đo Lường & Chất Lượng Mã Nguồn
10. As a Monitoring Engineer, I want `result.metrics` to include `duration_ms`, so that I can track validation execution latency in APM and dashboards.
11. As a Code Maintainer, I want SQL CASE-builder logic to be unified in a single helper function, so that SQL generation is not duplicated across code paths.
12. As a CLI Developer, I want alert banner formatting to reside in `valkyrie.exceptions`, so that the database engine module focuses strictly on query execution.

---

## Implementation Decisions

### 1. Pre-Flight Validator (`valkyrie.engine`)
```python
rel = con.sql(source_sql)
source_cols = {col.lower(): col for col in rel.columns}
source_types = dict(zip([c.lower() for c in rel.columns], [str(t).upper() for t in rel.types]))

# Kiểm tra thiếu cột bắt buộc
for col in contract.schema_:
    col_key = col.name.lower()
    if col.not_null and col_key not in source_cols:
        raise SchemaConformanceException(f"Missing mandatory column: '{col.name}'")

# Kiểm tra cột lạ khi strict_schema = True
if contract.policies.strict_schema:
    contract_keys = {c.name.lower() for c in contract.schema_}
    extra_cols = [orig for low, orig in source_cols.items() if low not in contract_keys]
    if extra_cols:
        raise SchemaConformanceException(f"Unexpected columns found under strict_schema: {extra_cols}")
```

### 2. Native DuckDB S3 Export (`valkyrie.engine`)
Thay thế `pq.write_table` bằng lệnh DuckDB SQL để sử dụng trực tiếp S3 Secret:
```python
if write_clean:
    con.execute(f"COPY (SELECT * EXCLUDE (_error_code, _error_column, _raw_value, _ingestion_timestamp) FROM _valkyrie_annotated WHERE _error_code IS NULL) TO '{write_clean}' (FORMAT PARQUET)")

if write_dlq and error_rows > 0:
    con.execute(f"COPY (SELECT * FROM _valkyrie_annotated WHERE _error_code IS NOT NULL) TO '{write_dlq}' (FORMAT PARQUET)")
```

### 3. Cú Pháp Range Đa Năng (`valkyrie.contract`)
```python
class RangeConstraint(BaseModel):
    min: Optional[float] = None
    max: Optional[float] = None

    @model_validator(mode="before")
    @classmethod
    def parse_range(cls, v: Any) -> Any:
        if isinstance(v, (list, tuple)) and len(v) == 2:
            return {"min": v[0], "max": v[1]}
        return v
```

---

## Testing Decisions

### 1. Triết Lý Kiểm Thử
- Giữ nguyên ranh giới kiểm thử cao nhất (Public Seams): `valkyrie.run()`, `valkyrie.execute()`, CLI.
- Kiểm thử toàn bộ các ca phát hiện mới từ Code Review.

### 2. Các Bài Test Bổ Sung Cần Viết
1. `test_preflight_strict_schema_unexpected_columns`: Ném `SchemaConformanceException` khi có cột lạ.
2. `test_preflight_missing_mandatory_columns`: Ném `SchemaConformanceException` khi thiếu cột not_null.
3. `test_flexible_range_syntax_list`: Kiểm tra `range: [0.01, 100.0]`.
4. `test_duckdb_native_copy_to_s3_export`: Kiểm tra `execute()` sử dụng lệnh `COPY ... TO` xuất Parquet thành công.
5. `test_duration_ms_measured`: Xác minh `metrics['duration_ms']` tồn tại và `> 0`.
6. `test_temporary_table_dropped_after_run`: Xác minh bảng tạm biến mất hoàn toàn sau khi chạy xong.

---

## Out of Scope

1. Không tự động sửa đổi cấu trúc bảng nguồn (Schema Evolution).
2. Không tự động chuyển đổi kiểu dữ liệu mất mát (lossy type coercion).
