# Valkyrie Data Contract Engine

**Valkyrie** là động cơ kiểm định và cưỡng chế Hợp đồng Dữ liệu (Data Contract Enforcement Engine) nhúng thế hệ mới, được thiết kế chuyên biệt cho hệ sinh thái Data Lakehouse (Yggdrasil).

Valkyrie vận hành như một **DuckDB SQL Single-Pass Compiler**: biên dịch các hợp đồng kỹ thuật YAML (No-Fluff) thành câu lệnh SQL vector hóa duy nhất, loại bỏ hoàn toàn việc đọc đĩa 2 lần của chiến thuật Anti-Join truyền thống, xử lý dữ liệu lớn (Out-of-Core) với dung lượng RAM tối thiểu và tự động quản lý cấu hình Cloud Storage (S3/MinIO).

## Tính năng Nổi bật

- **100% Executable Contract**: Loại bỏ văn xuôi và thủ tục rườm rà của ODCS. Tập trung vào Schema, Ràng buộc cột, Custom Rules và `owner` bắt buộc.
- **Single-Pass Evaluation**: Duyệt dữ liệu đúng 1 lần từ đĩa, tiết kiệm 50% I/O và tăng gấp đôi tốc độ xử lý so với Anti-Join.
- **First-Violation Short-Circuit**: Tự động gán mã lỗi đầu tiên phát hiện vào DLQ (`_error_code`, `_error_column`, `_raw_value`, `_ingestion_timestamp`).
- **Zero-Boilerplate S3/MinIO**: Tự động đọc biến môi trường AWS/MinIO và cấu hình DuckDB Secret.
- **1-Line Developer Experience**:
  ```python
  import valkyrie

  clean_arrow, dlq_arrow = valkyrie.run(
      sql="SELECT * FROM 's3://bronze/orders/*.parquet'",
      contract="contract.yaml"
  )
  ```
- **Tích hợp Yggdrasil Lakehouse**: Đọc bằng DuckLake/DuckDB, thẩm định bằng Valkyrie, ghi dữ liệu sạch vào Apache Iceberg qua Bifrost Proxy.

## Cài đặt

```bash
pip install -e .
```
