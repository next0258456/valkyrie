# Valkyrie Domain Glossary

Tài liệu này định nghĩa ngôn ngữ chung (Ubiquitous Language) và các khái niệm nghiệp vụ cốt lõi trong hệ sinh thái **Valkyrie Data Contract Engine** theo bản thiết kế chốt cuối cùng.

---

### 1. Khái Niệm Hợp Đồng & Bản Đặc Tả Kỹ Thuật (Contracts & Specification)

- **Executable Contract (Hợp đồng Kỹ thuật Thực thi được)**: Bản đặc tả kỹ thuật dạng khai báo YAML, chứa 100% các quy tắc và ràng buộc có thể kiểm chứng tự động bằng máy (machine-verifiable). Valkyrie thẳng tay loại bỏ ("cook") toàn bộ các phần văn xuôi, thủ tục con người, hoặc điều khoản định tính (như Pricing, Team, Roles, SLA, prose description text) vốn làm cồng kềnh chuẩn ODCS truyền thống.
- **Contract Owner (`owner`)**: Định danh trách nhiệm giải trình bắt buộc (email, Slack channel, team tag) của đội ngũ sở hữu và sản xuất dữ liệu. Khi batch bị kích hoạt Circuit Breaker hoặc dòng lỗi bị đẩy vào DLQ, `owner` được tự động trích xuất để gắn vào thông báo cảnh báo và log lỗi để truy vết trách nhiệm tức thì.
- **Valkyrie Contract Schema**: Cấu trúc YAML tinh gọn của Valkyrie, bao gồm 4 khối chính: Metadata nhận diện (`version`, `dataset`, `owner`), Chính sách vận hành (`policies`), Khung dữ liệu và ràng buộc cột (`schema`), và Quy tắc logic kiểm tra chéo (`custom_rules`).
- **Compiled SQL Plan (Kế hoạch SQL Đã biên dịch)**: Cây truy vấn SQL tối ưu hóa cho DuckDB được sinh ra từ file `contract.yaml`. Bao gồm các biểu thức `CASE WHEN` phân loại lỗi và các nhánh CTE lọc dữ liệu, có thể tái sử dụng qua nhiều batch dữ liệu liên tiếp.

---

### 2. Mô Hình Động Cơ & Cơ Chế Phán Xét (Engine & Mechanics)

- **Valkyrie Engine (Động cơ Thực thi All-in-One)**: Thư viện Python nội bộ (`valkyrie`), nhúng trực tiếp **DuckDB** làm động cơ tính toán vector hóa và quản lý I/O cốt lõi. Đóng vai trò là Gatekeeper (Gate 1 / Gate 2) trong kiến trúc Lakehouse Yggdrasil.
- **DuckLake / DuckDB Reader**: Cơ chế quét và nạp dữ liệu đầu vào tận dụng DuckDB và DuckLake để truy vấn các bảng Bronze/Silver với tốc độ đọc tuần tự tối đa.
- **Bifrost Proxy**: Cổng kiểm soát và bảo vệ giao thức mạng (REST Catalog & Write Path) của Apache Iceberg trong Yggdrasil. Dữ liệu sau khi được Valkyrie thẩm định là sạch (`clean_df`) sẽ được bàn giao cho Bifrost Proxy để ghi chuẩn mực (ACID Append) vào các bảng Apache Iceberg tầng Silver.
- **Zero-Boilerplate Storage Auto-Config (Tự động Cấu hình S3/MinIO)**: Khi câu lệnh SQL hoặc đường dẫn đầu ra chứa giao thức `s3://` hoặc `minio://`, Valkyrie tự động nạp extension `httpfs` của DuckDB và ánh xạ tự động các biến môi trường chuẩn (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT_URL`, `S3_USE_SSL`, `S3_URL_STYLE`) vào DuckDB Secret. Data Engineer không cần viết bất kỳ dòng cấu hình kết nối nào trong code.
- **Single-Pass Evaluation (Đánh giá Một Lần Quét)**: Cơ chế quét dữ liệu tối ưu nhất của DuckDB: toàn bộ các điều kiện vi phạm được đánh giá trong đúng một lượt đọc từ đĩa (Single Scan), loại bỏ hoàn toàn việc đọc đĩa 2 lần như chiến thuật Anti-Join truyền thống, giúp tiết kiệm 50% dung lượng I/O và tăng gấp đôi tốc độ xử lý.
- **First-Violation Short-Circuit (Ngắt sớm tại vi phạm đầu tiên)**: Chính sách gán lỗi theo thứ tự ưu tiên của các cột: một dòng vi phạm nhiều điều kiện sẽ chỉ bị ghi nhận một lần duy nhất vào DLQ với mã lỗi và tên cột của vi phạm đầu tiên được phát hiện, bảo toàn tương quan 1:1 giữa số lượng dòng hỏng và số bản ghi DLQ.
- **Dead Letter Queue (DLQ / Bảng Dữ liệu Rác)**: Tập dữ liệu chứa toàn bộ các dòng vi phạm hợp đồng, được giữ nguyên vẹn cấu trúc các cột gốc và đính kèm 4 cột chẩn đoán chuẩn:
  - `_error_column` (`VARCHAR`): Cột đầu tiên phát hiện lỗi.
  - `_error_code` (`VARCHAR`): Mã lỗi kỹ thuật chuẩn hóa (như `ERR_NULL_VIOLATION`, `ERR_OUT_OF_RANGE`, `ERR_TOTAL_MISMATCH`).
  - `_raw_value` (`VARCHAR`): Giá trị nguyên bản vi phạm hợp đồng.
  - `_ingestion_timestamp` (`TIMESTAMP_TZ`): Thời điểm phán xét dữ liệu.
- **Clean Data (Dữ liệu Sạch)**: Tập dữ liệu gồm các dòng vượt qua toàn bộ các ràng buộc của contract (`_error_code IS NULL`), tự động loại bỏ các cột chẩn đoán lỗi, sẵn sàng nạp thẳng vào tầng Silver của Lakehouse.
- **ValidationResult (Kết quả Kiểm định Đa năng)**: Đối tượng trả về của Valkyrie, vừa hỗ trợ unpack trực tiếp như một tuple `clean_df, dlq_df = valkyrie.run(...)`, vừa cho phép truy cập các trường chi tiết: `.clean_df`, `.dlq_df`, `.metrics`, `.owner`.

---

### 3. Chính Sách Vận Hành & Phòng Vệ (Operational Policies)

- **Circuit Breaker (Cầu dao Tự ngắt)**: Cơ chế an toàn kích hoạt dừng pipeline khi tỷ lệ dòng lỗi (`error_rate = error_rows / total_rows`) vượt quá ngưỡng `max_error_rate` cho phép.
- **Quarantine Action (Hành động Cách ly)**:
  - `DLQ`: Cách ly các dòng lỗi sang bảng DLQ và cho phép các dòng sạch đi tiếp vào Silver layer (nếu chưa chạm ngưỡng Circuit Breaker).
  - `FAIL_FAST`: Dừng pipeline ngay lập tức nếu phát hiện bất kỳ dòng lỗi nào.
- **ContractBreachException**: Ngoại lệ được ném ra khi vi phạm Circuit Breaker hoặc gặp lệnh FAIL_FAST, tự động in khung cảnh báo trực quan nêu rõ `owner`, tỷ lệ lỗi thực tế và danh sách lỗi phổ biến nhất; đồng thời lưu kèm thuộc tính `.dlq_df` để pipeline lưu vết trước khi dừng.
- **Strict Schema (Cưỡng chế Khung dữ liệu Nghiêm ngặt)**: Chính sách bắt buộc dữ liệu đầu vào phải khớp hoàn toàn với contract: từ chối batch nếu thiếu cột bắt buộc hoặc xuất hiện cột lạ ngoài khai báo.
