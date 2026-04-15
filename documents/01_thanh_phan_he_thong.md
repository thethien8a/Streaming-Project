# Chi Tiết Các Thành Phần Hệ Thống

Hệ thống bao gồm **sáu lớp thành phần** được tích hợp chặt chẽ, mỗi lớp giải quyết một thách thức cụ thể trong vòng đời của dữ liệu.

---

## 1. Lớp Sinh Dữ Liệu và OLTP (PostgreSQL)

**Thành phần:** Data Generator + PostgreSQL

- **Data Generator** là kịch bản viết bằng Python/Java, mô phỏng các hoạt động giao dịch thực tế
- Không chỉ chèn dữ liệu hợp lệ mà còn chủ động tạo **bản ghi lỗi** (giao dịch có số tiền âm, trạng thái không xác định) để kiểm thử khả năng lọc
- **PostgreSQL** hỗ trợ cơ chế **logical decoding** — bắt buộc để CDC hoạt động
- Cấu hình yêu cầu: `wal_level = logical`

---

## 2. Lớp Thu Thập Dữ Liệu Thay Đổi — CDC (Debezium)

**Debezium** đóng vai trò là "người lắng nghe" các thay đổi từ **Write-Ahead Log (WAL)** của PostgreSQL.

### Ưu điểm so với Polling
Khác với truy vấn định kỳ (polling) vốn gây tải nặng cho database, CDC ghi nhận mọi sự kiện `INSERT`, `UPDATE`, `DELETE` **ngay khi được commit** mà không làm gián đoạn luồng giao dịch.

### Tại sao dùng Debezium thay vì Application-level Events?
Cách tiếp cận **Dual Write** (ứng dụng tự gửi sự kiện vào Kafka khi ghi database) rất dễ dẫn đến **không nhất quán dữ liệu** nếu một trong hai thao tác thất bại. CDC giải quyết triệt để bằng cách sử dụng chính bản ghi thay đổi của database làm **nguồn sự thật duy nhất**.

### Định dạng dữ liệu
- Dữ liệu được đóng gói dưới định dạng **Avro** — nhị phân nhỏ gọn, tối ưu hơn JSON truyền thống
- Kết hợp **Schema Registry** để quản lý schema evolution an toàn khi cấu trúc bảng thay đổi

---

## 3. Lớp Môi Giới Tin Nhắn — Message Broker (Kafka KRaft)

**Apache Kafka** với chế độ **KRaft** (không phụ thuộc Zookeeper) là hệ thống môi giới tin nhắn trung tâm.

### Các thành phần Kafka

| Thành phần | Vai trò kỹ thuật | Lợi ích cốt lõi |
|------------|-----------------|-----------------|
| Kafka Brokers | Lưu trữ và phân phối dòng sự kiện | Đảm bảo tính sẵn sàng cao và khả năng mở rộng |
| Schema Registry | Quản lý phiên bản Avro Schemas | Đảm bảo tính tương thích giữa Producer và Consumer |
| Kafka Connect | Tích hợp nguồn và đích dữ liệu | Tự động hóa di chuyển dữ liệu không cần code |

### Lợi ích của KRaft
- Đơn giản hóa việc quản trị cụm Kafka
- Cải thiện tốc độ phục hồi khi có sự cố

---

## 4. Lớp Xử Lý Luồng — Stream Processing (Apache Flink)

**Apache Flink** là trái tim của hệ thống xử lý, chịu trách nhiệm biến dữ liệu thô thành thông tin có giá trị.

### Quy trình xử lý

```
Kafka (raw) → Flink → Kafka (clean-transactions)
```

**Bước 1 — Giải mã Debezium envelope:**
Flink hỗ trợ format `debezium-avro-confluent`, tự động hiểu dữ liệu cần thiết nằm trong trường `after` (bao gồm các phần `before`, `after`, và `op`)

**Bước 2 — Filter (Lọc):**
```sql
WHERE amount > 0 AND status = 'SUCCESS'
```

**Bước 3 — Windowing (Gom nhóm theo cửa sổ):**
Sử dụng **Tumbling Window** để tính toán chỉ số tổng hợp (ví dụ: tổng doanh thu mỗi phút theo từng cửa hàng)

**Bước 4 — Xuất dữ liệu:**
Kết quả được đẩy vào Kafka topic `clean-transactions`

### Flink SQL
**Flink SQL** được ưu tiên nhờ tính declarative — các kỹ sư dữ liệu dễ dàng định nghĩa phép biến đổi mà không cần kiến thức sâu về Java.

---

## 5. Lớp Phân Tích Trực Tuyến — OLAP (ClickHouse)

**ClickHouse** được chọn làm kho dữ liệu phân tích nhờ tốc độ truy vấn vượt trội với tập dữ liệu quy mô **hàng tỷ dòng**.

### Cơ chế tích hợp Kafka
ClickHouse sử dụng **Kafka Table Engine** (`ENGINE = Kafka`) để tự động subscribe vào topic `clean-transactions`, đóng vai trò như một buffer tạm giữ dữ liệu trước khi đẩy vào kho lưu trữ chính.

### Xử lý Duplicate với ReplacingMergeTree
Do Flink có thể gửi lại dữ liệu (retries) hoặc cập nhật kết quả trong cùng một cửa sổ thời gian, hệ thống dùng:

- **ReplacingMergeTree**: ghi đè bản ghi cũ bằng phiên bản mới nhất dựa trên khóa định danh duy nhất
- **Materialized View**: tự động di chuyển dữ liệu từ bảng Kafka Engine sang bảng ReplacingMergeTree

> ⚠️ **Lưu ý:** ReplacingMergeTree là "eventual consistency". Để lấy dữ liệu chính xác ngay lập tức, dùng từ khóa `FINAL` hoặc `GROUP BY` trên khóa chính.

---

## 6. Lớp Hồ Dữ Liệu — Data Lake (MinIO)

**MinIO** được cấu hình như hệ thống lưu trữ đối tượng tương thích S3, phục vụ nhu cầu lưu trữ dài hạn và phân tích hồi cứu (back-testing).

- Dữ liệu thô được đẩy vào MinIO qua **Kafka Connect S3 Sink**
- Lưu dưới định dạng **Parquet** — lưu trữ hướng cột, tối ưu cho analytics
- Phân chia theo cấu trúc thư mục thời gian: `year=.../month=...`
- Tương thích với Apache Spark, Trino để phân tích chuyên sâu

### So sánh định dạng dữ liệu

| Định dạng | Ưu điểm chính | Sử dụng phù hợp |
|-----------|--------------|-----------------|
| Avro | Có schema đi kèm, nén tốt | Truyền tải giữa các thành phần real-time |
| Parquet | Lưu trữ hướng cột, cực nhanh cho analytics | Lưu trữ dài hạn trong Data Lake (MinIO) |
| JSON | Dễ đọc, linh hoạt | Debug và phát triển ban đầu |
