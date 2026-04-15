# Kế Hoạch Triển Khai Chi Tiết — 4 Tuần

Dự án được chia thành **bốn giai đoạn chính**, đi từ việc thiết lập nền tảng hạ tầng đến tối ưu hóa và đóng gói vận hành.

---

## Tuần 1: Xây Dựng Nền Móng OLTP và CDC

**Mục tiêu:** Thiết lập luồng dữ liệu tự động từ database vào Kafka mà không cần can thiệp vào mã nguồn ứng dụng hiện có.

### Ngày 1–2: Setup Hạ Tầng Cơ Bản

Hệ thống được khởi tạo thông qua `docker-compose.yml`, bao gồm:
- PostgreSQL
- Cụm Kafka KRaft
- Schema Registry
- Kafka Connect (tích hợp plugin Debezium)

> Việc dùng Docker đảm bảo nhất quán môi trường giữa các nhà phát triển và đơn giản hóa triển khai.

### Ngày 3–4: Data Generator và Cấu Hình PostgreSQL

- Cấu hình PostgreSQL: `wal_level = logical` (bắt buộc để CDC hoạt động)
- Viết **Data Generator** bằng Python (thư viện `Faker`)
- Bắt đầu chèn hàng ngàn bản ghi vào bảng `transactions`, tạo "dòng chảy" dữ liệu thực tế cho toàn bộ pipeline

### Ngày 5–7: Triển Khai Debezium

- Cấu hình Debezium qua **REST API** đến Kafka Connect
- Khi Connector được kích hoạt:
  1. **Snapshot mode:** Quét toàn bộ dữ liệu hiện có trong bảng
  2. **Streaming mode:** Chuyển sang theo dõi các thay đổi trực tiếp

---

## Tuần 2: Xử Lý Luồng Hạng Nặng với Apache Flink

**Mục tiêu:** Xử lý dữ liệu thô và áp dụng các quy tắc kinh doanh vào dòng sự kiện.

### Ngày 1–2: Setup Flink Cluster

Thêm vào hệ sinh thái Docker:
- **JobManager**: điều phối các tác vụ
- **TaskManager**: thực hiện tính toán thực tế

### Ngày 3–5: Viết Logic Xử Lý bằng Flink SQL

Các câu lệnh SQL sẽ thực hiện:

```sql
-- Lọc giao dịch hợp lệ
SELECT * FROM transactions
WHERE amount > 0 AND status = 'SUCCESS'

-- Tính tổng doanh thu theo Tumbling Window 1 phút
SELECT
  TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
  store_id,
  SUM(amount) AS total_revenue
FROM clean_transactions
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), store_id
```

> Format `debezium-avro-confluent` giúp Flink tự động hiểu dữ liệu cần thiết nằm trong trường `after`.

### Ngày 6–7: Xuất Dữ Liệu ra Kafka

- Kết quả từ Flink được ghi vào topic **`clean-transactions`**
- Bước này thực hiện **decoupling** (tách biệt) lớp xử lý và lớp lưu trữ phân tích

---

## Tuần 3: Tích Hợp OLAP với ClickHouse

**Mục tiêu:** Đưa dữ liệu đã xử lý sạch vào môi trường phân tích hiệu năng cao.

### Ngày 1–2: Setup ClickHouse Server

- Khởi tạo và cấu hình ClickHouse
- ClickHouse được thiết kế để xử lý **ghi theo lô (bulk insert)** — rất phù hợp với luồng dữ liệu từ Kafka

### Ngày 3–5: Cấu Hình Kafka Engine

```sql
CREATE TABLE kafka_transactions (
  transaction_id String,
  store_id String,
  amount Float64,
  status String,
  event_time DateTime
) ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'clean-transactions',
  kafka_group_name = 'clickhouse-consumer',
  kafka_format = 'Avro';
```

### Ngày 6–7: Xử Lý Upsert với Materialized View

```sql
-- Bảng lưu trữ chính
CREATE TABLE transactions_final
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY transaction_id
AS SELECT * FROM kafka_transactions WHERE 1=0;

-- Materialized View tự động di chuyển dữ liệu
CREATE MATERIALIZED VIEW mv_transactions TO transactions_final
AS SELECT * FROM kafka_transactions;
```

---

## Tuần 4: Hoàn Thiện Data Lake MinIO và Đóng Gói Dự Án

**Mục tiêu:** Đảm bảo tính bền vững của dữ liệu và khả năng bàn giao dự án.

### Ngày 1–2: Setup MinIO và Kafka Connect S3 Sink

- Cấu hình MinIO như hệ thống lưu trữ đối tượng tương thích S3
- Kafka Connect S3 Sink đọc từ topic **thô ban đầu** (đảm bảo không mất dữ liệu)
- Ghi dữ liệu dạng **Parquet** theo cấu trúc: `year=.../month=.../day=...`

### Ngày 3–5: Tối Ưu Hóa và Xử Lý Lỗi

Kiểm thử dưới tải trọng lớn (**1000 records/s**):
- Đo lường độ trễ và khả năng chịu tải của Flink và ClickHouse
- Giả lập các kịch bản lỗi:
  - Mất kết nối mạng
  - Server chết
- Kiểm tra **Flink Checkpointing** và khả năng lưu giữ offset của Kafka

### Ngày 6–7: Viết Tài Liệu (Documentation)

Tài liệu bao gồm:
- [ ] Sơ đồ kiến trúc tổng thể
- [ ] Hướng dẫn thiết lập từng bước (step-by-step setup guide)
- [ ] Giải thích các quyết định thiết kế cốt lõi (Architecture Decision Records)
- [ ] Runbook vận hành và xử lý sự cố

---

## Tóm Tắt Timeline

```
Tuần 1: PostgreSQL → Debezium → Kafka          [CDC Pipeline]
Tuần 2: Kafka → Flink → clean-transactions     [Stream Processing]
Tuần 3: clean-transactions → ClickHouse        [OLAP Integration]
Tuần 4: Kafka → MinIO + Optimization + Docs    [Data Lake & Hardening]
```
