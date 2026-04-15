# Tổng Quan và Tầm Nhìn Kiến Trúc

## Bối Cảnh

Sự bùng nổ của nền kinh tế số và yêu cầu về tính tức thời trong việc ra quyết định đã thúc đẩy các doanh nghiệp chuyển dịch từ kiến trúc xử lý dữ liệu theo lô (**batch processing**) sang xử lý luồng thời gian thực (**real-time stream processing**).

Kiến trúc dữ liệu truyền thống thường dựa trên các quy trình **ETL (Extract, Transform, Load)** chạy định kỳ, dẫn đến độ trễ dữ liệu tính bằng giờ hoặc thậm chí bằng ngày. Điều này tạo ra một *"khoảng trống thông tin"* khiến doanh nghiệp không thể phản ứng kịp thời với các biến động thị trường hoặc sự cố hệ thống.

## Tầm Nhìn Dự Án

Tầm nhìn của dự án là thiết lập một **"đường ống" dữ liệu (data pipeline)** liên tục, nơi dữ liệu giao dịch từ cơ sở dữ liệu lõi (OLTP) được đồng bộ hóa, làm sạch và đưa vào kho phân tích (OLAP) với **độ trễ tính bằng giây**.

Việc xây dựng hệ thống này không chỉ là lợi thế cạnh tranh mà còn là yếu tố sống còn để:
- Tối ưu hóa vận hành
- Phát hiện gian lận
- Nâng cao trải nghiệm khách hàng

## Nguyên Lý Thiết Kế

Hệ thống được thiết kế dựa trên hai nguyên lý cốt lõi:

1. **Tách biệt hoàn toàn** giữa lớp ghi dữ liệu và lớp phân tích
2. **Kiến trúc hướng sự kiện (Event-Driven Architecture)** — cho phép các thành phần hạ nguồn tiêu thụ dữ liệu theo tốc độ riêng mà không gây ảnh hưởng đến hiệu năng của cơ sở dữ liệu sản xuất

**Apache Kafka** đóng vai trò xương sống để đảm bảo tính đàn hồi và khả năng mở rộng của toàn bộ hệ thống.

## Stack Công Nghệ

| Lớp | Công nghệ |
|-----|-----------|
| OLTP (nguồn dữ liệu) | PostgreSQL |
| CDC | Debezium |
| Message Broker | Apache Kafka (KRaft mode) |
| Stream Processing | Apache Flink |
| OLAP (phân tích) | ClickHouse |
| Data Lake | MinIO (S3-compatible) |
| Schema Management | Confluent Schema Registry |
| Monitoring | Prometheus + Grafana |
