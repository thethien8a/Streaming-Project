-- 1) Kafka Engine table = ClickHouse consumer subscribed to the topic.
--    Reads each message once per consumer group; do NOT SELECT from it directly
--    in production (it consumes the offset).
CREATE TABLE IF NOT EXISTS streaming.kafka_enriched_transactions
(
    transaction_id     String,
    customer_id        Int32,
    customer_name      String,
    customer_city      String,
    merchant_id        Int32,
    merchant_name      String,
    merchant_category  String,
    amount             Decimal(15, 2),
    currency           String,
    status             String,
    payment_method     String,
    transaction_time   DateTime64(3)
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list      = 'kafka:9092',
    kafka_topic_list       = 'processed.enriched_transactions',
    kafka_group_name       = 'clickhouse-enriched-transactions',
    kafka_format           = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_handle_error_mode = 'stream';  -- expose errors as virtual columns instead of stalling

-- 2) Final storage table. ReplacingMergeTree keeps the latest version per
--    transaction_id; on read use FINAL or GROUP BY to dedup.
--    transaction_time is the version column: newer rows win.
CREATE TABLE IF NOT EXISTS streaming.enriched_transactions
(
    transaction_id     String,
    customer_id        Int32,
    customer_name      String,
    customer_city      String,
    merchant_id        Int32,
    merchant_name      String,
    merchant_category  String,
    amount             Decimal(15, 2),
    currency           String,
    status             String,
    payment_method     String,
    transaction_time   DateTime64(3),
    ingested_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(transaction_time) -- If have any duplicate transaction_id, keep the one with the latest transaction_time
PARTITION BY toYYYYMM(transaction_time)
ORDER BY transaction_id;

-- 3) Materialized View = trigger that flushes Kafka batches into the
--    storage table automatically.
CREATE MATERIALIZED VIEW IF NOT EXISTS streaming.mv_enriched_transactions 
TO streaming.enriched_transactions AS
SELECT
    transaction_id,
    customer_id,
    customer_name,
    customer_city,
    merchant_id,
    merchant_name,
    merchant_category,
    amount,
    currency,
    status,
    payment_method,
    transaction_time
FROM streaming.kafka_enriched_transactions;
