-- =====================================================================
-- merchant_revenue_1min: pre-aggregated 1-minute tumbling windows from Flink
-- Source topic: processed.merchant_revenue_1min (Avro via Schema Registry)
-- =====================================================================

CREATE TABLE IF NOT EXISTS streaming.kafka_merchant_revenue_1min
(
    window_start       DateTime64(3),
    window_end         DateTime64(3),
    merchant_id        Int32,
    merchant_name      String,
    merchant_category  String,
    total_revenue      Decimal(15, 2),
    transaction_count  Int64
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list      = 'kafka:9092',
    kafka_topic_list       = 'processed.merchant_revenue_1min',
    kafka_group_name       = 'clickhouse-merchant-revenue-1min',
    kafka_format           = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers    = 1,
    kafka_thread_per_consumer = 0,
    kafka_handle_error_mode = 'stream';  -- expose errors as virtual columns instead of stalling

-- A given (window_start, merchant_id) is unique => ReplacingMergeTree
-- keyed on it absorbs any Flink retry that re-emits the same window.
CREATE TABLE IF NOT EXISTS streaming.merchant_revenue_1min
(
    window_start       DateTime64(3),
    window_end         DateTime64(3),
    merchant_id        Int32,
    merchant_name      String,
    merchant_category  String,
    total_revenue      Decimal(15, 2),
    transaction_count  Int64,
    ingested_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, merchant_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS streaming.mv_merchant_revenue_1min
TO streaming.merchant_revenue_1min AS
SELECT
    window_start,
    window_end,
    merchant_id,
    merchant_name,
    merchant_category,
    total_revenue,
    transaction_count
FROM streaming.kafka_merchant_revenue_1min;
