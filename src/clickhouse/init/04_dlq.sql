-- =====================================================================
-- Dead Letter Queue (DLQ)
-- Captures messages that fail to parse from any Kafka Engine table.
-- Requires `kafka_handle_error_mode = 'stream'` on the source tables.
--
-- When a row fails to parse, ClickHouse exposes two virtual columns on
-- the Kafka table:
--   _error        - the parser error message
--   _raw_message  - the raw bytes of the offending message
-- We route those to a single MergeTree so the consumer keeps moving
-- instead of stalling on a single poison pill.
-- =====================================================================

CREATE TABLE IF NOT EXISTS streaming.kafka_dlq
(
    ingested_at  DateTime DEFAULT now(),
    source_table LowCardinality(String),
    topic        LowCardinality(String),
    partition    Int64,
    offset       Int64,
    error        String,
    raw_message  String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_table, ingested_at)
TTL ingested_at + INTERVAL 30 DAY;

-- Routing MV: enriched_transactions errors -> DLQ
CREATE MATERIALIZED VIEW IF NOT EXISTS streaming.mv_dlq_enriched_transactions
TO streaming.kafka_dlq AS
SELECT
    now()                              AS ingested_at,
    'kafka_enriched_transactions'      AS source_table,
    _topic                             AS topic,
    _partition                         AS partition,
    _offset                            AS offset,
    _error                             AS error,
    _raw_message                       AS raw_message
FROM streaming.kafka_enriched_transactions
WHERE length(_error) > 0;

-- Routing MV: merchant_revenue_1min errors -> DLQ
CREATE MATERIALIZED VIEW IF NOT EXISTS streaming.mv_dlq_merchant_revenue_1min
TO streaming.kafka_dlq AS
SELECT
    now()                              AS ingested_at,
    'kafka_merchant_revenue_1min'      AS source_table,
    _topic                             AS topic,
    _partition                         AS partition,
    _offset                            AS offset,
    _error                             AS error,
    _raw_message                       AS raw_message
FROM streaming.kafka_merchant_revenue_1min
WHERE length(_error) > 0;
