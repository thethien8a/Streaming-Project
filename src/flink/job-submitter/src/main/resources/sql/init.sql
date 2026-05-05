-- Auto-init: Create all tables on Flink job startup.

-- Dimension tables: read directly from Postgres via JDBC lookup.
-- Avoids the "back-dated transactions vs late-snapshotted CDC topic" mismatch:
-- we always look up the *current* customer/merchant row at processing time,
-- which is correct for stream enrichment when no SCD-type-2 history exists.
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    full_name STRING,
    email STRING,
    city STRING,
    created_at BIGINT,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/data_source',
    'table-name' = 'customers',
    'username' = '${POSTGRES_USER}',
    'password' = '${POSTGRES_PASSWORD}',
    'lookup.cache' = 'PARTIAL',
    'lookup.partial-cache.max-rows' = '100000',
    'lookup.partial-cache.expire-after-write' = '10 min',
    'lookup.partial-cache.cache-missing-key' = 'true',
    'lookup.max-retries' = '3'
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id INT,
    merchant_name STRING,
    category STRING,
    created_at BIGINT,
    PRIMARY KEY (merchant_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/data_source',
    'table-name' = 'merchants',
    'username' = '${POSTGRES_USER}',
    'password' = '${POSTGRES_PASSWORD}',
    'lookup.cache' = 'PARTIAL',
    'lookup.partial-cache.max-rows' = '10000',
    'lookup.partial-cache.expire-after-write' = '10 min',
    'lookup.partial-cache.cache-missing-key' = 'true',
    'lookup.max-retries' = '3'
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id STRING,
    customer_id INT,
    merchant_id INT,
    amount DECIMAL(15, 2),
    currency STRING,
    status STRING,
    payment_method STRING,
    -- Postgres TIMESTAMP(6) -> Debezium MicroTimestamp (Avro long microseconds since epoch).
    -- Flink-Avro can't decode long-as-TIMESTAMP(p>3), and declaring TIMESTAMP(3) misreads
    -- micros as millis (1000x off, watermark in year ~58000). Read as BIGINT and convert.
    transaction_time BIGINT,
    updated_at BIGINT,
    -- Rowtime = the actual Postgres commit time of this transaction (business event time).
    -- Used by the windowed aggregation downstream.
    event_time AS TO_TIMESTAMP_LTZ(transaction_time / 1000, 3),
    -- Processing-time attribute used to drive the JDBC lookup join.
    -- Required because the lookup join syntax `FOR SYSTEM_TIME AS OF <col>` must reference
    -- a PROCTIME() column when the build side is a JDBC lookup table.
    proc_time AS PROCTIME(),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'streaming.public.transactions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-transaction-reader',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'debezium-avro-confluent',
    'value.debezium-avro-confluent.url' = 'http://schema-registry:8081'
);

-- Sink tables
-- Sink-only: no WATERMARK needed (we no longer read this table back as a source;
-- the aggregation now reads from the in-memory `enriched_view` instead).
CREATE TABLE IF NOT EXISTS enriched_transactions (
    transaction_id STRING,
    customer_id INT,
    customer_name STRING,
    customer_city STRING,
    merchant_id INT,
    merchant_name STRING,
    merchant_category STRING,
    amount DECIMAL(15, 2),
    currency STRING,
    status STRING,
    payment_method STRING,
    -- Sink column is TIMESTAMP(3) 
    transaction_time TIMESTAMP(3),
    PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'processed.enriched_transactions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'raw',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TABLE IF NOT EXISTS merchant_revenue_1min (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    merchant_id INT,
    merchant_name STRING,
    merchant_category STRING,
    total_revenue DECIMAL(15, 2),
    transaction_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'processed.merchant_revenue_1min',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'key.fields' = 'window_start;merchant_id',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'EXCEPT_KEY'
);
