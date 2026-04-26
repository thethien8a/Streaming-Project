-- Auto-init: Create all tables on SQL Client startup
-- This file is loaded via sql-client -i flag

-- Source tables
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT,
    full_name STRING,
    email STRING,
    city STRING,
    created_at TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'streaming.public.customers',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-customer-reader',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'debezium-avro-confluent',
    'value.debezium-avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id INT,
    merchant_name STRING,
    category STRING,
    created_at TIMESTAMP(3) 
) WITH (
    'connector' = 'kafka',
    'topic' = 'streaming.public.merchants',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-merchant-reader',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'debezium-avro-confluent',
    'value.debezium-avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id STRING,
    customer_id INT,
    merchant_id INT,
    amount DECIMAL(15, 2),
    currency STRING,
    status STRING,
    payment_method STRING,
    transaction_time TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
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
    transaction_time TIMESTAMP(3),
    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND,
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


