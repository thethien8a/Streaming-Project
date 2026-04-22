-- Sink tables: Write processed results to new Kafka topics
-- ClickHouse will consume from these processed topics via its Kafka Engine

CREATE TABLE enriched_transactions (
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

CREATE TABLE merchant_revenue_1min (
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
