-- Source tables: Read raw CDC events from Debezium Kafka topics
-- Format: avro-confluent (Avro + Confluent Schema Registry)

CREATE TABLE customers (
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

CREATE TABLE merchants (
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

CREATE TABLE transactions (
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
