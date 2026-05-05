CREATE TEMPORARY VIEW enriched_view AS
SELECT
    t.transaction_id,
    t.customer_id,
    c.full_name AS customer_name,
    c.city AS customer_city,
    t.merchant_id,
    m.merchant_name,
    m.category AS merchant_category,
    t.amount,
    t.currency,
    t.status,
    t.payment_method,
    -- t.event_time is the TIMESTAMP_LTZ(3) rowtime, derived from transactions.transaction_time
    -- (= Postgres commit time of the row). Renamed to transaction_time for the downstream
    -- sink/aggregation; CAST to TIMESTAMP(3) happens at the INSERT site.
    t.event_time AS transaction_time
FROM transactions AS t
-- Processing-time temporal join against the JDBC-backed dimension tables.
-- Each transaction is enriched with the *current* customer/merchant state at the time
-- this row is processed by Flink. This is the correct pattern when the dimension data
-- has no full change history (e.g. CDC topic only has the latest snapshot, or the
-- transactions are back-dated relative to when the dimension topic was first populated).
LEFT JOIN customers FOR SYSTEM_TIME AS OF t.proc_time AS c
    ON t.customer_id = c.customer_id
LEFT JOIN merchants FOR SYSTEM_TIME AS OF t.proc_time AS m
    ON t.merchant_id = m.merchant_id
WHERE t.status = 'SUCCESS' AND t.amount > 0;

INSERT INTO enriched_transactions
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
    -- Sink column is TIMESTAMP(3) (avro-confluent can't derive a schema for TIMESTAMP_LTZ).
    -- The session timezone is pinned to UTC in FlinkJobSubmitter.java so this CAST is deterministic.
    CAST(transaction_time AS TIMESTAMP(3)) AS transaction_time
FROM enriched_view;

INSERT INTO merchant_revenue_1min
SELECT
    CAST(window_start AS TIMESTAMP(3)) AS window_start,
    CAST(window_end AS TIMESTAMP(3)) AS window_end,
    merchant_id,
    merchant_name,
    merchant_category,
    SUM(amount) AS total_revenue,
    COUNT(*) AS transaction_count
FROM TABLE(
    TUMBLE(TABLE enriched_view, DESCRIPTOR(transaction_time), INTERVAL '1' MINUTE)
)
GROUP BY
    window_start,
    window_end,
    merchant_id,
    merchant_name,
    merchant_category;
