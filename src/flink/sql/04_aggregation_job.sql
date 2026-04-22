-- Aggregation Job: Real-time revenue by merchant in 1-minute tumbling windows
-- Uses the enriched_transactions as source

INSERT INTO merchant_revenue_1min
SELECT
    window_start,
    window_end,
    merchant_id,
    merchant_name,
    merchant_category,
    SUM(amount) AS total_revenue,
    COUNT(*) AS transaction_count
FROM TABLE(
    TUMBLE(TABLE enriched_transactions, DESCRIPTOR(transaction_time), INTERVAL '1' MINUTE)
)
GROUP BY
    window_start,
    window_end,
    merchant_id,
    merchant_name,
    merchant_category;
