INSERT INTO enriched_transactions
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
    t.transaction_time
FROM transactions AS t
LEFT JOIN customers c
    ON t.customer_id = c.customer_id
LEFT JOIN merchants m
    ON t.merchant_id = m.merchant_id
WHERE t.status = 'SUCCESS' AND t.amount > 0;

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
