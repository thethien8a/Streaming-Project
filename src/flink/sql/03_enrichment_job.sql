-- Enrichment Job: Join transactions with customer and merchant data
-- Produces enriched_transactions with full context for analytics

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
WHERE t.status = 'SUCCESS';
