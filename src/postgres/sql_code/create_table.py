create_customers_table = """
    -- 1. Bảng Khách hàng
    CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    city VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    ALTER TABLE customers REPLICA IDENTITY FULL;
"""

create_merchants_table = """
    -- 2. Bảng Cửa hàng
    CREATE TABLE merchants (
    merchant_id SERIAL PRIMARY KEY,
    merchant_name VARCHAR(100),
    category VARCHAR(50), -- Ví dụ: Food, Electronics, Fashion
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    ALTER TABLE merchants REPLICA IDENTITY FULL;
"""

create_transactions_table = """
    -- 3. Bảng Giao dịch (Bảng chính cho Pipeline)
    CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    merchant_id INT REFERENCES merchants(merchant_id),
    amount DECIMAL(15, 2), -- Số tiền giao dịch
    currency VARCHAR(3) DEFAULT 'VND',
    status VARCHAR(20), -- SUCCESS, FAILED, PENDING, REFUNDED
    payment_method VARCHAR(20), -- CREDIT_CARD, E-WALLET, CASH
    transaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    ALTER TABLE transactions REPLICA IDENTITY FULL;
"""
