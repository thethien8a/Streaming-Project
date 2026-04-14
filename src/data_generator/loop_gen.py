import logging
import random
import time
import uuid
from faker import Faker
from src.postgres.sql_code.create_table import create_transactions_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

def loop_generate(connection, customer_ids, merchant_ids):
    cursor = connection.cursor()

    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'transactions')")
    if cursor.fetchone()[0] == 0:
        try:
            cursor.execute(create_transactions_table)
            connection.commit()
        except Exception as e:
            logger.error(f"Lỗi tạo bảng transactions: {e}")
            return
    
    while True:
        try:
            action = random.choices(
                ['INSERT_TX', 'UPDATE_TX', 'NEW_CUSTOMER', 'NEW_MERCHANT', 'INVALID_TX'], 
                weights=[65, 15, 7, 3, 10], # 10% dữ liệu lỗi để test Flink Filter
                k=1
            )[0]

            if action == 'NEW_CUSTOMER':
                # Giả lập có người dùng mới đăng ký
                cursor.execute(
                    "INSERT INTO customers (full_name, email, city) VALUES (%s, %s, %s) RETURNING customer_id",
                    (fake.name(), fake.unique.email(), fake.city())
                )
                new_id = cursor.fetchone()[0]
                customer_ids.append(new_id)
                logger.info(f"[NEW_CUSTOMER] ID: {new_id}")

            elif action == 'NEW_MERCHANT':
                # Giả lập có cửa hàng mới tham gia hệ thống
                cursor.execute(
                    "INSERT INTO merchants (merchant_name, category) VALUES (%s, %s) RETURNING merchant_id",
                    (fake.company(), random.choice(['Food', 'Electronics', 'Fashion', 'Groceries']))
                )
                new_id = cursor.fetchone()[0]
                merchant_ids.append(new_id)
                logger.info(f"[NEW_MERCHANT] ID: {new_id}")

            elif action == 'INSERT_TX':
                # Giao dịch hợp lệ (Trạng thái ban đầu thường là PENDING)
                tx_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO transactions (transaction_id, customer_id, merchant_id, amount, status, payment_method)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    tx_id, random.choice(customer_ids), random.choice(merchant_ids), 
                    round(random.uniform(50000, 10000000), 2), 
                    'PENDING', 
                    random.choice(['CREDIT_CARD', 'E-WALLET', 'CASH'])
                ))
                logger.info(f"[INSERT_TX] ID: {tx_id[:8]}... - Status: PENDING")

            elif action == 'UPDATE_TX':
                # Giả lập hệ thống thanh toán xác nhận giao dịch thành công (Test CDC Update)
                # Tìm một giao dịch PENDING ngẫu nhiên để chuyển sang SUCCESS
                cursor.execute("""
                    UPDATE transactions 
                    SET status = 'SUCCESS', updated_at = CURRENT_TIMESTAMP 
                    WHERE transaction_id IN (
                        SELECT transaction_id FROM transactions 
                        WHERE status = 'PENDING' 
                        ORDER BY random() LIMIT 1
                    )
                    RETURNING transaction_id
                """)
                updated = cursor.fetchone()
                if updated:
                    logger.info(f"[UPDATE_TX] ID: {updated[0][:8]}... -> SUCCESS")

            elif action == 'INVALID_TX':
                # Tạo dữ liệu lỗi: Amount âm hoặc Status không hợp lệ
                # Mục đích: Test Flink SQL Filter (amount > 0 AND status = 'SUCCESS')
                tx_id = str(uuid.uuid4())
                is_negative = random.choice([True, False])
                amount = round(random.uniform(-500000, -10000), 2) if is_negative else round(random.uniform(10000, 100000), 2)
                status = 'ERROR_SYSTEM' if not is_negative else 'SUCCESS'
                
                cursor.execute("""
                    INSERT INTO transactions (transaction_id, customer_id, merchant_id, amount, status, payment_method)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    tx_id, random.choice(customer_ids), random.choice(merchant_ids), 
                    amount, status, 'CASH'
                ))
                logger.info(f"[INVALID_TX] ID: {tx_id[:8]}... - Amount: {amount} - Status: {status}")

            connection.commit()
            
        except Exception as e:
            logger.error(f"Lỗi trong vòng lặp: {e}")
            connection.rollback()
        
        time.sleep(random.uniform(0.2, 1.5))