from faker import Faker
import random
from src.postgres.python.connector import PostgresSQL
from src.postgres.sql_code.create_table import create_customers_table, create_merchants_table, create_transactions_table
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

def check_table_exists(cursor, table_name):
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
    return cursor.fetchone()[0]

def seed_initial_data(connection):
    cursor = connection.cursor()
    try:
        # 1. Kiểm tra và nạp Khách hàng
        if not check_table_exists(cursor, 'customers'):
            logger.warning("Bảng customers chưa được tạo. Khởi tạo bảng customers...")
            cursor.execute(create_customers_table)
            connection.commit()

        cursor.execute("SELECT count(*) FROM customers")
        if cursor.fetchone()[0] == 0:
            logger.info("Đang nạp 100 khách hàng đầu tiên...")
            for _ in range(100):
                cursor.execute("INSERT INTO customers (full_name, email, city) VALUES (%s, %s, %s)", 
                            (fake.name(), fake.unique.email(), fake.city()))
            logger.info("Đã nạp 100 khách hàng đầu tiên...")
        
        # 2. Kiểm tra và nạp Cửa hàng
        if not check_table_exists(cursor, 'merchants'):
            logger.warning("Bảng merchants chưa được tạo. Khởi tạo bảng merchants...")
            cursor.execute(create_merchants_table)
            connection.commit()
        
        cursor.execute("SELECT count(*) FROM merchants")
        if cursor.fetchone()[0] == 0:
            logger.info("Đang nạp 10 cửa hàng đầu tiên...")
            for _ in range(10):
                cursor.execute("INSERT INTO merchants (merchant_name, category) VALUES (%s, %s)", 
                            (fake.company(), random.choice(['Food', 'Electronics', 'Fashion'])))
            logger.info("Đã nạp 10 cửa hàng đầu tiên...")
        
        connection.commit()
    except Exception as e:
        logger.error(f"Lỗi khi seed dữ liệu: {e}")
        connection.rollback()
    finally:
        cursor.close()
        