from src.postgres.python.connector import PostgresSQL
from src.data_generator.init_data import seed_initial_data
from src.data_generator.loop_gen import loop_generate
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_id_cus_mer(connection):
    cursor = connection.cursor()
    # Lấy danh sách ID hiện tại vào bộ nhớ
    cursor.execute("SELECT customer_id FROM customers")
    c_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT merchant_id FROM merchants")
    m_ids = [row[0] for row in cursor.fetchall()]

    return c_ids, m_ids

def main():
    try:
        p = PostgresSQL()
        
        conn = p.connect()
        
        # Bước 1: Seed dữ liệu ban đầu
        seed_initial_data(conn)
        
        # Bước 2: Lấy danh sách ID hiện tại
        customer_ids, merchant_ids = get_id_cus_mer(conn)

        # Bước 3: Vòng lặp sinh dữ liệu
        loop_generate(conn, customer_ids, merchant_ids)
        
    except KeyboardInterrupt:
        logger.info("Đã dừng script theo yêu cầu.")
    except Exception as e:
        logger.error(f"Lỗi hệ thống: {e}")
    finally:
        p.close()

if __name__ == "__main__":
    main()