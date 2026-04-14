
from faker import Faker
import random
from postgres.python.connector import PostgresSQL

fake = Faker()

def seed_initial_data(connection):
    cursor = connection.cursor()
    # 1. Kiểm tra và nạp Khách hàng
    cursor.execute("SELECT count(*) FROM customers")
    if cursor.fetchone()[0] == 0:
        print("Đang nạp 100 khách hàng đầu tiên...")
        for _ in range(100):
            cursor.execute("INSERT INTO customers (full_name, email, city) VALUES (%s, %s, %s)", 
                           (fake.name(), fake.unique.email(), fake.city()))
    
    # 2. Kiểm tra và nạp Cửa hàng
    cursor.execute("SELECT count(*) FROM merchants")
    if cursor.fetchone()[0] == 0:
        print("Đang nạp 10 cửa hàng đầu tiên...")
        for _ in range(10):
            cursor.execute("INSERT INTO merchants (merchant_name, category) VALUES (%s, %s)", 
                           (fake.company(), random.choice(['Food', 'Electronics', 'Fashion'])))
    
    connection.commit()

if __name__ == "__main__":
    postgres = PostgresSQL()
    seed_initial_data(postgres.connect())
    postgres.close()