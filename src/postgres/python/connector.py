import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

class PostgresSQL():
    def __init__(self):
        self.host = "localhost"
        self.port = 5432
        self.database = "data_source"
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
 
    def connect(self):
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        return connection
    
    def close(self):
        self.connect().close()
