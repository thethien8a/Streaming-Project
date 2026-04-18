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
        self._connection = None

    def connect(self):
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        return self._connection

    def close(self):
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None
