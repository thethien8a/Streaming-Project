import os
from dotenv import load_dotenv

load_dotenv()

CONNECT_URL = "http://localhost:8083"

CONNECTOR_CONFIG = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": "data_source",
        "topic.prefix": "streaming",
        "schema.include.list": "public",
        "table.include.list": "public.customers,public.merchants,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
    },
}
