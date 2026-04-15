import json
import time
import requests
from dotenv import load_dotenv
from config.debezium import CONNECTOR_CONFIG, CONNECT_URL

load_dotenv()

def wait_for_connect(max_retries: int = 30, delay: int = 2) -> bool:
    """Wait until Kafka Connect is ready."""
    for i in range(max_retries):
        try:
            resp = requests.get(f"{CONNECT_URL}/connectors")
            if resp.status_code == 200:
                print("Kafka Connect is ready!")
                return True
        except requests.ConnectionError:
            pass
        print(f"Waiting for Kafka Connect... ({i + 1}/{max_retries})")
        time.sleep(delay)
    return False


def register_connector() -> None:
    """Register the Debezium PostgreSQL connector."""
    if not wait_for_connect():
        print("ERROR: Kafka Connect is not available.")
        return

    connector_name = CONNECTOR_CONFIG["name"]

    resp = requests.get(f"{CONNECT_URL}/connectors/{connector_name}")
    if resp.status_code == 200:
        print(f"Connector '{connector_name}' already exists. Updating...")
        resp = requests.put(
            f"{CONNECT_URL}/connectors/{connector_name}/config",
            headers={"Content-Type": "application/json"},
            json=CONNECTOR_CONFIG["config"],
        )
    else:
        print(f"Creating connector '{connector_name}'...")
        resp = requests.post(
            f"{CONNECT_URL}/connectors",
            headers={"Content-Type": "application/json"},
            json=CONNECTOR_CONFIG,
        )

    if resp.status_code in (200, 201):
        print(f"Connector '{connector_name}' registered successfully!")
        print(json.dumps(resp.json(), indent=2))
    else:
        print(f"ERROR: Failed to register connector. Status: {resp.status_code}")
        print(resp.text)


if __name__ == "__main__":
    register_connector()
