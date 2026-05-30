import os
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_RAW_LOGS = os.getenv("TOPIC_RAW_LOGS", "raw-logs")
TOPIC_ALERTS   = os.getenv("TOPIC_ALERTS",   "alerts")
TOPIC_METRICS  = os.getenv("TOPIC_METRICS",  "metrics")

ES_HOST        = os.getenv("ES_HOST",        "http://localhost:9200")
ES_INDEX_LOGS  = os.getenv("ES_INDEX_LOGS",  "logs")
ES_INDEX_ALERTS = os.getenv("ES_INDEX_ALERTS", "alerts")

PRODUCER_CONFIG = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 3,
    "linger_ms": 10,
}

def consumer_config(group_id: str) -> dict:
    return {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "group_id": group_id,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": True,
        "auto_commit_interval_ms": 1000,
    }


LOG_LEVELS   = ["INFO", "INFO", "INFO", "WARN", "ERROR"]
ALERT_LEVELS = {"ERROR", "WARN"}
