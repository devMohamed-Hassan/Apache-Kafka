import sys
import os
import json
from datetime import datetime, timezone

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from config.kafka_config import (
    consumer_config,
    TOPIC_ALERTS,
    ES_HOST,
    ES_INDEX_ALERTS,
)

CONSUMER_GROUP = "alert-handler"

LEVEL_COLOR = {
    "ERROR": "\033[91m",
    "WARN":  "\033[93m",
    "RESET": "\033[0m",
}


def get_es_client() -> Elasticsearch:
    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError(f"Cannot connect to Elasticsearch at {ES_HOST}")
    print(f"[alert-consumer] Connected to Elasticsearch at {ES_HOST}")
    return es


def ensure_index(es: Elasticsearch):
    if not es.indices.exists(index=ES_INDEX_ALERTS):
        es.indices.create(
            index=ES_INDEX_ALERTS,
            body={
                "mappings": {
                    "properties": {
                        "timestamp":  {"type": "date"},
                        "service":    {"type": "keyword"},
                        "level":      {"type": "keyword"},
                        "message":    {"type": "text"},
                        "alerted_at": {"type": "date"},
                    }
                }
            },
        )
        print(f"[alert-consumer] Created index: {ES_INDEX_ALERTS}")


def handle_alert(alert: dict, es: Elasticsearch):
    level = alert.get("level", "UNKNOWN")
    color = LEVEL_COLOR.get(level, "")
    reset = LEVEL_COLOR["RESET"]

    print(
        f"{color}ALERT [{level}] [{alert.get('service')}] "
        f"{alert.get('message')}{reset}"
    )

    alert["alerted_at"] = datetime.now(timezone.utc).isoformat()
    es.index(index=ES_INDEX_ALERTS, document=alert)


def main():
    es = get_es_client()
    ensure_index(es)

    consumer = KafkaConsumer(
        TOPIC_ALERTS,
        **consumer_config(CONSUMER_GROUP),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print(f"[alert-consumer] Listening on topic: {TOPIC_ALERTS}")

    try:
        for msg in consumer:
            handle_alert(msg.value, es)

    except KeyboardInterrupt:
        print("\n[alert-consumer] Shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()