import sys
import os
import json
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from config.kafka_config import (
    consumer_config,
    TOPIC_RAW_LOGS,
    ES_HOST,
    ES_INDEX_LOGS,
)

CONSUMER_GROUP = "log-indexer"


def get_es_client() -> Elasticsearch:
    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError(f"Cannot connect to Elasticsearch at {ES_HOST}")
    print(f"[log-consumer] Connected to Elasticsearch at {ES_HOST}")
    return es


def ensure_index(es: Elasticsearch):
    if not es.indices.exists(index=ES_INDEX_LOGS):
        es.indices.create(
            index=ES_INDEX_LOGS,
            body={
                "mappings": {
                    "properties": {
                        "timestamp": {"type": "date"},
                        "service":   {"type": "keyword"},
                        "level":     {"type": "keyword"},
                        "message":   {"type": "text"},
                        "host":      {"type": "keyword"},
                        "trace_id":  {"type": "keyword"},
                    }
                }
            },
        )
        print(f"[log-consumer] Created index: {ES_INDEX_LOGS}")


def main():
    es = get_es_client()
    ensure_index(es)

    consumer = KafkaConsumer(
        TOPIC_RAW_LOGS,
        **consumer_config(CONSUMER_GROUP),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print(f"[log-consumer] Listening on topic: {TOPIC_RAW_LOGS}")

    try:
        for msg in consumer:
            log = msg.value

            es.index(index=ES_INDEX_LOGS, document=log)

            print(
                f"[{log.get('level')}] [{log.get('service')}] "
                f"{log.get('message')} → indexed to ES"
            )

    except KeyboardInterrupt:
        print("\n[log-consumer] Shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
