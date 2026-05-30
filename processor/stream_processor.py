import sys
import os
import json
import threading
import time
from datetime import datetime, timezone
from collections import defaultdict

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from kafka import KafkaConsumer, KafkaProducer
from config.kafka_config import (
    PRODUCER_CONFIG,
    consumer_config,
    TOPIC_RAW_LOGS,
    TOPIC_ALERTS,
    TOPIC_METRICS,
    ALERT_LEVELS,
)

CONSUMER_GROUP  = "stream-processor"
METRICS_FLUSH_S = 60

_counts_lock = threading.Lock()
_counts: dict = defaultdict(lambda: defaultdict(int))


def metrics_flusher(producer: KafkaProducer):
    while True:
        time.sleep(METRICS_FLUSH_S)
        with _counts_lock:
            if not _counts:
                continue
            snapshot = {svc: dict(levels) for svc, levels in _counts.items()}
            _counts.clear()

        metric = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "window_seconds": METRICS_FLUSH_S,
            "services": snapshot,
        }

        producer.send(
            TOPIC_METRICS,
            key="metrics",
            value=metric,
        )
        producer.flush()

        print(f"\n[processor] Metrics flushed → {snapshot}\n")

def process(log: dict, producer: KafkaProducer):
    level   = log.get("level", "INFO")
    service = log.get("service", "unknown")

    with _counts_lock:
        _counts[service][level] += 1

    if level in ALERT_LEVELS:
        producer.send(
            TOPIC_ALERTS,
            key=service,
            value=log,
        )
        print(f"[processor] Alert forwarded: [{level}] [{service}] {log.get('message')}")
    else:
        print(f"[processor] Processed: [{level}] [{service}] {log.get('message')}")


def main():
    producer = KafkaProducer(
        **PRODUCER_CONFIG,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    consumer = KafkaConsumer(
        TOPIC_RAW_LOGS,
        **consumer_config(CONSUMER_GROUP),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    flusher = threading.Thread(
        target=metrics_flusher,
        args=(producer,),
        daemon=True,
    )
    flusher.start()

    print(f"[processor] Listening on: {TOPIC_RAW_LOGS}")
    print(f"[processor] Forwarding alerts → {TOPIC_ALERTS}")
    print(f"[processor] Metrics flush every {METRICS_FLUSH_S}s → {TOPIC_METRICS}")

    try:
        for msg in consumer:
            process(msg.value, producer)

    except KeyboardInterrupt:
        print("\n[processor] Shutting down...")
    finally:
        consumer.close()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
