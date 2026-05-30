import sys
import os
import json
import random
import time
from datetime import datetime, timezone

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from kafka import KafkaProducer
from faker import Faker
from config.kafka_config import PRODUCER_CONFIG, TOPIC_RAW_LOGS, LOG_LEVELS

fake = Faker()

PAYMENT_MESSAGES = {
    "INFO": [
        "Payment of ${amount} processed for order {order}",
        "Refund of ${amount} initiated for order {order}",
        "Invoice {invoice} generated for customer {customer}",
        "Subscription renewed for customer {customer}",
    ],
    "WARN": [
        "Payment retry #{retry} for order {order}",
        "High transaction volume detected: {count} tx/min",
        "Currency conversion rate deviation for order {order}",
    ],
    "ERROR": [
        "Payment FAILED for order {order}: gateway timeout",
        "Fraud detection triggered for transaction {tx}",
        "Insufficient funds for order {order}",
        "Payment gateway unreachable: {error}",
    ],
}


def build_log(level: str) -> dict:
    template = random.choice(PAYMENT_MESSAGES[level])
    message = template.format(
        amount=round(random.uniform(5, 500), 2),
        order=fake.uuid4()[:8].upper(),
        invoice=f"INV-{random.randint(10000, 99999)}",
        customer=fake.user_name(),
        retry=random.randint(1, 3),
        count=random.randint(500, 2000),
        tx=fake.uuid4()[:8].upper(),
        error=fake.sentence(nb_words=3),
    )
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "service-b",
        "service_display": "Payment Service",
        "level": level,
        "message": message,
        "host": fake.hostname(),
        "trace_id": fake.uuid4(),
    }


def main():
    producer = KafkaProducer(
        **PRODUCER_CONFIG,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"[service-b] Starting producer → topic: {TOPIC_RAW_LOGS}")

    try:
        while True:
            level = random.choice(LOG_LEVELS)
            log = build_log(level)

            producer.send(
                TOPIC_RAW_LOGS,
                key="service-b",
                value=log,
            )

            print(f"[{log['level']}] {log['message']}")
            time.sleep(random.uniform(0.8, 2.5))

    except KeyboardInterrupt:
        print("\n[service-b] Shutting down producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
