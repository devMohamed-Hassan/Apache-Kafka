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

AUTH_MESSAGES = {
    "INFO": [
        "User {user} logged in successfully",
        "Token refreshed for user {user}",
        "Password changed for user {user}",
        "Session started for user {user}",
    ],
    "WARN": [
        "Failed login attempt for user {user} from IP {ip}",
        "Token expiring soon for user {user}",
        "Multiple login attempts detected for user {user}",
    ],
    "ERROR": [
        "Authentication failed: invalid credentials for user {user}",
        "Token validation error for user {user}: {error}",
        "Database connection timeout during auth for user {user}",
    ],
}


def build_log(level: str) -> dict:
    template = random.choice(AUTH_MESSAGES[level])
    message = template.format(
        user=fake.user_name(),
        ip=fake.ipv4(),
        error=fake.sentence(nb_words=4),
    )
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "service-a",
        "service_display": "Auth Service",
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

    print(f"[service-a] Starting producer → topic: {TOPIC_RAW_LOGS}")

    try:
        while True:
            level = random.choice(LOG_LEVELS)
            log = build_log(level)

            producer.send(
                TOPIC_RAW_LOGS,
                key="service-a",
                value=log,
            )

            print(f"[{log['level']}] {log['message']}")
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\n[service-a] Shutting down producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()