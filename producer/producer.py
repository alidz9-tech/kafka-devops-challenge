import json
import os
import socket
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-1:9097,kafka-2:9097",
)

TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")

SASL_USERNAME = os.environ["KAFKA_SASL_USERNAME"]
SASL_PASSWORD = os.environ["KAFKA_SASL_PASSWORD"]

producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": socket.gethostname(),

        # SASL + TLS
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": SASL_USERNAME,
        "sasl.password": SASL_PASSWORD,

        # CA certificate
        "ssl.ca.location": "/etc/kafka/secrets/ca.crt",

        # Reliability
        "acks": "all",
        "enable.idempotence": True,
    }
)


def delivery_report(err, msg):
    if err is not None:
        print(
            f"[ERROR] Message delivery failed: {err}",
            flush=True,
        )
    else:
        print(
            f"[OK] topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}",
            flush=True,
        )


counter = 0

while True:
    counter += 1

    event = {
        "event_id": counter,
        "hostname": socket.gethostname(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": f"DevOps Challenge event #{counter}",
    }

    payload = json.dumps(event)

    try:
        producer.produce(
            TOPIC,
            value=payload.encode("utf-8"),
            callback=delivery_report,
        )

        producer.poll(0)

        print(
            f"[PRODUCER] {payload}",
            flush=True,
        )

    except Exception as exc:
        print(
            f"[ERROR] {exc}",
            flush=True,
        )

    time.sleep(5)
