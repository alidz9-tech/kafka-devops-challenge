import json
import os
import socket
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-1:9092,kafka-2:9092",
)

TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Message delivery failed: {err}", flush=True)
    else:
        print(
            f"[OK] topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}",
            flush=True,
        )


producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": socket.gethostname(),
        "acks": "all",
        "enable.idempotence": True,
    }
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

        print(f"[PRODUCER] {payload}", flush=True)

    except Exception as exc:
        print(f"[ERROR] {exc}", flush=True)

    time.sleep(5)
