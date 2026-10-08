import json
import os
import socket

from confluent_kafka import Consumer, KafkaException


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-1:9097,kafka-2:9097",
)

TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")

GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "devops-challenge-consumer",
)

SASL_USERNAME = os.environ["KAFKA_SASL_USERNAME"]
SASL_PASSWORD = os.environ["KAFKA_SASL_PASSWORD"]


consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "client.id": socket.gethostname(),

        # SASL + TLS
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": SASL_USERNAME,
        "sasl.password": SASL_PASSWORD,

        # CA certificate
        "ssl.ca.location": "/etc/kafka/secrets/ca.crt",

        # Consumer settings
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
)


consumer.subscribe([TOPIC])

print(
    f"[CONSUMER] Started. "
    f"topic={TOPIC} group={GROUP_ID}",
    flush=True,
)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        raw_value = msg.value().decode("utf-8")

        try:
            event = json.loads(raw_value)

            print(
                f"[CONSUMER] "
                f"partition={msg.partition()} "
                f"offset={msg.offset()} "
                f"event={event}",
                flush=True,
            )

        except json.JSONDecodeError:
            print(
                f"[CONSUMER] "
                f"partition={msg.partition()} "
                f"offset={msg.offset()} "
                f"value={raw_value}",
                flush=True,
            )

finally:
    consumer.close()
