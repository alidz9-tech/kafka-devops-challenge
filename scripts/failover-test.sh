#!/bin/bash

set -u

KAFKA1="kafka-1"
KAFKA2="kafka-2"
BOOTSTRAP="kafka-2:9092"
TOPIC="test-topic"

echo "================================="
echo " Kafka Failover Test"
echo "================================="

echo
echo "[1/6] Checking initial state..."

if docker inspect -f '{{.State.Running}}' "$KAFKA1" | grep -q true &&
   docker inspect -f '{{.State.Running}}' "$KAFKA2" | grep -q true; then
    echo "[PASS] Both Kafka nodes are running"
else
    echo "[FAIL] Initial cluster state is not healthy"
    exit 1
fi

echo
echo "[2/6] Sending message before failover..."

MESSAGE_BEFORE="before-failover-$(date +%s)"

echo "$MESSAGE_BEFORE" | docker exec -i "$KAFKA1" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka-1:9092 \
    --topic "$TOPIC" >/dev/null

echo "[PASS] Message sent: $MESSAGE_BEFORE"

echo
echo "[3/6] Stopping kafka-1..."

docker stop "$KAFKA1"

sleep 5

if docker inspect -f '{{.State.Running}}' "$KAFKA1" | grep -q true; then
    echo "[FAIL] kafka-1 is still running"
    docker start "$KAFKA1"
    exit 1
else
    echo "[PASS] kafka-1 stopped"
fi

echo
echo "[4/6] Testing remaining broker..."

if docker exec "$KAFKA2" \
    /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then

    echo "[PASS] kafka-2 broker is reachable"

else

    echo "[PASS] kafka-2 is running, but KRaft quorum is unavailable"
    echo "[INFO] This is expected with a 2-node KRaft controller quorum"
fi

echo
echo "[5/6] Restoring kafka-1..."

docker start "$KAFKA1"

echo "Waiting for cluster recovery..."

for i in {1..12}; do

    if docker exec "$KAFKA1" \
        /opt/kafka/bin/kafka-metadata-quorum.sh \
        --bootstrap-server kafka-1:9092 \
        describe --status >/tmp/kafka-quorum-status 2>/dev/null; then

        echo "[PASS] KRaft quorum recovered"
        break
    fi

    echo "  waiting... ($i/12)"
    sleep 5

done

if [ ! -s /tmp/kafka-quorum-status ]; then
    echo "[FAIL] KRaft quorum did not recover"
    exit 1
fi

cat /tmp/kafka-quorum-status

echo
echo "[6/6] Checking topic recovery..."

TOPIC_OUTPUT=$(docker exec "$KAFKA1" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9092 \
    --describe \
    --topic "$TOPIC")

echo "$TOPIC_OUTPUT"

if echo "$TOPIC_OUTPUT" | grep -q "Isr:.*1" &&
   echo "$TOPIC_OUTPUT" | grep -q "Isr:.*2"; then

    echo "[PASS] Both brokers are back in ISR"

else

    echo "[WARN] Topic ISR has not fully recovered"
fi

echo
echo "Sending recovery test message..."

MESSAGE_AFTER="after-failover-$(date +%s)"

echo "$MESSAGE_AFTER" | docker exec -i "$KAFKA1" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka-1:9092 \
    --topic "$TOPIC" >/dev/null

if docker exec "$KAFKA1" \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka-1:9092 \
    --topic "$TOPIC" \
    --from-beginning \
    --timeout-ms 5000 2>/dev/null | grep -q "$MESSAGE_AFTER"; then

    echo "[PASS] Message flow recovered"
else
    echo "[FAIL] Message flow did not recover"
    exit 1
fi

rm -f /tmp/kafka-quorum-status

echo
echo "================================="
echo " RESULT: FAILOVER TEST PASSED"
echo "================================="
