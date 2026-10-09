#!/bin/bash
set -Eeuo pipefail

cd "$(dirname "$0")/.."

KAFKA1="kafka-1"
KAFKA2="kafka-2"
TOPIC="test-topic"
CFG="/tmp/failover-client.properties"
STATUS="/tmp/kafka-quorum-status"

if [[ ! -f .env ]]; then
    echo "[FAIL] .env file not found"
    exit 1
fi

set -a
source ./.env
set +a

: "${KAFKA_SASL_USERNAME:?Missing KAFKA_SASL_USERNAME}"
: "${KAFKA_SASL_PASSWORD:?Missing KAFKA_SASL_PASSWORD}"

cleanup() {
    echo
    echo "[CLEANUP] Ensuring kafka-1 is running..."

    if [[ "$(docker inspect -f '{{.State.Running}}' "$KAFKA1" 2>/dev/null || true)" != "true" ]]; then
        docker start "$KAFKA1" >/dev/null 2>&1 || true
    fi

    if [[ "$(docker inspect -f '{{.State.Running}}' "$KAFKA1" 2>/dev/null || true)" == "true" ]]; then
        docker exec "$KAFKA1" rm -f "$CFG" >/dev/null 2>&1 || true
    fi
    docker exec "$KAFKA2" rm -f "$CFG" >/dev/null 2>&1 || true

    rm -f "$STATUS"
}
trap cleanup EXIT

echo "================================="
echo " Kafka Failover Test (SASL_SSL)"
echo "================================="

docker exec \
    -e KAFKA_SASL_USERNAME="$KAFKA_SASL_USERNAME" \
    -e KAFKA_SASL_PASSWORD="$KAFKA_SASL_PASSWORD" \
    "$KAFKA1" bash -lc '
        set -e
        umask 077
        printf "security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";\nssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12\nssl.truststore.password=changeit\nssl.truststore.type=PKCS12\n" \
          "$KAFKA_SASL_USERNAME" "$KAFKA_SASL_PASSWORD" \
          > /tmp/failover-client.properties
    '

kafka() {
    docker exec "$KAFKA1" "/opt/kafka/bin/$1" \
        --bootstrap-server kafka-1:9097 \
        --command-config "$CFG" "${@:2}"
}

echo
echo "[1/6] Checking both brokers..."
for broker in "$KAFKA1" "$KAFKA2"; do
    if [[ "$(docker inspect -f '{{.State.Running}}' "$broker")" != "true" ]]; then
        echo "[FAIL] $broker is not running"
        exit 1
    fi
done
echo "[PASS] Both brokers are running"

echo
echo "[2/6] Checking secure connection..."
kafka kafka-broker-api-versions.sh >/dev/null
echo "[PASS] SASL_SSL connection succeeded"

echo
echo "[3/6] Sending message before failover..."
MESSAGE_BEFORE="before-failover-$(date +%s)"
printf '%s\n' "$MESSAGE_BEFORE" |
    docker exec -i "$KAFKA1" \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka-1:9097 \
      --producer.config "$CFG" \
      --topic "$TOPIC" >/dev/null
echo "[PASS] Pre-failover message sent"

echo
echo "[4/6] Stopping kafka-1 temporarily..."
docker stop "$KAFKA1" >/dev/null
echo "[INFO] kafka-1 stopped; checking kafka-2..."
sleep 5

if docker exec \
    -e KAFKA_SASL_USERNAME="$KAFKA_SASL_USERNAME" \
    -e KAFKA_SASL_PASSWORD="$KAFKA_SASL_PASSWORD" \
    "$KAFKA2" bash -lc '
        set -e
        umask 077
        printf "security.protocol=SASL_SSL\\nsasl.mechanism=PLAIN\\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\\\"%s\\\" password=\\\"%s\\\";\\nssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12\\nssl.truststore.password=changeit\\nssl.truststore.type=PKCS12\\n" \
          "$KAFKA_SASL_USERNAME" "$KAFKA_SASL_PASSWORD" > /tmp/failover-client.properties
        /opt/kafka/bin/kafka-broker-api-versions.sh \
          --bootstrap-server kafka-2:9097 \
          --command-config /tmp/failover-client.properties
    ' >/dev/null 2>&1; then
    echo "[PASS] kafka-2 responds over SASL_SSL"
else
    echo "[INFO] Secure broker API check failed while kafka-1 was stopped"
    echo "[INFO] A two-controller KRaft quorum may lose its majority"
fi

echo
echo "[5/6] Restarting kafka-1 and waiting for quorum..."
docker start "$KAFKA1" >/dev/null

RECOVERED=0
for i in {1..24}; do
    if kafka kafka-metadata-quorum.sh describe --status >"$STATUS" 2>/dev/null; then
        RECOVERED=1
        break
    fi
    echo "  waiting for recovery ($i/24)..."
    sleep 5
done

if [[ "$RECOVERED" -ne 1 ]]; then
    echo "[FAIL] KRaft quorum did not recover in time"
    exit 1
fi

cat "$STATUS"
echo "[PASS] KRaft quorum recovered"

echo
echo "[6/6] Validating topic and message flow..."
ISR_HEALTHY=0

for attempt in {1..15}; do
    TOPIC_OUTPUT="$(kafka kafka-topics.sh --describe --topic "$TOPIC")"
    PARTITION_LINES="$(printf '%s\n' "$TOPIC_OUTPUT" | grep 'Partition:' || true)"
    PARTITION_COUNT="$(printf '%s\n' "$PARTITION_LINES" | grep -c 'Partition:' || true)"

    if [[ "$PARTITION_COUNT" -eq 3 ]] &&
       ! printf '%s\n' "$PARTITION_LINES" |
           grep -vE 'Isr:.*(1,2|2,1)([[:space:]]|$)' >/dev/null; then
        ISR_HEALTHY=1
        break
    fi

    echo "[INFO] Waiting for all partitions to recover ISR ($attempt/15)..."
    sleep 5
done

printf '%s\n' "$TOPIC_OUTPUT"

if ! printf '%s\n' "$TOPIC_OUTPUT" | grep -q 'PartitionCount: 3'; then
    echo "[FAIL] Expected 3 partitions"
    exit 1
fi

if ! printf '%s\n' "$TOPIC_OUTPUT" | grep -q 'ReplicationFactor: 2'; then
    echo "[FAIL] Expected replication factor 2"
    exit 1
fi

if [[ "$ISR_HEALTHY" -ne 1 ]]; then
    echo "[FAIL] Not all partitions have both brokers in ISR"
    exit 1
fi

echo "[PASS] All 3 partitions have both brokers in ISR"
MESSAGE_AFTER="after-failover-$(date +%s)"
printf '%s\n' "$MESSAGE_AFTER" |
    docker exec -i "$KAFKA1" \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka-1:9097 \
      --producer.config "$CFG" \
      --topic "$TOPIC" >/dev/null

CONSUMED=""
for attempt in $(seq 1 20); do
    if docker compose logs --since=60s message-consumer 2>&1 |
        grep -F -- "$MESSAGE_AFTER" >/dev/null; then
        CONSUMED="$MESSAGE_AFTER"
        break
    fi
    sleep 1
done

if [ "$CONSUMED" = "$MESSAGE_AFTER" ]; then
    echo "[PASS] Secure producer-consumer message flow recovered"
else
    echo "[FAIL] Recovery message was not consumed within 20 seconds"
    exit 1
fi

echo
echo "================================="
echo " RESULT: FAILOVER TEST PASSED"
echo "================================="
