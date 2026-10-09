#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
    echo "[FAIL] .env file not found"
    exit 1
fi

set -a
source ./.env
set +a

: "${KAFKA_SASL_USERNAME:?Missing KAFKA_SASL_USERNAME}"
: "${KAFKA_SASL_PASSWORD:?Missing KAFKA_SASL_PASSWORD}"

docker exec \
    -e KAFKA_SASL_USERNAME="$KAFKA_SASL_USERNAME" \
    -e KAFKA_SASL_PASSWORD="$KAFKA_SASL_PASSWORD" \
    kafka-1 bash -lc '
set -euo pipefail
umask 077

BOOTSTRAP="kafka-1:9097"
TOPIC="test-topic"
CFG=$(mktemp)
trap '\''rm -f "$CFG"'\'' EXIT

cat > "$CFG" <<EOF_CFG
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_SASL_USERNAME}" password="${KAFKA_SASL_PASSWORD}";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12
ssl.truststore.password=changeit
ssl.truststore.type=PKCS12
EOF_CFG

kafka() {
    "/opt/kafka/bin/$1" \
        --bootstrap-server "$BOOTSTRAP" \
        --command-config "$CFG" "${@:2}"
}

echo "================================="
echo " Kafka DevOps Challenge Test"
echo " Secure connection: SASL_SSL"
echo "================================="

echo
echo "[1/5] Checking secure Kafka connection..."
kafka kafka-broker-api-versions.sh >/dev/null
echo "[PASS] SASL_SSL connection succeeded"

echo
echo "[2/5] Checking topic..."
if kafka kafka-topics.sh --list | grep -qx "$TOPIC"; then
    echo "[PASS] Topic $TOPIC exists"
else
    kafka kafka-topics.sh \
        --create \
        --topic "$TOPIC" \
        --partitions 3 \
        --replication-factor 2
    echo "[PASS] Topic $TOPIC created"
fi

echo
echo "[3/5] Topic configuration..."
kafka kafka-topics.sh --describe --topic "$TOPIC"

echo
echo "[4/5] Preparing temporary consumer..."
MESSAGE="kafka-devops-test-$(date +%s)-$$"
GROUP="test-script-$(date +%s)-$$"
OUT=$(mktemp)
CONSUMER_PID=""

cleanup_consumer() {
    if [[ -n "$CONSUMER_PID" ]]; then
        kill "$CONSUMER_PID" 2>/dev/null || true
        wait "$CONSUMER_PID" 2>/dev/null || true
    fi
    rm -f "$OUT"
}
trap "cleanup_consumer; rm -f \"$CFG\"" EXIT

/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --consumer.config "$CFG" \
    --topic "$TOPIC" \
    --group "$GROUP" \
    --consumer-property auto.offset.reset=latest \
    --timeout-ms 20000 >"$OUT" 2>&1 &
CONSUMER_PID=$!

sleep 3

echo
echo "[5/5] Sending and verifying test message..."
printf "%s\n" "$MESSAGE" | \
    /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server "$BOOTSTRAP" \
        --producer.config "$CFG" \
        --topic "$TOPIC"

for i in $(seq 1 15); do
    if grep -Fq "$MESSAGE" "$OUT"; then
        break
    fi
    sleep 1
done

if grep -Fq "$MESSAGE" "$OUT"; then
    echo "[PASS] Message sent and received: $MESSAGE"
else
    echo "[FAIL] Message was not received"
    echo "--- Consumer output ---"
    cat "$OUT"
    exit 1
fi

echo
echo "================================="
echo " RESULT: PASS"
echo "================================="
'
