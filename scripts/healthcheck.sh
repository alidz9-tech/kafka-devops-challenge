#!/bin/bash
# Kafka health check. Exits 0 only when every check passes.
set -uo pipefail

# shellcheck source=scripts/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env

CFG="/tmp/healthcheck-client.properties"
CLIENT=""

cleanup() {
    [[ -n "$CLIENT" ]] && docker exec "$CLIENT" rm -f "$CFG" >/dev/null 2>&1 || true
}
trap cleanup EXIT

finish() {
    echo
    echo "================================="
    if (( FAILED == 0 )); then
        echo " RESULT: HEALTHY"
        echo "================================="
        exit 0
    fi
    echo " RESULT: UNHEALTHY"
    echo "================================="
    exit 1
}

echo "================================="
echo " Kafka Health Check (SASL_SSL)"
echo "================================="

echo
echo "[1/6] Checking containers..."
for c in kafka-1 kafka-2 message-producer message-consumer; do
    if is_running "$c"; then
        pass "$c is running"
    else
        fail "$c is not running"
    fi
done

if is_running kafka-1; then
    CLIENT="kafka-1"
elif is_running kafka-2; then
    CLIENT="kafka-2"
else
    fail "No Kafka broker is running, cannot continue"
    finish
fi

echo
echo "[2/6] Checking SASL_SSL connection (via $CLIENT)..."
if ! write_client_config "$CLIENT" "$CFG"; then
    fail "Could not write client configuration into $CLIENT"
    finish
fi
if kafka_cmd "$CLIENT" "$CFG" kafka-broker-api-versions.sh >/dev/null 2>&1; then
    pass "SASL_SSL broker connection"
else
    fail "SASL_SSL broker connection (check .env credentials and certificates)"
    finish
fi

echo
echo "[3/6] Checking KRaft quorum..."
if QUORUM="$(kafka_cmd "$CLIENT" "$CFG" kafka-metadata-quorum.sh describe --status 2>&1)"; then
    echo "$QUORUM"
    if grep -q "CurrentVoters:" <<<"$QUORUM" && grep -Eq 'LeaderId:[[:space:]]+[0-9]+' <<<"$QUORUM"; then
        pass "KRaft quorum has a leader"
    else
        fail "KRaft quorum has no leader or no voters"
    fi
else
    echo "$QUORUM"
    fail "KRaft quorum status unavailable"
fi

echo
echo "[4/6] Checking topic..."
if kafka_cmd "$CLIENT" "$CFG" kafka-topics.sh --list 2>/dev/null | grep -qx "$TOPIC"; then
    pass "Topic $TOPIC exists"
else
    fail "Topic $TOPIC does not exist (run: ./kafka_manager.py create-topic)"
    finish
fi

echo
echo "[5/6] Checking partitions, replication and ISR..."
if TOPIC_OUTPUT="$(kafka_cmd "$CLIENT" "$CFG" kafka-topics.sh --describe --topic "$TOPIC" 2>&1)"; then
    echo "$TOPIC_OUTPUT"
    PARTITIONS="$(grep -c "Partition:" <<<"$TOPIC_OUTPUT" || true)"
    if (( PARTITIONS == 3 )); then
        pass "Partition count: 3"
    else
        fail "Expected 3 partitions, found $PARTITIONS"
    fi
    if grep -q "ReplicationFactor: 2" <<<"$TOPIC_OUTPUT"; then
        pass "Replication factor: 2"
    else
        fail "Expected replication factor 2"
    fi
    BAD="$(bad_isr_lines <<<"$TOPIC_OUTPUT")"
    if [[ -z "$BAD" ]]; then
        pass "All replicas are in ISR"
    else
        fail "Replica/ISR mismatch:"
        echo "$BAD"
    fi
else
    echo "$TOPIC_OUTPUT"
    fail "Could not describe topic $TOPIC"
fi

echo
echo "[6/6] Checking message flow (producer -> Kafka -> message-consumer)..."
if send_marker "healthcheck" "$CLIENT" "$CFG"; then
    pass "Message accepted by Kafka"
    if wait_for_consumer "$MARKER" 30; then
        pass "message-consumer received: $MARKER"
    else
        fail "message-consumer did not receive the test message within 30s"
    fi
else
    fail "Could not produce a test message"
fi

finish
