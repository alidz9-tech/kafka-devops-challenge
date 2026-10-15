#!/bin/bash
# Failover test: stop one broker, check the other, restart, and verify the cluster recovers.
# Usage: scripts/failover-test.sh [kafka-1|kafka-2]
#
# Note: both nodes are KRaft controllers, so the metadata quorum has 2 voters and needs
# both of them for a majority. While one node is down the cluster cannot elect new
# leaders, so producing during the outage is only reported (WARN), not required.
# The hard requirement is that the cluster fully recovers after the node returns.
set -uo pipefail

# shellcheck source=scripts/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env

TARGET="${1:-kafka-1}"
case "$TARGET" in
    kafka-1) OTHER="kafka-2" ;;
    kafka-2) OTHER="kafka-1" ;;
    *) echo "Usage: $0 [kafka-1|kafka-2]"; exit 2 ;;
esac

CFG="/tmp/failover-client.properties"

# Always bring the stopped broker back, even if the test aborts half-way.
restore() {
    if ! is_running "$TARGET"; then
        echo
        echo "[CLEANUP] Starting $TARGET again..."
        docker start "$TARGET" >/dev/null 2>&1 || true
    fi
    docker exec "$OTHER" rm -f "$CFG" >/dev/null 2>&1 || true
    docker exec "$TARGET" rm -f "$CFG" >/dev/null 2>&1 || true
}
trap restore EXIT

# True once the restarted broker answers and every replica is back in ISR.
recovery_ok() {
    write_client_config "$TARGET" "$CFG" 2>/dev/null || return 1
    DESC="$(kafka_cmd "$TARGET" "$CFG" kafka-topics.sh --describe --topic "$TOPIC" 2>/dev/null)" || return 1
    [[ -n "$DESC" ]] || return 1
    [[ -z "$(bad_isr_lines <<<"$DESC")" ]]
}

finish() {
    echo
    echo "================================="
    if (( FAILED == 0 )); then
        echo " RESULT: FAILOVER TEST PASSED"
        echo "================================="
        exit 0
    fi
    echo " RESULT: FAILOVER TEST FAILED"
    echo "================================="
    exit 1
}

echo "================================="
echo " Kafka Failover Test (target: $TARGET)"
echo "================================="

echo
echo "[1/6] Checking the cluster before the test..."
for c in kafka-1 kafka-2 message-producer message-consumer; do
    if is_running "$c"; then
        pass "$c is running"
    else
        fail "$c is not running"
    fi
done
(( FAILED == 0 )) || finish

write_client_config "$OTHER" "$CFG" || { fail "Could not write client config into $OTHER"; finish; }
if kafka_cmd "$OTHER" "$CFG" kafka-broker-api-versions.sh >/dev/null 2>&1; then
    pass "SASL_SSL connection works"
else
    fail "SASL_SSL connection failed (check .env credentials and certificates)"
    finish
fi

echo
echo "[2/6] Message flow before failover..."
if send_marker "before-failover" "$OTHER" "$CFG" && wait_for_consumer "$MARKER" 30; then
    pass "message-consumer received: $MARKER"
else
    fail "Message flow is not working before the test starts"
    finish
fi

echo
echo "[3/6] Stopping $TARGET..."
docker stop "$TARGET" >/dev/null
if is_running "$TARGET"; then
    fail "$TARGET is still running"
    finish
fi
pass "$TARGET stopped"
echo "Waiting 15s for the cluster to notice..."
sleep 15

echo
echo "[4/6] Checking $OTHER during the outage..."
if is_running "$OTHER"; then
    pass "$OTHER is still running"
else
    fail "$OTHER stopped as well"
    finish
fi
if send_marker "during-outage" "$OTHER" "$CFG"; then
    pass "Producing during the outage worked"
else
    warn "Producing during the outage failed (expected with a 2-voter KRaft quorum)"
fi

echo
echo "[5/6] Restarting $TARGET and waiting for recovery (up to 120s)..."
docker start "$TARGET" >/dev/null
deadline=$((SECONDS + 120))
recovered=0
while (( SECONDS < deadline )); do
    if recovery_ok; then
        recovered=1
        break
    fi
    sleep 3
done
if (( recovered == 1 )); then
    pass "$TARGET is back and every replica is in ISR"
else
    fail "Cluster did not recover within 120s"
    finish
fi

echo
echo "[6/6] Message flow after recovery..."
if send_marker "after-failover" "$TARGET" "$CFG"; then
    if wait_for_consumer "$MARKER" 60; then
        pass "message-consumer received: $MARKER"
    else
        fail "message-consumer did not receive the message within 60s"
    fi
else
    fail "Could not produce a message after recovery"
fi

finish
