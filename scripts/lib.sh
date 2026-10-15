#!/bin/bash
# Shared helpers for the Kafka challenge scripts.
# Source this file from another script; it is not meant to be executed directly.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR" || exit 1

BOOTSTRAP_PORT=9097
TOPIC="${TOPIC:-test-topic}"
TRUSTSTORE_PASS="changeit"   # must match PASSWORD in scripts/setup-certs.sh
FAILED=0

pass() { echo "[PASS] $*"; }
fail() { echo "[FAIL] $*"; FAILED=1; }
warn() { echo "[WARN] $*"; }

load_env() {
    if [[ ! -f "$ROOT_DIR/.env" ]]; then
        echo "[FAIL] .env file not found. Run: cp .env.example .env"
        exit 1
    fi
    set -a
    # shellcheck disable=SC1091
    source "$ROOT_DIR/.env"
    set +a
    : "${KAFKA_SASL_USERNAME:?Missing KAFKA_SASL_USERNAME in .env}"
    : "${KAFKA_SASL_PASSWORD:?Missing KAFKA_SASL_PASSWORD in .env}"
}

is_running() {
    [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" == "true" ]]
}

# write_client_config <container> <path-inside-container>
# The credentials travel over stdin, so they never appear in a process list.
write_client_config() {
    local container="$1" cfg="$2"
    docker exec -i "$container" bash -c "umask 077; cat > '$cfg'" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_SASL_USERNAME}" password="${KAFKA_SASL_PASSWORD}";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.p12
ssl.truststore.password=${TRUSTSTORE_PASS}
ssl.truststore.type=PKCS12
EOF
}

# kafka_cmd <container> <cfg> <tool.sh> [args...]
# Runs a Kafka admin tool inside <container>, talking to that same broker over SASL_SSL.
kafka_cmd() {
    local container="$1" cfg="$2" tool="$3"
    shift 3
    docker exec "$container" "/opt/kafka/bin/$tool" --bootstrap-server "$container:$BOOTSTRAP_PORT" --command-config "$cfg" "$@"
}

# send_marker <prefix> <container> <cfg>
# Produces one unique line to $TOPIC and stores it in $MARKER. Returns the producer's status.
send_marker() {
    local tool="/opt/kafka/bin/kafka-console-producer.sh"
    MARKER="$1-$(date +%s)-$$"
    printf '%s\n' "$MARKER" | timeout 40 docker exec -i "$2" "$tool" --bootstrap-server "$2:$BOOTSTRAP_PORT" --producer.config "$3" --topic "$TOPIC" >/dev/null 2>&1
}

# wait_for_consumer <marker> <seconds>
# Succeeds once the message-consumer container has printed the marker.
# (Logs are read into a variable first: "docker logs | grep -q" can fail under pipefail.)
wait_for_consumer() {
    local marker="$1" deadline=$((SECONDS + $2)) logs
    while (( SECONDS < deadline )); do
        logs="$(docker logs --since 10m message-consumer 2>&1 || true)"
        if grep -Fq -- "$marker" <<<"$logs"; then
            return 0
        fi
        sleep 2
    done
    return 1
}

# bad_isr_lines  (reads `kafka-topics.sh --describe` output on stdin)
# Prints every partition whose ISR is smaller than its replica list.
bad_isr_lines() {
    grep "Partition:" | awk '{
        replicas = ""; isr = ""
        for (i = 1; i <= NF; i++) {
            if ($i == "Replicas:") replicas = $(i + 1)
            if ($i == "Isr:")      isr      = $(i + 1)
        }
        if (isr ~ /:$/) isr = ""
        nr = split(replicas, r, ",")
        ns = split(isr, s, ",")
        if (replicas == "" || isr == "" || nr != ns) print
    }'
}
