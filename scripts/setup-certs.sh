#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load project SASL credentials when .env exists.
if [[ -f "$ROOT_DIR/.env" ]]; then
    set -a
    source "$ROOT_DIR/.env"
    set +a
fi
KAFKA_SASL_USERNAME="${KAFKA_SASL_USERNAME:-admin}"
CERT_DIR="$ROOT_DIR/certs"
DOCKER_CERT_DIR="$CERT_DIR/docker"

CA_KEY="$CERT_DIR/ca.key"
CA_CRT="$CERT_DIR/ca.crt"

KAFKA1_KEY="$CERT_DIR/kafka-1.key"
KAFKA1_CRT="$CERT_DIR/kafka-1.crt"
KAFKA1_CSR="$CERT_DIR/kafka-1.csr"

KAFKA2_KEY="$CERT_DIR/kafka-2.key"
KAFKA2_CRT="$CERT_DIR/kafka-2.crt"
KAFKA2_CSR="$CERT_DIR/kafka-2.csr"

KAFKA1_P12="$DOCKER_CERT_DIR/kafka-1.p12"
KAFKA2_P12="$DOCKER_CERT_DIR/kafka-2.p12"
TRUSTSTORE="$DOCKER_CERT_DIR/kafka.truststore.p12"

PASSWORD="changeit"

echo "================================="
echo " Kafka TLS Certificate Setup"
echo "================================="

mkdir -p "$CERT_DIR" "$DOCKER_CERT_DIR"

# Do not overwrite an existing TLS setup.
REQUIRED_FILES=(
    "$CA_KEY"
    "$CA_CRT"
    "$KAFKA1_KEY"
    "$KAFKA1_CRT"
    "$KAFKA2_KEY"
    "$KAFKA2_CRT"
    "$KAFKA1_P12"
    "$KAFKA2_P12"
    "$TRUSTSTORE"
    "$DOCKER_CERT_DIR/kafka-1-key-creds"
    "$DOCKER_CERT_DIR/kafka-1-keystore-creds"
    "$DOCKER_CERT_DIR/kafka-2-key-creds"
    "$DOCKER_CERT_DIR/kafka-2-keystore-creds"
    "$DOCKER_CERT_DIR/kafka_server_jaas.conf"
)

ALL_EXIST=true

for file in "${REQUIRED_FILES[@]}"; do
    if [[ ! -f "$file" ]]; then
        ALL_EXIST=false
        break
    fi
done

if [[ "$ALL_EXIST" == true ]]; then
    echo
    echo "[INFO] TLS material already exists."
    echo "[INFO] Nothing was changed."
    echo
    echo "[PASS] TLS setup already available"
    exit 0
fi

# Prevent accidental partial setups.
for file in "${REQUIRED_FILES[@]}"; do
    if [[ -e "$file" ]]; then
        echo "[ERROR] Partial TLS setup detected:"
        echo "        $file"
        echo
        echo "Remove/restore the incomplete TLS material before"
        echo "running this script again."
        exit 1
    fi
done

echo
echo "[1/7] Generating CA..."

openssl genrsa -out "$CA_KEY" 4096

openssl req -x509 \
    -new \
    -nodes \
    -key "$CA_KEY" \
    -sha256 \
    -days 3650 \
    -out "$CA_CRT" \
    -subj "/CN=Kafka-DevOps-CA"

chmod 600 "$CA_KEY"
chmod 644 "$CA_CRT"

echo "[PASS] CA generated"

echo
echo "[2/7] Generating kafka-1 certificate..."

cat > "$CERT_DIR/kafka-1.cnf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = kafka-1

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = kafka-1
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF

openssl genrsa -out "$KAFKA1_KEY" 2048

openssl req \
    -new \
    -key "$KAFKA1_KEY" \
    -out "$KAFKA1_CSR" \
    -config "$CERT_DIR/kafka-1.cnf"

openssl x509 \
    -req \
    -in "$KAFKA1_CSR" \
    -CA "$CA_CRT" \
    -CAkey "$CA_KEY" \
    -CAcreateserial \
    -out "$KAFKA1_CRT" \
    -days 1095 \
    -sha256 \
    -extensions v3_req \
    -extfile "$CERT_DIR/kafka-1.cnf"

chmod 600 "$KAFKA1_KEY"
chmod 644 "$KAFKA1_CRT"

echo "[PASS] kafka-1 certificate generated"

echo
echo "[3/7] Generating kafka-2 certificate..."

cat > "$CERT_DIR/kafka-2.cnf" <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = kafka-2

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = kafka-2
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF

openssl genrsa -out "$KAFKA2_KEY" 2048

openssl req \
    -new \
    -key "$KAFKA2_KEY" \
    -out "$KAFKA2_CSR" \
    -config "$CERT_DIR/kafka-2.cnf"

openssl x509 \
    -req \
    -in "$KAFKA2_CSR" \
    -CA "$CA_CRT" \
    -CAkey "$CA_KEY" \
    -CAcreateserial \
    -out "$KAFKA2_CRT" \
    -days 1095 \
    -sha256 \
    -extensions v3_req \
    -extfile "$CERT_DIR/kafka-2.cnf"

chmod 600 "$KAFKA2_KEY"
chmod 644 "$KAFKA2_CRT"

echo "[PASS] kafka-2 certificate generated"

echo
echo "[4/7] Generating PKCS12 keystores..."

openssl pkcs12 -export \
    -out "$KAFKA1_P12" \
    -inkey "$KAFKA1_KEY" \
    -in "$KAFKA1_CRT" \
    -certfile "$CA_CRT" \
    -name kafka-1 \
    -passout "pass:$PASSWORD"

openssl pkcs12 -export \
    -out "$KAFKA2_P12" \
    -inkey "$KAFKA2_KEY" \
    -in "$KAFKA2_CRT" \
    -certfile "$CA_CRT" \
    -name kafka-2 \
    -passout "pass:$PASSWORD"

echo "[PASS] PKCS12 keystores generated"

echo
echo "[5/7] Generating truststore..."

keytool -importcert \
    -noprompt \
    -trustcacerts \
    -alias kafka-devops-ca \
    -file "$CA_CRT" \
    -keystore "$TRUSTSTORE" \
    -storetype PKCS12 \
    -storepass "$PASSWORD"

echo "[PASS] Truststore generated"

echo
echo "[6/7] Creating credential files..."

printf '%s' "$PASSWORD" > "$DOCKER_CERT_DIR/kafka-1-key-creds"
printf '%s' "$PASSWORD" > "$DOCKER_CERT_DIR/kafka-1-keystore-creds"
printf '%s' "$PASSWORD" > "$DOCKER_CERT_DIR/kafka-2-key-creds"
printf '%s' "$PASSWORD" > "$DOCKER_CERT_DIR/kafka-2-keystore-creds"

chmod 644 "$DOCKER_CERT_DIR/kafka-1-key-creds"
chmod 644 "$DOCKER_CERT_DIR/kafka-1-keystore-creds"
chmod 644 "$DOCKER_CERT_DIR/kafka-2-key-creds"
chmod 644 "$DOCKER_CERT_DIR/kafka-2-keystore-creds"

: "${KAFKA_SASL_PASSWORD:?Set KAFKA_SASL_PASSWORD in .env before creating JAAS config}"

cat > "$DOCKER_CERT_DIR/kafka_server_jaas.conf" <<EOF
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="${KAFKA_SASL_USERNAME}"
  password="${KAFKA_SASL_PASSWORD}"
  user_${KAFKA_SASL_USERNAME}="${KAFKA_SASL_PASSWORD}";
};
EOF

chmod 644 "$DOCKER_CERT_DIR/kafka_server_jaas.conf"

echo "[PASS] Credential files created"

echo
echo "[7/7] Cleaning temporary files..."

rm -f "$KAFKA1_CSR" "$KAFKA2_CSR"
rm -f "$CERT_DIR/kafka-1.cnf" "$CERT_DIR/kafka-2.cnf"
rm -f "$CERT_DIR/ca.srl"

echo "[PASS] Temporary files removed"

echo
echo "================================="
echo " RESULT: TLS SETUP COMPLETE"
echo "================================="
echo
echo "Generated:"
echo "  CA certificate"
echo "  kafka-1 certificate"
echo "  kafka-2 certificate"
echo "  kafka-1 PKCS12 keystore"
echo "  kafka-2 PKCS12 keystore"
echo "  Kafka truststore"
echo "  Kafka credential files"
echo "  Kafka JAAS configuration"
echo
echo "Private material is excluded from Git."
