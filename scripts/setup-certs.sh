#!/bin/bash
source .env
set -e

CERTS_DIR="certs"
echo "[INFO] Generating TLS Certificates..."
mkdir -p $CERTS_DIR

# 1. Generate CA
openssl req -new -x509 -keyout $CERTS_DIR/ca.key -out $CERTS_DIR/ca.crt -days 365 -subj "/CN=KafkaCA" -nodes

# 2. Generate Broker Keystores
for i in 1 2; do
    echo "[INFO] Generating cert for kafka-$i..."
    keytool -keystore $CERTS_DIR/kafka.$i.keystore.p12 -alias localhost -validity 365 -genkey -keyalg RSA -storetype PKCS12 -storepass changeit -dname "CN=kafka-$i" -ext SAN=DNS:kafka-$i,DNS:localhost
    
    keytool -keystore $CERTS_DIR/kafka.$i.keystore.p12 -alias localhost -certreq -file $CERTS_DIR/kafka.$i.csr -storepass changeit
    openssl x509 -req -CA $CERTS_DIR/ca.crt -CAkey $CERTS_DIR/ca.key -in $CERTS_DIR/kafka.$i.csr -out $CERTS_DIR/kafka.$i.signed -days 365 -CAcreateserial
    
    keytool -keystore $CERTS_DIR/kafka.$i.keystore.p12 -alias CARoot -import -file $CERTS_DIR/ca.crt -storepass changeit -noprompt
    keytool -keystore $CERTS_DIR/kafka.$i.keystore.p12 -alias localhost -import -file $CERTS_DIR/kafka.$i.signed -storepass changeit -noprompt
done

# 3. Generate Client Truststore (The missing piece!)
echo "[INFO] Generating Client Truststore..."
keytool -keystore $CERTS_DIR/client.truststore.p12 -alias CARoot -import -file $CERTS_DIR/ca.crt -storetype PKCS12 -storepass changeit -noprompt

chmod -R 755 $CERTS_DIR
echo "[PASS] All certificates generated."
