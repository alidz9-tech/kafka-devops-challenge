# Kafka DevOps Challenge

A production-style Apache Kafka setup using Docker Compose with:

* 2 Kafka brokers
* KRaft mode
* 3 partitions
* Replication Factor = 2
* SASL/PLAIN authentication
* TLS encryption
* Python Producer and Consumer
* Failover and recovery testing
* Automated health checks

## Architecture

```text
                    +----------------------+
                    |   message-producer   |
                    |     Python Client    |
                    +----------+-----------+
                               |
                               | SASL/PLAIN
                               | TLS
                               v
                  +------------+-------------+
                  |       Kafka Cluster      |
                  |          KRaft           |
                  +-------------+-------------+
                                |
                 +--------------+--------------+
                 |                             |
                 v                             v
          +-------------+               +-------------+
          |   kafka-1   |               |   kafka-2   |
          |   Broker 1  |               |   Broker 2  |
          +------+------+               +------+------+
                 |                             |
                 +---------- Replication ------+
                                |
                                v
                    +-----------+-----------+
                    |   message-consumer    |
                    |     Python Client     |
                    +-----------------------+
```

## Requirements

* Ubuntu Server 24.04 or similar Linux system
* Docker
* Docker Compose
* Git
* OpenSSL

Check Docker:

```bash
docker --version
docker compose version
```

## Clone the Repository

```bash
git clone git@github.com:alidz9-tech/kafka-devops-challenge.git
cd kafka-devops-challenge
```

## Environment Configuration

Create the environment file:

```bash
cp .env.example .env
```

Edit it if necessary:

```bash
nano .env
```

Example:

```env
KAFKA_SASL_USERNAME=admin
KAFKA_SASL_PASSWORD=replace-with-a-strong-password
```

The Kafka brokers use the same SASL/PLAIN credentials.

## Generate TLS Certificates

Generate the required certificates before starting Kafka:

```bash
chmod +x scripts/setup-certs.sh
./scripts/setup-certs.sh
```

The script creates the required CA, broker certificates, PKCS12 keystores and credential files.

Generated certificates and credentials are intentionally excluded from Git.

## Start Kafka

Build and start all services:

```bash
docker compose up --build -d
```

Check containers:

```bash
docker compose ps
```

Expected services:

```text
kafka-1
kafka-2
message-producer
message-consumer
```

## Create the Topic

Automatic topic creation is disabled.

Create the required topic:

```bash
./scripts/create-topic.sh
```

The topic configuration is:

* Topic: `test-topic`
* Partitions: `3`
* Replication Factor: `2`

Verify:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic test-topic
```

## Health Check

Run the complete health check:

```bash
./scripts/healthcheck.sh
```

The health check verifies:

1. Kafka containers
2. Broker connectivity
3. KRaft quorum
4. Topic availability
5. Partition and replication status
6. Producer → Kafka → Consumer message flow

A successful result should end with:

```text
RESULT: HEALTHY
```

## Kafka Message Test

Run:

```bash
./scripts/test-kafka.sh
```

This verifies that a message can be produced and consumed successfully.

Expected result:

```text
RESULT: PASS
```

## Failover Test

The failover test stops one Kafka broker and verifies that the remaining broker continues operating.

Run:

```bash
./scripts/failover-test.sh
```

The test verifies:

* Broker failure
* Remaining broker availability
* KRaft quorum recovery
* Replica recovery
* ISR recovery
* Message flow recovery

Expected result:

```text
RESULT: FAILOVER TEST PASSED
```

## TLS Verification

The Kafka `SASL_SSL` listener provides TLS encryption.

Test the TLS connection:

```bash
docker exec message-producer openssl s_client \
  -connect kafka-1:9097 \
  -CAfile /etc/kafka/secrets/ca.crt \
  -brief
```

A successful connection should show:

```text
Protocol version: TLSv1.3
Verification: OK
```

## SASL/PLAIN Authentication

Kafka uses SASL/PLAIN authentication on the `SASL_SSL` listener.

The client configuration uses:

```text
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
```

The configured username is:

```text
admin
```

The password is stored in `.env` and is not committed to Git.

Authentication can be tested using the Producer client.

An incorrect password must result in an authentication error.

## View Logs

Kafka broker logs:

```bash
docker logs kafka-1
docker logs kafka-2
```

Follow logs:

```bash
docker logs -f kafka-1
```

Producer logs:

```bash
docker logs message-producer
```

Consumer logs:

```bash
docker logs message-consumer
```

## Useful Commands

Check running containers:

```bash
docker ps
```

Check Kafka service status:

```bash
docker compose ps
```

Restart the cluster:

```bash
docker compose restart
```

Stop the cluster:

```bash
docker compose down
```

Stop and remove volumes:

```bash
docker compose down -v
```

## Security

Sensitive files are excluded from Git:

* `.env`
* TLS certificates
* PKCS12 keystores
* Kafka credential files
* JAAS-generated secret material

The repository contains `.env.example` instead of the real `.env`.

## Troubleshooting

### Kafka containers stop unexpectedly

Check the broker logs:

```bash
docker logs kafka-1
docker logs kafka-2
```

Look specifically for:

```text
ERROR
FATAL
Exception
Caused by
```

### Authentication failure

Verify:

```bash
cat .env
```

The credentials must match the Kafka SASL/PLAIN configuration.

### Certificate problems

Regenerate certificates:

```bash
rm -rf certs
./scripts/setup-certs.sh
```

Then restart:

```bash
docker compose down -v
docker compose up --build -d
```

### Check Kafka cluster state

```bash
./scripts/healthcheck.sh
```

## Cleanup

Stop the services:

```bash
docker compose down
```

To remove Kafka data as well:

```bash
docker compose down -v
```

## Validation

The project has been validated with:

* Kafka broker health check
* KRaft quorum validation
* Topic validation
* Replication and ISR validation
* Producer/Consumer message flow
* TLS 1.3 verification
* SASL/PLAIN authentication
* Invalid password rejection
* Broker failover
* Cluster recovery
## 🐍 Unified Management CLI (New!)

To simplify operations, all management tasks are now consolidated into a single Python CLI tool: `kafka_manager.py`.

Instead of running multiple bash scripts, use this unified interface:

```bash
# Generate certificates
./kafka_manager.py setup-certs

# Create topic securely
./kafka_manager.py create-topic --topic test-topic

# Run comprehensive health check
./kafka_manager.py healthcheck

# Test cluster failover (High Availability)
./kafka_manager.py failover --target kafka-1

# Quick message flow test
./kafka_manager.py test
```
