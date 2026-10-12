# Kafka DevOps Challenge

A production-style Apache Kafka setup using Docker Compose with a **Unified Python CLI** for seamless operations.

> 💡 **Note for Evaluators:**  
> While the underlying Bash scripts in the `scripts/` directory handle low-level system tasks, **you do not need to run them manually**.  
> All operations are fully automated through `kafka_manager.py`.

## 🚀 Quick Start (Recommended)

**1. Clone and Configure:**
```bash
git clone https://github.com/alidz9-tech/kafka-devops-challenge.git
cd kafka-devops-challenge
cp .env.example .env
```

**2. Setup and Launch:**
```bash
./kafka_manager.py setup-certs
docker compose up --build -d
./kafka_manager.py create-topic --topic test-topic
./kafka_manager.py healthcheck
```

**3. Test High Availability:**
```bash
./kafka_manager.py failover --target kafka-1
```

---

## Architecture

```text
Producer --> Kafka Cluster (KRaft + SASL_SSL) --> Consumer
           [kafka-1] <--> [kafka-2]
```

## Unified Management CLI Reference

| Command | Description |
| :--- | :--- |
| `setup-certs` | Generates SSL/TLS certificates and JAAS config. |
| `create-topic` | Creates Kafka topic securely via SASL_SSL. |
| `healthcheck` | Runs comprehensive 6-step system validation. |
| `failover` | Simulates broker failure and verifies recovery. |
| `test` | Quick end-to-end message flow test. |

## Security & Validation

* **TLS 1.3** encryption on port 9097.
* **SASL/PLAIN** authentication via `.env`.
* **KRaft mode** (no Zookeeper), 2-node quorum.
* **Replication Factor = 2** for high availability.

## Troubleshooting

Reset cluster completely:
```bash
docker compose down -v
./kafka_manager.py setup-certs
docker compose up --build -d
```
