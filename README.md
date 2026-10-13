# Kafka DevOps Challenge 🚀

A robust, secure, and automated Kafka cluster setup using Docker Compose with SASL_SSL authentication. This project includes a custom Python CLI tool (`kafka_manager.py`) for seamless management of Kafka operations.

## ✨ Features
- **Secure Communication:** End-to-end encryption using TLS/SSL (PKCS12 keystores).
- **Authentication:** SASL/PLAIN mechanism for secure client-broker communication.
- **High Availability:** Multi-broker cluster (kafka-1, kafka-2) with automatic failover support.
- **Automated Tooling:** A unified CLI tool for certificate generation, topic management, and health checks.
- **Dockerized:** Easy deployment using Docker Compose.

## 🛠️ Prerequisites
- Docker & Docker Compose
- Python 3.x
- OpenSSL & Java Keytool (for certificate generation)

## 🚀 Quick Start

### 1. Setup Certificates
Generate the necessary TLS certificates and keystores:
```bash
./kafka_manager.py setup-certs
```

### 2. Start the Cluster
Build and start all services (Kafka brokers, producer, and consumer):
```bash
docker compose up --build -d
```

### 3. Create a Topic
Create a new topic with default partitions and replication factor:
```bash
./kafka_manager.py create-topic --topic my-topic
```

## 📋 Available Commands

| Command | Description |
| :--- | :--- |
| `setup-certs` | Generates CA, broker certs, and truststores. |
| `create-topic` | Creates a new Kafka topic. |
| `healthcheck` | Checks the status of all containers and cluster quorum. |
| `test` | Runs a quick end-to-end test (produce & consume). |
| `failover` | Simulates a broker failure to test cluster resilience. |

## 🔐 Security Notes
- Credentials are managed via `.env` file.
- Private keys (`.key`) and keystores (`.p12`) are excluded from Git via `.gitignore`.
- Default SASL user: `admin`

## 🤝 Contributing
Feel free to open issues or submit pull requests to enhance the `kafka_manager.py` tool!
