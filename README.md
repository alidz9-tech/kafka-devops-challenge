# Kafka DevOps Challenge 🚀

A robust, secure, and automated Kafka cluster setup using Docker Compose with SASL_SSL authentication.

## ✨ Features
- **Secure Communication:** End-to-end encryption using TLS/SSL.
- **Authentication:** SASL/PLAIN mechanism.
- **High Availability:** Multi-broker cluster with failover support.
- **Automated Tooling:** Custom CLI tool (`kafka_manager.py`).

## 🚀 Quick Start

### 1. Setup Certificates
```bash
./kafka_manager.py setup-certs
```

### 2. Start the Cluster
```bash
docker compose up --build -d
```

### 3. Create a Topic
```bash
./kafka_manager.py create-topic --topic my-topic
```

## 📋 Available Commands

| Command | Description |
| --- | --- |
| `setup-certs` | Generates CA, broker certs, and truststores. |
| `create-topic` | Creates a new Kafka topic. |
| `healthcheck` | Checks status of all containers. |
| `test` | Runs end-to-end test. |
| `failover` | Simulates broker failure. |

## 🔐 Security Notes
- Credentials managed via `.env`.
- Private keys excluded from Git.

## 🤝 Contributing
Feel free to open issues or submit pull requests!
