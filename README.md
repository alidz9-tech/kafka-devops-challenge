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
