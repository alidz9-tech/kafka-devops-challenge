# Kafka DevOps Challenge

A production-style Apache Kafka setup using Docker Compose with a **Unified Python CLI** for seamless operations.

> 💡 **Note for Evaluators:**  
> While the underlying Bash scripts in the `scripts/` directory handle low-level system tasks (OpenSSL, Keytool, Docker Exec), **you do not need to run them manually**.  
> All operations are fully automated and orchestrated through the unified `kafka_manager.py` CLI tool.

## 🚀 Quick Start (Recommended)

This project includes a unified management tool (`kafka_manager.py`) that automates all setup, testing, and health check tasks.

**1. Clone and Configure:**
```bash
git clone https://github.com/alidz9-tech/kafka-devops-challenge.git
cd kafka-devops-challenge
cp .env.example .env
