# 📈 Real-Time Stock Data Pipeline

A real-time financial data platform that streams live stock prices, processes them using Kafka, stores in TimescaleDB, and visualizes via Grafana.

## 🚀 Tech Stack

- Python
- Apache Kafka
- TimescaleDB (PostgreSQL)
- Grafana
- Alpaca API

## 🧠 Features

- Real-time stock streaming via WebSocket
- Dynamic stock selection (user input)
- Historical + live data ingestion
- Kafka-based event streaming
- Interactive Grafana dashboard
- Multi-stock visualization

## 🏗 Architecture

Alpaca → Kafka → Consumer → TimescaleDB → Grafana

## ▶️ How to Run

1. Start Docker:
```bash
docker compose up -d

2. Run Producer:
python producer.py

3. Run Consumer:
python consumer.py

4. Open Grafana:
http://localhost:3000
