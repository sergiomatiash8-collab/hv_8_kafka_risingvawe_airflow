# Twitter Sentiment Analysis Pipeline

A distributed streaming system for real-time sentiment analysis of tweets using Kafka, Python, and PostgreSQL/RisingWave.

---

## Architecture

The system consists of the following components:

- **Producer Service**
  - Streams tweets from a CSV dataset
  - Sends messages to Kafka topic

- **Kafka Broker**
  - Handles message streaming between services

- **Consumer Service**
  - Reads messages from Kafka
  - Performs sentiment analysis using TextBlob
  - Stores results in PostgreSQL / RisingWave

- **Metrics Exporter**
  - Exposes Prometheus metrics
  - Tracks Kafka lag, processing time, and DB records

- **Health Check Service**
  - Verifies Kafka, PostgreSQL, and network connectivity

---

## Tech Stack

- Python 3.x
- Apache Kafka
- PostgreSQL / RisingWave
- Prometheus
- TextBlob (sentiment analysis)
- psycopg2
- kafka-python

---

## Project Structure


src/
├── domain/
├── infrastructure/
├── services/
└── main entrypoints


---

## How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
2. Start Kafka + DB (Docker recommended)

Make sure Kafka and PostgreSQL/RisingWave are running.

3. Run Consumer
python consumer_main.py
4. Run Producer
python producer_main.py
5. Optional: Run Metrics Exporter
python metrics_exporter.py
Windows Quick Start
start consumer_main.py
timeout /t 5
start producer_main.py
Features
Real-time streaming pipeline
Sentiment classification (positive / neutral / negative)
Fault-tolerant Kafka consumer
Graceful shutdown (SIGINT / SIGTERM handling)
Metrics monitoring (Prometheus-ready)
Health checks for all services
Dataset

Uses Kaggle dataset:
thoughtvector/customer-support-on-twitter

Notes
Kafka messages are processed in real-time
Consumer uses manual JSON parsing for resilience
System is designed for learning data engineering + streaming architecture patterns