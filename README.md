# 🌊 Real-Time Fraud Detection Streaming

> **Event-Driven fraud detection pipeline on Apache Kafka**
> Producer → Kafka Broker → Consumer Group → LightGBM API → Real-time Decision

---

## 📊 Architecture

```
💳 Transactions (thousands/sec)
        │
        ▼
┌─────────────────────────────────┐
│         producer.py             │
│  Simulates POS terminal stream  │
│  Generates JSON events to Kafka │
│  KZ merchants: Glovo, Kaspi,    │
│  inDrive, Air Astana            │
└──────────────┬──────────────────┘
               │  Kafka Topic: "transactions"
               ▼
┌─────────────────────────────────┐
│      Apache Kafka Broker        │
│      (Docker Compose 7.4.0)     │
└──────┬──────────┬───────────────┘
       │          │
       ▼          ▼
┌──────────┐ ┌──────────┐  ← Consumer Group
│consumer 1│ │consumer N│    Kafka auto-balances load
└────┬─────┘ └────┬─────┘
     │             │
     ▼             ▼
┌─────────────────────────────────┐
│   fraud-detection-api (Render)  │
│   LightGBM + Redis Feature Store│
│   A/B Testing: 80% Champion /   │
│   20% Challenger                │
└──────────────┬──────────────────┘
               │
               ▼
        APPROVE ✅ / BLOCK 🚨
```

---

## 🛠 Tech Stack

| Component | Technology |
|---|---|
| **Message Broker** | Apache Kafka 7.4.0 |
| **Coordination** | Zookeeper 7.4.0 |
| **Orchestration** | Docker Compose |
| **Data Processing** | Python, `confluent-kafka`, `requests` |
| **ML Inference** | LightGBM via fraud-detection-api (Render) |
| **Architecture** | Event-Driven, Pub/Sub, Consumer Groups |

---

## 🔑 Key Features

### 1. Producer — Transaction Stream Generator
`src/producer.py` simulates a stream of transactions from thousands of POS terminals. Each event is a JSON message sent continuously to a Kafka topic in real-time. Uses realistic KZ card IDs and merchant names.

### 2. Consumer — ML Fraud Detector
`src/consumer.py` asynchronously reads the stream from Kafka and calls the **real LightGBM ML API** for fraud scoring:

```python
response = requests.post(
    "https://fraud-detection-api-1-hf78.onrender.com/scan",
    json={"card_id": card_id, "amount": amount}
)
decision = response.json()["decision"]  # APPROVE or BLOCK
```

Falls back to rule-based detection if API is unavailable.

### 3. Horizontal Scaling — Consumer Groups
The main feature: **Consumer Groups**. Launch multiple `consumer.py` instances — Kafka automatically redistributes partitions between workers (Rebalance) without changing a single line of code.

```bash
# Terminal 2
python src/consumer.py  # Worker 1 — handles partitions 0-1

# Terminal 3
python src/consumer.py  # Worker 2 — handles partitions 2-3
# Kafka rebalances automatically!
```

On Black Friday — just spin up 10 more containers. Zero code changes.

### 4. Real-time Stats Counter
Consumer tracks live statistics every 10 transactions:

```
📊 Stats | Total: 50 | Fraud: 5 (10.0%) | Approved: 45 | API Errors: 0
```

### 5. Graceful Fallback
If the ML API is sleeping (Render free tier) — rule-based detector kicks in automatically. No downtime.

---

## 🚀 Quick Start

### 1. Start Kafka infrastructure
```bash
docker-compose up -d
```
Wait 15 seconds for Kafka to fully initialize.

### 2. Install dependencies
```bash
pip install confluent-kafka requests
```

### 3. Run the pipeline (3 terminals)

**Terminal 1** — Kafka is already running via Docker

**Terminal 2** — Start transaction generator:
```bash
python src/producer.py
```

**Terminal 3** — Start ML fraud detector:
```bash
python src/consumer.py
```

### 4. Expected output

**Producer:**
```
📤 Sent: card_0042 | ₸1,250
📤 Sent: fraud_card_001 | ₸35,000
📤 Sent: card_0017 | ₸3,100
```

**Consumer:**
```
✅ APPROVED | User: card_0042 | Amount: 1,250 | Risk: 8.2% | Model: Model A (Champion)
🚨 FRAUD BLOCKED | User: fraud_card_001 | Amount: 35,000 | Risk: 89.3% | Level: HIGH | Model: Model A (Champion)

📊 Stats | Total: 10 | Fraud: 1 (10.0%) | Approved: 9 | API Errors: 0
```

### 5. Stop the cluster
```bash
docker-compose down
```

---

## 📁 Project Structure

```
kafka-fraud-streaming/
├── src/
│   ├── producer.py        # Transaction stream generator
│   └── consumer.py        # ML fraud detector (Consumer Group)
├── docker-compose.yml     # Kafka 7.4.0 + Zookeeper infrastructure
├── requirements.txt
└── README.md
```

---

## 🔗 Related Projects

This project is part of a complete Fintech ML pipeline:

- [**fraud-detection-api**](https://github.com/RaNurbekov/fraud-detection-api) — LightGBM + Redis Feature Store + A/B Testing ← **called by this Consumer**
- [**fraud-gnn**](https://github.com/RaNurbekov/fraud-gnn) — Graph Neural Networks for collaborative fraud detection
- [**credit-risk-api**](https://github.com/RaNurbekov/credit-scoring-ml-api.) — Credit scoring with MLflow + SHAP + Evidently AI

> 💡 **Complete production pipeline:**
> ```
> Kafka (this project) → fraud-detection-api → LightGBM decision
> ```
> Transactions stream through Kafka → Consumer calls ML API → real-time APPROVE/BLOCK.
> This is exactly how anti-fraud systems work at Kaspi and Halyk Bank.

---

## 📫 Author

**Rashid Nurbekov** — ML Engineer | Fintech & Generative AI | Almaty, Kazakhstan 🇰🇿

[![Telegram](https://img.shields.io/badge/Telegram-@RaNurbek-2CA5E0?style=flat&logo=telegram&logoColor=white)](https://t.me/RaNurbek)
[![Email](https://img.shields.io/badge/Email-nurbekovrashidjob@gmail.com-D14836?style=flat&logo=gmail&logoColor=white)](mailto:nurbekovrashidjob@gmail.com)
[![GitHub](https://img.shields.io/badge/GitHub-RaNurbekov-181717?style=flat&logo=github&logoColor=white)](https://github.com/RaNurbekov)
> 💡 **Production-интеграция:** Consumer из этого проекта может вызывать `/predict` эндпоинт `fraud-detection-api` для ML-инференса вместо Rule-Based логики — замыкая полный real-time пайплайн.
=======
> 💡 **Production-интеграция:** Consumer из этого проекта может вызывать `/predict` эндпоинт `fraud-detection-api` для ML-инференса вместо Rule-Based логики — замыкая полный real-time пайплайн.
>>>>>>> dc0b3e3445f83555889f11721527a40fa2ece94b
