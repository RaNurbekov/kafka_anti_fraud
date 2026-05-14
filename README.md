markdown

# 🌊 Real-Time Fraud Detection Streaming

> **Потоковая микросервисная архитектура для обнаружения мошенничества в реальном времени**
> на базе Apache Kafka с горизонтальным масштабированием

---

## 🛠 Стек технологий

| Компонент | Технология |
|---|---|
| **Message Broker** | Apache Kafka |
| **Координация** | Zookeeper |
| **Оркестрация** | Docker Compose |
| **Data Processing** | Python, `confluent-kafka` |
| **Архитектура** | Event-Driven, Pub/Sub, Consumer Groups |

---

## ⚙️ Архитектура системы

```
💳 Транзакции (тысячи/сек)
        │
        ▼
┌─────────────────┐
│   producer.py   │  ← Имитирует поток транзакций от POS-терминалов
│  (Publisher)    │    Генерирует JSON-события в топик Kafka
└────────┬────────┘
         │  Kafka Topic: "transactions"
         ▼
┌─────────────────────────────────────┐
│         Apache Kafka Broker         │
│         (Docker Compose)            │
└──────┬──────────┬───────────┬───────┘
       │          │           │
       ▼          ▼           ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│consumer 1│ │consumer 2│ │consumer N│  ← Consumer Group
│(воркер)  │ │(воркер)  │ │(воркер)  │    Kafka авто-балансирует нагрузку
└──────────┘ └──────────┘ └──────────┘
       │
       ▼
  Rule-Based детектор
  (сумма > 4000 → 🚨 FRAUD ALERT)
  
  [Production: ML-инференс LightGBM/XGBoost]
```

---

## 🔑 Ключевые особенности

### 1. Producer — Генератор транзакций
`src/producer.py` имитирует поток транзакций от тысяч POS-терминалов. Каждое событие — JSON с данными транзакции, отправляется в топик Kafka непрерывно в реальном времени.

### 2. Consumer — Детектор мошенничества
`src/consumer.py` асинхронно читает поток из Kafka и применяет детектирующую логику. Текущая реализация — Rule-Based фильтр (аномальные суммы > 4000). В production-среде заменяется на ML-инференс (LightGBM, XGBoost).

### 3. Horizontal Scaling — Масштабирование под нагрузку
Главная фишка проекта — демонстрация **Consumer Groups**. При запуске нескольких инстансов `consumer.py` Kafka автоматически перераспределяет партиции между воркерами (Rebalance) без изменения единой строки кода. Это означает: в Чёрную Пятницу достаточно поднять 10 дополнительных контейнеров — и система справится с пиковой нагрузкой.

---

## 🚀 Быстрый старт

### 1. Поднять инфраструктуру Kafka
```bash
docker-compose up -d
```
Kafka и Zookeeper запустятся в фоне в Docker-контейнерах.

### 2. Установить зависимости
```bash
pip install confluent-kafka
```

### 3. Запустить пайплайн

**Терминал 1** — запускаем генератор транзакций:
```bash
python src/producer.py
```

**Терминал 2** — запускаем детектор мошенничества:
```bash
python src/consumer.py
```

**Терминал 3 (опционально)** — второй воркер для демонстрации масштабирования:
```bash
python src/consumer.py
```
Kafka автоматически распределит поток между двумя Consumer инстансами.

### 4. Остановить кластер
```bash
docker-compose down
```

---

## 📁 Структура проекта

```
kafka-fraud-streaming/
├── src/
│   ├── producer.py        # Генератор потока транзакций
│   └── consumer.py        # Детектор мошенничества (Consumer Group)
├── docker-compose.yml     # Kafka + Zookeeper инфраструктура
└── README.md
```

---

## 🔗 Связанные проекты

Этот проект является частью финтех ML-экосистемы:

- [**fraud-detection-api**](https://github.com/RaNurbekov/fraud-detection-api) — REST API с LightGBM + Redis Feature Store + A/B Testing
- [**credit-risk-api**](https://github.com/RaNurbekov/credit-scoring-ml-api.) — Кредитный скоринг с MLflow + SHAP + Evidently AI

> 💡 **Production-интеграция:** Consumer из этого проекта может вызывать `/predict` эндпоинт `fraud-detection-api` для ML-инференса вместо Rule-Based логики — замыкая полный real-time пайплайн.
