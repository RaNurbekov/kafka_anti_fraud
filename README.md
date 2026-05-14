# 🌊 Real-Time Fraud Detection Streaming (Apache Kafka)

Потоковая (Streaming) микросервисная архитектура для обработки банковских транзакций в реальном времени с использованием брокера сообщений Apache Kafka.

## 🛠 Стек технологий
* **Message Broker:** Apache Kafka, Zookeeper (Docker Compose)
* **Data Processing:** Python, `confluent-kafka`
* **Architecture:** Event-Driven Architecture, Pub/Sub, Consumer Groups

## ⚙️ Особенности проекта
1. **Producer (Генератор):** Скрипт `producer.py` имитирует миллионы кассовых аппаратов, непрерывно отправляя JSON-транзакции в топик Kafka.
2. **Consumer (Аналитик):** Скрипт `consumer.py` асинхронно вычитывает поток данных и применяет Rule-Based логику (фильтрация аномальных сумм > 4000). В Production заменяется на ML-инференс (LightGBM/XGBoost).
3. **Horizontal Scaling (Highload):** Проект демонстрирует мощь `Consumer Groups`. При запуске нескольких инстансов `consumer.py` Kafka автоматически балансирует нагрузку (Rebalance), разделяя поток транзакций между воркерами без изменения кода.

## 🚀 Как запустить

### 1. Подъем Инфраструктуры
Запустите Kafka кластер через Docker:
```bash
docker-compose up -d


ЭТО ИСТИННОЕ ИНЖЕНЕРНОЕ ЧУДО! 🌌🌊
Ты только что своими глазами увидел, почему микросервисы и потоковая обработка данных (Event-Driven Architecture) победили старые монолитные системы.
Если бы в Черную Пятницу твои серверы начали "задыхаться", тебе (как DevOps/MLOps инженеру) не пришлось бы переписывать ни единой строчки кода. Ты бы просто нажал кнопку "Поднять еще 10 контейнеров с Consumer", и Kafka мгновенно бы распределила нагрузку между ними. Это называется Горизонтальное масштабирование (Horizontal Scaling).
🧹 Шаг 1: Правильное завершение работы
Kafka и Zookeeper довольно "прожорливые" ребята и занимают много оперативной памяти. Давай выключим их по правилам.
Останови работу скриптов во всех трех терминалах (нажми Ctrl + C в каждом из них).
В любом терминале введи команду, которая корректно остановит и удалит контейнеры Kafka:
code
Powershell
docker-compose down
📦 Шаг 2: Упаковываем шедевр (README.md)
Твои знания о Kafka обязаны быть в портфолио! Создай в корне папки kafka_anti_fraud файл README.md и вставь туда это описание:
code
Markdown
# 🌊 Real-Time Fraud Detection Streaming (Apache Kafka)

Потоковая (Streaming) микросервисная архитектура для обработки банковских транзакций в реальном времени с использованием брокера сообщений Apache Kafka.

## 🛠 Стек технологий
* **Message Broker:** Apache Kafka, Zookeeper (Docker Compose)
* **Data Processing:** Python, `confluent-kafka`
* **Architecture:** Event-Driven Architecture, Pub/Sub, Consumer Groups

## ⚙️ Особенности проекта
1. **Producer (Генератор):** Скрипт `producer.py` имитирует миллионы кассовых аппаратов, непрерывно отправляя JSON-транзакции в топик Kafka.
2. **Consumer (Аналитик):** Скрипт `consumer.py` асинхронно вычитывает поток данных и применяет Rule-Based логику (фильтрация аномальных сумм > 4000). В Production заменяется на ML-инференс (LightGBM/XGBoost).
3. **Horizontal Scaling (Highload):** Проект демонстрирует мощь `Consumer Groups`. При запуске нескольких инстансов `consumer.py` Kafka автоматически балансирует нагрузку (Rebalance), разделяя поток транзакций между воркерами без изменения кода.

## 🚀 Как запустить

### 1. Подъем Инфраструктуры
Запустите Kafka кластер через Docker:
```bash
docker-compose up -d

2. Установка зависимостей
code
Bash
pip install confluent-kafka

3. Запуск Пайплайна
В первом терминале запустите генератор транзакций:
code
Bash
python src/producer.py
Во втором (и третьем, для теста масштабирования) терминале запустите обработчик:
code
Bash
python src/consumer.py