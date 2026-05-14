import json
import time
import random
from confluent_kafka import Producer

# 1. Настраиваем подключение к нашей трубе (Kafka)
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

TOPIC_NAME = "transactions"

print("🚀 Кассовые аппараты запущены. Начинаем отправку данных в Kafka...")

# Бесконечный цикл генерации транзакций
while True:
    # 1. Создаем СЛОВАРЬ транзакции
    transaction_data = {
        "user_id": random.randint(1, 100),
        "amount": round(random.uniform(10.0, 5000.0), 2), # Округлим до 2 знаков для красоты
        "timestamp": time.time()
    }
    
    # 2. Превращаем словарь в JSON-строку
    json_string = json.dumps(transaction_data)
    
    # 3. Отправляем в Kafka (обязательно кодируем строку в байты)
    producer.produce(TOPIC_NAME, value=json_string.encode('utf-8'))
    
    # 4. Проталкиваем в трубу
    producer.flush()
    
    # 5. Выводим на экран, чтобы видеть работу
    print(f"✅ Отправлено в Kafka: {json_string}")
    
    # 6. Пауза полсекунды (ВЫЗОВ функции, а не присваивание)
    time.sleep(0.5)