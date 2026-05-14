import json
from confluent_kafka import Consumer

# 1. Настройка Потребителя (Consumer)
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud_detection_group', # Имя нашей ML-команды
    'auto.offset.reset': 'earliest'      # Читаем все накопленные сообщения с самого начала
}

consumer = Consumer(conf)
TOPIC_NAME = "transactions"

# Подключаемся к трубе
consumer.subscribe([TOPIC_NAME])

print("🕵️‍♂️ ИИ-Аналитик запущен. Слушаем поток Kafka...")

try:
    while True:
        # Проверяем трубу (ждем сообщение максимум 1 секунду)
        msg = consumer.poll(1.0)

        if msg is None:
            continue # Если труба пока пуста - идем на следующий круг
        if msg.error():
            print(f"❌ Ошибка Kafka: {msg.error()}")
            continue

        # Декодируем байты обратно в текст и превращаем в словарь (dict)
        raw_data = msg.value().decode('utf-8')
        transaction = json.loads(raw_data)
        
    
        
         
        amount = transaction["amount"]
        user_id = transaction["user_id"]
        
       
        if amount > 4000.0:
            print(f"🚨 ФРОД! Аномальная сумма {amount} у пользователя {user_id}")
        
        elif amount <= 4000.0 :
            print(f"✅ ОК. Пользователь {user_id} потратил {amount}")
        
       

except KeyboardInterrupt:
    print("\nОстановка аналитика...")
finally:
    # Обязательно закрываем соединение при выходе
    consumer.close()