import json
import time
import random
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
TOPIC_NAME = "transactions"

# Use real card IDs from fraud-detection-api Redis store
CARD_IDS = [f"card_{i:04d}" for i in range(100)] + ["fraud_card_001"]

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")

print("💳 Transaction Producer started!")
print(f"📡 Sending to Kafka topic: {TOPIC_NAME}")
print("─" * 60)

try:
    while True:
        card_id = random.choice(CARD_IDS)

        # fraud_card_001 gets higher amounts
        if card_id == "fraud_card_001":
            amount = round(random.uniform(10000, 50000), 2)
        else:
            amount = round(random.uniform(100, 8000), 2)

        transaction = {
            "user_id": card_id,
            "amount": amount,
            "timestamp": time.time(),
            "merchant": random.choice([
                "Glovo", "Kaspi Shop", "inDrive",
                "Air Astana", "Kcell", "Unknown Merchant"
            ])
        }

        producer.produce(
            TOPIC_NAME,
            key=card_id,
            value=json.dumps(transaction),
            callback=delivery_report
        )
        producer.flush()

        print(f"📤 Sent: {card_id} | ₸{amount:,.0f}")
        time.sleep(0.5)

except KeyboardInterrupt:
    print("\n🛑 Producer stopped.")