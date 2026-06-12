import json
import requests
from confluent_kafka import Consumer

# ── Configuration ─────────────────────────────────────────
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud_detection_group',
    'auto.offset.reset': 'earliest'
}

TOPIC_NAME = "transactions"

# ── Fraud Detection API ───────────────────────────────────
# Live API on Render — real LightGBM model!
FRAUD_API_URL = "https://fraud-detection-api-1-hf78.onrender.com/scan"

# ── Stats counters ────────────────────────────────────────
stats = {
    'total': 0,
    'fraud': 0,
    'approved': 0,
    'api_errors': 0
}

def detect_fraud_ml(transaction: dict) -> dict:
    """
    Call real ML fraud detection API (LightGBM + Redis Feature Store)
    Falls back to rule-based if API unavailable
    """
    try:
        response = requests.post(
            FRAUD_API_URL,
            json={
                "card_id": transaction.get("user_id", "card_0001"),
                "amount": transaction["amount"]
            },
            timeout=3
        )

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            # Card not in Redis — use rule-based fallback
            return rule_based_fallback(transaction)
        else:
            stats['api_errors'] += 1
            return rule_based_fallback(transaction)

    except requests.exceptions.RequestException:
        stats['api_errors'] += 1
        return rule_based_fallback(transaction)


def rule_based_fallback(transaction: dict) -> dict:
    """Rule-based fallback when API is unavailable"""
    amount = transaction["amount"]
    is_fraud = amount > 4000.0
    return {
        "card_id": transaction.get("user_id", "unknown"),
        "amount": amount,
        "decision": "BLOCK" if is_fraud else "APPROVE",
        "risk_level": "HIGH" if is_fraud else "LOW",
        "risk_probability": 0.95 if is_fraud else 0.05,
        "model_used": "Rule-Based Fallback"
    }


def process_transaction(transaction: dict):
    """Process single transaction through ML fraud detector"""
    stats['total'] += 1
    amount = transaction["amount"]
    user_id = transaction.get("user_id", "unknown")

    # Call ML API
    result = detect_fraud_ml(transaction)

    decision = result.get("decision", "APPROVE")
    risk_prob = result.get("risk_probability", 0.0)
    risk_level = result.get("risk_level", "LOW")
    model = result.get("model_used", "Unknown")

    if decision == "BLOCK":
        stats['fraud'] += 1
        print(
            f"🚨 FRAUD BLOCKED | "
            f"User: {user_id} | "
            f"Amount: {amount:,.0f} | "
            f"Risk: {risk_prob:.1%} | "
            f"Level: {risk_level} | "
            f"Model: {model}"
        )
    else:
        stats['approved'] += 1
        print(
            f"✅ APPROVED | "
            f"User: {user_id} | "
            f"Amount: {amount:,.0f} | "
            f"Risk: {risk_prob:.1%} | "
            f"Model: {model}"
        )

    # Print stats every 10 transactions
    if stats['total'] % 10 == 0:
        fraud_rate = stats['fraud'] / stats['total'] * 100
        print(
            f"\n📊 Stats | "
            f"Total: {stats['total']} | "
            f"Fraud: {stats['fraud']} ({fraud_rate:.1f}%) | "
            f"Approved: {stats['approved']} | "
            f"API Errors: {stats['api_errors']}\n"
        )


# ── Main Consumer Loop ────────────────────────────────────
consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe([TOPIC_NAME])

print("🕵️ ML Fraud Detector started!")
print(f"📡 Kafka: {KAFKA_CONFIG['bootstrap.servers']}")
print(f"🤖 ML API: {FRAUD_API_URL}")
print(f"👥 Consumer Group: {KAFKA_CONFIG['group.id']}")
print("─" * 60)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"❌ Kafka error: {msg.error()}")
            continue

        raw_data = msg.value().decode('utf-8')
        transaction = json.loads(raw_data)
        process_transaction(transaction)

except KeyboardInterrupt:
    print("\n🛑 Stopping fraud detector...")
    print(f"📊 Final Stats: {stats}")
finally:
    consumer.close()
    print("✅ Consumer closed.")