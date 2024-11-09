from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Initialize Kafka producer with the correct broker address
producer = KafkaProducer(
    bootstrap_servers='dataeng-kafka:9092',  # Changed from localhost to container name
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_purchase_event():
    """Generate a random purchase event"""
    return {
        "timestamp": datetime.now().isoformat(),
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 100),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10.0, 1000.0), 2)
    }

def produce_events():
    """Continuously produce purchase events"""
    topic_name = "purchase_events"  # Make sure this matches your KAFKA_TOPIC_NAME in .env
    
    print("Starting to produce events...")
    print(f"Connecting to Kafka at dataeng-kafka:9092")
    print(f"Producing to topic: {topic_name}")
    
    while True:
        try:
            event = generate_purchase_event()
            producer.send(topic_name, event)
            print(f"Produced event: {event}")
            time.sleep(random.uniform(1, 5))
        except Exception as e:
            print(f"Error producing event: {e}")
            time.sleep(5)  # Wait before retrying

if __name__ == "__main__":
    print("Starting Purchase Event Producer...")
    produce_events()