import time
import random
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'random_events'

while True:
    event = {
        'event_id': random.randint(1, 100000),
        'value': random.random(),
        'timestamp': time.time()
    }
    producer.send(topic_name, event)
    print(f"Produced event: {event}")
    time.sleep(5)
