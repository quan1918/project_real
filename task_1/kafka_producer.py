# kafka_producer.py
from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='localhost:9092')
branches = ['Branch1','Branch2','Branch3','Branch4','Branch5']

for i in range(100):
    event = {
        'timestamp': datetime.utcnow().isoformat(),
        'branch_id': random.choice(branches),
        'revenue': round(random.uniform(50, 500),2),
        'service_time': round(random.uniform(5, 20),2),
        'rating': round(random.uniform(3,5),2)
    }
    producer.send('orders', json.dumps(event).encode('utf-8'))
    print("Sent:", event)

producer.flush()
producer.close()
