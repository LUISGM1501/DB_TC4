import threading
import time
import random
from kafka import KafkaProducer
import json

class ProductInteractionProducer:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.is_running = False
        self.thread = None
        
    def produce_interaction(self):
        interaction = {
            "user_id": random.randint(1, 1000),
            "product_id": random.randint(1, 100),
            "interaction_type": random.choice(['review', 'rating']),
            "value": random.randint(1, 5) if random.choice(['review', 'rating']) == 'rating' else "Great product!",
            "timestamp": int(time.time())
        }
        self.producer.send('product_interactions', interaction)
        
    def run(self):
        while self.is_running:
            self.produce_interaction()
            time.sleep(random.uniform(0.5, 3))  # Random delay between interactions

    def start(self):
        self.is_running = True
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def stop(self):
        self.is_running = False
        if self.thread:
            self.thread.join()
        self.producer.close()