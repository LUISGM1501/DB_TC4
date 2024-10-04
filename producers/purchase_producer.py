import threading
import time
import random
from kafka import KafkaProducer
import json

class PurchaseProducer:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.is_running = False
        self.thread = None
        
    def produce_purchase(self):
        purchase = {
            "user_id": random.randint(1, 1000),
            "product_id": random.randint(1, 100),
            "quantity": random.randint(1, 10),
            "timestamp": int(time.time())
        }
        self.producer.send('purchases', purchase)
        
    def run(self):
        while self.is_running:
            self.produce_purchase()
            time.sleep(random.uniform(0.1, 2))  # Random delay between purchases

    def start(self):
        self.is_running = True
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def stop(self):
        self.is_running = False
        if self.thread:
            self.thread.join()
        self.producer.close()