import threading
import time
import random
from kafka import KafkaProducer
import json

class UserConnectionProducer:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.is_running = False
        self.thread = None
        
    def produce_connection(self):
        connection = {
            "follower_id": random.randint(1, 1000),
            "followed_id": random.randint(1, 1000),
            "timestamp": int(time.time())
        }
        self.producer.send('user_connections', connection)
        
    def run(self):
        while self.is_running:
            self.produce_connection()
            time.sleep(random.uniform(1, 5))  # Random delay between connections

    def start(self):
        self.is_running = True
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def stop(self):
        self.is_running = False
        if self.thread:
            self.thread.join()
        self.producer.close()