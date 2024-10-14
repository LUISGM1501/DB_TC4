import threading
from kafka import KafkaConsumer
from neo4j import GraphDatabase
import json
import uuid
import random

class PurchaseConsumer:
    def __init__(self, bootstrap_servers, neo4j_uri, neo4j_user, neo4j_password):
        self.consumer = KafkaConsumer(
            'purchases',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.is_running = False
        self.thread = None
        
    def process_message(self, message):
        with self.driver.session() as session:
            session.execute_write(self._create_purchase, message)
            
    def _create_purchase(self, tx, purchase):
        categories = ["Electronics", "Clothing", "Books"]
        category = random.choice(categories)

        query = (
            "MERGE (u:User {userID: $user_id}) "
            "MERGE (p:Product {productID: $product_id}) "
            "ON CREATE SET p.category = $category "
            "ON MATCH SET p.category = coalesce(p.category, $category) "
            "CREATE (t:Transaction {transactionID: $transaction_id, quantity: $quantity, timestamp: $timestamp}) "
            "CREATE (u)-[:PURCHASED]->(t)-[:OF]->(p)"
        )
        tx.run(query, user_id=purchase['user_id'], product_id=purchase['product_id'],
               category=category, transaction_id=str(uuid.uuid4()), 
               quantity=purchase['quantity'], timestamp=purchase['timestamp'])
        
    def run(self):
        while self.is_running:
            for message in self.consumer:
                if not self.is_running:
                    break
                self.process_message(message.value)

    def start(self):
        self.is_running = True
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def stop(self):
        self.is_running = False
        if self.thread:
            self.thread.join()
        self.consumer.close()
        self.driver.close()

if __name__ == "__main__":
    consumer = PurchaseConsumer(['kafka:9092'], 'neo4j://neo4j:7687', 'neo4j', 'password')
    consumer.start()
    # Agregar algun metodo para detener el consumidor, por ejemplo, esperar una entrada del usuario
    input("Presione Enter para detener...")
    consumer.stop()
