import threading
from kafka import KafkaConsumer
from neo4j import GraphDatabase
import json
import random

class ProductInteractionConsumer:
    def __init__(self, bootstrap_servers, neo4j_uri, neo4j_user, neo4j_password):
        self.consumer = KafkaConsumer(
            'product_interactions',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.is_running = False
        self.thread = None
        
    def process_message(self, message):
        with self.driver.session() as session:
            session.execute_write(self._create_interaction, message)
            
    def _create_interaction(self, tx, interaction):
        categories = ["Electronics", "Clothing", "Books"]
        category = random.choice(categories)

        if interaction['interaction_type'] == 'review':
            relationship_type = 'REVIEWED'
            value_property = 'text'
        elif interaction['interaction_type'] == 'rating':
            relationship_type = 'RATED'
            value_property = 'rating_value'

        query = (
            "MERGE (u:User {userID: $user_id}) "
            "MERGE (p:Product {productID: $product_id}) "
            "ON CREATE SET p.category = $category "
            "ON MATCH SET p.category = coalesce(p.category, $category) "
            f"CREATE (u)-[:{relationship_type} {{ {value_property}: $value, timestamp: $timestamp }}]->(p)"
        )
        tx.run(query, user_id=interaction['user_id'], product_id=interaction['product_id'],
               category=category, value=interaction['value'], timestamp=interaction['timestamp'])
        
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
    consumer = ProductInteractionConsumer(['kafka:9092'], 'neo4j://neo4j:7687', 'neo4j', 'password')
    consumer.start()
    # Add some way to stop the consumer, e.g., wait for user input
    input("Press Enter to stop...")
    consumer.stop()