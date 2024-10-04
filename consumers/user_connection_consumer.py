import threading
from kafka import KafkaConsumer
from neo4j import GraphDatabase
import json

class UserConnectionConsumer:
    def __init__(self, bootstrap_servers, neo4j_uri, neo4j_user, neo4j_password):
        self.consumer = KafkaConsumer(
            'user_connections',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.is_running = False
        self.thread = None
        
    def process_message(self, message):
        with self.driver.session() as session:
            session.execute_write(self._create_connection, message)
            
    def _create_connection(self, tx, connection):
        query = (
            "MERGE (follower:User {userID: $follower_id}) "
            "MERGE (followed:User {userID: $followed_id}) "
            "CREATE (follower)-[:FOLLOWS {timestamp: $timestamp}]->(followed)"
        )
        tx.run(query, follower_id=connection['follower_id'], followed_id=connection['followed_id'],
               timestamp=connection['timestamp'])
        
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
    consumer = UserConnectionConsumer(['kafka:9092'], 'neo4j://neo4j:7687', 'neo4j', 'password')
    consumer.start()
    # Add some way to stop the consumer, e.g., wait for user input
    input("Press Enter to stop...")
    consumer.stop()