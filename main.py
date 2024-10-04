import os
import time
import logging
import signal
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from producers.purchase_producer import PurchaseProducer
from producers.user_connection_producer import UserConnectionProducer
from producers.product_interaction_producer import ProductInteractionProducer
from consumers.purchase_consumer import PurchaseConsumer
from consumers.user_connection_consumer import UserConnectionConsumer
from consumers.product_interaction_consumer import ProductInteractionConsumer
from neo4j_utils.graph_operations import GraphOperations

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_kafka(bootstrap_servers, max_retries=30, retry_interval=10):
    retries = 0
    while retries < max_retries:
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            producer.close()
            logger.info("Successfully connected to Kafka")
            return
        except NoBrokersAvailable:
            logger.warning(f"Kafka not yet available. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
            retries += 1
    raise Exception("Failed to connect to Kafka after maximum retries")

def main():
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092').split(',')
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'password')

    logger.info(f"Connecting to Kafka at: {kafka_servers}")
    logger.info(f"Connecting to Neo4j at: {neo4j_uri}")

    wait_for_kafka(kafka_servers)

    producers_and_consumers = [
        PurchaseProducer(kafka_servers),
        UserConnectionProducer(kafka_servers),
        ProductInteractionProducer(kafka_servers),
        PurchaseConsumer(kafka_servers, neo4j_uri, neo4j_user, neo4j_password),
        UserConnectionConsumer(kafka_servers, neo4j_uri, neo4j_user, neo4j_password),
        ProductInteractionConsumer(kafka_servers, neo4j_uri, neo4j_user, neo4j_password)
    ]

    logger.info("Starting producers and consumers")
    for component in producers_and_consumers:
        try:
            logger.info(f"Starting {component.__class__.__name__}")
            component.start()
        except Exception as e:
            logger.error(f"Error starting {component.__class__.__name__}: {str(e)}")
            # Optionally, you might want to stop all started components and exit
            for c in producers_and_consumers:
                if hasattr(c, 'stop'):
                    c.stop()
            raise

    def signal_handler(signum, frame):
        logger.info("Interrupt received, stopping producers and consumers...")
        for component in producers_and_consumers:
            component.stop()
        logger.info("All components stopped")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
    finally:
        logger.info("Stopping producers and consumers")
        for component in producers_and_consumers:
            component.stop()

    graph_ops = GraphOperations(neo4j_uri, neo4j_user, neo4j_password)
    result = graph_ops.most_purchased_products_by_category()
    logger.info(f"Most purchased products by category: {result}")

    logger.info("Application completed successfully")

if __name__ == "__main__":
    main()