"""Kafka Transaction Consumer — Consumes transactions from Kafka,
calls ML API for scoring, and writes results to database.
"""

import json
import logging
import os
import requests
from kafka import KafkaConsumer


logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s [SPARK] %(message)s'
)
logger = logging.getLogger(__name__)
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)


def main():
    """Kafka consumer that scores transactions via ML API."""
    kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'raw-transactions')
    ml_api_url = os.getenv('ML_API_URL', 'http://ml:5001/enrich_and_score')

    logger.info(f"Starting Kafka Consumer")
    logger.info(f"  Kafka: {kafka_broker} (topic: {kafka_topic})")
    logger.info(f"  ML API: {ml_api_url}")
    logger.info("=" * 60)

    # Create Kafka consumer
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_broker,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='fraud-detection-scorer',
        consumer_timeout_ms=10000,
    )

    logger.info(f"Connected to Kafka. Listening for transactions...")

    processed_count = 0
    error_count = 0

    try:
        for message in consumer:
            transaction = message.value
            try:
                # Call ML API to score and log transaction
                response = requests.post(ml_api_url, json=transaction, timeout=10)
                response.raise_for_status()
                
                result = response.json()
                processed_count += 1

                if processed_count % 500 == 0:
                    logger.info(f"Processed {processed_count:,} transactions scored by ML pipeline")

            except Exception as e:
                error_count += 1
                logger.warning(f"Error scoring transaction: {e}")

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    finally:
        consumer.close()
        logger.info(f"DONE — Processed {processed_count} transactions ({error_count} errors)")


if __name__ == '__main__':
    main()

