"""
Producer Service — Event Enrichment
====================================

Consumes raw transactions from Kafka, enriches with cardholder profiles,
and uses shared feature engineering to create training/serving-consistent
features for the scoring pipeline.

TODO: Implement full Kafka producer logic
"""

import logging
import os
from datetime import datetime

# from kafka import KafkaConsumer, KafkaProducer
# from ml.src import features

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s [PRODUCER] %(message)s'
)
logger = logging.getLogger(__name__)


def enrich_transaction(raw_transaction: dict) -> dict:
    """
    Enrich raw transaction with features.
    
    Args:
        raw_transaction: {'trans_date_trans_time', 'amt', 'cc_num', ...}
    
    Returns:
        Enriched transaction with engineered features
    """
    # TODO: Fetch cardholder profile from Redis
    # profile = redis.hgetall(f"profile:{raw_transaction['cc_num']}")
    
    # TODO: Calculate rolling card statistics
    # card_avg_amt = profile.get('avg_amt', 0)
    
    # TODO: Call features.engineer_features()
    # engineered = features.engineer_features(...)
    
    logger.debug(f"Enriched txn: {raw_transaction.get('transaction_id')}")
    return raw_transaction


def main():
    """Main producer loop."""
    logger.info("Fraud Detection Producer starting...")
    
    # TODO: Connect to Kafka
    # consumer = KafkaConsumer(
    #     'transactions',
    #     bootstrap_servers=[os.getenv('KAFKA_BROKER', 'localhost:9092')],
    #     group_id='fraud-detector-producer',
    # )
    # producer = KafkaProducer(
    #     bootstrap_servers=[os.getenv('KAFKA_BROKER', 'localhost:9092')],
    # )
    
    # TODO: Implement enrichment loop
    # for message in consumer:
    #     txn = json.loads(message.value)
    #     enriched = enrich_transaction(txn)
    #     producer.send('transactions_enriched', json.dumps(enriched).encode())
    
    logger.info("Producer loop would start here")


if __name__ == '__main__':
    main()
