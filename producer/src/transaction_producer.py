"""
Kafka Producer — Sends sample transactions to raw-transactions topic
for consumption by the Spark streaming job.

Run with:
    python producer/src/transaction_producer.py
"""
import json
import time
import logging
import random
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [PRODUCER] %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'raw-transactions'

MERCHANTS = ['AMAZON', 'WALMART', 'TARGET', 'COSTCO', 'TRADER_JOES', 'WHOLE_FOODS', 'SAFEWAY', 'CVS']
CATEGORIES = ['grocery', 'gas', 'shopping', 'entertainment', 'food', 'travel', 'utilities', 'health']
STATES = ['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA']
GENDERS = ['M', 'F']

def load_sample_transactions():
    """Load sample transactions from training data."""
    try:
        csv_path = Path(__file__).parent.parent.parent / 'data' / 'raw' / 'fraudTest.csv'
        if csv_path.exists():
            df = pd.read_csv(csv_path, nrows=100)
            logger.info(f"Loaded {len(df)} sample transactions from {csv_path}")
            return df
    except Exception as e:
        logger.warning(f"Could not load CSV: {e}")
    return None


def generate_random_transaction(df=None):
    """Generate a random transaction."""
    if df is not None and random.random() < 0.8:
        # Use actual data 80% of the time
        row = df.sample(1).iloc[0]
        return {
            'transaction_id': f"txn_{int(time.time() * 1000)}_{random.randint(0, 999)}",
            'cc_num': f"{random.randint(1000, 9999)}{random.randint(1000, 9999)}{random.randint(1000, 9999)}{random.randint(1000, 9999)}",
            'merchant': str(row.get('Merchant Name', random.choice(MERCHANTS))),
            'category': str(row.get('MCC', random.choice(CATEGORIES))),
            'amt': float(row.get('Amount', random.uniform(5, 500))),
            'first': row.get('First', 'John'),
            'last': row.get('Last', 'Doe'),
            'gender': row.get('Gender', random.choice(GENDERS)),
            'street': row.get('Street', '123 Main St'),
            'city': row.get('City', 'San Francisco'),
            'state': row.get('State', random.choice(STATES)),
            'zip': row.get('Zip', '94105'),
            'lat': float(row.get('Latitude', random.uniform(25, 50))),
            'long': float(row.get('Longitude', random.uniform(-125, -65))),
            'city_pop': int(row.get('City Population', 500000)),
            'job': row.get('Job', 'Engineer'),
            'dob': row.get('DOB', '1980-01-01'),
            'trans_num': row.get('Transaction #', 'TXN123'),
            'merch_lat': float(row.get('Merchant Latitude', random.uniform(25, 50))),
            'merch_long': float(row.get('Merchant Longitude', random.uniform(-125, -65))),
            'is_fraud': int(row.get('Is Fraud?', 0)),
            'trans_date_trans_time': (datetime.now() - timedelta(seconds=random.randint(0, 3600))).isoformat(),
            'produced_at': datetime.now().isoformat(),
        }
    
    # Generate synthetic transaction
    return {
        'transaction_id': f"txn_{int(time.time() * 1000)}_{random.randint(0, 999)}",
        'cc_num': f"{random.randint(1000, 9999)}{random.randint(1000, 9999)}{random.randint(1000, 9999)}{random.randint(1000, 9999)}",
        'merchant': random.choice(MERCHANTS),
        'category': random.choice(CATEGORIES),
        'amt': random.uniform(5, 500),
        'first': random.choice(['John', 'Jane', 'Bob', 'Alice']),
        'last': random.choice(['Smith', 'Johnson', 'Williams', 'Brown']),
        'gender': random.choice(GENDERS),
        'street': f"{random.randint(100, 9999)} Main St",
        'city': random.choice(['San Francisco', 'New York', 'Los Angeles', 'Chicago']),
        'state': random.choice(STATES),
        'zip': f"{random.randint(10000, 99999)}",
        'lat': random.uniform(25, 50),
        'long': random.uniform(-125, -65),
        'city_pop': random.randint(100000, 5000000),
        'job': random.choice(['Engineer', 'Doctor', 'Teacher', 'Manager']),
        'dob': f"19{random.randint(50, 90)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
        'trans_num': f"TXN{random.randint(100000, 999999)}",
        'merch_lat': random.uniform(25, 50),
        'merch_long': random.uniform(-125, -65),
        'is_fraud': 1 if random.random() < 0.01 else 0,  # 1% fraud rate
        'trans_date_trans_time': (datetime.now() - timedelta(seconds=random.randint(0, 3600))).isoformat(),
        'produced_at': datetime.now().isoformat(),
    }


def main():
    """Send transactions to Kafka."""
    logger.info(f"Connecting to Kafka at {KAFKA_SERVERS}...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
        )
        logger.info("Connected to Kafka!")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        logger.info("Make sure Kafka is running: docker-compose up -d kafka zookeeper")
        return

    df = load_sample_transactions()
    txn_count = 0

    try:
        while True:
            transaction = generate_random_transaction(df)
            
            # Send to Kafka
            producer.send(KAFKA_TOPIC, value=transaction)
            
            txn_count += 1
            if txn_count % 10 == 0:
                logger.info(f"Sent {txn_count} transactions ({transaction.get('is_fraud', 0)} fraud flag)")
            
            # Send one transaction every 0.5-2 seconds
            time.sleep(random.uniform(0.5, 2))
    
    except KeyboardInterrupt:
        logger.info(f"Shutting down. Sent {txn_count} transactions total.")
        producer.close()


if __name__ == '__main__':
    main()
