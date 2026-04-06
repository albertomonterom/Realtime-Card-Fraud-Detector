"""
Transaction Producer — Reads the Sparkov CSV and publishes
each row to Kafka's raw-transactions topic, simulating real-time
credit card transactions.
"""
import json
import time
import os
import logging
import hashlib
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer
import redis

logging.basicConfig(level=logging.INFO, format='%(asctime)s [PRODUCER] %(message)s')
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC   = os.getenv('KAFKA_TOPIC', 'raw-transactions')
REDIS_HOST    = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT    = int(os.getenv('REDIS_PORT', 6379))
TPS           = int(os.getenv('TRANSACTIONS_PER_SECOND', 10))
DATA_PATH     = os.getenv('DATA_PATH', '/opt/data/fraudTrain.csv')


def create_producer() -> KafkaProducer:
    """Create Kafka producer with JSON serialization."""
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                linger_ms=10,       # batch messages for 10ms for throughput
                batch_size=32768,
            )
            logger.info(f"Connected to Kafka at {KAFKA_SERVERS}")
            return producer
        except Exception as e:
            logger.warning(f"Kafka not ready (attempt {attempt+1}/10): {e}")
            time.sleep(5)
    raise ConnectionError("Could not connect to Kafka after 10 attempts")


def init_redis_profiles(df: pd.DataFrame, r: redis.Redis):
    """
    Pre-compute cardholder spending profiles and store in Redis.
    These are used by the scoring service for feature enrichment.
    """
    logger.info("Building cardholder profiles in Redis...")
    profiles = df.groupby('cc_num').agg(
        avg_amt=('amt', 'mean'),
        std_amt=('amt', 'std'),
        txn_count=('amt', 'count'),
        most_common_category=('category', lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'unknown'),
        most_common_state=('state', lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'unknown'),
    ).reset_index()

    pipe = r.pipeline()
    for _, row in profiles.iterrows():
        key = f"profile:{row['cc_num']}"
        pipe.hset(key, mapping={
            'avg_amt':    round(row['avg_amt'], 2),
            'std_amt':    round(row['std_amt'], 2) if pd.notna(row['std_amt']) else 0,
            'txn_count':  int(row['txn_count']),
            'top_category': row['most_common_category'],
            'top_state':    row['most_common_state'],
        })
        pipe.expire(key, 86400 * 7)  # 7-day TTL
    pipe.execute()
    logger.info(f"Loaded {len(profiles)} cardholder profiles into Redis")


def row_to_event(row: pd.Series) -> dict:
    """Convert a DataFrame row to a Kafka-ready event dict."""
    txn_id = hashlib.md5(
        f"{row['cc_num']}_{row['trans_date_trans_time']}_{row['amt']}".encode()
    ).hexdigest()[:16]

    return {
        'transaction_id':   txn_id,
        'cc_num':           str(row['cc_num']),
        'merchant':         row['merchant'],
        'category':         row['category'],
        'amt':              float(row['amt']),
        'first':            row['first'],
        'last':             row['last'],
        'gender':           row['gender'],
        'street':           row['street'],
        'city':             row['city'],
        'state':            row['state'],
        'zip':              str(row['zip']),
        'lat':              float(row['lat']),
        'long':             float(row['long']),
        'city_pop':         int(row['city_pop']),
        'job':              row['job'],
        'dob':              row['dob'],
        'trans_num':        row['trans_num'],
        'merch_lat':        float(row['merch_lat']),
        'merch_long':       float(row['merch_long']),
        'is_fraud':         int(row['is_fraud']),
        'trans_date_trans_time': row['trans_date_trans_time'],
        'produced_at':      datetime.utcnow().isoformat(),
    }


def main():
    logger.info("=" * 60)
    logger.info("TRANSACTION PRODUCER STARTING")
    logger.info(f"  Kafka:  {KAFKA_SERVERS} / topic: {KAFKA_TOPIC}")
    logger.info(f"  Redis:  {REDIS_HOST}:{REDIS_PORT}")
    logger.info(f"  Rate:   {TPS} transactions/sec")
    logger.info(f"  Data:   {DATA_PATH}")
    logger.info("=" * 60)

    # Load dataset
    logger.info("Loading dataset (this may take a moment)...")
    df = pd.read_csv(DATA_PATH)
    logger.info(f"Loaded {len(df):,} transactions | Fraud rate: {df['is_fraud'].mean():.4%}")

    # Init Redis profiles
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    init_redis_profiles(df, r)

    # Start producing
    producer = create_producer()
    delay = 1.0 / TPS
    total_sent = 0
    total_fraud = 0

    for idx, row in df.iterrows():
        event = row_to_event(row)

        # Use cc_num as partition key → same card always goes to same partition
        producer.send(
            KAFKA_TOPIC,
            key=event['cc_num'],
            value=event
        )

        total_sent += 1
        if event['is_fraud'] == 1:
            total_fraud += 1

        if total_sent % 1000 == 0:
            logger.info(
                f"Sent {total_sent:,} transactions "
                f"({total_fraud} fraudulent, "
                f"{total_fraud/total_sent:.2%} fraud rate)"
            )

        time.sleep(delay)

    producer.flush()
    producer.close()
    logger.info(f"DONE — Sent {total_sent:,} transactions total")


if __name__ == '__main__':
    main()
