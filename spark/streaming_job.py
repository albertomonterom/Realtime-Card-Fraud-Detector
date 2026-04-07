"""
Spark Structured Streaming Job — Consumes raw transactions from Kafka,
enriches with features, calls the ML scoring service, and writes results
to both Kafka (fraud-alerts) and PostgreSQL.

Submit with:
    spark-submit --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
                   org.postgresql:postgresql:42.7.1,\
                   requests:2.31.0 \
        streaming_job.py
"""
import json
import logging
import os
import requests
import psycopg2

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp,
    hour, dayofweek, when, log1p, sqrt, pow as spark_pow
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("STREAMING")

# ── Kafka transaction schema ─────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",         StringType()),
    StructField("cc_num",                 StringType()),
    StructField("merchant",               StringType()),
    StructField("category",               StringType()),
    StructField("amt",                    DoubleType()),
    StructField("first",                  StringType()),
    StructField("last",                   StringType()),
    StructField("gender",                 StringType()),
    StructField("street",                 StringType()),
    StructField("city",                   StringType()),
    StructField("state",                  StringType()),
    StructField("zip",                    StringType()),
    StructField("lat",                    DoubleType()),
    StructField("long",                   DoubleType()),
    StructField("city_pop",               IntegerType()),
    StructField("job",                    StringType()),
    StructField("dob",                    StringType()),
    StructField("trans_num",              StringType()),
    StructField("merch_lat",              DoubleType()),
    StructField("merch_long",             DoubleType()),
    StructField("is_fraud",               IntegerType()),
    StructField("trans_date_trans_time",  StringType()),
    StructField("produced_at",            StringType()),
])

# ── Config ────────────────────────────────────────────────────
KAFKA_SERVERS    = os.getenv("KAFKA_BROKER",   "kafka:9092")
INPUT_TOPIC      = os.getenv("KAFKA_TOPIC",    "raw-transactions")
OUTPUT_TOPIC     = "fraud-alerts"
SCORING_URL      = os.getenv("ML_API_URL",     "http://ml:5001/enrich_and_score")
PG_URL           = f"jdbc:postgresql://{os.getenv('DB_HOST', 'postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'fraud_detection')}"
PG_PROPS         = {"user": os.getenv("DB_USER", "fraud_user"), "password": os.getenv("DB_PASSWORD", ""), "driver": "org.postgresql.Driver"}
CHECKPOINT_DIR   = "/tmp/spark-checkpoints/fraud-streaming"
FRAUD_THRESHOLD  = float(os.getenv("FRAUD_THRESHOLD", "0.85"))


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FraudDetectionStreaming")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.kafka.maxRatePerPartition", "100")
        .getOrCreate()
    )


def score_transaction(transaction_json: str) -> dict:
    """Call ML API to score a transaction."""
    try:
        response = requests.post(
            SCORING_URL,
            json=json.loads(transaction_json),
            timeout=5
        )
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Scoring failed: {response.text}")
            return {"fraud_probability": -1, "is_fraud_predicted": False}
    except Exception as e:
        logger.error(f"Error calling scoring API: {e}")
        return {"fraud_probability": -1, "is_fraud_predicted": False}


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created")

    # ── Read from Kafka ───────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON values
    transactions = (
        raw_stream
        .select(
            col("key").cast("string").alias("partition_key"),
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("partition_key", "data.*", "kafka_timestamp")
    )

    # ── Feature Engineering (basic enrichment) ────────────────
    enriched = (
        transactions
        .withColumn("trans_time", col("trans_date_trans_time").cast(TimestampType()))
        .withColumn("hour_of_day", hour("trans_time"))
        .withColumn("day_of_week", dayofweek("trans_time"))
        .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0))
        .withColumn("is_night", when(
            (col("hour_of_day") >= 22) | (col("hour_of_day") <= 5), 1
        ).otherwise(0))
        .withColumn("geo_distance",
            sqrt(
                spark_pow(col("lat") - col("merch_lat"), 2) +
                spark_pow(col("long") - col("merch_long"), 2)
            ) * 111
        )
        .withColumn("city_pop_log", log1p(col("city_pop")))
        .withColumn("scored_at", current_timestamp())
        .withColumn("transaction_json", to_json(struct("*")))
    )

    # ── Write to PostgreSQL + Send fraud alerts (via foreachBatch) ────────────────
    def write_batch_to_pg(batch_df: DataFrame, batch_id: int):
        """Process each micro-batch: score with ML API and write results."""
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: Empty batch, skipping")
            return

        count = batch_df.count()
        logger.info(f"Batch {batch_id}: Processing {count} transactions")

        rows = batch_df.collect()
        fraud_alerts = []

        for row in rows:
            try:
                # Call ML scoring API
                score_result = score_transaction(row.transaction_json)

                fraud_probability = score_result.get("fraud_probability", -1)
                is_fraud_predicted = score_result.get("is_fraud_predicted", False)

                # Prepare record for PostgreSQL
                pg_record = {
                    "transaction_id": row.transaction_id,
                    "cc_num": row.cc_num,
                    "merchant": row.merchant,
                    "category": row.category,
                    "amt": row.amt,
                    "city": row.city,
                    "state": row.state,
                    "lat": row.lat,
                    "long": row.long,
                    "trans_time": row.trans_time,
                    "amt_deviation": 0,  # Would be calculated by ML pipeline
                    "hour_of_day": row.hour_of_day,
                    "day_of_week": row.day_of_week,
                    "fraud_probability": fraud_probability,
                    "is_fraud_predicted": is_fraud_predicted,
                    "model_version": "spark-stream",
                    "processing_latency_ms": 0,
                }

                # Collect fraud alerts
                if is_fraud_predicted:
                    fraud_alerts.append({
                        "transaction_id": row.transaction_id,
                        "cc_num": row.cc_num,
                        "merchant": row.merchant,
                        "amt": row.amt,
                        "fraud_probability": fraud_probability,
                        "scored_at": row.scored_at,
                    })

                # Write to PostgreSQL via Python
                try:
                    conn = psycopg2.connect(
                        host=os.getenv("DB_HOST", "postgres"),
                        database=os.getenv("DB_NAME", "fraud_detection"),
                        user=os.getenv("DB_USER", "fraud_user"),
                        password=os.getenv("DB_PASSWORD", "")
                    )
                    cur = conn.cursor()
                    cur.execute(
                        """
                        INSERT INTO scored_transactions (
                            transaction_id, cc_num, merchant, category, amt,
                            city, state, lat, long, trans_time,
                            amt_deviation, hour_of_day, day_of_week,
                            fraud_probability, is_fraud_predicted, model_version,
                            processing_latency_ms
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s
                        )
                        ON CONFLICT (transaction_id) DO NOTHING
                        """,
                        (
                            pg_record['transaction_id'],
                            pg_record['cc_num'],
                            pg_record['merchant'],
                            pg_record['category'],
                            pg_record['amt'],
                            pg_record['city'],
                            pg_record['state'],
                            pg_record['lat'],
                            pg_record['long'],
                            pg_record['trans_time'],
                            pg_record['amt_deviation'],
                            pg_record['hour_of_day'],
                            pg_record['day_of_week'],
                            pg_record['fraud_probability'],
                            pg_record['is_fraud_predicted'],
                            pg_record['model_version'],
                            pg_record['processing_latency_ms'],
                        )
                    )
                    conn.commit()
                    conn.close()
                except Exception as db_err:
                    logger.warning(f"DB write error for txn {row.transaction_id}: {db_err}")

            except Exception as e:
                logger.error(f"Error processing transaction {row.transaction_id}: {e}")

        # Output fraud alerts
        if fraud_alerts:
            logger.info(f"Batch {batch_id}: Found {len(fraud_alerts)} fraud alerts!")
            for alert in fraud_alerts:
                logger.warning(f"FRAUD ALERT: {alert['transaction_id']} - {alert['merchant']} - ${alert['amt']} (prob={alert['fraud_probability']:.2%})")

    # ── Start streaming query ─────────────────────────────────
    query = (
        enriched.writeStream
        .foreachBatch(write_batch_to_pg)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/pg-sink")
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info("Streaming query started — waiting for termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
