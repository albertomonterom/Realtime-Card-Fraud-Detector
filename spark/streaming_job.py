"""
Spark Structured Streaming Job — Consumes raw transactions from Kafka,
enriches with features, calls the ML scoring service, and writes results
to both Kafka (fraud-alerts) and PostgreSQL.

Submit with:
    spark-submit --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
                   org.postgresql:postgresql:42.7.1 \
        streaming_job.py
"""
import json
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, udf, current_timestamp,
    hour, dayofweek, when, log1p, sqrt, pow as spark_pow, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, BooleanType, FloatType
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
KAFKA_SERVERS    = "kafka:9092"
INPUT_TOPIC      = "raw-transactions"
OUTPUT_TOPIC     = "fraud-alerts"
SCORING_URL      = "http://ml-scorer:5001/enrich_and_score"
PG_URL           = "jdbc:postgresql://postgres:5432/fraud_detection"
PG_PROPS         = {"user": "fraud_user", "password": "fraud_pass", "driver": "org.postgresql.Driver"}
CHECKPOINT_DIR   = "/tmp/spark-checkpoints/fraud-streaming"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FraudDetectionStreaming")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.kafka.maxRatePerPartition", "100")
        .getOrCreate()
    )


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

    # ── Feature Engineering (in-Spark) ────────────────────────
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
    )

    # ── Write to PostgreSQL (via foreachBatch) ────────────────
    def write_batch_to_pg(batch_df, batch_id):
        """Process each micro-batch: write to PostgreSQL."""
        if batch_df.isEmpty():
            return

        count = batch_df.count()
        logger.info(f"Batch {batch_id}: Processing {count} transactions")

        # Select columns matching our PG schema
        pg_df = batch_df.select(
            col("transaction_id"),
            col("cc_num"),
            col("merchant"),
            col("category"),
            col("amt"),
            col("city"),
            col("state"),
            col("lat"),
            col("long"),
            col("trans_time"),
            col("hour_of_day"),
            col("day_of_week"),
            col("geo_distance").alias("geo_velocity_kmh"),
            col("is_fraud").alias("is_fraud_actual"),
            col("scored_at"),
        )

        # Write to PostgreSQL
        (
            pg_df.write
            .format("jdbc")
            .option("url", PG_URL)
            .option("dbtable", "scored_transactions")
            .options(**PG_PROPS)
            .mode("append")
            .save()
        )

        # Also write fraud alerts to Kafka
        fraud_alerts = batch_df.filter(col("is_fraud") == 1)
        if not fraud_alerts.isEmpty():
            fraud_count = fraud_alerts.count()
            logger.info(f"Batch {batch_id}: {fraud_count} fraud alerts!")
            (
                fraud_alerts
                .select(
                    col("cc_num").alias("key"),
                    to_json(struct("*")).alias("value")
                )
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_SERVERS)
                .option("topic", OUTPUT_TOPIC)
                .save()
            )

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
