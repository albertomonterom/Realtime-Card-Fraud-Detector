"""
Database operations for fraud detection system.
Handles logging predictions and transactions to PostgreSQL.
"""

import os
import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class FraudDB:
    """PostgreSQL database connection and operations"""
    
    def __init__(self):
        self.conn_string = (
            f"dbname={os.getenv('DB_NAME', 'fraud_detection')} "
            f"user={os.getenv('DB_USER', 'fraud_user')} "
            f"password={os.getenv('DB_PASSWORD', 'fraud_password')} "
            f"host={os.getenv('DB_HOST', 'localhost')} "
            f"port={os.getenv('DB_PORT', '5432')}"
        )
        self.test_connection()
    
    def test_connection(self):
        """Verify database is accessible"""
        try:
            conn = psycopg2.connect(self.conn_string)
            conn.close()
            logger.info("✓ Database connection successful")
        except Exception as e:
            logger.warning(f"Database not available yet: {e}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(self.conn_string)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def log_prediction(self, transaction_data, fraud_prob, is_fraud, latency_ms, model_version):
        """
        Log a prediction to scored_transactions table.
        
        Args:
            transaction_data: dict with transaction fields
            fraud_prob: float, fraud probability (0-1)
            is_fraud: bool, prediction
            latency_ms: float, scoring latency in milliseconds
            model_version: str, model version identifier
        """
        try:
            with self.get_connection() as conn:
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
                        transaction_data.get('transaction_id'),
                        transaction_data.get('cc_num'),
                        transaction_data.get('merchant'),
                        transaction_data.get('category'),
                        transaction_data.get('amt'),
                        transaction_data.get('city'),
                        transaction_data.get('state'),
                        transaction_data.get('lat'),
                        transaction_data.get('long'),
                        transaction_data.get('trans_time', datetime.now(timezone.utc)),
                        transaction_data.get('amt_deviation'),
                        transaction_data.get('hour'),
                        transaction_data.get('day_of_week'),
                        fraud_prob,
                        is_fraud,
                        model_version,
                        latency_ms,
                    )
                )
                logger.debug(f"Logged prediction for txn {transaction_data.get('transaction_id')}")
        except Exception as e:
            logger.error(f"Failed to log prediction: {e}")
    
    def get_fraud_rate(self, hours=24):
        """Get fraud rate in last N hours"""
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as fraud_count
                    FROM scored_transactions
                    WHERE scored_at > NOW() - INTERVAL '%s hours'
                    """,
                    (hours,)
                )
                result = cur.fetchone()
                if result and result[0] > 0:
                    return {
                        'total_transactions': result[0],
                        'fraud_detected': result[1] or 0,
                        'fraud_rate': (result[1] or 0) / result[0]
                    }
                return {'total_transactions': 0, 'fraud_detected': 0, 'fraud_rate': 0}
        except Exception as e:
            logger.error(f"Failed to get fraud rate: {e}")
            return {}


# Global instance
db = None

def get_db():
    """Get or create database instance"""
    global db
    if db is None:
        db = FraudDB()
    return db
