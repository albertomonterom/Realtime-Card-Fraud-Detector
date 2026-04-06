"""
ML Scoring Service — Flask API that receives transaction features
and returns a fraud probability score. Exposes /metrics for Prometheus.
"""
import json
import os
import time
import logging
import pickle
from datetime import datetime

import numpy as np
import pandas as pd
import xgboost as xgb
import redis
from flask import Flask, request, jsonify
from prometheus_client import (
    Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
)

from src import config, features

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s [SCORER] %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── Prometheus Metrics ────────────────────────────────────────
PREDICTIONS_TOTAL = Counter(
    'fraud_predictions_total', 'Total predictions made', ['result']
)
PREDICTION_LATENCY = Histogram(
    'fraud_prediction_latency_seconds', 'Time to score one transaction',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25]
)
FRAUD_PROBABILITY = Histogram(
    'fraud_probability_distribution', 'Distribution of fraud scores',
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
)
MODEL_VERSION_GAUGE = Gauge(
    'fraud_model_version_info', 'Current model version timestamp'
)

# ── Globals ────────────────────────────────────────────────

model = None
metadata = None
redis_client = None


def load_model():
    """Load XGBoost model and metadata from pickle file."""
    global model, metadata
    logger.info(f"Loading model from {config.MODEL_PATH}")
    
    with open(config.MODEL_PATH, 'rb') as f:
        model_data = pickle.load(f)
    
    model = model_data['model']
    metadata = model_data['metrics']
    metadata['feature_columns'] = model_data['feature_columns']
    metadata['decision_threshold'] = model_data.get('decision_threshold', config.FRAUD_THRESHOLD)
    
    logger.info(f"✓ Model loaded successfully")
    logger.info(f"✓ Feature count: {len(metadata.get('feature_columns', []))}")
    logger.info(f"✓ ROC AUC: {metadata.get('roc_auc', 'N/A')}")
    logger.info(f"✓ Decision threshold: {metadata.get('decision_threshold', config.FRAUD_THRESHOLD)}")


def get_redis():
    """Get or create Redis connection."""
    global redis_client
    if redis_client is None:
        redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=True
        )
    return redis_client


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'model_loaded': model is not None,
        'model_version': metadata.get('model_version', 'unknown') if metadata else None,
    })


@app.route('/metrics', methods=['GET'])
def metrics():
    """Prometheus scrapes this endpoint."""
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}


@app.route('/score', methods=['POST'])
def score_transaction():
    """
    Score a single transaction.

    Expects JSON with feature values matching the model's feature_columns.
    Returns fraud_probability, is_fraud_predicted, and the threshold used.
    """
    start = time.time()

    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON payload'}), 400

    try:
        feature_cols = metadata.get('feature_columns', [])
        features = np.array([[data.get(col, 0) for col in feature_cols]])

        proba = float(model.predict_proba(features)[0][1])
        threshold = metadata.get('decision_threshold', config.FRAUD_THRESHOLD)
        is_fraud = proba >= threshold

        # Update Prometheus metrics
        latency = time.time() - start
        PREDICTION_LATENCY.observe(latency)
        FRAUD_PROBABILITY.observe(proba)
        PREDICTIONS_TOTAL.labels(result='fraud' if is_fraud else 'legit').inc()

        return jsonify({
            'fraud_probability':  round(proba, 5),
            'is_fraud_predicted': is_fraud,
            'threshold':          threshold,
            'model_version':      metadata.get('model_version', 'unknown'),
            'latency_ms':         round(latency * 1000, 2),
        })

    except Exception as e:
        logger.error(f"Scoring error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/enrich_and_score', methods=['POST'])
def enrich_and_score():
    """
    Full pipeline: receive raw transaction → enrich with Redis
    profile → engineer features → score.
    """
    start = time.time()
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON payload'}), 400

    try:
        r = get_redis()
        cc_num = data.get('cc_num', '')

        # Fetch cardholder profile from Redis
        profile = r.hgetall(f"profile:{cc_num}")
        card_avg = float(profile.get('avg_amt', 0))
        card_std = float(profile.get('std_amt', 1))

        amt = float(data.get('amt', 0))

        # Engineer features using canonical feature module
        trans_dt = datetime.fromisoformat(data.get('trans_date_trans_time', datetime.utcnow().isoformat()))

        features = {
            'amt':              amt,
            'hour':             trans_dt.hour,
            'day_of_week':      trans_dt.weekday(),
            'is_weekend':       1 if trans_dt.weekday() >= 5 else 0,
            'is_night':         1 if trans_dt.hour >= 22 or trans_dt.hour <= 5 else 0,
            'card_avg_amt':     card_avg,
            'card_std_amt':     card_std,
            'amt_deviation':    (amt - card_avg) / card_std if card_std > 0 else 0,
            'amt_to_avg_ratio': amt / (card_avg + 1),
            'geo_distance':     np.sqrt(
                (float(data.get('lat', 0)) - float(data.get('merch_lat', 0)))**2 +
                (float(data.get('long', 0)) - float(data.get('merch_long', 0)))**2
            ) * 111,
            'card_txn_count':   int(profile.get('txn_count', 0)),
            'category_encoded': hash(data.get('category', '')) % 15,  # simplified
            'gender_encoded':   1 if data.get('gender') == 'M' else 0,
            'age':              0,  # computed from DOB if available
            'city_pop_log':     np.log1p(int(data.get('city_pop', 0))),
        }

        # Compute age if DOB provided
        if 'dob' in data:
            dob = datetime.fromisoformat(data['dob']) if isinstance(data['dob'], str) else data['dob']
            features['age'] = (trans_dt - dob).days / 365.25

        # Track transaction velocity in Redis
        velocity_key = f"velocity:{cc_num}"
        now_ts = time.time()
        r.zadd(velocity_key, {str(now_ts): now_ts})
        r.zremrangebyscore(velocity_key, 0, now_ts - 300)  # keep last 5 min
        r.expire(velocity_key, 600)

        # Score
        feature_cols = metadata.get('feature_columns', [])
        X = np.array([[features.get(col, 0) for col in feature_cols]])
        proba = float(model.predict_proba(X)[0][1])
        threshold = metadata.get('decision_threshold', config.FRAUD_THRESHOLD)
        is_fraud = proba >= threshold

        latency = time.time() - start
        PREDICTION_LATENCY.observe(latency)
        FRAUD_PROBABILITY.observe(proba)
        PREDICTIONS_TOTAL.labels(result='fraud' if is_fraud else 'legit').inc()

        return jsonify({
            'transaction_id':     data.get('transaction_id', ''),
            'fraud_probability':  round(proba, 5),
            'is_fraud_predicted': is_fraud,
            'features':           {k: round(v, 4) if isinstance(v, float) else v for k, v in features.items()},
            'threshold':          threshold,
            'model_version':      metadata.get('model_version', 'unknown'),
            'latency_ms':         round(latency * 1000, 2),
        })

    except Exception as e:
        logger.error(f"Enrich+score error: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    load_model()
    logger.info(
        f"Starting scoring service on {config.API_HOST}:{config.API_PORT} "
        f"(threshold={config.FRAUD_THRESHOLD})"
    )
    app.run(host=config.API_HOST, port=config.API_PORT, debug=config.API_DEBUG)
