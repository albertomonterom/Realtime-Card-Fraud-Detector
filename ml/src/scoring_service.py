"""
ML Scoring Service — Flask API that receives transaction data
and returns fraud probability scores with real-time metrics.

Endpoints:
    GET  /health              Health check
    GET  /metrics             Prometheus metrics (Pull model)
    POST /score               Score pre-engineered features
    POST /enrich_and_score    Raw txn → enrich → engineer → score

Usage:
    python -m ml.src.scoring_service
"""
import json
import logging
import os
import time
import pickle
from datetime import datetime

import numpy as np
import pandas as pd
import xgboost as xgb
from flask import Flask, request, jsonify
from prometheus_client import (
    Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
)

from . import features, config, database

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s [SCORER] %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── Prometheus Metrics ────────────────────────────────────────
PREDICTIONS_TOTAL = Counter(
    'fraud_predictions_total',
    'Total predictions made',
    ['result']
)
PREDICTION_LATENCY = Histogram(
    'fraud_prediction_latency_seconds',
    'Time to score one transaction',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25]
)
FRAUD_PROBABILITY = Histogram(
    'fraud_probability_distribution',
    'Distribution of fraud scores',
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
)
MODEL_VERSION_GAUGE = Gauge(
    'fraud_model_version_info',
    'Current model version timestamp'
)

# ── Load model + metadata ────────────────────────────────────
model = None
metadata = None
model_encoders = None


def load_model():
    """Load XGBoost model and metadata from config paths."""
    global model, metadata, model_encoders
    logger.info(f"Loading model from {config.MODEL_PATH}")
    
    with open(config.MODEL_PATH, 'rb') as f:
        model_data = pickle.load(f)
    
    model = model_data['model']
    metadata = model_data['metrics']
    metadata['feature_columns'] = model_data['feature_columns']
    metadata['decision_threshold'] = model_data.get('decision_threshold', config.FRAUD_THRESHOLD)
    model_encoders = model_data.get('encoders', {})  # Load encoders from model
    
    logger.info(f"✓ Model loaded successfully")
    logger.info(f"✓ Feature count: {len(metadata.get('feature_columns', []))}")
    logger.info(f"✓ Encoders loaded: {list(model_encoders.keys())}")
    logger.info(f"✓ ROC AUC: {metadata.get('roc_auc', 'N/A'):.4f}")
    logger.info(f"✓ Decision threshold: {metadata.get('decision_threshold', config.FRAUD_THRESHOLD)}")
    MODEL_VERSION_GAUGE.set(1)


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
    """Prometheus metrics endpoint (scrape target)."""
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}


@app.route('/score', methods=['POST'])
def score_transaction():
    """
    Score a single transaction with pre-engineered features.
    
    Expects JSON dict mapping feature names to values.
    Returns fraud_probability, is_fraud_predicted, etc.
    
    Example:
        POST /score
        {
            "amt": 123.45,
            "hour": 14,
            "day_of_week": 2,
            ... (all feature columns from metadata)
        }
    """
    start = time.time()
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No JSON payload'}), 400

    try:
        feature_cols = metadata.get('feature_columns', [])
        features_array = np.array([[data.get(col, 0) for col in feature_cols]])

        proba = float(model.predict_proba(features_array)[0][1])
        threshold = metadata.get('decision_threshold', config.FRAUD_THRESHOLD)
        is_fraud = proba >= threshold

        # Update Prometheus metrics
        latency = time.time() - start
        PREDICTION_LATENCY.observe(latency)
        FRAUD_PROBABILITY.observe(proba)
        PREDICTIONS_TOTAL.labels(result='fraud' if is_fraud else 'legit').inc()

        return jsonify({
            'fraud_probability': round(proba, 5),
            'is_fraud_predicted': bool(is_fraud),
            'threshold': threshold,
            'model_version': metadata.get('model_version', 'unknown'),
            'latency_ms': round(latency * 1000, 2),
        })

    except Exception as e:
        logger.error(f"Scoring error: {e}", exc_info=True)
        PREDICTIONS_TOTAL.labels(result='error').inc()
        return jsonify({'error': str(e)}), 500


@app.route('/enrich_and_score', methods=['POST'])
def enrich_and_score():
    """
    Full pipeline: receive raw transaction → engineer features → score.
    
    This endpoint uses the shared feature engineering module to transform
    raw transaction data, ensuring consistency with training pipelines.
    
    Expects JSON with raw transaction fields (trans_date_trans_time, amt,
    cc_num, lat, long, merch_lat, merch_long, category, gender, dob, city_pop, etc.)
    
    Returns fraud score + engineered features for debugging/logging.
    """
    start = time.time()
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No JSON payload'}), 400

    try:
        # Build minimal DataFrame for feature engineering
        df = pd.DataFrame([data])
        df, _ = features.engineer_features(df, fit_encoders=False, encoders=model_encoders)
        
        # Extract feature vectors
        feature_cols = features.get_feature_columns()
        features_array = np.array([[df[col].iloc[0] for col in feature_cols]])

        proba = float(model.predict_proba(features_array)[0][1])
        threshold = metadata.get('decision_threshold', config.FRAUD_THRESHOLD)
        is_fraud = proba >= threshold

        latency = time.time() - start
        PREDICTION_LATENCY.observe(latency)
        FRAUD_PROBABILITY.observe(proba)
        PREDICTIONS_TOTAL.labels(result='fraud' if is_fraud else 'legit').inc()

        # Log prediction to database
        try:
            db = database.get_db()
            prediction_log = {
                'transaction_id': data.get('transaction_id', ''),
                'cc_num': data.get('cc_num', ''),
                'merchant': data.get('merchant', ''),
                'category': data.get('category', ''),
                'amt': float(data.get('amt', 0)),
                'city': data.get('city', ''),
                'state': data.get('state', ''),
                'lat': float(data.get('lat', 0)),
                'long': float(data.get('long', 0)),
                'trans_time': data.get('trans_date_trans_time', ''),
                'amt_deviation': float(df['amt_deviation'].iloc[0]) if 'amt_deviation' in df.columns else 0,
                'hour': int(df['hour'].iloc[0]) if 'hour' in df.columns else 0,
                'day_of_week': int(df['day_of_week'].iloc[0]) if 'day_of_week' in df.columns else 0,
            }
            db.log_prediction(
                transaction_data=prediction_log,
                fraud_prob=float(proba),
                is_fraud=bool(is_fraud),
                latency_ms=round(latency * 1000, 2),
                model_version=metadata.get('model_version', 'unknown'),
            )
        except Exception as db_error:
            logger.warning(f"Failed to log prediction to database: {db_error}")

        # Return engineered features for debugging
        engineered = {col: float(df[col].iloc[0]) for col in feature_cols}

        return jsonify({
            'transaction_id': data.get('transaction_id', ''),
            'fraud_probability': round(proba, 5),
            'is_fraud_predicted': bool(is_fraud),
            'threshold': threshold,
            'engineered_features': engineered,
            'model_version': metadata.get('model_version', 'unknown'),
            'latency_ms': round(latency * 1000, 2),
        })

    except Exception as e:
        logger.error(f"Enrich+score error: {e}", exc_info=True)
        PREDICTIONS_TOTAL.labels(result='error').inc()
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    load_model()
    logger.info(
        f"Starting scoring service on {config.API_HOST}:{config.API_PORT} "
        f"(threshold={config.FRAUD_THRESHOLD})"
    )
    app.run(host=config.API_HOST, port=config.API_PORT, debug=config.API_DEBUG)
