"""
Model Training — Train an XGBoost fraud detection model on the
Sparkov dataset and save it for real-time scoring.

Usage:
    python -m ml.src.train_model --data /path/to/fraudTrain.csv
    
This module uses shared feature engineering from ml.src.features
to ensure consistency with real-time scoring pipelines.
"""
import argparse
import json
import logging
import os
import pickle
from datetime import datetime

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report, roc_auc_score, precision_recall_curve,
    f1_score, precision_score, recall_score, confusion_matrix
)

from . import features, config

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s [TRAIN] %(message)s'
)
logger = logging.getLogger(__name__)


def train(data_path: str, output_dir: str = None):
    """
    Train fraud detection model using shared feature engineering.
    
    This script:
    - Loads fraudTrain.csv only (production training data, no test leakage)
    - Performs internal 70-30 stratified split for validation
    - Uses the same feature engineering as scoring_service.py
    
    Args:
        data_path: Path to training CSV (fraudTrain.csv)
        output_dir: Where to save model + metadata (default: config.MODELS_DIR)
    """
    if output_dir is None:
        output_dir = str(config.MODELS_DIR)
    
    logger.info(f"Loading training data from {data_path}")
    df = pd.read_csv(data_path)
    df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'])
    logger.info(
        f"Dataset: {len(df):,} rows | "
        f"Fraud: {df['is_fraud'].sum():,} ({df['is_fraud'].mean():.4%})"
    )

    # Use shared feature engineering
    df, encoders = features.engineer_features(df, fit_encoders=True)
    feature_cols = features.get_feature_columns()

    X = df[feature_cols].copy()
    y = df['is_fraud'].values

    # Stratified split to preserve fraud ratio
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=1 - config.TRAIN_TEST_SPLIT_RATIO,  # 70-30 split
        random_state=config.RANDOM_STATE,
        stratify=y
    )

    logger.info(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
    logger.info(
        f"Train fraud rate: {y_train.mean():.4%} | "
        f"Test fraud rate: {y_test.mean():.4%}"
    )

    # Class imbalance: scale_pos_weight = count(negative) / count(positive)
    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    logger.info(f"scale_pos_weight (class imbalance adjustment): {scale_pos_weight:.1f}")

    # Train XGBoost with hyperparameters from config (optimized via notebook)
    # Remove scale_pos_weight from config params and pass dynamically (calculated per dataset)
    xgb_params = {k: v for k, v in config.XGBOOST_PARAMS.items() if k != 'scale_pos_weight'}
    model = xgb.XGBClassifier(
        **xgb_params,
        scale_pos_weight=scale_pos_weight,
        n_jobs=-1,
    )

    logger.info("Training XGBoost model...")
    model.fit(X_train, y_train, verbose=False)

    # Evaluate
    y_proba = model.predict_proba(X_test)[:, 1]
    roc_auc = roc_auc_score(y_test, y_proba)

    # Use optimized threshold from .env config (0.80 provides 59% precision, 94% recall)
    best_threshold = config.FRAUD_THRESHOLD
    y_pred = (y_proba >= best_threshold).astype(int)

    logger.info("=" * 70)
    logger.info(f"ROC AUC:        {roc_auc:.5f}")
    logger.info(f"Best threshold: {best_threshold:.4f}")
    logger.info(f"Precision:      {precision_score(y_test, y_pred):.5f}")
    logger.info(f"Recall:         {recall_score(y_test, y_pred):.5f}")
    logger.info(f"F1:             {f1_score(y_test, y_pred):.5f}")
    logger.info("=" * 70)
    logger.info(f"\n{classification_report(y_test, y_pred, target_names=['Legit', 'Fraud'])}")

    cm = confusion_matrix(y_test, y_pred)
    logger.info(f"Confusion Matrix:\n{cm}")

    # Feature importance
    importance = dict(zip(feature_cols, model.feature_importances_))
    importance_sorted = sorted(importance.items(), key=lambda x: x[1], reverse=True)
    logger.info("Feature importances:")
    for feat, imp in importance_sorted:
        logger.info(f"  {feat:25s} {imp:.4f}")

    # Save model
    os.makedirs(output_dir, exist_ok=True)
    model_version = datetime.utcnow().strftime("v%Y%m%d_%H%M%S")
    
    # Metrics dict for metadata
    metrics = {
        'model_version':    model_version,
        'trained_at':       datetime.utcnow().isoformat(),
        'roc_auc':          round(roc_auc, 5),
        'precision':        round(precision_score(y_test, y_pred), 5),
        'recall':           round(recall_score(y_test, y_pred), 5),
        'f1':               round(f1_score(y_test, y_pred), 5),
        'train_size':       len(X_train),
        'test_size':        len(X_test),
        'scale_pos_weight': round(scale_pos_weight, 2),
        'feature_importance': {k: round(v, 4) for k, v in importance_sorted},
    }
    
    # Save as pickle with model + metrics + encoders + feature columns
    # This format is expected by scoring_service.py
    model_data = {
        'model':              model,
        'metrics':            metrics,
        'feature_columns':    feature_cols,
        'decision_threshold': round(best_threshold, 4),
        'encoders':           encoders,
    }
    model_path = os.path.join(output_dir, 'xgboost_model.pkl')
    with open(model_path, 'wb') as f:
        pickle.dump(model_data, f)
    logger.info(f"✓ Model saved to {model_path}")
    
    # Also save metadata as JSON for inspection
    metadata_json = {
        **metrics,
        'feature_columns':    feature_cols,
        'decision_threshold': round(best_threshold, 4),
    }
    # Convert numpy float32 values to Python float for JSON serialization
    metadata_json['feature_importance'] = {k: float(v) for k, v in metadata_json['feature_importance'].items()}
    meta_path = os.path.join(output_dir, 'model_metadata.json')
    with open(meta_path, 'w') as f:
        json.dump(metadata_json, f, indent=2)
    logger.info(f"✓ Metadata saved to {meta_path}")

    return model, metrics


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Train XGBoost fraud detection model'
    )
    parser.add_argument(
        '--data',
        default='./data/raw/fraudTrain.csv',
        help='Path to fraudTrain.csv'
    )
    parser.add_argument(
        '--output',
        default=None,
        help='Output directory for model + metadata (default: config.MODELS_DIR)'
    )
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("Fraud Detection Model Training")
    logger.info("=" * 70)
    train(args.data, args.output)