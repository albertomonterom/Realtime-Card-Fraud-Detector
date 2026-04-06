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
    
    Args:
        data_path: Path to CSV with training data
        output_dir: Where to save model + metadata (default: config.MODELS_DIR)
    """
    if output_dir is None:
        output_dir = str(config.MODELS_DIR)
    
    logger.info(f"Loading data from {data_path}")
    df = pd.read_csv(data_path)
    logger.info(
        f"Dataset: {len(df):,} rows | "
        f"Fraud: {df['is_fraud'].sum():,} ({df['is_fraud'].mean():.4%})"
    )

    # Use shared feature engineering
    df, encoders = features.engineer_features(df, fit_encoders=True)
    feature_cols = features.get_feature_columns()

    X = df[feature_cols].values
    y = df['is_fraud'].values

    # Stratified split to preserve fraud ratio
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=1 - config.TRAIN_TEST_SPLIT_RATIO,
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

    # Train XGBoost with configured hyperparameters
    model = xgb.XGBClassifier(
        **config.XGBOOST_PARAMS,
        scale_pos_weight=scale_pos_weight,
        min_child_weight=5,
        reg_alpha=0.1,
        reg_lambda=1.0,
        eval_metric='aucpr',
        early_stopping_rounds=20,
        n_jobs=-1,
    )

    logger.info("Training model...")
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50,
    )

    # Evaluate
    y_proba = model.predict_proba(X_test)[:, 1]
    roc_auc = roc_auc_score(y_test, y_proba)

    # Find optimal threshold using F1
    precisions, recalls, thresholds = precision_recall_curve(y_test, y_proba)
    f1_scores = 2 * (precisions * recalls) / (precisions + recalls + 1e-8)
    best_idx = np.argmax(f1_scores)
    best_threshold = thresholds[best_idx] if best_idx < len(thresholds) else 0.5

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
    model_path = os.path.join(output_dir, 'fraud_model.json')
    model.save_model(model_path)
    logger.info(f"✓ Model saved to {model_path}")

    # Save metadata (used by scoring service)
    metadata = {
        'model_version':    model_version,
        'trained_at':       datetime.utcnow().isoformat(),
        'feature_columns':  feature_cols,
        'best_threshold':   round(best_threshold, 4),
        'roc_auc':          round(roc_auc, 5),
        'precision':        round(precision_score(y_test, y_pred), 5),
        'recall':           round(recall_score(y_test, y_pred), 5),
        'f1':               round(f1_score(y_test, y_pred), 5),
        'train_size':       len(X_train),
        'test_size':        len(X_test),
        'scale_pos_weight': round(scale_pos_weight, 2),
        'feature_importance': {k: round(v, 4) for k, v in importance_sorted},
    }
    meta_path = os.path.join(output_dir, 'model_metadata.json')
    with open(meta_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"✓ Metadata saved to {meta_path}")

    return model, metadata


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Train XGBoost fraud detection model'
    )
    parser.add_argument(
        '--data',
        default='/opt/data/fraudTrain.csv',
        help='Path to training CSV file'
    )
    parser.add_argument(
        '--output',
        default=None,
        help='Output directory for model + metadata (default: config.MODELS_DIR)'
    )
    args = parser.parse_args()
    train(args.data, args.output)
