"""
Model Training — Train an XGBoost fraud detection model on the
Sparkov dataset and save it for real-time scoring.

Usage:
    python train_model.py --data /path/to/fraudTrain.csv --output /opt/models/
"""
import argparse
import json
import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.metrics import (
    classification_report, roc_auc_score, precision_recall_curve,
    f1_score, precision_score, recall_score, confusion_matrix
)
from sklearn.preprocessing import LabelEncoder

logging.basicConfig(level=logging.INFO, format='%(asctime)s [TRAIN] %(message)s')
logger = logging.getLogger(__name__)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Feature engineering — these same transformations must be
    replicated in the real-time Spark/scoring pipeline.
    """
    logger.info("Engineering features...")

    # Parse datetime
    df['trans_dt'] = pd.to_datetime(df['trans_date_trans_time'])
    df['hour']     = df['trans_dt'].dt.hour
    df['day_of_week'] = df['trans_dt'].dt.dayofweek
    df['is_weekend']  = (df['day_of_week'] >= 5).astype(int)
    df['is_night']    = ((df['hour'] >= 22) | (df['hour'] <= 5)).astype(int)

    # Amount features
    card_stats = df.groupby('cc_num')['amt'].agg(['mean', 'std']).reset_index()
    card_stats.columns = ['cc_num', 'card_avg_amt', 'card_std_amt']
    card_stats['card_std_amt'] = card_stats['card_std_amt'].fillna(0)
    df = df.merge(card_stats, on='cc_num', how='left')
    df['amt_deviation'] = np.where(
        df['card_std_amt'] > 0,
        (df['amt'] - df['card_avg_amt']) / df['card_std_amt'],
        0
    )
    df['amt_to_avg_ratio'] = df['amt'] / (df['card_avg_amt'] + 1)

    # Geographic distance between cardholder and merchant
    df['geo_distance'] = np.sqrt(
        (df['lat'] - df['merch_lat'])**2 +
        (df['long'] - df['merch_long'])**2
    ) * 111  # rough conversion to km

    # Transaction frequency per card (count in dataset as proxy)
    txn_counts = df.groupby('cc_num').size().reset_index(name='card_txn_count')
    df = df.merge(txn_counts, on='cc_num', how='left')

    # Category encoding
    le_cat = LabelEncoder()
    df['category_encoded'] = le_cat.fit_transform(df['category'].astype(str))

    # Gender encoding
    df['gender_encoded'] = (df['gender'] == 'M').astype(int)

    # Age from DOB
    df['dob_dt'] = pd.to_datetime(df['dob'])
    df['age'] = (df['trans_dt'] - df['dob_dt']).dt.days / 365.25

    # City population log
    df['city_pop_log'] = np.log1p(df['city_pop'])

    return df


def get_feature_columns() -> list:
    """Return the list of feature columns used by the model."""
    return [
        'amt', 'hour', 'day_of_week', 'is_weekend', 'is_night',
        'card_avg_amt', 'card_std_amt', 'amt_deviation', 'amt_to_avg_ratio',
        'geo_distance', 'card_txn_count', 'category_encoded',
        'gender_encoded', 'age', 'city_pop_log',
    ]


def train(data_path: str, output_dir: str):
    logger.info(f"Loading data from {data_path}")
    df = pd.read_csv(data_path)
    logger.info(f"Dataset: {len(df):,} rows | Fraud: {df['is_fraud'].sum():,} ({df['is_fraud'].mean():.4%})")

    df = engineer_features(df)
    feature_cols = get_feature_columns()

    X = df[feature_cols].values
    y = df['is_fraud'].values

    # Stratified split to preserve fraud ratio
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    logger.info(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
    logger.info(f"Train fraud rate: {y_train.mean():.4%} | Test fraud rate: {y_test.mean():.4%}")

    # Class imbalance: scale_pos_weight = count(negative) / count(positive)
    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    logger.info(f"scale_pos_weight: {scale_pos_weight:.1f}")

    # Train XGBoost
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        scale_pos_weight=scale_pos_weight,
        min_child_weight=5,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.1,
        reg_lambda=1.0,
        eval_metric='aucpr',
        early_stopping_rounds=20,
        random_state=42,
        n_jobs=-1,
    )

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

    logger.info("=" * 60)
    logger.info(f"ROC AUC:        {roc_auc:.5f}")
    logger.info(f"Best threshold: {best_threshold:.4f}")
    logger.info(f"Precision:      {precision_score(y_test, y_pred):.5f}")
    logger.info(f"Recall:         {recall_score(y_test, y_pred):.5f}")
    logger.info(f"F1:             {f1_score(y_test, y_pred):.5f}")
    logger.info("=" * 60)
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
    logger.info(f"Model saved to {model_path}")

    # Save metadata
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
    logger.info(f"Metadata saved to {meta_path}")

    return model, metadata


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', default='/opt/data/fraudTrain.csv')
    parser.add_argument('--output', default='/opt/models')
    args = parser.parse_args()
    train(args.data, args.output)
