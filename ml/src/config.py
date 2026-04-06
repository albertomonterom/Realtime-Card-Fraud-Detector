"""
ML Configuration
==================
Centralized settings for training, model paths, hyperparameters, and thresholds.
Load from environment variables for deployment flexibility.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file before reading environment variables (at project root, 2 levels up)
load_dotenv(Path(__file__).parent.parent.parent / '.env')

# ── Model Paths ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
MODELS_DIR = Path(os.getenv('MODELS_DIR', PROJECT_ROOT / 'models'))
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# Direct path from .env (supports both .pkl pickle format and .json formats)
MODEL_PATH = os.getenv('MODEL_PATH', str(MODELS_DIR / 'fraud_model.pkl'))

# ── Training Config ──────────────────────────────────────────
TRAIN_TEST_SPLIT_RATIO = float(os.getenv('TRAIN_TEST_SPLIT', 0.7))
RANDOM_STATE = int(os.getenv('RANDOM_STATE', 42))
N_FOLDS = int(os.getenv('N_FOLDS', 3))

# ── XGBoost Hyperparameters (from notebook optimization) ──────────────────────────────────
XGBOOST_PARAMS = {
    'n_estimators': int(os.getenv('XGBOOST_N_ESTIMATORS', 100)),
    'max_depth': int(os.getenv('XGBOOST_MAX_DEPTH', 7)),
    'learning_rate': float(os.getenv('XGBOOST_LEARNING_RATE', 0.1)),
    'subsample': float(os.getenv('XGBOOST_SUBSAMPLE', 0.8)),
    'colsample_bytree': float(os.getenv('XGBOOST_COLSAMPLE_BYTREE', 0.8)),
    'scale_pos_weight': float(os.getenv('XGBOOST_SCALE_POS_WEIGHT', 5)),
    'random_state': RANDOM_STATE,
}

# ── Scoring Config ───────────────────────────────────────────
FRAUD_THRESHOLD = float(os.getenv('FRAUD_THRESHOLD', 0.7))
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# ── Logging ──────────────────────────────────────────────────
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
# ── Flask API Configuration ──────────────────────────────────
API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('API_PORT', 5001))
API_DEBUG = os.getenv('API_DEBUG', 'False').lower() in ('true', '1', 'yes')
# ── Feature Store (for future: Redis cache, feature DB) ──────
FEATURE_CACHE_ENABLED = os.getenv('FEATURE_CACHE_ENABLED', 'false').lower() == 'true'
FEATURE_CACHE_TTL = int(os.getenv('FEATURE_CACHE_TTL', 3600))
