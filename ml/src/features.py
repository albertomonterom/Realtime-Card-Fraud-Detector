"""
Feature Engineering Module
===================================
Single source of truth for feature transformations used across:
  - Training (train_model.py)
  - Real-time scoring (scoring_service.py)
  - Spark stream processing (../spark/src/features.py imports this)
  - Producer event enrichment (../producer/src/features.py imports this)
"""

import logging
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)


# ── Feature Column Definitions ───────────────────────────────
FEATURE_COLUMNS = [
    'amt', 'hour', 'day_of_week', 'month', 'is_weekend', 'is_night',
    'card_avg_amt', 'card_std_amt', 'amt_deviation', 'amt_to_avg_ratio',
    'geo_distance', 'card_txn_count', 'category_encoded',
    'state_encoded', 'gender_encoded', 'age', 'city_pop_log',
]

REQUIRED_INPUT_COLUMNS = {
    'trans_date_trans_time',   # Datetime string
    'amt',                     # Transaction amount
    'cc_num',                  # Card number
    'lat', 'long',             # Cardholder location
    'merch_lat', 'merch_long', # Merchant location
    'category',                # Transaction category
    'gender',                  # M/F
    'dob',                     # Date of birth
    'state',                   # State (fraud concentration varies by region)
    'city_pop',                # City population
    'is_fraud',                # Target (only in training)
}


def engineer_features(df: pd.DataFrame, fit_encoders: bool = False) -> tuple:
    """
    Transform raw transaction data into model features.
    
    This is the CANONICAL feature engineering logic. All services must use it.
    
    Args:
        df: DataFrame with raw transaction data
        fit_encoders: If True, fit LabelEncoders & return them.
                     If False, assume they're already fit (for inference).
                     
    Returns:
        (features_df, encoders) if fit_encoders=True
        (features_df, None) if fit_encoders=False
        
    Raises:
        ValueError: If required columns are missing
    """
    df = df.copy()  # Don't mutate input
    required = REQUIRED_INPUT_COLUMNS - {'is_fraud'}  # is_fraud optional
    missing = required - set(df.columns)
    
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    logger.info("Engineering features...")
    encoders = {}

    # ─── Temporal Features ───────────────────────────────────
    # Extract time-based patterns from transaction timestamp
    # Fraud patterns vary by time: late night (22-5am) and weekends show higher fraud rates
    # From EDA: fraud peaks 22:00-03:00 when cardholders are less vigilant
    df['trans_dt'] = pd.to_datetime(df['trans_date_trans_time'])
    df['hour'] = df['trans_dt'].dt.hour                           # Hour of day (0-23)
    df['day_of_week'] = df['trans_dt'].dt.dayofweek              # Day of week (0=Mon, 6=Sun)
    df['month'] = df['trans_dt'].dt.month                         # Month of year for seasonality
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)       # Binary: weekend flag
    df['is_night'] = ((df['hour'] >= 22) | (df['hour'] <= 5)).astype(int)  # Binary: night hours

    # ─── Amount Anomaly Features ─────────────────────────────
    # Detect unusual transaction amounts relative to cardholder's spending history
    # EDA finding: Fraudsters stay under $1,400 to evade detection; legitimate txns average higher
    # Calculate per-card statistics to identify outlier transactions
    card_stats = df.groupby('cc_num')['amt'].agg(['mean', 'std']).reset_index()
    card_stats.columns = ['cc_num', 'card_avg_amt', 'card_std_amt']
    card_stats['card_std_amt'] = card_stats['card_std_amt'].fillna(0)  # Handle single-txn cards
    df = df.merge(card_stats, on='cc_num', how='left')
    
    # Z-score: how many standard deviations from cardholder's average (anomaly detector)
    df['amt_deviation'] = np.where(
        df['card_std_amt'] > 0,
        (df['amt'] - df['card_avg_amt']) / df['card_std_amt'],
        0
    )

    # Ratio of transaction amount to cardholder's typical spending (spending pattern change)
    df['amt_to_avg_ratio'] = df['amt'] / (df['card_avg_amt'] + 1)

    # ─── Geographic Distance Features ────────────────────────
    # Calculate travel distance between cardholder location and merchant
    # Uses accurate spherical distance (Haversine formula) accounting for Earth's curvature
    # Detects impossible/suspicious travel patterns (e.g., transaction in 2 distant locations)
    def haversine_distance(lat1, lon1, lat2, lon2):
        """Calculate great-circle distance between two points on Earth in kilometers"""
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        # Haversine formula for great-circle distance
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        return c * 6371  # Earth's radius = 6371 km
    
    df['geo_distance'] = haversine_distance(
        df['lat'].values, df['long'].values,
        df['merch_lat'].values, df['merch_long'].values
    )

    # ─── Card Risk Features ──────────────────────────────────
    # Measure transaction velocity (activity level) per card
    # High velocity in short timespan indicates card compromise or testing fraud
    # Calculated as total historical transaction count for each card
    txn_counts = df.groupby('cc_num').size().reset_index(name='card_txn_count')
    df = df.merge(txn_counts, on='cc_num', how='left')

    # ─── Categorical Encodings ──────────────────────────────
    # Convert categorical variables to numeric labels for ML models
    # Fit encoders during training; use same encoders for inference to ensure consistency
    
    # Category encoding: EDA found online (shopping_net, misc_net) are fraud hotspots
    le_cat = LabelEncoder()
    df['category_encoded'] = le_cat.fit_transform(df['category'].astype(str))
    encoders['category'] = le_cat
    
    # State encoding: EDA found geographic concentration (NY highest fraud count)
    le_state = LabelEncoder()
    df['state_encoded'] = le_state.fit_transform(df['state'].astype(str))
    encoders['state'] = le_state
    
    # Gender encoding: Binary feature (M/F) - low discriminative power but preserves signal
    le_gender = LabelEncoder()
    df['gender_encoded'] = le_gender.fit_transform(df['gender'].astype(str))
    encoders['gender'] = le_gender

    # ─── Demographic Features ───────────────────────────────
    # Extract customer demographics that may correlate with fraud patterns
    
    # Age: EDA finding - older customers (40-60 years) more frequently targeted
    df['dob_dt'] = pd.to_datetime(df['dob'])
    df['age'] = (df['trans_dt'] - df['dob_dt']).dt.days / 365.25  # Age in years

    # City population (log-scaled): Addresses socioeconomic variations in fraud
    # Log transform compresses skewed distribution and captures non-linear effects
    df['city_pop_log'] = np.log1p(df['city_pop'])  # log(1 + pop) to handle zero values

    logger.info(f"✓ Engineered {len(FEATURE_COLUMNS)} features")
    
    return (df, encoders) if fit_encoders else (df, None)


def get_feature_columns() -> list:
    """Return the list of feature column names used by the model."""
    return FEATURE_COLUMNS.copy()


def get_required_columns() -> set:
    """Return the set of input columns required for feature engineering."""
    return REQUIRED_INPUT_COLUMNS.copy()


def validate_features(df: pd.DataFrame) -> tuple[bool, list]:
    """
    Validate that a DataFrame has all required features after engineering.
    
    Args:
        df: DataFrame after engineer_features() has been called
        
    Returns:
        (is_valid, missing_columns)
    """
    missing = set(FEATURE_COLUMNS) - set(df.columns)
    is_valid = len(missing) == 0
    return (is_valid, list(missing) if missing else [])
