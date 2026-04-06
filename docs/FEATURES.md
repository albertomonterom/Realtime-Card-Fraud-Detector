# Shared Feature Engineering

## Overview

The `ml/src/features.py` module is the **canonical source of truth** for all feature transformations. All services (training, scoring, streaming) must import and use this module to ensure consistency.

**Golden Rule**: If you change how a feature is computed, update `ml/src/features.py` and document the change here.

---

## Feature Definitions

All 15 features used by the XGBoost model:

### Temporal Features
| Feature | Computation | Purpose |
|---------|-------------|---------|
| `hour` | Extract from `trans_date_trans_time` | Time-of-day risk (late-night purchases riskier) |
| `day_of_week` | 0=Monday, 6=Sunday | Day-of-week patterns (weekends differ) |
| `is_weekend` | `day_of_week >= 5` | Binary: weekend vs weekday |
| `is_night` | `hour >= 22 or hour <= 5` | Binary: late-night flag |

### Amount Anomaly Features
| Feature | Computation | Purpose |
|---------|-------------|---------|
| `card_avg_amt` | Rolling avg of `amt` per card | Baseline spending |
| `card_std_amt` | Rolling std of `amt` per card | Spending volatility |
| `amt_deviation` | `(amt - avg) / std` | How many std devs from usual |
| `amt_to_avg_ratio` | `amt / (avg + 1)` | Ratio to historical average |

### Geographic Features
| Feature | Computation | Purpose |
|---------|-------------|---------|
| `geo_distance` | `sqrt((lat-merch_lat)² + (long-merch_long)²) * 111` | Distance in km from home to merchant |

### Card Risk Features
| Feature | Computation | Purpose |
|---------|-------------|---------|
| `card_txn_count` | Count of txns per card in dataset | Card activity level (proxy for card age) |

### Categorical Features
| Feature | Encoding | Purpose |
|---------|----------|---------|
| `category_encoded` | LabelEncoder on merchant category | 15+ unique categories |
| `gender_encoded` | LabelEncoder on gender (M/F) | Gender patterns in fraud |

### Demographic Features
| Feature | Computation | Purpose |
|---------|-------------|---------|
| `age` | `(trans_dt - dob) / 365.25` days | Age of cardholder |
| `city_pop_log` | `log1p(city_pop)` | Urban vs rural risk |

---

## Input Requirements

The `engineer_features()` function requires these raw columns:

```python
REQUIRED_INPUT_COLUMNS = {
    'trans_date_trans_time',  # datetime string (ISO format)
    'amt',                     # float, transaction amount
    'cc_num',                  # str, card number (for grouping)
    'lat', 'long',            # float, cardholder location
    'merch_lat', 'merch_long',# float, merchant location
    'category',               # str, transaction category
    'gender',                 # str, 'M' or 'F'
    'dob',                    # str, date of birth (ISO format)
    'city_pop',               # int, population at cardholder's city
    'is_fraud',               # int (optional), 0 or 1 (only in training)
}
```

---

## API Reference

### `engineer_features(df, fit_encoders=False)`

Transform raw transaction data into model features.

**Args:**
- `df` (pd.DataFrame): Raw transaction data
- `fit_encoders` (bool): If True, fit & return LabelEncoders. If False, assume they're already fit.

**Returns:**
- `(features_df, encoders)` if `fit_encoders=True`
- `(features_df, None)` if `fit_encoders=False`

**Raises:**
- `ValueError` if required columns are missing

**Example:**
```python
from ml.src.features import engineer_features, get_feature_columns

# Training: fit encoders
train_df = pd.read_csv('fraudTrain.csv')
train_df, encoders = engineer_features(train_df, fit_encoders=True)
feature_cols = get_feature_columns()
X = train_df[feature_cols].values

# Inference: use existing encoders
new_txn = pd.DataFrame([...])  # raw transaction
new_txn, _ = engineer_features(new_txn, fit_encoders=False)
X_new = new_txn[feature_cols].values
score = model.predict_proba(X_new)
```

### `get_feature_columns()`
Returns list of 15 feature column names.

### `get_required_columns()`
Returns set of required input columns.

### `validate_features(df)`
Check if DataFrame has all engineered features. Returns `(is_valid, missing_columns)`.

---

## Known Issues & Constraints

### 1. LabelEncoding Not Saved
Currently, `LabelEncoder` objects are fit during training but **not persisted**. This means:

**Problem**: In inference, if a new `category` value appears (unseen during training), LabelEncoder will crash.

**Workaround**: Currently using `fit_encode` with the training data. For production, save encoders to JSON/pickle:
```python
import pickle
with open('encoders.pkl', 'wb') as f:
    pickle.dump(encoders, f)
```

### 2. Card-Level Aggregations Need Stateful Data
Features like `card_avg_amt`, `card_std_amt`, `card_txn_count` require **historical data per card**. 

**For training**: Compute from CSV (full history available).
**For inference**: Must maintain a **feature store** (Redis, feature DB) with rolling aggregations.

**Current flow**:
```
Spark Streaming → Compute rolling card stats → Store in Redis
Producer → Fetch stats from Redis → Enrich transaction
```

### 3. Age Computation Sensitive to Datetime
The `age` feature subtracts `dob` from transaction time. Ensure all datetimes are in UTC.

---

## Testing

Run feature tests:
```bash
pytest ml/tests/test_features.py -v
```

**Test coverage:**
- ✓ All 15 features are generated
- ✓ Required columns are validated
- ✓ Feature engineering is deterministic
- ✓ No NaN values in outputs

---

## Changelog

### v1.0.0 (Started: April 2024)
- Initial 15-feature set based on Sparkov dataset
- Added geographic and demographic features
- Implemented categorical encoding

### Future
- Add `velocity` features (txns per hour per card)
- Add `merchant_recency` (days since last txn at this merchant)
- Add `location_change_flag` (abnormal geographic distance)

---

## References

- **Sparkov Dataset**: https://www.kaggle.com/datasets/elattar/sparkov-data-corpus
- **Feature Engineering for Fraud**: https://fraud-detection.readthedocs.io/
- **Scikit-Learn LabelEncoder**: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.LabelEncoder.html
