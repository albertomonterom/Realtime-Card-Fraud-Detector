# Setup & Development

## Prerequisites

- **Python 3.9+**
- **Docker & Docker Compose** (for containerized services)
- **Kafka** (message broker)
- **Redis** (feature cache)
- **PostgreSQL** (optional, for historical data)

---

## Quick Start

### 1. Clone & Install

```bash
git clone <repo>
cd Realtime-Card-Fraud-Detector

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: .venv\Scripts\activate  # Windows

# Install ML dependencies
cd ml
pip install -r requirements.txt
cd ..
```

### 2. Set Environment Variables

```bash
cp .env.example .env
# Edit .env with your local settings
```

### 3. Prepare Data

```bash
# Download Sparkov dataset (if not already in data/raw/)
# https://www.kaggle.com/datasets/elattar/sparkov-data-corpus

ls data/raw/
# Should see: fraudTrain.csv, fraudTest.csv
```

### 4. Train Initial Model

```bash
cd ml
python -m src.train_model --data ../data/raw/fraudTrain.csv --output models/
cd ..
```

You'll see:
```
Dataset: 1,296,675 rows | Fraud: 7,145 (0.55%)
Train: 1,037,340 | Test: 259,335
ROC AUC: 0.9456
...
✓ Model saved to models/fraud_model.json
✓ Metadata saved to models/model_metadata.json
```

### 5. Start Scoring Service

```bash
cd ml
python -m src.scoring_service
# Server runs on http://localhost:5001
```

Test it:
```bash
curl -X POST http://localhost:5001/health
# {"status": "healthy", "model_loaded": true, ...}
```

---

## Running Services with Docker Compose

### Single Command Startup

```bash
docker-compose up -d
```

This starts:
- **ml** (Flask scoring service on :5001)
- **prometheus** (metrics on :9090)
- **grafana** (dashboards on :3000)
- **kafka** (broker on :9092)
- **redis** (cache on :6379)

Check service health:
```bash
docker-compose ps
docker-compose logs -f ml
```

---

## Development Workflow

### 1. **Test Feature Changes**

Edit `ml/src/features.py`, then:
```bash
pytest ml/tests/test_features.py -v
```

### 2. **Retrain Model**

```bash
cd ml
python -m src.train_model \
  --data ../data/raw/fraudTrain.csv \
  --output models/
```

Check new model on test set:
```python
# Interactive Python
import json
with open('models/model_metadata.json') as f:
    meta = json.load(f)
    print(f"ROC AUC: {meta['roc_auc']}")
    print(f"F1: {meta['f1']}")
```

### 3. **Test Scoring Endpoint**

```bash
# Score pre-engineered features
curl -X POST http://localhost:5001/score \
  -H "Content-Type: application/json" \
  -d '{
    "amt": 100.0,
    "hour": 14,
    "day_of_week": 2,
    "is_weekend": 0,
    "is_night": 0,
    "card_avg_amt": 50.0,
    "card_std_amt": 20.0,
    "amt_deviation": 2.5,
    "amt_to_avg_ratio": 2.0,
    "geo_distance": 5.5,
    "card_txn_count": 100,
    "category_encoded": 5,
    "gender_encoded": 1,
    "age": 35.0,
    "city_pop_log": 15.0
  }'

# Response:
{
  "fraud_probability": 0.15234,
  "is_fraud_predicted": false,
  "threshold": 0.7,
  "model_version": "v20260405_120000",
  "latency_ms": 2.34
}
```

### 4. **Monitor Metrics**

Open http://localhost:3000 (Grafana):
- Default user: `admin`, password: `admin`
- Dashboards → Fraud Detection

---

## Project Structure

```
Realtime-Card-Fraud-Detector/
├── .env.example                 ← Copy to .env
├── .gitignore
├── docker-compose.yml           ← Orchestrates all services
├── README.md
│
├── data/
│   ├── raw/                     ← Training/test CSV files
│   ├── processed/               ← Feature-engineered outputs
│   └── sample/                  ← Small data for quick tests
│
├── ml/                          ← Model training & serving
│   ├── src/
│   │   ├── __init__.py
│   │   ├── features.py          ← 🔴 SHARED FEATURE LOGIC
│   │   ├── train_model.py       ← Training entry point
│   │   ├── scoring_service.py   ← Flask API
│   │   └── config.py            ← Configuration
│   ├── models/                  ← Trained XGBoost models
│   ├── tests/
│   │   ├── test_features.py
│   │   └── test_model.py (TODO)
│   ├── requirements.txt
│   ├── Dockerfile
│   └── .env.example
│
├── producer/                    ← Event enrichment (TODO)
├── spark/                       ← Stream processing (TODO)
├── airflow/                     ← Orchestration (TODO)
├── prometheus/                  ← Metrics
├── grafana/                     ← Dashboards
│
└── docs/
    ├── ARCHITECTURE.md          ← System design
    ├── FEATURES.md              ← Feature engineering details
    ├── SETUP.md                 ← This file
    └── API.md                   ← Endpoint reference
```

---

## Troubleshooting

### Model not loading?
```bash
cd ml
ls -la models/
# Should see fraud_model.json, model_metadata.json
```

### Feature mismatch errors?
```bash
# Check feature columns in metadata
python -c "import json; print(json.load(open('ml/models/model_metadata.json'))['feature_columns'])"
```

### Predictions too slow?
- Check model path in `/health` endpoint
- Verify Redis is running (for card stats caching)
- Profile with: `python -m cProfile -s cumtime ml/src/scoring_service.py`

### Data issues?
```bash
# Validate training CSV
python -c "
import pandas as pd
df = pd.read_csv('data/raw/fraudTrain.csv')
print(f'Rows: {len(df)}, Cols: {len(df.columns)}')
print(df.isnull().sum())
"
```

---

## Next Steps

1. **Implement Producer** (`producer/src/producer.py`)
   - Consume raw transactions from Kafka
   - Enrich with cardholder profiles (Redis)
   - Use `ml.src.features` to engineer features
   - Publish enriched features

2. **Implement Spark** (`spark/src/processor.py`)
   - Process feature streams
   - Compute card-level aggregations
   - Store in Redis for producer to fetch

3. **Implement Airflow** (`airflow/dags/fraud_detection.py`)
   - Schedule daily model retraining
   - Run feature validation checks
   - Export metrics

4. **Add Integration Tests** (`tests/test_integration.py`)
   - End-to-end flow: raw txn → engineered → scored
   - Compare training & inference feature distributions

---

## Contributing

- Always update `ml/src/features.py` as the source of truth
- Run `pytest ml/tests/` before committing
- Document feature changes in `docs/FEATURES.md`
- Log model versions in `ml/models/model_metadata.json`

---

**Last Updated**: April 5, 2026
