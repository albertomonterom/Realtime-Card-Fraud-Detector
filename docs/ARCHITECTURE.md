# Architecture: Realtime Fraud Detection System

## Overview

This is a **decoupled, microservices-based** fraud detection system that separates concerns into specialized services:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Incoming Transactions (Kafka)                   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
   ┌─────────┐      ┌──────────┐      ┌──────────────┐
   │ Producer│      │  Spark   │      │  Batch ML    │
   │         │      │ Streaming│      │  (Airflow)   │
   └────┬────┘      └────┬─────┘      └──────┬───────┘
        │                │                    │
        │ Enrich &       │ Process &          │ Train &
        │ Feature        │ Feature            │ Validate
        │                │                    │
        │                │        ┌───────────┘
        │                │        │
        └────────┬───────┴────────┴────────────┐
                 │                             │
                 ▼                             ▼
           ┌─────────────┐          ┌──────────────────┐
           │   ML Model  │          │ Model Registry   │
           │ Scoring API │          │ (Version Control)│
           └──────┬──────┘          └──────────────────┘
                  │
                  │ Predictions
                  ▼
           ┌──────────────┐
           │   Alerting   │
           │   (Webhooks) │
           └──────────────┘
                  │
                  ▼
           ┌──────────────┐
           │ Prometheus   │
           │ + Grafana    │
           └──────────────┘
```

---

## Services

### 1. **ml/** — Model Training & Batch Scoring
- **Purpose**: Train fraud detection models offline, save them, and provide a real-time scoring API
- **Tech Stack**: XGBoost, Flask, Prometheus
- **Key Files**:
  - `src/train_model.py` — Trains on full dataset, saves best model
  - `src/scoring_service.py` — Flask API (/score, /enrich_and_score endpoints)
  - `src/features.py` — **Canonical feature engineering** (shared with all services)
  - `models/` — Saved XGBoost models + metadata

### 2. **producer/** — Event Enrichment & Streaming
- **Purpose**: Consume raw transactions, enrich with features, publish to Kafka
- **Tech Stack**: Kafka (Consumer/Producer), Python
- **Imports from**: `ml.src.features` (ensures feature consistency)
- **Outputs**: Features to Kafka topic (consumed by Spark, alerts)

### 3. **spark/** — Real-Time Stream Processing
- **Purpose**: Process transaction streams, compute aggregated risk features, trigger alerts
- **Tech Stack**: Apache Spark, Kafka
- **Imports from**: `ml.src.features` (same transformations as training)
- **Outputs**: Risk scores to Redis, low-latency alerts

### 4. **airflow/** — Workflow Orchestration
- **Purpose**: Schedule model retraining, data pipelines, metric exports
- **Tech Stack**: Apache Airflow
- **Key DAGs**:
  - `fraud_detection.py` — Retrains model daily/weekly using `train_model.py`

### 5. **prometheus/** — Metrics Collection
- **Purpose**: Scrape metrics from all services
- **Metrics**:
  - `fraud_predictions_total` — Count of predictions by result
  - `fraud_prediction_latency_seconds` — P50, P95, P99 scoring latency
  - `fraud_probability_distribution` — Histogram of fraud scores

### 6. **grafana/** — Real-Time Dashboards
- **Purpose**: Visualize fraud rates, detection latency, model performance
- **Dashboards**:
  - Overview: Fraud rate trends, alert volume
  - Model: Feature importance, threshold decisions
  - Operations: Service health, API latency

---

## Critical Design Pattern: Shared Feature Engineering

**The biggest risk in production fraud detection is train-serving skew:** the features used to train a model differ from those computed in production.

### Solution: Single Source of Truth

```
ml/src/features.py
    ↑
    ├─ Imported by ml.src.train_model.py (training)
    ├─ Imported by ml.src.scoring_service.py (real-time scoring)
    ├─ Imported by producer (stream enrichment)
    └─ Imported by spark (stream processing)
```

**When you change feature logic:**
1. Update `ml/src/features.py`
2. All consumers automatically use the new features
3. Document in `docs/FEATURES.md`

---

## Data Flow: A Transaction's Journey

### 1. **Batch Training** (daily/weekly via Airflow)
```
data/raw/fraudTrain.csv
    ↓
train_model.py
    ├─ engineer_features() from ml.src.features
    ├─ Train XGBoost model with stratified CV
    ├─ Evaluate on held-out test set
    └─ Save model + metadata to ml/models/
```

### 2. **Real-Time Scoring** (per transaction)
```
Raw Transaction (Kafka)
    ↓
Producer
    ├─ Enrich with cardholder profile (from Redis)
    └─ Call engineer_features() from ml.src.features
    └─ Publish enriched features to Kafka
    
Enriched Features
    ↓
Spark Streaming
    ├─ Compute aggregated risk features
    ├─ Call engineer_features() from ml.src.features
    └─ Publish to Redis (fast lookups)
    
    
Engineered Features
    ↓
ML Scoring Service (/score endpoint)
    ├─ Load XGBoost model + best threshold from metadata
    ├─ Predict fraud_probability
    └─ Return decision + score
    
Decision
    ↓
Alerting / Routing
    ├─ If is_fraud → Block transaction / Send alert
    └─ Log metrics to Prometheus
```

---

## Configuration

All services use environment variables (`.env`). See `.env.example` for complete list.

**Key variables:**
- `FRAUD_THRESHOLD` — Decision boundary (0.7 = alert if P(fraud) > 70%)
- `LOG_LEVEL` — INFO, DEBUG, WARNING
- `KAFKA_BROKER` — Kafka endpoint
- `REDIS_HOST/PORT` — Feature cache location
- Model paths, hyperparameters, etc.

---

## Deployment

### Local Development
```bash
cp .env.example .env
docker-compose up
```

### Production
- Use **Kubernetes** manifests (not yet included)
- Separate **training cluster** from **serving cluster**
- Mount volumes for model persistence
- Use **cloud-native secrets** (AWS Secrets Manager, GCP Secret Manager, etc.)

---

## Monitoring & Observability

### Prometheus Scrapes
- `ml:5001/metrics` — Scoring service
- `spark:8080/metrics` — Spark executor metrics (if configured)
- `prometheus:9090/metrics` — Prometheus self-metrics

### Alerting Rules (in `prometheus/prometheus.yml`)
```yaml
- alert: HighFraudRate
  expr: rate(fraud_predictions_total{result="fraud"}[5m]) > 0.1
  for: 5m
  
- alert: ScoringLatencyHigh
  expr: histogram_quantile(0.95, fraud_prediction_latency_seconds) > 0.1
```

---

## Future Improvements

1. **Feature Store** — Centralized storage for training/inference features (e.g., Feast, Tecton)
2. **A/B Testing** — Run two models in parallel, compare metrics
3. **Explanability** — SHAP values for feature importance per transaction
4. **Online Learning** — Update model incrementally on new fraud patterns
5. **Data Drift Detection** — Monitor feature distributions for distribution shift
6. **Shadow Mode** — Score with new model, don't alert, measure performance first

---

**Created**: April 2024  
**Last Updated**: April 5, 2026
