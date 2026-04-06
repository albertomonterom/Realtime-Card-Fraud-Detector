# ML Scoring API Reference

## Base URL

```
http://localhost:5001  (development)
http://ml:5001         (docker network)
```

---

## Endpoints

### `GET /health`

Health check. Returns model status and version.

**Request:**
```bash
curl http://localhost:5001/health
```

**Response (200 OK):**
```json
{
  "status": "healthy",
  "model_loaded": true,
  "model_version": "v20260405_120000"
}
```

---

### `GET /metrics`

Prometheus metrics in OpenMetrics format. Scrapped by Prometheus.

**Request:**
```bash
curl http://localhost:5001/metrics
```

**Response (200 OK):**
```
# HELP fraud_predictions_total Total predictions made
# TYPE fraud_predictions_total counter
fraud_predictions_total{result="fraud"} 145.0
fraud_predictions_total{result="legit"} 9855.0
...
```

**Metrics exported:**
- `fraud_predictions_total{result=["fraud","legit","error"]}` — Counter of predictions by outcome
- `fraud_prediction_latency_seconds` — Histogram of P50, P95, P99 latency
- `fraud_probability_distribution` — Histogram of fraud scores (0-1)

---

### `POST /score`

Score a single transaction with pre-engineered features.

**Use this endpoint if you've already engineered features yourself.**

**Request:**
```bash
curl -X POST http://localhost:5001/score \
  -H "Content-Type: application/json" \
  -d '{
    "amt": 150.00,
    "hour": 14,
    "day_of_week": 2,
    "is_weekend": 0,
    "is_night": 0,
    "card_avg_amt": 75.50,
    "card_std_amt": 25.30,
    "amt_deviation": 2.95,
    "amt_to_avg_ratio": 1.99,
    "geo_distance": 12.5,
    "card_txn_count": 250,
    "category_encoded": 3,
    "gender_encoded": 1,
    "age": 42.5,
    "city_pop_log": 15.2
  }'
```

**Response (200 OK):**
```json
{
  "fraud_probability": 0.23456,
  "is_fraud_predicted": false,
  "threshold": 0.7,
  "model_version": "v20260405_120000",
  "latency_ms": 2.45
}
```

**Required fields:**
- All 15 feature columns (see `ml/src/features.py:FEATURE_COLUMNS`)

**Response fields:**
- `fraud_probability` (0-1) — P(fraud) from model
- `is_fraud_predicted` (bool) — fraud_probability ≥ threshold?
- `threshold` (float) — Decision boundary (from model metadata)
- `model_version` (str) — Version ID for audit trail
- `latency_ms` (float) — How long scoring took

**Error responses:**

```json
// 400 Bad Request
{
  "error": "No JSON payload"
}

// 500 Internal Server Error
{
  "error": "division by zero"
}
```

---

### `POST /enrich_and_score`

**Full pipeline**: receive raw transaction data → engineer features → score.

**Use this endpoint if you don't have pre-engineered features.**

This endpoint uses the shared `ml.src.features` module to transform your raw data, ensuring consistency with training.

**Request:**
```bash
curl -X POST http://localhost:5001/enrich_and_score \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "txn_20260405_001234",
    "trans_date_trans_time": "2026-01-15T14:30:00Z",
    "amt": 150.00,
    "cc_num": "4532015112830366",
    "lat": 40.7128,
    "long": -74.0060,
    "merch_lat": 40.7580,
    "merch_long": -73.9855,
    "category": "shopping_net",
    "gender": "M",
    "dob": "1990-05-15T00:00:00Z",
    "city_pop": 8000000,
    "is_fraud": null
  }'
```

**Response (200 OK):**
```json
{
  "transaction_id": "txn_20260405_001234",
  "fraud_probability": 0.15234,
  "is_fraud_predicted": false,
  "threshold": 0.7,
  "engineered_features": {
    "amt": 150.0,
    "hour": 14,
    "day_of_week": 2,
    "is_weekend": 0,
    "is_night": 0,
    "card_avg_amt": 75.5,
    "card_std_amt": 25.3,
    "amt_deviation": 2.95,
    "amt_to_avg_ratio": 1.99,
    "geo_distance": 12.5,
    "card_txn_count": 250,
    "category_encoded": 3,
    "gender_encoded": 1,
    "age": 35.67,
    "city_pop_log": 15.2
  },
  "model_version": "v20260405_120000",
  "latency_ms": 15.32
}
```

**Required fields (raw transaction data):**
- `trans_date_trans_time` (ISO datetime string)
- `amt` (float)
- `cc_num` (str, card number)
- `lat`, `long` (float, cardholder location)
- `merch_lat`, `merch_long` (float, merchant location)
- `category` (str)
- `gender` (str, 'M' or 'F')
- `dob` (ISO date string)
- `city_pop` (int)

**Optional fields:**
- `transaction_id` — Included in response for tracing

**Response fields:**
- `engineered_features` — All 15 features (for debugging/logging)
- `fraud_probability`, `is_fraud_predicted`, `latency_ms` — Same as `/score`

---

## Examples

### Python Client

```python
import requests
import json
from datetime import datetime

SCORER_URL = "http://localhost:5001"

# Pre-engineered features
def score_features(features_dict):
    response = requests.post(
        f"{SCORER_URL}/score",
        json=features_dict,
        timeout=1.0
    )
    return response.json()

# Raw transaction
def enrich_and_score(raw_txn):
    response = requests.post(
        f"{SCORER_URL}/enrich_and_score",
        json=raw_txn,
        timeout=2.0
    )
    return response.json()

# Example usage
raw_txn = {
    "transaction_id": "txn_001",
    "trans_date_trans_time": datetime.utcnow().isoformat() + "Z",
    "amt": 250.00,
    "cc_num": "4532015112830366",
    "lat": 40.7128,
    "long": -74.0060,
    "merch_lat": 40.7580,
    "merch_long": -73.9855,
    "category": "shopping_online",
    "gender": "F",
    "dob": "1985-03-20",
    "city_pop": 8000000,
}

result = enrich_and_score(raw_txn)
print(f"Fraud probability: {result['fraud_probability']:.4f}")
if result['is_fraud_predicted']:
    print("⚠️  FRAUD ALERT!")
```

### cURL Examples

```bash
# Health check
curl -s http://localhost:5001/health | jq .

# Score with pre-engineered features (faster)
curl -X POST http://localhost:5001/score \
  -H "Content-Type: application/json" \
  -d @features.json

# Full pipeline (raw transaction)
curl -X POST http://localhost:5001/enrich_and_score \
  -H "Content-Type: application/json" \
  -d @transaction.json | jq '.fraud_probability'

# Prometheus metrics (for monitoring)
curl http://localhost:5001/metrics | grep fraud_predictions_total
```

---

## Performance & SLAs

| Endpoint | P50 Latency | P99 Latency | Target |
|----------|-------------|-------------|--------|
| `/score` | 2-5 ms | <10 ms | <5ms |
| `/enrich_and_score` | 10-15 ms | <30 ms | <20ms |
| `/health` | <1 ms | <2 ms | <1ms |

---

## Error Handling

All errors return JSON with an `error` field:

```json
{
  "error": "Missing required columns: ['amt', 'hour']"
}
```

**Common errors:**
- `400 Bad Request` — Malformed JSON, missing required fields
- `500 Internal Server Error` — Feature engineering failed, model error

---

## Integration Patterns

### Pattern 1: Batch Scoring
```python
import pandas as pd
import requests

df = pd.read_csv("transactions.csv")
scores = []

for _, row in df.iterrows():
    result = requests.post(
        "http://localhost:5001/enrich_and_score",
        json=row.to_dict(),
        timeout=2.0
    )
    scores.append(result.json()["fraud_probability"])

df["fraud_score"] = scores
```

### Pattern 2: Real-Time Kafka Stream
```python
from kafka import KafkaConsumer, KafkaProducer
import requests
import json

consumer = KafkaConsumer("transactions", bootstrap_servers=["localhost:9092"])
producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

for msg in consumer:
    txn = json.loads(msg.value)
    result = requests.post(
        "http://localhost:5001/enrich_and_score",
        json=txn
    ).json()
    
    if result["is_fraud_predicted"]:
        producer.send("fraud_alerts", json.dumps(result).encode())
```

---

**API Version**: 1.0  
**Last Updated**: April 5, 2026
