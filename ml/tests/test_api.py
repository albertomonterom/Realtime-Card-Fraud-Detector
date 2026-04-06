#!/usr/bin/env python
"""Test the scoring service API endpoints."""

import requests
import json

BASE_URL = "http://127.0.0.1:5001"

# Test 1: Health check
print("\nTEST 1: /health endpoint")
try:
    resp = requests.get(f"{BASE_URL}/health")
    print(f"Status: {resp.status_code}")
    print(json.dumps(resp.json(), indent=2))
except Exception as e:
    print(f"ERROR: {e}")

# Test 2: /score - LEGITIMATE transaction
print("\nTEST 2: /score - LEGITIMATE transaction")
legit_features = {
    "amt": 45.67, "hour": 14, "day_of_week": 2, "month": 4, "is_weekend": 0, "is_night": 0,
    "card_avg_amt": 75.50, "card_std_amt": 25.30, "amt_deviation": -1.18, "amt_to_avg_ratio": 0.605,
    "geo_distance": 0.5, "card_txn_count": 45, "category_encoded": 3, "state_encoded": 5,
    "gender_encoded": 1, "age": 35.5, "city_pop_log": 12.3,
}
try:
    resp = requests.post(f"{BASE_URL}/score", json=legit_features)
    print(f"Status: {resp.status_code}")
    result = resp.json()
    print(json.dumps(result, indent=2))
    print(f"Prediction: {'FRAUD' if result['is_fraud_predicted'] else 'LEGITIMATE'}")
except Exception as e:
    print(f"ERROR: {e}")

# Test 3: /score - SUSPICIOUS transaction
print("\nTEST 3: /score - SUSPICIOUS transaction")
fraud_features = {
    "amt": 1299.99, "hour": 3, "day_of_week": 5, "month": 4, "is_weekend": 1, "is_night": 1,
    "card_avg_amt": 75.50, "card_std_amt": 25.30, "amt_deviation": 48.99, "amt_to_avg_ratio": 17.22,
    "geo_distance": 850.0, "card_txn_count": 3, "category_encoded": 7, "state_encoded": 2,
    "gender_encoded": 0, "age": 28.2, "city_pop_log": 10.1,
}
try:
    resp = requests.post(f"{BASE_URL}/score", json=fraud_features)
    print(f"Status: {resp.status_code}")
    result = resp.json()
    print(json.dumps(result, indent=2))
    print(f"Prediction: {'FRAUD' if result['is_fraud_predicted'] else 'LEGITIMATE'}")
except Exception as e:
    print(f"ERROR: {e}")

# Test 4: /enrich_and_score - RAW transaction
print("\nTEST 4: /enrich_and_score - RAW transaction")
raw_transaction = {
    "transaction_id": "txn_001", "trans_date_trans_time": "2023-04-06 14:30:00", "amt": 89.50,
    "cc_num": "4111111111111111", "lat": 40.7128, "long": -74.0060, "merch_lat": 40.7150,
    "merch_long": -74.0090, "category": "gas_transport", "gender": "M", "dob": "1988-05-15",
    "state": "NY", "city_pop": 8000000,
}
try:
    resp = requests.post(f"{BASE_URL}/enrich_and_score", json=raw_transaction)
    print(f"Status: {resp.status_code}")
    result = resp.json()
    print(f"Transaction ID: {result['transaction_id']}")
    print(f"Fraud Probability: {result['fraud_probability']:.5f}")
    print(f"Is Fraud: {result['is_fraud_predicted']}")
    print(f"Threshold: {result['threshold']}")
    print(f"Latency: {result['latency_ms']}ms")
    print(f"Prediction: {'FRAUD' if result['is_fraud_predicted'] else 'LEGITIMATE'}")
except Exception as e:
    print(f"ERROR: {e}")

# Test 5: /metrics
print("\nTEST 5: /metrics")
try:
    resp = requests.get(f"{BASE_URL}/metrics")
    print(f"Status: {resp.status_code}")
    print("Sample metrics (first 500 chars):")
    print(resp.text[:500])
except Exception as e:
    print(f"ERROR: {e}")

print("\nTEST COMPLETE")
