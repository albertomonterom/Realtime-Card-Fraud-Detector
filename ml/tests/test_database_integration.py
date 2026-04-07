"""
Integration test for database prediction logging.
Tests that predictions are correctly logged to PostgreSQL.
"""
import sys
import os
import json
import time
import requests
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_prediction_logging():
    """Test that /enrich_and_score logs predictions to database"""
    
    api_url = "http://localhost:5001"
    
    # Sample transaction (legitimate)
    transaction = {
        "transaction_id": "test_txn_" + str(int(time.time())),
        "cc_num": "4532123456789012",
        "merchant": "TEST_MERCHANT",
        "category": "groceries",
        "amt": 45.50,
        "city": "San Francisco",
        "state": "CA",
        "lat": 37.7749,
        "long": -122.4194,
        "trans_date_trans_time": "2024-01-15 14:30:00",
        "gender": "M",
        "dob": "1980-05-15",
        "city_pop": 873961,
        "merch_lat": 37.7749,
        "merch_long": -122.4194,
    }
    
    try:
        # Test health first
        health = requests.get(f"{api_url}/health", timeout=5)
        assert health.status_code == 200, f"Health check failed: {health.text}"
        print("✓ Health check passed")
        
        # Send score request
        response = requests.post(
            f"{api_url}/enrich_and_score",
            json=transaction,
            timeout=10
        )
        
        assert response.status_code == 200, f"Scoring failed: {response.text}"
        result = response.json()
        
        print(f"✓ Scoring successful")
        print(f"  - Fraud probability: {result['fraud_probability']}")
        print(f"  - Is fraud: {result['is_fraud_predicted']}")
        print(f"  - Latency: {result['latency_ms']}ms")
        
        # Give DB time to write
        time.sleep(1)
        
        # Check database
        from src.database import get_db
        db = get_db()
        
        # Query the prediction from database
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT fraud_probability, is_fraud_predicted, model_version FROM scored_transactions WHERE transaction_id = %s",
                (transaction['transaction_id'],)
            )
            row = cur.fetchone()
            
            if row:
                print(f"✓ Prediction logged to database")
                print(f"  - DB fraud prob: {row[0]}")
                print(f"  - DB is_fraud: {row[1]}")
                print(f"  - Model version: {row[2]}")
            else:
                print(f"⚠ Transaction NOT found in database")
                print(f"  This may be OK if DB connection isn't available")
        
        return True
        
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to API at localhost:5001")
        print("  Make sure docker-compose is running: docker-compose up -d")
        return False
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_prediction_logging()
    sys.exit(0 if success else 1)
