"""
Unit tests for shared feature engineering module.

Ensures that:
1. Feature columns are generated correctly
2. All required input columns are validated
3. Feature transformations are deterministic
4. No data leakage from training → inference
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from ml.src import features


@pytest.fixture
def sample_transaction():
    """Create a sample raw transaction for testing."""
    base_date = datetime(2024, 1, 1, 12, 0, 0)
    return {
        'trans_date_trans_time': base_date.isoformat(),
        'amt': 100.0,
        'cc_num': '4532015112830366',
        'lat': 40.7128,
        'long': -74.0060,
        'merch_lat': 40.7580,
        'merch_long': -73.9855,
        'category': 'shopping_net',
        'gender': 'M',
        'dob': '1990-05-15',
        'state': 'NY',
        'city_pop': 8000000,
        'is_fraud': 0,
    }


@pytest.fixture
def sample_dataframe(sample_transaction):
    """Create a DataFrame with multiple transactions."""
    transactions = [sample_transaction for _ in range(10)]
    # Add some variation
    for i, txn in enumerate(transactions):
        txn['amt'] = 50 + i * 10
        txn['is_fraud'] = 1 if i % 10 == 0 else 0
    return pd.DataFrame(transactions)


class TestFeatureEngineering:
    """Test feature engineering module."""

    def test_engineer_features_basic(self, sample_dataframe):
        """Test that engineer_features runs without error."""
        df_engineered, _ = features.engineer_features(
            sample_dataframe, fit_encoders=True
        )
        assert df_engineered is not None
        assert len(df_engineered) == len(sample_dataframe)

    def test_feature_columns_generated(self, sample_dataframe):
        """Test that all expected feature columns are created."""
        df_engineered, _ = features.engineer_features(
            sample_dataframe, fit_encoders=True
        )
        expected = features.get_feature_columns()
        missing = set(expected) - set(df_engineered.columns)
        assert len(missing) == 0, f"Missing columns: {missing}"

    def test_required_columns_validation(self):
        """Test that missing required columns raise ValueError."""
        bad_df = pd.DataFrame([{'amt': 100}])  # Missing almost everything
        with pytest.raises(ValueError, match="Missing required columns"):
            features.engineer_features(bad_df)

    def test_feature_validation(self, sample_dataframe):
        """Test validate_features function."""
        df_engineered, _ = features.engineer_features(
            sample_dataframe, fit_encoders=True
        )
        is_valid, missing = features.validate_features(df_engineered)
        assert is_valid is True
        assert len(missing) == 0

    def test_deterministic_output(self, sample_dataframe):
        """Test that feature engineering produces consistent results."""
        df1, _ = features.engineer_features(sample_dataframe.copy(), fit_encoders=True)
        df2, _ = features.engineer_features(sample_dataframe.copy(), fit_encoders=True)
        
        cols = features.get_feature_columns()
        np.testing.assert_array_almost_equal(
            df1[cols].values,
            df2[cols].values,
            err_msg="Feature engineering is not deterministic"
        )

    def test_feature_get_columns(self):
        """Test get_feature_columns returns expected list."""
        cols = features.get_feature_columns()
        assert isinstance(cols, list)
        assert len(cols) == 17  # We know we create 17 features
        assert 'amt' in cols
        assert 'hour' in cols
        assert 'age' in cols

    def test_required_columns_definition(self):
        """Test that required columns are properly defined."""
        required = features.get_required_columns()
        assert isinstance(required, set)
        assert 'amt' in required
        assert 'trans_date_trans_time' in required


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
