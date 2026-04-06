-- ============================================================
-- Fraud Detection Pipeline — Database Schema
-- ============================================================

-- Scored transactions (main table)
CREATE TABLE IF NOT EXISTS scored_transactions (
    id                  BIGSERIAL PRIMARY KEY,
    transaction_id      VARCHAR(64) UNIQUE NOT NULL,
    cc_num              VARCHAR(20) NOT NULL,
    merchant            VARCHAR(255),
    category            VARCHAR(100),
    amt                 DECIMAL(12, 2) NOT NULL,
    city                VARCHAR(100),
    state               VARCHAR(10),
    lat                 DECIMAL(9, 6),
    long                DECIMAL(9, 6),
    trans_time          TIMESTAMP NOT NULL,

    -- Engineered features
    amt_deviation       DECIMAL(8, 4),       -- how many std devs from cardholder mean
    hour_of_day         SMALLINT,
    day_of_week         SMALLINT,
    geo_velocity_kmh    DECIMAL(10, 2),      -- km/h between this and previous txn
    txn_count_5min      SMALLINT,            -- rolling 5-min transaction count
    avg_amt_30day       DECIMAL(12, 2),      -- cardholder 30-day avg amount

    -- ML output
    fraud_probability   DECIMAL(6, 5) NOT NULL,
    is_fraud_predicted  BOOLEAN NOT NULL,
    is_fraud_actual     BOOLEAN,             -- ground truth (from dataset label)
    model_version       VARCHAR(50),

    -- Metadata
    scored_at           TIMESTAMP DEFAULT NOW(),
    processing_latency_ms INTEGER
);

-- Indexes for Grafana dashboard queries
CREATE INDEX idx_scored_trans_time ON scored_transactions(trans_time DESC);
CREATE INDEX idx_scored_fraud_pred ON scored_transactions(is_fraud_predicted) WHERE is_fraud_predicted = TRUE;
CREATE INDEX idx_scored_category   ON scored_transactions(category);
CREATE INDEX idx_scored_cc_num     ON scored_transactions(cc_num);

-- Model performance tracking (filled by Airflow)
CREATE TABLE IF NOT EXISTS model_metrics (
    id              SERIAL PRIMARY KEY,
    model_version   VARCHAR(50) NOT NULL,
    evaluated_at    TIMESTAMP DEFAULT NOW(),
    precision_score DECIMAL(6, 5),
    recall_score    DECIMAL(6, 5),
    f1_score        DECIMAL(6, 5),
    roc_auc         DECIMAL(6, 5),
    dataset_size    INTEGER,
    fraud_rate      DECIMAL(6, 5),
    notes           TEXT
);

-- Alerting rules config
CREATE TABLE IF NOT EXISTS alert_rules (
    id              SERIAL PRIMARY KEY,
    rule_name       VARCHAR(100) NOT NULL,
    condition_sql   TEXT NOT NULL,
    threshold       DECIMAL(6, 5),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Insert default alert rules
INSERT INTO alert_rules (rule_name, condition_sql, threshold) VALUES
    ('high_fraud_rate', 'SELECT COUNT(*) FILTER (WHERE is_fraud_predicted) * 1.0 / COUNT(*) FROM scored_transactions WHERE trans_time > NOW() - INTERVAL ''5 minutes''', 0.05),
    ('model_degradation', 'SELECT roc_auc FROM model_metrics ORDER BY evaluated_at DESC LIMIT 1', 0.85);
