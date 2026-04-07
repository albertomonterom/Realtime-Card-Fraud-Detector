"""
Fraud Sentinel — Real-time dashboard backend.
Serves the HTML dashboard and JSON API endpoints backed by PostgreSQL.
"""
import os
from decimal import Decimal
from datetime import datetime

import psycopg2
import psycopg2.extras
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI()

DB = dict(
    host=os.getenv("DB_HOST", "postgres"),
    database=os.getenv("DB_NAME", "fraud_detection"),
    user=os.getenv("DB_USER", "fraud_user"),
    password=os.getenv("DB_PASSWORD", "fraud_password"),
)


def query(sql: str, params=None) -> list[dict]:
    with psycopg2.connect(**DB) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    return [_serialize(r) for r in rows]


def _serialize(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


@app.get("/api/stats")
def stats():
    return query("""
        SELECT
            COUNT(*)                                                          AS total,
            COUNT(*) FILTER (WHERE is_fraud_predicted)                        AS fraud_count,
            ROUND(COUNT(*) FILTER (WHERE is_fraud_predicted) * 100.0
                  / NULLIF(COUNT(*), 0), 2)                                   AS fraud_rate,
            ROUND(AVG(amt)::numeric, 2)                                       AS avg_amt,
            COUNT(*) FILTER (WHERE scored_at > NOW() - INTERVAL '1 minute')  AS tpm
        FROM scored_transactions
    """)[0]


@app.get("/api/transactions")
def transactions(limit: int = 28):
    rows = query("""
        SELECT transaction_id, cc_num, merchant, category, amt,
               city, state, fraud_probability, is_fraud_predicted, scored_at
        FROM scored_transactions
        ORDER BY scored_at DESC
        LIMIT %s
    """, (limit,))
    for r in rows:
        r["cc_num"] = f"****{str(r['cc_num'])[-4:]}"
        r["merchant"] = r["merchant"].replace("fraud_", "") if r["merchant"] else "—"
    return rows


@app.get("/api/alerts")
def alerts(limit: int = 7):
    rows = query("""
        SELECT transaction_id, cc_num, merchant, category, amt,
               city, state, fraud_probability, scored_at
        FROM scored_transactions
        WHERE is_fraud_predicted = TRUE
        ORDER BY scored_at DESC
        LIMIT %s
    """, (limit,))
    for r in rows:
        r["cc_num"] = f"****{str(r['cc_num'])[-4:]}"
        r["merchant"] = r["merchant"].replace("fraud_", "") if r["merchant"] else "—"
    return rows


@app.get("/api/chart")
def chart():
    return query("""
        SELECT
            to_char(date_trunc('minute', scored_at), 'HH24:MI')  AS label,
            COUNT(*)                                               AS total,
            COUNT(*) FILTER (WHERE is_fraud_predicted)            AS fraud
        FROM scored_transactions
        WHERE scored_at > NOW() - INTERVAL '30 minutes'
        GROUP BY date_trunc('minute', scored_at)
        ORDER BY date_trunc('minute', scored_at)
    """)


@app.get("/api/categories")
def categories():
    return query("""
        SELECT
            category,
            COUNT(*) FILTER (WHERE is_fraud_predicted)                     AS fraud_count,
            ROUND(COUNT(*) FILTER (WHERE is_fraud_predicted) * 100.0
                  / NULLIF(COUNT(*), 0), 1)                                AS fraud_rate
        FROM scored_transactions
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY fraud_count DESC
        LIMIT 7
    """)


@app.get("/", response_class=HTMLResponse)
def dashboard():
    with open("templates/index.html") as f:
        return f.read()
