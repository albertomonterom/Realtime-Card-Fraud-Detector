"""
Airflow DAG — Weekly Model Retraining Pipeline

Schedule: Every Sunday at 3 AM
Steps:
  1. Export recent labeled data from PostgreSQL
  2. Run data quality checks
  3. Retrain XGBoost model
  4. Evaluate against current champion
  5. Promote if improved
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'fraud-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['team@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='fraud_model_retraining',
    default_args=default_args,
    description='Weekly retraining of the fraud detection XGBoost model',
    schedule_interval='0 3 * * 0',  # Every Sunday 3 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'fraud', 'retraining'],
)


def check_data_quality(**context):
    """Verify we have enough new labeled data to retrain."""
    import psycopg2

    conn = psycopg2.connect(
        host='postgres', port=5432, database='fraud_detection',
        user='fraud_user', password='fraud_pass'
    )
    cur = conn.cursor()

    # Check total rows since last training
    cur.execute("""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE is_fraud_actual = TRUE),
               COUNT(*) FILTER (WHERE is_fraud_actual IS NOT NULL)
        FROM scored_transactions
        WHERE scored_at > NOW() - INTERVAL '7 days'
    """)
    total, fraud_count, labeled = cur.fetchone()
    conn.close()

    context['ti'].xcom_push(key='total_new', value=total)
    context['ti'].xcom_push(key='fraud_new', value=fraud_count)
    context['ti'].xcom_push(key='labeled_new', value=labeled)

    if labeled < 1000:
        raise ValueError(
            f"Not enough labeled data for retraining: {labeled} rows (need 1000+)"
        )

    print(f"Data quality check passed: {total} total, {fraud_count} fraud, {labeled} labeled")


def export_training_data(**context):
    """Export labeled transactions to CSV for training."""
    import psycopg2
    import csv

    conn = psycopg2.connect(
        host='postgres', port=5432, database='fraud_detection',
        user='fraud_user', password='fraud_pass'
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT cc_num, merchant, category, amt, city, state, lat, long,
               hour_of_day, day_of_week, geo_velocity_kmh, is_fraud_actual,
               trans_time, amt_deviation, txn_count_5min, avg_amt_30day
        FROM scored_transactions
        WHERE is_fraud_actual IS NOT NULL
        ORDER BY trans_time DESC
        LIMIT 500000
    """)

    output_path = '/tmp/retrain_data.csv'
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in cur.description])
        writer.writerows(cur.fetchall())

    conn.close()
    context['ti'].xcom_push(key='data_path', value=output_path)
    print(f"Exported training data to {output_path}")


def train_new_model(**context):
    """Retrain the model and save metrics."""
    import json
    import subprocess

    data_path = context['ti'].xcom_pull(key='data_path')

    result = subprocess.run(
        ['python', '/opt/ml/train_model.py',
         '--data', data_path,
         '--output', '/tmp/new_model'],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"Training failed: {result.stderr}")

    # Read new model metrics
    with open('/tmp/new_model/model_metadata.json') as f:
        new_metrics = json.load(f)

    context['ti'].xcom_push(key='new_roc_auc', value=new_metrics['roc_auc'])
    context['ti'].xcom_push(key='new_f1', value=new_metrics['f1'])
    context['ti'].xcom_push(key='new_version', value=new_metrics['model_version'])

    print(f"New model: ROC AUC={new_metrics['roc_auc']}, F1={new_metrics['f1']}")


def decide_promotion(**context):
    """Compare new model vs champion and decide whether to promote."""
    import json

    new_auc = context['ti'].xcom_pull(key='new_roc_auc')

    # Load current champion metrics
    try:
        with open('/opt/models/model_metadata.json') as f:
            current = json.load(f)
        current_auc = current.get('roc_auc', 0)
    except FileNotFoundError:
        current_auc = 0

    print(f"Champion AUC: {current_auc} | Challenger AUC: {new_auc}")

    if new_auc > current_auc:
        return 'promote_model'
    else:
        return 'skip_promotion'


def promote_model(**context):
    """Copy new model to production path and log to DB."""
    import shutil
    import psycopg2

    shutil.copy('/tmp/new_model/fraud_model.json', '/opt/models/fraud_model.json')
    shutil.copy('/tmp/new_model/model_metadata.json', '/opt/models/model_metadata.json')

    new_version = context['ti'].xcom_pull(key='new_version')
    new_auc = context['ti'].xcom_pull(key='new_roc_auc')
    new_f1 = context['ti'].xcom_pull(key='new_f1')

    conn = psycopg2.connect(
        host='postgres', port=5432, database='fraud_detection',
        user='fraud_user', password='fraud_pass'
    )
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO model_metrics (model_version, roc_auc, f1_score, notes)
        VALUES (%s, %s, %s, 'Auto-promoted by Airflow')
    """, (new_version, new_auc, new_f1))
    conn.commit()
    conn.close()

    print(f"Model {new_version} promoted to production!")


def skip_promotion(**context):
    print("New model did not outperform champion — skipping promotion.")


# ── DAG Tasks ─────────────────────────────────────────────────
t_check = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)

t_export = PythonOperator(
    task_id='export_training_data',
    python_callable=export_training_data,
    dag=dag,
)

t_train = PythonOperator(
    task_id='train_new_model',
    python_callable=train_new_model,
    dag=dag,
)

t_decide = BranchPythonOperator(
    task_id='decide_promotion',
    python_callable=decide_promotion,
    dag=dag,
)

t_promote = PythonOperator(
    task_id='promote_model',
    python_callable=promote_model,
    dag=dag,
)

t_skip = PythonOperator(
    task_id='skip_promotion',
    python_callable=skip_promotion,
    dag=dag,
)

t_notify = BashOperator(
    task_id='notify_complete',
    bash_command='echo "Retraining pipeline complete at $(date)"',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

# ── Task Dependencies ────────────────────────────────────────
t_check >> t_export >> t_train >> t_decide
t_decide >> [t_promote, t_skip]
[t_promote, t_skip] >> t_notify
