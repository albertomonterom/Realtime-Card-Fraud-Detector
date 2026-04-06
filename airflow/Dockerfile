FROM python:3.11-slim

WORKDIR /opt/airflow

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs
COPY dags/ ./dags/

ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/opt/airflow

# Initialize Airflow database (in real deployment, use separate init command)
# RUN airflow db init

# Run Airflow webserver
CMD ["airflow", "webserver", "--port", "8080"]
