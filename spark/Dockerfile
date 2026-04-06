FROM python:3.11-slim

WORKDIR /opt/app

# Install Java for Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

ENV PYTHONUNBUFFERED=1

# Run Spark processor
CMD ["python", "-m", "src.processor"]
