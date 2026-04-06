FROM python:3.10-slim
WORKDIR /opt/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scoring_service.py .
COPY train_model.py .
CMD ["python", "-u", "scoring_service.py"]
