FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/load_to_kafka.py .

# -u: логи без буферизации
ENTRYPOINT ["python", "-u", "load_to_kafka.py"]
