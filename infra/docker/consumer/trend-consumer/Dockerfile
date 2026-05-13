FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY services/consumers/trend_consumer/app/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy ENTIRE services folder
# Needed because consumer imports shared modules
COPY services /app/services

ENV PYTHONPATH=/app

CMD ["python", "-m", "services.consumers.trend_consumer.app.run"]