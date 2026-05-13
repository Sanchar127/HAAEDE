FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Use /app as the workspace
WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 1. Copy requirements from the specific service path
COPY services/ingestion/app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 2. CREATE the 'app' directory inside /app and copy the service code there
# This ensures that 'import app' works
COPY services/ingestion/app/ /app/app/

# 3. Set PYTHONPATH to /app so it can see the 'app' package
ENV PYTHONPATH=/app

# 4. Run using the module flag (-m) to handle package imports correctly
CMD ["python", "-m", "app.run"]