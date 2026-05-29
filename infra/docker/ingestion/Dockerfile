
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# =========================================================
# WORKDIR
# =========================================================

WORKDIR /app

# =========================================================
# SYSTEM DEPENDENCIES
# =========================================================

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# =========================================================
# INSTALL PYTHON DEPENDENCIES
# =========================================================

COPY services/ingestion/app/requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# =========================================================
# COPY ENTIRE SERVICES DIRECTORY
# =========================================================

COPY services /app/services

# =========================================================
# PYTHON PATH
# =========================================================

ENV PYTHONPATH=/app

# =========================================================
# START APPLICATION
# =========================================================

CMD ["python", "-m", "services.ingestion.app.run"]
