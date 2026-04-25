import os

# =========================
# Kafka Config
# =========================
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "recovery-events"
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "feature-processor-group"
)

KAFKA_AUTO_OFFSET_RESET = os.getenv(
    "KAFKA_AUTO_OFFSET_RESET",
    "earliest"  # or latest in prod depending on strategy
)

# =========================
# Postgres Config
# =========================
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://postgres:postgres@localhost:5432/recovery"
)

# =========================
# Feature Settings
# =========================
FEATURE_WINDOW_DAYS = int(
    os.getenv("FEATURE_WINDOW_DAYS", "30")
)

# =========================
# Logging
# =========================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")