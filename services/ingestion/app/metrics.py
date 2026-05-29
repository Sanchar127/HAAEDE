from prometheus_client import Counter, Histogram, Gauge

# =========================================================
# 📥 INGESTION METRICS
# =========================================================

INGESTED_EVENTS_TOTAL = Counter(
    "ingestion_events_collected_total",
    "Total number of events collected from sources",
    ["source"]
)

INGESTION_ERRORS_TOTAL = Counter(
    "ingestion_errors_total",
    "Total ingestion errors",
    ["source"]
)

INGESTION_LATENCY_SECONDS = Histogram(
    "ingestion_latency_seconds",
    "Time taken to fetch + normalize events",
    ["source"]
)

INGESTION_LOOP_DURATION_SECONDS = Histogram(
    "ingestion_loop_duration_seconds",
    "Duration of one ingestion cycle"
)

LAST_SUCCESS_TIMESTAMP = Gauge(
    "ingestion_last_success_timestamp",
    "Timestamp of last successful ingestion cycle"
)