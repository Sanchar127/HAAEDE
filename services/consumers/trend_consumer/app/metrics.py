from prometheus_client import Counter, Histogram, Gauge

# =========================================================
# 📈 TREND CONSUMER METRICS
# =========================================================

TREND_EVENTS_PROCESSED_TOTAL = Counter(
    "trend_events_processed_total",
    "Total events processed by trend consumer",
    ["source"]
)

TREND_PROCESSING_ERRORS_TOTAL = Counter(
    "trend_processing_errors_total",
    "Total trend consumer processing errors"
)

TREND_PROCESSING_LATENCY_SECONDS = Histogram(
    "trend_processing_latency_seconds",
    "Time taken to process trend events"
)

TREND_SCORE_GENERATED_TOTAL = Counter(
    "trend_score_generated_total",
    "Total trend scores generated",
    ["source"]
)

TREND_LAST_EVENT_TIMESTAMP = Gauge(
    "trend_last_event_timestamp",
    "Timestamp of last processed trend event"
)