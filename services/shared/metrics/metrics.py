from prometheus_client import Counter, Histogram, Gauge

# =========================================================
# 📊 KAFKA PRODUCER METRICS
# =========================================================

KAFKA_MESSAGES_PRODUCED_TOTAL = Counter(
    "kafka_messages_produced_total",
    "Total messages successfully produced to Kafka",
    ["topic", "source"]
)

KAFKA_PRODUCE_ERRORS_TOTAL = Counter(
    "kafka_produce_errors_total",
    "Total Kafka produce failures",
    ["topic"]
)

KAFKA_PRODUCE_LATENCY_SECONDS = Histogram(
    "kafka_produce_latency_seconds",
    "Time taken to publish message to Kafka",
    ["topic"]
)

# =========================================================
# 📊 CONSUMER METRICS
# =========================================================

CONSUMED_MESSAGES_TOTAL = Counter(
    "consumer_messages_consumed_total",
    "Total Kafka messages consumed",
    ["topic", "source"]
)

CONSUMER_ERRORS_TOTAL = Counter(
    "consumer_errors_total",
    "Total consumer processing errors"
)

CONSUMER_PROCESSING_LATENCY = Histogram(
    "consumer_processing_latency_seconds",
    "Time taken to process consumed events"
)

CONSUMER_LAST_CONSUMED_TIMESTAMP = Gauge(
    "consumer_last_consumed_timestamp",
    "Timestamp of last successfully consumed message"
)

# =========================================================
# 📊 PIPELINE HEALTH METRICS
# =========================================================

PIPELINE_SUCCESS_RATIO = Gauge(
    "pipeline_success_ratio",
    "Ratio of successful pipeline executions"
)

PIPELINE_LAST_RUN_TIMESTAMP = Gauge(
    "pipeline_last_run_timestamp",
    "Last successful pipeline execution timestamp"
)