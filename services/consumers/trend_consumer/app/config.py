import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

TOPIC_NAME = os.getenv(
    "TOPIC_NAME",
    "raw.events"
)

GROUP_ID = os.getenv(
    "GROUP_ID",
    "trend-consumer-group"
)