import os
import json
from confluent_kafka import Producer  # Added this import

from app.log.logger import get_logger

logger = get_logger("kafka-producer")

kafka_server = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

if not kafka_server:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS not set")

logger.info(f"Connecting to Kafka at {kafka_server}")

# Initialize the Producer
producer = Producer({
    "bootstrap.servers": kafka_server,
    "client.id": "pulse-scope-producer"
})


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [{msg.partition()}]")


def stream_to_kafka(topic, data_chunk):
    # If using Pandas DataFrame
    if hasattr(data_chunk, "iterrows"):
        iterator = data_chunk.iterrows()
        for _, row in iterator:
            payload = row.to_json()
            producer.produce(topic, value=payload, callback=delivery_report)
    # If using standard list/dict
    else:
        for row in data_chunk:
            payload = json.dumps(row)
            producer.produce(topic, value=payload, callback=delivery_report)

    producer.flush()