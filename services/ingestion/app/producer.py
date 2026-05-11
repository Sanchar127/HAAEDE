import os
import json
from confluent_kafka import Producer

kafka_server = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

if not kafka_server:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS not set")

print(f"DEBUG: Connecting to Kafka at: {kafka_server}")

producer = Producer({
    "bootstrap.servers": kafka_server,
    "client.id": "debt-recovery-ingestor"
})


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")


def stream_to_kafka(topic, data_chunk):
    """
    Accepts pandas DataFrame or list of dicts
    """

    # Case 1: DataFrame
    if hasattr(data_chunk, "iterrows"):
        iterator = data_chunk.iterrows()
        for _, row in iterator:
            payload = row.to_json()
            producer.produce(topic, value=payload, callback=delivery_report)

    # Case 2: list of dicts
    else:
        for row in data_chunk:
            payload = json.dumps(row)
            producer.produce(topic, value=payload, callback=delivery_report)

    producer.flush()