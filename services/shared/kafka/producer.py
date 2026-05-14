import json

from confluent_kafka import Producer
from .config import KAFKA_BOOTSTRAP_SERVERS

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "haaede-producer"
})

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ Delivered to "
            f"{msg.topic()} [{msg.partition()}]"
        )

def stream_to_kafka(topic, data_chunk):

    for row in data_chunk:
        payload = json.dumps(row)

        producer.produce(
            topic,
            value=payload,
            callback=delivery_report
        )

    producer.flush()