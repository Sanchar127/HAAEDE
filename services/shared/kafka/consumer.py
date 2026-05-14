from confluent_kafka import Consumer

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_NAME
)

def create_consumer(group_id: str):

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([TOPIC_NAME])

    print(
        f"✅ Consumer connected to "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    return consumer