import json
from kafka import KafkaProducer
from app.core.config import KAFKA_BOOTSTRAP_SERVERS

class EventProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def send(self, topic: str, event: dict):
        self.producer.send(topic, event)
        self.producer.flush()