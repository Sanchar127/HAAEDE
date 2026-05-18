import asyncio
import json
from aiokafka import AIOKafkaConsumer
from app.processors.feature_builder import build_features
from app.store.feature_store import save_features

consumer = AIOKafkaConsumer(
    "fintech.debt.raw",
    bootstrap_servers="recovery-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
    group_id="feature-processor"
)

async def run():
    await consumer.start()

    try:
        async for msg in consumer:
            event = json.loads(msg.value)

            features = await build_features(event)

            await save_features(features)

    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(run())