import asyncio
import json
from aiokafka import AIOKafkaConsumer
from app.processors.feature_builder import build_features
from app.store.feature_store import save_features

consumer = AIOKafkaConsumer(
    "recovery-events",
    bootstrap_servers="recovery-cluster-kafka-bootstrap.kafka:9092",
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