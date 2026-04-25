import json
import asyncio
from aiokafka import AIOKafkaConsumer
from core.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_AUTO_OFFSET_RESET,
)

class KafkaConsumerService:
    def __init__(self):
        self.consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
            enable_auto_commit=True
        )

    async def start(self):
        await self.consumer.start()
        print("✅ Kafka consumer started")

    async def stop(self):
        await self.consumer.stop()
        print("🛑 Kafka consumer stopped")

    async def consume(self, handler):
        """
        handler: async function(event_dict)
        """
        try:
            async for msg in self.consumer:
                try:
                    event = json.loads(msg.value)

                    # Debug log (keep minimal in prod)
                    print(
                        f"📥 Event received | partition={msg.partition} offset={msg.offset}"
                    )

                    await handler(event)

                except json.JSONDecodeError:
                    print("❌ Failed to decode message")

                except Exception as e:
                    print(f"❌ Error processing event: {e}")

        except asyncio.CancelledError:
            print("⚠️ Consumer cancelled")

        finally:
            await self.stop()