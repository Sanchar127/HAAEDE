import json

from services.shared.kafka.consumer import create_consumer
from services.shared.logger.logger import get_logger
from .processor import process_event

logger = get_logger("trend-consumer")


def start_worker():
    consumer = create_consumer("trend-consumer-group")

    logger.info("🚀 Trend Consumer Worker Started")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))

                result = process_event(event)

                logger.info(f"📈 Trend Update: {result}")

            except Exception as e:
                logger.error(f"Processing Error: {e}")

    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped manually")

    except Exception as e:
        logger.error(f"Fatal error: {e}")

    finally:
        consumer.close()
        logger.info("👋 Consumer closed safely")