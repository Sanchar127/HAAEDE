import json

from services.shared.kafka.consumer import create_consumer
from services.shared.logger.logger import get_logger

from processor import process_event

logger = get_logger("trend-consumer")

consumer = create_consumer(
    "trend-consumer-group"
)

def start_worker():

    logger.info(
        "🚀 Trend Consumer Worker Started"
    )

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            logger.error(msg.error())
            continue

        try:
            event = json.loads(
                msg.value().decode("utf-8")
            )

            result = process_event(event)

            logger.info(
                f"📈 Trend Update: {result}"
            )

        except Exception as e:
            logger.error(
                f"Processing Error: {e}"
            )