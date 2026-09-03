
import json
import time

from services.shared.kafka.consumer import create_consumer
from services.shared.logger.logger import get_logger
from .processor import process_event

# =========================================================
# 📊 METRICS
# =========================================================

from services.shared.metrics.metrics import (
    CONSUMED_MESSAGES_TOTAL,
    CONSUMER_ERRORS_TOTAL,
    CONSUMER_PROCESSING_LATENCY,
    CONSUMER_LAST_CONSUMED_TIMESTAMP
)

from .metrics import (
    TREND_EVENTS_PROCESSED_TOTAL,
    TREND_PROCESSING_ERRORS_TOTAL,
    TREND_PROCESSING_LATENCY_SECONDS,
    TREND_SCORE_GENERATED_TOTAL,
    TREND_LAST_EVENT_TIMESTAMP
)

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

                CONSUMER_ERRORS_TOTAL.inc()
                TREND_PROCESSING_ERRORS_TOTAL.inc()

                continue

            start_time = time.time()

            try:
                event = json.loads(
                    msg.value().decode("utf-8")
                )

                source = event.get("source", "unknown")

                result = process_event(event)

                # =================================================
                # 📊 SHARED CONSUMER METRICS
                # =================================================

                CONSUMED_MESSAGES_TOTAL.labels(
                    topic=msg.topic(),
                    source=source
                ).inc()

                CONSUMER_LAST_CONSUMED_TIMESTAMP.set(
                    time.time()
                )

                # =================================================
                # 📊 TREND CONSUMER METRICS
                # =================================================

                TREND_EVENTS_PROCESSED_TOTAL.labels(
                    source=source
                ).inc()

                TREND_SCORE_GENERATED_TOTAL.labels(
                    source=source
                ).inc()

                TREND_LAST_EVENT_TIMESTAMP.set(
                    time.time()
                )

                logger.info(f"📈 Trend Update: {result}")

            except Exception as e:

                CONSUMER_ERRORS_TOTAL.inc()
                TREND_PROCESSING_ERRORS_TOTAL.inc()

                logger.error(f"Processing Error: {e}")

            finally:

                duration = time.time() - start_time

                CONSUMER_PROCESSING_LATENCY.observe(
                    duration
                )

                TREND_PROCESSING_LATENCY_SECONDS.observe(
                    duration
                )

    except KeyboardInterrupt:
        logger.info("🛑 Consumer stopped manually")

    except Exception as e:
        logger.error(f"Fatal error: {e}")

    finally:
        consumer.close()

        logger.info("👋 Consumer closed safely")

