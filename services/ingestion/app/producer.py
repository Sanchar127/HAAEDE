import os
import json
import time

from confluent_kafka import Producer

from services.ingestion.app.log.logger import get_logger
from services.shared.metrics.metrics import (
    KAFKA_MESSAGES_PRODUCED_TOTAL,
    KAFKA_PRODUCE_ERRORS_TOTAL,
    KAFKA_PRODUCE_LATENCY_SECONDS,
)

logger = get_logger("kafka-producer")


kafka_server = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

if not kafka_server:
    raise ValueError(
        "KAFKA_BOOTSTRAP_SERVERS not set"
    )


logger.info(
    f"Connecting to Kafka at {kafka_server}"
)


producer = Producer({
    "bootstrap.servers": kafka_server,
    "client.id": "pulse-scope-producer",
})


def delivery_report(err, msg, delivery_errors):

    if err:

        delivery_errors.append(err)

        logger.error(
            f"Delivery failed: {err}"
        )

    else:

        logger.info(
            f"Delivered to "
            f"{msg.topic()} "
            f"[{msg.partition()}]"
        )


def stream_to_kafka(topic, data_chunk):

    delivery_errors = []

    def callback(err, msg):
        delivery_report(
            err,
            msg,
            delivery_errors,
        )

    # DataFrame support
    if hasattr(data_chunk, "iterrows"):

        iterator = data_chunk.iterrows()

        for _, row in iterator:

            payload = row.to_json()

            _produce(
                topic,
                payload,
                "unknown",
                callback,
            )

    # List/dict support
    else:

        for row in data_chunk:

            payload = json.dumps(row)

            _produce(
                topic,
                payload,
                row.get("source", "unknown"),
                callback,
            )

    # Wait until all outstanding messages have been
    # delivered or failed.
    remaining = producer.flush()

    if delivery_errors:

        KAFKA_PRODUCE_ERRORS_TOTAL.labels(
            topic=topic
        ).inc(len(delivery_errors))

        raise RuntimeError(
            f"Kafka delivery failed for "
            f"{len(delivery_errors)} message(s)"
        )

    if remaining != 0:

        raise RuntimeError(
            f"Kafka flush completed with "
            f"{remaining} undelivered message(s)"
        )


def _produce(
    topic,
    payload,
    source,
    callback,
):
    start = time.time()

    try:

        producer.produce(
            topic,
            value=payload,
            callback=callback,
        )

        KAFKA_MESSAGES_PRODUCED_TOTAL.labels(
            topic=topic,
            source=source,
        ).inc()

    except Exception as e:

        KAFKA_PRODUCE_ERRORS_TOTAL.labels(
            topic=topic
        ).inc()

        logger.error(
            f"Kafka produce error: {e}"
        )

        raise

    finally:

        KAFKA_PRODUCE_LATENCY_SECONDS.labels(
            topic=topic
        ).observe(
            time.time() - start
        )