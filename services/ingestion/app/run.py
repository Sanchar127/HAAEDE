import os
import time

from prometheus_client import start_http_server

from services.ingestion.app.producer import stream_to_kafka
from services.ingestion.app.collectors.github_collector import GitHubCollector
from services.ingestion.app.collectors.hackernews_collector import HackerNewsCollector
from services.ingestion.app.log.logger import get_logger

logger = get_logger("ingestion-service")

TOPIC_NAME = os.getenv("TOPIC_NAME", "raw.events")
INTERVAL = int(os.getenv("POLL_INTERVAL", "140"))


def start_ingestion():
    logger.info("🚀 Starting Layer 1 Ingestion Service")

    # Expose Prometheus /metrics endpoint
    start_http_server(8001)
    logger.info("📊 Metrics server running on :8001/metrics")

    github = GitHubCollector()
    hackernews = HackerNewsCollector()

    while True:
        start_time = time.time()
        success = False

        try:
            logger.info("🔄 Collecting events from sources...")

            # ---------------- COLLECT ----------------
            github_events = github.run() or []
            hn_events = hackernews.run() or []

            all_events = github_events + hn_events

            logger.info(f"Collected {len(all_events)} total events")

            # ---------------- SEND TO KAFKA ----------------
            if all_events:
                stream_to_kafka(TOPIC_NAME, all_events)
                logger.info(f"Sent {len(all_events)} events to Kafka topic={TOPIC_NAME}")
            else:
                logger.warning("No events collected in this cycle")

            success = True

        except Exception as e:
            logger.exception(f"Ingestion error occurred: {e}")

        finally:
            duration = time.time() - start_time
            logger.info(f"Cycle completed in {duration:.3f}s")

            if not success:
                logger.warning("Ingestion cycle failed")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    start_ingestion()