import os
import time

from producer import stream_to_kafka
from collectors.github_collector import GitHubCollector
from collectors.hackernews_collector import HackerNewsCollector
from shared.logger import get_logger

logger = get_logger("ingestion-service")

TOPIC_NAME = os.getenv("TOPIC_NAME", "raw.events")
INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))


def start_ingestion():
    logger.info("🚀 Starting Layer 1 Ingestion Service")

    github = GitHubCollector()
    hackernews = HackerNewsCollector()

    while True:
        try:
            logger.info("🔄 Collecting events...")

            github_events = github.run()
            hn_events = hackernews.run()

            all_events = github_events + hn_events

            logger.info(f"Collected {len(all_events)} events")

            if all_events:
                stream_to_kafka(TOPIC_NAME, all_events)
                logger.info(f"Sent {len(all_events)} events to Kafka topic={TOPIC_NAME}")
            else:
                logger.warning("No events collected")

        except Exception as e:
            logger.error(f"Ingestion error: {e}")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    start_ingestion()