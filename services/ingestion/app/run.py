import os
import time

from prometheus_client import start_http_server

from services.ingestion.app.collectors.github_collector import GitHubCollector
from services.ingestion.app.collectors.hackernews_collector import (
    HackerNewsCollector,
)
from services.ingestion.app.log.logger import get_logger
from services.ingestion.app.producer import stream_to_kafka


logger = get_logger("ingestion-service")


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

TOPIC_NAME = os.getenv(
    "TOPIC_NAME",
    "fintech.debt.raw",
)

# GitHub: every 5 minutes
GITHUB_INTERVAL = int(
    os.getenv("GITHUB_POLL_INTERVAL", "300")
)

# HackerNews: every 2 minutes
HACKERNEWS_INTERVAL = int(
    os.getenv("HACKERNEWS_POLL_INTERVAL", "120")
)


# ------------------------------------------------------------
# Run one collector safely
# ------------------------------------------------------------

def collect_source(name, collector):
    """
    Run one collector without allowing its failure
    to stop the other collectors.
    """

    try:
        logger.info(
            f"Collecting from {name}..."
        )

        events = collector.run() or []

        logger.info(
            f"{name} collected {len(events)} events"
        )

        return events

    except Exception as exc:

        logger.exception(
            f"{name} collection failed: {exc}"
        )

        return []


# ------------------------------------------------------------
# Ingestion service
# ------------------------------------------------------------

def start_ingestion():

    logger.info(
        "🚀 Starting Layer 1 Ingestion Service"
    )

    logger.info(
        f"GitHub polling interval: "
        f"{GITHUB_INTERVAL}s"
    )

    logger.info(
        f"HackerNews polling interval: "
        f"{HACKERNEWS_INTERVAL}s"
    )

    # --------------------------------------------------------
    # Prometheus
    # --------------------------------------------------------

    start_http_server(8001)

    logger.info(
        "📊 Metrics server running on :8001/metrics"
    )

    # --------------------------------------------------------
    # Collectors
    # --------------------------------------------------------

    github = GitHubCollector()
    hackernews = HackerNewsCollector()

    # Run both sources immediately on startup.
    next_github_run = 0
    next_hackernews_run = 0

    # --------------------------------------------------------
    # Main scheduler
    # --------------------------------------------------------

    while True:

        now = time.time()

        all_events = []

        # ====================================================
        # GitHub
        # ====================================================

        if now >= next_github_run:

            github_events = collect_source(
                "GitHub",
                github,
            )

            all_events.extend(github_events)

            next_github_run = now + GITHUB_INTERVAL

        # ====================================================
        # HackerNews
        # ====================================================

        if now >= next_hackernews_run:

            hackernews_events = collect_source(
                "HackerNews",
                hackernews,
            )

            all_events.extend(hackernews_events)

            next_hackernews_run = now + HACKERNEWS_INTERVAL

        # ====================================================
        # Kafka
        # ====================================================

        if all_events:

            try:

                stream_to_kafka(
                    TOPIC_NAME,
                    all_events,
                )

                # Kafka successfully delivered the batch.
                # Only now mark HackerNews events as seen.
                hackernews.mark_published(
                    hackernews_events
                )

                logger.info(
                    f"Sent {len(all_events)} events "
                    f"to Kafka topic={TOPIC_NAME}"
                )

            except Exception as exc:

                logger.exception(
                    f"Failed to send events to Kafka: {exc}"
                )

        # ====================================================
        # Scheduler sleep
        # ====================================================

        time.sleep(1)


# ------------------------------------------------------------
# Application entry point
# ------------------------------------------------------------

if __name__ == "__main__":
    start_ingestion()
