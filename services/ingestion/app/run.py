import os
import time

from prometheus_client import start_http_server

from services.ingestion.app.producer import stream_to_kafka
from services.ingestion.app.collectors.github_collector import GitHubCollector
from services.ingestion.app.collectors.hackernews_collector import HackerNewsCollector
from services.ingestion.app.log.logger import get_logger

logger = get_logger("ingestion-service")


TOPIC_NAME = os.getenv(
    "TOPIC_NAME",
    "raw.events",
)

# GitHub Events API is not intended for aggressive polling.
#
# Default:
#   300 seconds = 5 minutes
#
GITHUB_INTERVAL = int(
    os.getenv("GITHUB_POLL_INTERVAL", "300")
)

# Hacker News can use a different interval.
#
# Default:
#   120 seconds = 2 minutes
#
HACKERNEWS_INTERVAL = int(
    os.getenv("HACKERNEWS_POLL_INTERVAL", "120")
)


def start_ingestion():

    logger.info(
        "🚀 Starting Layer 1 Ingestion Service"
    )

    logger.info(
        f"GitHub polling interval: {GITHUB_INTERVAL}s"
    )

    logger.info(
        f"Hacker News polling interval: {HACKERNEWS_INTERVAL}s"
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

    # Run each source immediately on startup.
    next_github_run = 0
    next_hackernews_run = 0

    while True:

        now = time.time()

        all_events = []

        # ----------------------------------------------------
        # GitHub
        # ----------------------------------------------------

        if now >= next_github_run:

            logger.info(
                "🔄 Collecting GitHub events..."
            )

            try:
                github_events = github.run() or []

                all_events.extend(github_events)

                logger.info(
                    f"GitHub returned {len(github_events)} events"
                )

            except Exception as e:

                logger.exception(
                    f"GitHub collection failed: {e}"
                )

            finally:
                next_github_run = time.time() + GITHUB_INTERVAL

        # ----------------------------------------------------
        # Hacker News
        # ----------------------------------------------------

        if now >= next_hackernews_run:

            logger.info(
                "🔄 Collecting Hacker News events..."
            )

            try:
                hackernews_events = (
                    hackernews.run() or []
                )

                all_events.extend(
                    hackernews_events
                )

                logger.info(
                    "Hacker News returned "
                    f"{len(hackernews_events)} events"
                )

            except Exception as e:

                logger.exception(
                    f"Hacker News collection failed: {e}"
                )

            finally:
                next_hackernews_run = (
                    time.time()
                    + HACKERNEWS_INTERVAL
                )

        # ----------------------------------------------------
        # Kafka
        # ----------------------------------------------------

        if all_events:

            try:

                stream_to_kafka(
                    TOPIC_NAME,
                    all_events,
                )

                logger.info(
                    f"Sent {len(all_events)} events "
                    f"to Kafka topic={TOPIC_NAME}"
                )

            except Exception as e:

                logger.exception(
                    f"Kafka publishing failed: {e}"
                )

        # ----------------------------------------------------
        # Sleep
        # ----------------------------------------------------
        #
        # We don't need to wake up every second.
        #
        # 1 second keeps scheduling responsive while remaining
        # extremely cheap.
        # ----------------------------------------------------

        time.sleep(1)


if __name__ == "__main__":
    start_ingestion()
