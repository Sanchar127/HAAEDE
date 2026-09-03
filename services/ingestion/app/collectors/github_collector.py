import requests
from datetime import datetime

from services.ingestion.app.log.logger import get_logger
from services.ingestion.app.metrics import (
    INGESTED_EVENTS_TOTAL,
    INGESTION_ERRORS_TOTAL,
    INGESTION_LATENCY_SECONDS,
    LAST_SUCCESS_TIMESTAMP
)

logger = get_logger("github-collector")


class GitHubCollector:

    def __init__(self):
        self.url = "https://api.github.com/events"

    def fetch(self):
        logger.info("Fetching GitHub events...")
        response = requests.get(self.url, timeout=10)
        response.raise_for_status()
        return response.json()

    def normalize(self, event):
        return {
            "source": "github",
            "event_id": event.get("id"),
            "event_type": event.get("type"),
            "repo": event.get("repo", {}).get("name"),
            "actor": event.get("actor", {}).get("login"),
            "created_at": event.get("created_at"),
            "fetched_at": datetime.utcnow().isoformat()
        }

    def run(self):
        import time
        start = time.time()

        try:
            raw = self.fetch()
            logger.info(f"GitHub fetched {len(raw)} events")

            data = [self.normalize(e) for e in raw]

            INGESTED_EVENTS_TOTAL.labels(source="github").inc(len(data))
            LAST_SUCCESS_TIMESTAMP.set_to_current_time()

            return data

        except Exception as e:
            INGESTION_ERRORS_TOTAL.labels(source="github").inc()
            logger.error(f"GitHub collector failed: {e}")
            raise

        finally:
            INGESTION_LATENCY_SECONDS.labels(source="github").observe(
                time.time() - start
            )