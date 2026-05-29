import requests
from datetime import datetime

from services.ingestion.app.log.logger import get_logger
from services.ingestion.app.metrics import (
    INGESTED_EVENTS_TOTAL,
    INGESTION_ERRORS_TOTAL,
    INGESTION_LATENCY_SECONDS,
    LAST_SUCCESS_TIMESTAMP
)

logger = get_logger("hackernews-collector")


class HackerNewsCollector:

    BASE_URL = "https://hacker-news.firebaseio.com/v0"

    def fetch(self):
        logger.info("Fetching HackerNews data...")

        url = f"{self.BASE_URL}/newstories.json"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        story_ids = response.json()[:20]

        stories = []
        for sid in story_ids:
            item_url = f"{self.BASE_URL}/item/{sid}.json"
            item = requests.get(item_url, timeout=10).json()
            if item:
                stories.append(item)

        return stories

    def normalize(self, item):
        return {
            "source": "hackernews",
            "event_id": item.get("id"),
            "title": item.get("title"),
            "score": item.get("score"),
            "comments": item.get("descendants"),
            "url": item.get("url"),
            "author": item.get("by"),
            "created_at": item.get("time"),
            "fetched_at": datetime.utcnow().isoformat()
        }

    def run(self):
        import time
        start = time.time()

        try:
            raw = self.fetch()
            logger.info(f"HackerNews fetched {len(raw)} items")

            data = [self.normalize(i) for i in raw]

            INGESTED_EVENTS_TOTAL.labels(source="hackernews").inc(len(data))
            LAST_SUCCESS_TIMESTAMP.set_to_current_time()

            return data

        except Exception as e:
            INGESTION_ERRORS_TOTAL.labels(source="hackernews").inc()
            logger.error(f"HackerNews collector failed: {e}")
            raise

        finally:
            INGESTION_LATENCY_SECONDS.labels(source="hackernews").observe(
                time.time() - start
            )