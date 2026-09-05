import requests
from datetime import datetime, timezone

from services.ingestion.app.log.logger import get_logger
from services.ingestion.app.metrics import (
    INGESTED_EVENTS_TOTAL,
    INGESTION_ERRORS_TOTAL,
    INGESTION_LATENCY_SECONDS,
    LAST_SUCCESS_TIMESTAMP,
)

logger = get_logger("hackernews-collector")


class HackerNewsCollector:

    BASE_URL = "https://hacker-news.firebaseio.com/v0"
    STORY_LIMIT = 20

    def __init__(self):
        # IDs already emitted by this collector instance.
        #
        # This is intentionally an in-memory cache for Layer 1.
        # Silver will later provide the durable/idempotent guarantee.
        self.seen_event_ids: set[int] = set()

    def fetch(self):
        logger.info("Fetching HackerNews data...")

        url = f"{self.BASE_URL}/newstories.json"

        response = requests.get(
            url,
            timeout=10,
        )
        response.raise_for_status()

        story_ids = response.json()[:self.STORY_LIMIT]

        stories = []

        for sid in story_ids:
            item_url = f"{self.BASE_URL}/item/{sid}.json"

            item_response = requests.get(
                item_url,
                timeout=10,
            )
            item_response.raise_for_status()

            item = item_response.json()

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
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    def run(self):
        import time

        start = time.time()

        try:
            raw = self.fetch()

            logger.info(
                f"HackerNews fetched {len(raw)} items"
            )

            data = [
                self.normalize(item)
                for item in raw
            ]

            new_data = []

            for event in data:

                event_id = event.get("event_id")

                if event_id is None:
                    logger.warning(
                        "Skipping HackerNews event without event_id"
                    )
                    continue

                if event_id in self.seen_event_ids:
                    continue

                self.seen_event_ids.add(event_id)
                new_data.append(event)

            logger.info(
                "HackerNews deduplication: "
                f"fetched={len(data)}, "
                f"new={len(new_data)}, "
                f"duplicates={len(data) - len(new_data)}"
            )

            INGESTED_EVENTS_TOTAL.labels(
                source="hackernews"
            ).inc(len(new_data))

            LAST_SUCCESS_TIMESTAMP.set_to_current_time()

            return new_data

        except Exception as e:

            INGESTION_ERRORS_TOTAL.labels(
                source="hackernews"
            ).inc()

            logger.error(
                f"HackerNews collector failed: {e}"
            )

            raise

        finally:

            INGESTION_LATENCY_SECONDS.labels(
                source="hackernews"
            ).observe(
                time.time() - start
            )