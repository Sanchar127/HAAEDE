import requests
from app.log.logger import get_logger
from datetime import datetime

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
        raw = self.fetch()
        logger.info(f"HackerNews fetched {len(raw)} items")
        return [self.normalize(i) for i in raw]