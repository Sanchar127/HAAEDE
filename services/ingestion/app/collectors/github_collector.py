import requests
from log.logger import get_logger
from datetime import datetime

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
        raw = self.fetch()
        logger.info(f"GitHub fetched {len(raw)} events")
        return [self.normalize(e) for e in raw]