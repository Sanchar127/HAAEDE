import os
import time
from datetime import datetime, timezone

import requests

from services.ingestion.app.log.logger import get_logger
from services.ingestion.app.metrics import (
    INGESTED_EVENTS_TOTAL,
    INGESTION_ERRORS_TOTAL,
    INGESTION_LATENCY_SECONDS,
    LAST_SUCCESS_TIMESTAMP,
)


logger = get_logger("github-collector")


class GitHubCollector:

    def __init__(self):
        self.url = "https://api.github.com/events"

        # GitHub PAT is injected by Kubernetes as an
        # environment variable.
        self.token = os.getenv("INGESTION_GITHUB_TOKEN")

        self.session = requests.Session()

        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "HAAEDE-ingestion-service",
        })

        if self.token:
            self.session.headers.update({
                "Authorization": f"Bearer {self.token}",
            })

            logger.info("GitHub API authentication enabled")
        else:
            logger.warning(
                "INGESTION_GITHUB_TOKEN is not configured; "
                "GitHub API requests will be unauthenticated"
            )

    def fetch(self):
        logger.info("Fetching GitHub events...")

        response = self.session.get(
            self.url,
            timeout=10,
        )

        # ----------------------------------------------------
        # GitHub rate-limit handling
        # ----------------------------------------------------

        remaining = response.headers.get("X-RateLimit-Remaining")
        reset_timestamp = response.headers.get("X-RateLimit-Reset")

        # Primary GitHub rate limit exceeded.
        if response.status_code == 403 and remaining == "0":

            reset_message = "unknown"

            if reset_timestamp:
                try:
                    reset_time = datetime.fromtimestamp(
                        int(reset_timestamp),
                        tz=timezone.utc,
                    )

                    reset_message = reset_time.isoformat()

                except (ValueError, TypeError):
                    pass

            logger.warning(
                "GitHub API rate limit exceeded. "
                f"Rate limit resets at {reset_message}. "
                "Skipping this collection cycle."
            )

            return []

        # ----------------------------------------------------
        # Other HTTP errors
        # ----------------------------------------------------

        response.raise_for_status()

        data = response.json()

        logger.info(
            f"GitHub API request successful "
            f"(remaining={remaining})"
        )

        return data

    def normalize(self, event):
        return {
            "source": "github",
            "event_id": event.get("id"),
            "event_type": event.get("type"),
            "repo": event.get("repo", {}).get("name"),
            "actor": event.get("actor", {}).get("login"),
            "created_at": event.get("created_at"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    def run(self):
        start = time.time()

        try:
            raw = self.fetch()

            logger.info(
                f"GitHub fetched {len(raw)} events"
            )

            data = [
                self.normalize(event)
                for event in raw
            ]

            if data:
                INGESTED_EVENTS_TOTAL.labels(
                    source="github"
                ).inc(len(data))

                LAST_SUCCESS_TIMESTAMP.set_to_current_time()

            return data

        except Exception as exc:

            INGESTION_ERRORS_TOTAL.labels(
                source="github"
            ).inc()

            logger.error(
                f"GitHub collector failed: {exc}"
            )

            raise

        finally:

            INGESTION_LATENCY_SECONDS.labels(
                source="github"
            ).observe(time.time() - start)
