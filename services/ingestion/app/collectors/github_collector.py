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

        # Token is injected through the environment.
        #
        # Kubernetes:
        #
        # GitHub PAT
        #     ↓
        # Kubernetes Secret
        #     ↓
        # INGESTION_GITHUB_TOKEN
        #     ↓
        # this class
        #
        self.token = os.getenv("INGESTION_GITHUB_TOKEN")

        self.session = requests.Session()

        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "HAAEDE-ingestion-service",
            }
        )

        if self.token:
            self.session.headers.update(
                {
                    "Authorization": f"Bearer {self.token}",
                }
            )

            logger.info("GitHub authentication enabled")

        else:
            logger.warning(
                "INGESTION_GITHUB_TOKEN is not configured. "
                "GitHub requests will be unauthenticated."
            )

    def fetch(self):
        logger.info("Fetching GitHub events...")

        response = self.session.get(
            self.url,
            timeout=10,
        )

        # ----------------------------------------------------
        # GitHub primary rate limit
        # ----------------------------------------------------
        #
        # GitHub returns:
        #
        # X-RateLimit-Limit
        # X-RateLimit-Remaining
        # X-RateLimit-Reset
        #
        # When remaining == 0, do not repeatedly retry.
        # Instead return no events and let the caller continue
        # with the other ingestion sources.
        # ----------------------------------------------------

        remaining = response.headers.get("X-RateLimit-Remaining")
        reset = response.headers.get("X-RateLimit-Reset")

        if response.status_code == 403 and remaining == "0":

            reset_timestamp = None

            try:
                if reset:
                    reset_timestamp = int(reset)
            except ValueError:
                pass

            if reset_timestamp:
                now = int(time.time())
                wait_seconds = max(0, reset_timestamp - now)

                logger.warning(
                    "GitHub API rate limit exhausted. "
                    f"Reset in approximately {wait_seconds}s."
                )

            else:
                logger.warning(
                    "GitHub API rate limit exhausted, "
                    "but X-RateLimit-Reset was unavailable."
                )

            # Important:
            #
            # Do NOT raise here.
            #
            # Returning [] allows the ingestion service to continue
            # processing Hacker News instead of failing the entire
            # ingestion cycle.
            return []

        # ----------------------------------------------------
        # Other HTTP errors
        # ----------------------------------------------------

        response.raise_for_status()

        data = response.json()

        logger.info(
            "GitHub request successful "
            f"(remaining={remaining or 'unknown'})"
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

        except Exception as e:
            INGESTION_ERRORS_TOTAL.labels(
                source="github"
            ).inc()

            logger.error(
                f"GitHub collector failed: {e}"
            )

            raise

        finally:
            INGESTION_LATENCY_SECONDS.labels(
                source="github"
            ).observe(time.time() - start)
