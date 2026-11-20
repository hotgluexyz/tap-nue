"""Read-only Nue API transport shared by discovery and extraction."""

import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests


class NueClient:
    def __init__(self, config):
        self.base_url = config["base_url"].rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "nue-api-key": config["api_key"],
                "User-Agent": "tap-nue/0.0.1",
                "Accept": "application/json",
            }
        )

    def read(self, path, query=None):
        """GET metadata or POST a GraphQL query, never a mutation."""
        for attempt in range(5):
            try:
                response = self.session.request(
                    "POST" if query else "GET",
                    self.base_url + path,
                    json={"query": query} if query else None,
                    timeout=60,
                    allow_redirects=False,
                )
            except (requests.Timeout, requests.ConnectionError):
                if attempt == 4:
                    raise RuntimeError(f"Nue read failed after five attempts: {path}") from None
                time.sleep(2**attempt + random.random())
                continue
            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt == 4:
                    raise RuntimeError(f"Nue HTTP {response.status_code}: {path}")
                retry_after = response.headers.get("Retry-After")
                delay = 2**attempt + random.random()
                if retry_after:
                    try:
                        delay = max(0, float(retry_after))
                    except ValueError:
                        delay = max(
                            0,
                            (
                                parsedate_to_datetime(retry_after) - datetime.now(timezone.utc)
                            ).total_seconds(),
                        )
                time.sleep(delay)
                continue
            if not 200 <= response.status_code < 300:
                raise RuntimeError(f"Nue HTTP {response.status_code}: {path}")
            body = response.json()
            if isinstance(body, dict) and (body.get("errors") or body.get("status") == "FAILURE"):
                # Provider errors can contain record data; do not log response bodies.
                raise RuntimeError(f"Nue returned API errors: {path}")
            return body
