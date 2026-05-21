import time
import requests

from src.ingestion.geo_context.osm.config import (
    OVERPASS_URL,
    OVERPASS_TIMEOUT_SECONDS,
    OVERPASS_RETRIES,
    OVERPASS_RETRY_BACKOFF_SECONDS,
    OVERPASS_USER_AGENT,
)


def fetch_overpass(query: str) -> dict:
    headers = {
        "User-Agent": OVERPASS_USER_AGENT,
    }

    last_error = None

    for attempt in range(1, OVERPASS_RETRIES + 1):
        try:
            response = requests.post(
                OVERPASS_URL,
                data={"data": query},
                headers=headers,
                timeout=OVERPASS_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()

        except Exception as exc:
            last_error = exc

            if attempt < OVERPASS_RETRIES:
                time.sleep(OVERPASS_RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError(f"Overpass request failed after {OVERPASS_RETRIES} attempts: {last_error}")