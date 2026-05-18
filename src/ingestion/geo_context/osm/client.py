import osmnx as ox

from src.common.retry import retry


class OSMClient:
    @retry(max_attempts=3, delay_seconds=5)
    def fetch_features(self, place: str, tags: dict):
        return ox.features_from_place(place, tags)