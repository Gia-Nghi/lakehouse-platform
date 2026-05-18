import requests

from src.common.retry import retry


class ChototClient:
    def __init__(self, base_url: str, params: dict):
        self.base_url = base_url
        self.params = params

    @retry(max_attempts=3, delay_seconds=3)
    def fetch(self):
        response = requests.get(
            self.base_url,
            params=self.params,
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
        )
        response.raise_for_status()
        return response.json()