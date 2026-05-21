import time
import random
import logging
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode

import requests

logger = logging.getLogger(__name__)


class ChototClient:
    def __init__(
        self,
        list_url: str,
        detail_url: str,
        params: Dict[str, Any],
        headers: Dict[str, str],
        retries: int = 3,
        timeout: int = 10,
        sleep_base: float = 1.0,
        backoff_factor: float = 2.0,
    ):
        self.list_url = list_url
        self.detail_url = detail_url
        self.params = params
        self.headers = headers
        self.retries = retries
        self.timeout = timeout
        self.sleep_base = sleep_base
        self.backoff_factor = backoff_factor

    def request_with_retry(self, url: str) -> Optional[requests.Response]:
        for attempt in range(1, self.retries + 1):
            try:
                resp = requests.get(url, headers=self.headers, timeout=self.timeout)

                if resp.status_code == 200:
                    return resp

                preview = resp.text[:200].replace("\n", " ")
                logger.warning(
                    "Request failed url=%s status=%s attempt=%s/%s body=%s",
                    url,
                    resp.status_code,
                    attempt,
                    self.retries,
                    preview,
                )

            except Exception as e:
                logger.error(
                    "Network error url=%s attempt=%s/%s error=%s",
                    url,
                    attempt,
                    self.retries,
                    e,
                )

            sleep_seconds = (
                self.sleep_base
                * (self.backoff_factor ** (attempt - 1))
                + random.uniform(0, 0.5)
            )
            time.sleep(sleep_seconds)

        return None

    def build_list_url(self, limit: int) -> str:
        params = dict(self.params)
        params["limit"] = limit
        return f"{self.list_url}?{urlencode(params)}"

    def get_current_ads(self, limit: int) -> List[Dict[str, Any]]:
        url = self.build_list_url(limit)
        resp = self.request_with_retry(url)

        if not resp:
            logger.error("Cannot fetch Chotot listing API")
            return []

        try:
            data = resp.json()
            return data.get("ads", [])
        except Exception as e:
            logger.error("Cannot parse listing response: %s", e)
            return []

    def get_detail(self, list_id: int) -> Optional[Dict[str, Any]]:
        url = self.detail_url.format(list_id=list_id)
        resp = self.request_with_retry(url)

        if not resp:
            logger.warning("Cannot fetch detail for list_id=%s", list_id)
            return None

        try:
            return resp.json()
        except Exception as e:
            logger.error("Cannot parse detail response list_id=%s error=%s", list_id, e)
            return None