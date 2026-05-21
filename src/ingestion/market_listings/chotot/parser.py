import time
from typing import Dict, Any


def build_raw_record(
    ad: Dict[str, Any],
    detail: Dict[str, Any],
    source: str = "chotot",
    category: str = "market_listings",
) -> Dict[str, Any]:
    return {
        "source": source,
        "category": category,
        "entity": "real_estate_listing",
        "list": ad,
        "detail": detail,
        "crawled_at": int(time.time() * 1000),
        "ingestion_type": "streaming_kafka",
    }


def get_record_id(record: Dict[str, Any], deduplicate_key: str = "list_id"):
    list_part = record.get("list", {})
    detail_part = record.get("detail", {})

    return (
        list_part.get(deduplicate_key)
        or detail_part.get(deduplicate_key)
        or detail_part.get("ad_id")
    )