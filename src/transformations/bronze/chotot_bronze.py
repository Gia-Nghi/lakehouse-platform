from typing import Dict, List, Optional


def to_int(value) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def to_float(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def transform_chotot_to_bronze(records: List[Dict]) -> List[Dict]:
    bronze = []

    for r in records:
        raw = r.get("raw") or {}

        bronze.append(
            {
                "ad_id": to_int(r.get("ad_id")),
                "list_id": to_int(raw.get("list_id")),
                "subject": r.get("subject"),
                "body": r.get("body"),
                "price": to_int(r.get("price")),
                "area_m2": to_float(r.get("area")),
                "price_million_per_m2": to_float(raw.get("price_million_per_m2")),
                "region": r.get("region"),
                "district": r.get("area_name"),
                "ward": r.get("ward"),
                "street_name": raw.get("street_name"),
                "latitude": to_float(raw.get("latitude")),
                "longitude": to_float(raw.get("longitude")),
                "rooms": to_int(raw.get("rooms")),
                "toilets": to_int(raw.get("toilets")),
                "floors": to_int(raw.get("floors")),
                "house_type": raw.get("house_type"),
                "legal_document": raw.get("property_legal_document"),
                "seller_account_id": to_int(r.get("account_id")),
                "seller_name": raw.get("account_name") or raw.get("full_name"),
                "category": to_int(r.get("category")),
                "category_name": raw.get("category_name"),
                "status": raw.get("status"),
                "state": raw.get("state"),
                "posted_date_text": r.get("date"),
                "list_time": to_int(raw.get("list_time")),
                "thumbnail_image": raw.get("thumbnail_image"),
                "number_of_images": to_int(raw.get("number_of_images")),
                "_source": r.get("_source"),
                "_batch_id": r.get("_batch_id"),
                "_ingestion_time": r.get("_ingestion_time"),
                "_ingestion_date": r.get("_ingestion_date"),
            }
        )

    return bronze