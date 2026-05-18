from typing import Any, Dict, List


def parse_chotot_response(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    ads = payload.get("ads") or payload.get("data") or []

    records = []

    for item in ads:
        records.append(
            {
                "ad_id": item.get("ad_id") or item.get("list_id"),
                "subject": item.get("subject"),
                "body": item.get("body"),
                "price": item.get("price"),
                "area": item.get("size") or item.get("area"),
                "region": item.get("region_name"),
                "area_name": item.get("area_name"),
                "ward": item.get("ward_name"),
                "category": item.get("category"),
                "account_id": item.get("account_id"),
                "date": item.get("date"),
                "raw": item,
            }
        )

    return records