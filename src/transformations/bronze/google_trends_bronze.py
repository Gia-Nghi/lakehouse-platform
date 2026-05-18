from typing import Dict, List, Optional


def to_int(value) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def transform_google_trends_to_bronze(records: List[Dict]) -> List[Dict]:
    bronze = []

    for r in records:
        bronze.append(
            {
                "keyword": r.get("keyword"),
                "trend_datetime": r.get("date"),
                "interest": to_int(r.get("interest")),
                "geo": r.get("geo"),
                "timeframe": r.get("timeframe"),
                "_source": r.get("_source"),
                "_batch_id": r.get("_batch_id"),
                "_ingestion_time": r.get("_ingestion_time"),
                "_ingestion_date": r.get("_ingestion_date"),
            }
        )

    return bronze