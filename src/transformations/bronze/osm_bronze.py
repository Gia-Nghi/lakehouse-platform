from typing import Dict, List, Optional


def clean_null(value):
    if value in ["NaN", "nan", "None"]:
        return None
    return value


def to_float(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def transform_osm_to_bronze(records: List[Dict]) -> List[Dict]:
    bronze = []

    for r in records:
        bronze.append(
            {
                "osm_id": str(r.get("osm_id")) if r.get("osm_id") is not None else None,
                "element_type": clean_null(r.get("element_type")),
                "name": clean_null(r.get("name")),
                "amenity": clean_null(r.get("amenity")),
                "shop": clean_null(r.get("shop")),
                "public_transport": clean_null(r.get("public_transport")),
                "latitude": to_float(r.get("lat")),
                "longitude": to_float(r.get("lon")),
                "geometry_wkt": clean_null(r.get("geometry_wkt")),
                "_source": r.get("_source"),
                "_batch_id": r.get("_batch_id"),
                "_ingestion_time": r.get("_ingestion_time"),
                "_ingestion_date": r.get("_ingestion_date"),
            }
        )

    return bronze