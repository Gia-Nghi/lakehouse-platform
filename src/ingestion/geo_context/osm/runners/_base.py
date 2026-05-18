import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List

from common.io.minio_client import create_minio_client, ensure_bucket, upload_json_bytes
from common.utils.logger import get_logger
from ingestion.geo_context.osm.client import fetch_overpass
from ingestion.geo_context.osm.config import (
    GRID_BBOXES,
    MINIO_BUCKET,
    OVERPASS_SLEEP_BETWEEN_BBOX_SECONDS,
)

logger = get_logger(__name__)


def collect_by_grid(
    entity_name: str,
    query_builder: Callable[[float, float, float, float], str],
) -> List[Dict[str, Any]]:
    all_elements: List[Dict[str, Any]] = []

    for bbox in GRID_BBOXES:
        south, west, north, east = bbox
        logger.info("Fetching %s bbox=%s", entity_name, bbox)

        try:
            query = query_builder(south, west, north, east)
            data = fetch_overpass(query)
            elements = data.get("elements", [])
            all_elements.extend(elements)

            logger.info(
                "Fetched %s elements for %s bbox=%s",
                len(elements),
                entity_name,
                bbox,
            )

        except Exception as exc:
            logger.warning(
                "Skip bbox=%s for entity=%s because Overpass failed: %s",
                bbox,
                entity_name,
                exc,
            )

        time.sleep(OVERPASS_SLEEP_BETWEEN_BBOX_SECONDS)

    return all_elements


def upload_dataset(
    folder: str,
    entity_type: str,
    parsed_payload: List[Dict[str, Any]],
    raw_elements: List[Dict[str, Any]],
) -> None:
    client = create_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    now = datetime.now(timezone.utc)
    dt = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    raw_object = f"bronze/geo_context/osm/{folder}/dt={dt}/osm_{folder}_hcmc_raw_{ts}.json"
    parsed_object = f"bronze/geo_context/osm/{folder}/dt={dt}/osm_{folder}_hcmc_parsed_{ts}.json"

    upload_json_bytes(client, MINIO_BUCKET, raw_object, {"elements": raw_elements})
    upload_json_bytes(
        client,
        MINIO_BUCKET,
        parsed_object,
        {
            "source": "openstreetmap",
            "domain": "geo_context",
            "entity_type": entity_type,
            "target_region": "Ho Chi Minh City",
            "grid_bboxes": GRID_BBOXES,
            "collected_at_ms": int(now.timestamp() * 1000),
            "record_count": len(parsed_payload),
            "payload": parsed_payload,
        },
    )

    logger.info("Uploaded raw %s to s3://%s/%s", entity_type, MINIO_BUCKET, raw_object)
    logger.info("Uploaded parsed %s to s3://%s/%s", entity_type, MINIO_BUCKET, parsed_object)
    logger.info("Total %s parsed: %s", entity_type, len(parsed_payload))