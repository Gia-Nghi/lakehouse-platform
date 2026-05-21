import json
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List

from src.common.minio import create_minio_client
from src.ingestion.geo_context.osm.client import fetch_overpass
from src.ingestion.geo_context.osm.config import (
    BRONZE_PREFIX,
    GRID_BBOXES,
    MINIO_BUCKET,
    OVERPASS_SLEEP_BETWEEN_BBOX_SECONDS,
    TARGET_REGION,
)


def _ensure_bucket(client, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def _to_jsonl_bytes(records: List[Dict[str, Any]]) -> bytes:
    lines = []

    for record in records:
        lines.append(json.dumps(record, ensure_ascii=False))

    return ("\n".join(lines) + "\n").encode("utf-8")


def _upload_bytes(client, bucket_name: str, object_name: str, data: bytes, content_type: str) -> None:
    import io

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=io.BytesIO(data),
        length=len(data),
        content_type=content_type,
    )


def collect_by_grid(
    entity_name: str,
    query_builder: Callable[[float, float, float, float], str],
) -> List[Dict[str, Any]]:
    all_elements: List[Dict[str, Any]] = []

    for bbox in GRID_BBOXES:
        south, west, north, east = bbox
        print(f"[OSM] Fetching entity={entity_name}, bbox={bbox}")

        try:
            query = query_builder(south, west, north, east)
            data = fetch_overpass(query)
            elements = data.get("elements", [])

            all_elements.extend(elements)

            print(f"[OSM] Fetched {len(elements)} elements for {entity_name}, bbox={bbox}")

        except Exception as exc:
            print(f"[OSM] Skip bbox={bbox}, entity={entity_name}, error={exc}")

        time.sleep(OVERPASS_SLEEP_BETWEEN_BBOX_SECONDS)

    return all_elements


def upload_layer_jsonl(
    entity_type: str,
    parsed_payload: List[Dict[str, Any]],
    batch_id: int = 0,
) -> str:
    client = create_minio_client()
    _ensure_bucket(client, MINIO_BUCKET)

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ymd = now.strftime("%Y%m%d")

    object_name = (
        f"{BRONZE_PREFIX}/"
        f"date={date_str}/"
        f"{entity_type}_{ymd}_{batch_id:04d}.jsonl"
    )

    data = _to_jsonl_bytes(parsed_payload)

    _upload_bytes(
        client=client,
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=data,
        content_type="application/x-ndjson",
    )

    print(f"[OSM] Uploaded {entity_type}: s3://{MINIO_BUCKET}/{object_name}")
    return object_name


def upload_metadata(summary: Dict[str, Any]) -> str:
    client = create_minio_client()
    _ensure_bucket(client, MINIO_BUCKET)

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ymd = now.strftime("%Y%m%d")

    object_name = f"{BRONZE_PREFIX}/date={date_str}/metadata_{ymd}.json"

    payload = {
        "source": "openstreetmap",
        "domain": "geo_context",
        "target_region": TARGET_REGION,
        "ingested_at": now.isoformat(),
        "ingested_date": date_str,
        "grid_bboxes": GRID_BBOXES,
        "layers": summary,
    }

    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    _upload_bytes(
        client=client,
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=data,
        content_type="application/json",
    )

    print(f"[OSM] Uploaded metadata: s3://{MINIO_BUCKET}/{object_name}")
    return object_name