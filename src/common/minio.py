import io
import json
import math
import os
from typing import Any, Dict, List
 
from minio import Minio


def create_minio_client() -> Minio:
    return Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv(
            "MINIO_ACCESS_KEY",
            os.getenv("MINIO_ROOT_USER", "admin"),
        ),
        secret_key=os.getenv(
            "MINIO_SECRET_KEY",
            os.getenv("MINIO_ROOT_PASSWORD", "password123"),
        ),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def ensure_bucket_exists(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def sanitize_json(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, dict):
        return {k: sanitize_json(v) for k, v in value.items()}

    if isinstance(value, list):
        return [sanitize_json(v) for v in value]

    return value


def put_jsonl_object(
    client: Minio,
    bucket: str,
    object_name: str,
    records: List[Dict[str, Any]],
) -> str:
    ensure_bucket_exists(client, bucket)

    clean_records = [sanitize_json(record) for record in records]

    content = "\n".join(
        json.dumps(record, ensure_ascii=False, allow_nan=False)
        for record in clean_records
    ) + "\n"

    data = content.encode("utf-8")

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/jsonl",
    )

    return f"s3://{bucket}/{object_name}"


def has_objects(
    client: Minio,
    bucket: str,
    prefix: str,
) -> bool:
    if not client.bucket_exists(bucket):
        return False

    objects = client.list_objects(
        bucket,
        prefix=prefix,
        recursive=True,
    )

    for obj in objects:
        if obj.object_name.endswith(".jsonl"):
            return True

    return False