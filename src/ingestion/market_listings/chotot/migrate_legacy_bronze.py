import os
import io
import json
import logging

from minio import Minio

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "bronze")
TARGET_BUCKET = os.getenv("TARGET_BUCKET", "lakehouse")

LEGACY_PREFIX = os.getenv("LEGACY_PREFIX", "chotot/")
TARGET_PREFIX = os.getenv(
    "TARGET_PREFIX",
    "bronze/market_listings/chotot/",
)


def create_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def normalize_legacy_record(record: dict) -> dict:
    return {
        "source": record.get("source", "chotot"),
        "domain": record.get("domain", "market_listings"),
        "category": record.get("category", "market_listings"),
        "entity": record.get("entity", "real_estate_listing"),
        "list": record.get("list", {}),
        "detail": record.get("detail", {}),
        "crawled_at": record.get("crawled_at"),
        "ingestion_type": "legacy_migrated",
        "metadata": {
            "original_ingestion_type": record.get("ingestion_type"),
            "schema_version": "v2",
            "migrated_from": f"s3://{SOURCE_BUCKET}/{LEGACY_PREFIX}",
        },
    }


def build_target_object_name(source_object_name: str) -> str:
    """
    Input:
    chotot/date=2026-05-19/chotot_20260519_0000.jsonl

    Output:
    bronze/market_listings/chotot/date=2026-05-19/chotot_20260519_0000.jsonl
    """

    parts = source_object_name.split("/")

    if len(parts) < 3:
        raise ValueError(f"Unexpected legacy object path: {source_object_name}")

    date_part = parts[1]
    file_name = parts[-1]

    if not date_part.startswith("date="):
        raise ValueError(
            f"Cannot find date partition in path: {source_object_name}"
        )

    return f"{TARGET_PREFIX}{date_part}/{file_name}"


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        logger.info("Creating bucket=%s", bucket_name)
        client.make_bucket(bucket_name)


def migrate_file(client: Minio, object_name: str) -> bool:
    logger.info("Processing s3://%s/%s", SOURCE_BUCKET, object_name)

    response = client.get_object(
        SOURCE_BUCKET,
        object_name,
    )

    try:
        lines = response.read().decode("utf-8").splitlines()
    finally:
        response.close()
        response.release_conn()

    normalized_records = []
    bad_lines = 0

    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError as e:
            bad_lines += 1

            logger.warning(
                "Skip bad JSON object=%s line=%s error=%s preview=%s",
                object_name,
                line_number,
                e,
                line[:300],
            )

            continue

        normalized_records.append(normalize_legacy_record(record))

    if bad_lines > 0:
        logger.warning(
            "Object=%s skipped_bad_lines=%s",
            object_name,
            bad_lines,
        )

    if not normalized_records:
        logger.warning("No valid records found object=%s", object_name)
        return False

    target_object_name = build_target_object_name(object_name)

    payload = (
        "\n".join(
            json.dumps(record, ensure_ascii=False)
            for record in normalized_records
        )
        + "\n"
    )

    data = payload.encode("utf-8")

    client.put_object(
        bucket_name=TARGET_BUCKET,
        object_name=target_object_name,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )

    logger.info(
        "Migrated %s valid records: s3://%s/%s -> s3://%s/%s",
        len(normalized_records),
        SOURCE_BUCKET,
        object_name,
        TARGET_BUCKET,
        target_object_name,
    )

    return True


def run():
    client = create_minio_client()

    if not client.bucket_exists(SOURCE_BUCKET):
        raise RuntimeError(f"Source bucket does not exist: {SOURCE_BUCKET}")

    ensure_bucket_exists(client, TARGET_BUCKET)

    objects = client.list_objects(
        SOURCE_BUCKET,
        prefix=LEGACY_PREFIX,
        recursive=True,
    )

    processed = 0
    migrated = 0

    for obj in objects:
        if not obj.object_name.endswith(".jsonl"):
            continue

        processed += 1

        try:
            ok = migrate_file(client, obj.object_name)

            if ok:
                migrated += 1

        except Exception as e:
            logger.exception(
                "Failed processing object=%s error=%s",
                obj.object_name,
                e,
            )

    logger.info(
        "Migration completed processed_files=%s migrated_files=%s",
        processed,
        migrated,
    )


if __name__ == "__main__":
    run()