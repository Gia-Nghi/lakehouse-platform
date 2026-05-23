import os
import io
import json
import logging
from typing import Optional, Any

import yaml
from minio import Minio

from src.common.metadata import build_ingestion_metadata


# =========================
# CONFIG LOADER
# =========================

def load_config() -> dict:
    config_path = os.getenv(
        "CHOTOT_CONFIG_PATH",
        "/opt/airflow/config/sources/chotot.yaml",
    )

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


config = load_config()

STORAGE_CONFIG = config.get("storage", {})


# =========================
# LOGGING
# =========================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


# =========================
# CONFIG FROM YAML + ENV OVERRIDE
# =========================

SOURCE_NAME = config.get("source", "chotot")
CATEGORY = config.get("category", "market_listings")

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    STORAGE_CONFIG.get("endpoint", "minio:9000"),
)

MINIO_ACCESS_KEY_ENV = STORAGE_CONFIG.get("access_key_env", "MINIO_ROOT_USER")
MINIO_SECRET_KEY_ENV = STORAGE_CONFIG.get("secret_key_env", "MINIO_ROOT_PASSWORD")

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    os.getenv(MINIO_ACCESS_KEY_ENV, "minioadmin"),
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    os.getenv(MINIO_SECRET_KEY_ENV, "minioadmin123"),
)

MINIO_SECURE = os.getenv(
    "MINIO_SECURE",
    str(STORAGE_CONFIG.get("secure", False)),
).lower() == "true"

# Bucket cũ của project 1
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "bronze")

# Bucket mới của project 2
TARGET_BUCKET = os.getenv(
    "TARGET_BUCKET",
    STORAGE_CONFIG.get("bronze_bucket", "lakehouse"),
)

# Prefix cũ
LEGACY_PREFIX = os.getenv("LEGACY_PREFIX", "chotot/").strip("/")

# Prefix mới
TARGET_PREFIX = os.getenv(
    "TARGET_PREFIX",
    STORAGE_CONFIG.get("bronze_prefix", "bronze/market_listings/chotot"),
).strip("/")


# =========================
# MINIO CLIENT
# =========================

def create_minio_client() -> Minio:
    logger.info(
        "Connecting MinIO endpoint=%s source_bucket=%s target_bucket=%s",
        MINIO_ENDPOINT,
        SOURCE_BUCKET,
        TARGET_BUCKET,
    )

    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    if not client.bucket_exists(bucket_name):
        logger.info("Creating bucket=%s", bucket_name)
        client.make_bucket(bucket_name)
    else:
        logger.info("Bucket %s already exists", bucket_name)


# =========================
# HELPERS
# =========================

def get_record_id(record: dict) -> Optional[Any]:
    list_part = record.get("list") or {}
    detail_part = record.get("detail") or {}

    return (
        record.get("record_id")
        or list_part.get("list_id")
        or detail_part.get("list_id")
        or detail_part.get("ad_id")
        or record.get("id")
    )


def normalize_legacy_record(record: dict) -> dict:
    """
    Chuẩn hóa record cũ sang schema metadata chung.

    Record mới sẽ đồng bộ với output của:
    - chotot/run.py
    - chotot_raw_to_bronze.py
    """
    source = record.get("source", SOURCE_NAME)
    category = record.get("category", CATEGORY)
    entity = record.get("entity", "market_listing")

    base_metadata = build_ingestion_metadata(
        source=source,
        category=category,
        entity=entity,
        ingestion_type="legacy_migrated",
        pipeline_name="migrate_legacy_bronze",
        migrated_from=f"s3://{SOURCE_BUCKET}/{LEGACY_PREFIX}/",
        original_ingestion_type=record.get("ingestion_type"),
        extra={
            "source_bucket": SOURCE_BUCKET,
            "target_bucket": TARGET_BUCKET,
            "legacy_prefix": LEGACY_PREFIX,
            "target_prefix": TARGET_PREFIX,
        },
    )

    crawled_at = record.get("crawled_at")
    ingested_at = record.get("ingested_at") or crawled_at

    return {
        **base_metadata,
        "record_id": get_record_id(record),
        "list": record.get("list", {}),
        "detail": record.get("detail", {}),
        "crawled_at": crawled_at,
        "ingested_at": ingested_at,
    }


def build_target_object_name(source_object_name: str) -> str:
    """
    Input:
      chotot/date=2026-05-19/chotot_20260519_0000.jsonl

    Output:
      bronze/market_listings/chotot/date=2026-05-19/chotot_20260519_0000.jsonl
    """
    normalized_source_name = source_object_name.strip("/")
    parts = normalized_source_name.split("/")

    if len(parts) < 3:
        raise ValueError(f"Unexpected legacy object path: {source_object_name}")

    # Với path cũ: chotot/date=YYYY-MM-DD/file.jsonl
    date_part = parts[1]
    file_name = parts[-1]

    if not date_part.startswith("date="):
        raise ValueError(
            f"Cannot find date partition in path: {source_object_name}"
        )

    return f"{TARGET_PREFIX}/{date_part}/{file_name}"


# =========================
# MIGRATION
# =========================

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
        content_type="application/x-ndjson",
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


def run() -> None:
    client = create_minio_client()

    if not client.bucket_exists(SOURCE_BUCKET):
        raise RuntimeError(f"Source bucket does not exist: {SOURCE_BUCKET}")

    ensure_bucket_exists(client, TARGET_BUCKET)

    objects = client.list_objects(
        SOURCE_BUCKET,
        prefix=f"{LEGACY_PREFIX}/",
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