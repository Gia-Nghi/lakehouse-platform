import os
import io
import json
import signal
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Any

import yaml
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error

from src.common.metadata import build_ingestion_metadata


def load_config() -> dict:
    config_path = os.getenv(
        "CHOTOT_CONFIG_PATH",
        "/opt/airflow/config/sources/chotot.yaml",
    )

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


config = load_config()

KAFKA_CONFIG = config.get("kafka", {})
STORAGE_CONFIG = config.get("storage", {})
BRONZE_CONFIG = config.get("bronze", {})

TIMEZONE = BRONZE_CONFIG.get("timezone", "Asia/Ho_Chi_Minh")
TZ = ZoneInfo(TIMEZONE)

SOURCE_NAME = config.get("source", "chotot")
CATEGORY = config.get("category", "market_listings")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    KAFKA_CONFIG.get("bootstrap_servers", "kafka:29092"),
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    KAFKA_CONFIG.get("topic", "chotot_raw"),
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID_CHOTOT_BRONZE",
    KAFKA_CONFIG.get("group_id_bronze", "chotot_raw_to_bronze"),
)

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

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    STORAGE_CONFIG.get("bronze_bucket", "lakehouse"),
)

MINIO_SECURE = os.getenv(
    "MINIO_SECURE",
    str(STORAGE_CONFIG.get("secure", False)),
).lower() == "true"

BRONZE_PREFIX = os.getenv(
    "BRONZE_PREFIX_CHOTOT",
    STORAGE_CONFIG.get("bronze_prefix", "bronze/market_listings/chotot"),
).rstrip("/")

MAX_RECORDS_PER_FILE = int(os.getenv(
    "MAX_RECORDS_PER_FILE",
    BRONZE_CONFIG.get("max_records_per_file", 10000),
))

PARTITION_COLUMN = BRONZE_CONFIG.get("partition_column", "crawled_at")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

_running = True


def _handle_shutdown_signal(signum, frame):
    global _running
    logger.info("Received shutdown signal=%s", signum)
    _running = False


signal.signal(signal.SIGTERM, _handle_shutdown_signal)
signal.signal(signal.SIGINT, _handle_shutdown_signal)


def create_minio_client() -> Minio:
    logger.info(
        "Connecting MinIO endpoint=%s bucket=%s secure=%s",
        MINIO_ENDPOINT,
        MINIO_BUCKET,
        MINIO_SECURE,
    )

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    if not client.bucket_exists(MINIO_BUCKET):
        logger.info("Bucket %s does not exist. Creating...", MINIO_BUCKET)
        client.make_bucket(MINIO_BUCKET)
    else:
        logger.info("Bucket %s already exists", MINIO_BUCKET)

    return client


def get_event_date(record: dict):
    ts_ms = record.get(PARTITION_COLUMN) or record.get("ingested_at")

    if ts_ms is None:
        return datetime.now(TZ).date()

    try:
        return datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=TZ).date()
    except Exception:
        logger.warning(
            "Invalid timestamp field=%s value=%s. Fallback to current date.",
            PARTITION_COLUMN,
            ts_ms,
        )
        return datetime.now(TZ).date()


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


def enrich_bronze_record(record: dict) -> dict:
    old_metadata = record.get("metadata", {})

    bronze_metadata = build_ingestion_metadata(
        source=record.get("source", SOURCE_NAME),
        category=record.get("category", CATEGORY),
        entity=record.get("entity", "market_listing"),
        ingestion_type="kafka_to_bronze",
        pipeline_name="chotot_raw_to_bronze",
        extra={
            "kafka_topic": KAFKA_TOPIC,
            "kafka_group_id": KAFKA_GROUP_ID,
            "bronze_bucket": MINIO_BUCKET,
            "bronze_prefix": BRONZE_PREFIX,
            "partition_column": PARTITION_COLUMN,
            "upstream_metadata": old_metadata,
        },
    )

    enriched = dict(record)
    enriched.update(bronze_metadata)

    return enriched


def build_object_name(event_date, file_index: int) -> str:
    date_str = event_date.strftime("%Y-%m-%d")
    ymd = event_date.strftime("%Y%m%d")

    return (
        f"{BRONZE_PREFIX}/"
        f"date={date_str}/"
        f"{SOURCE_NAME}_{ymd}_{file_index:04d}.jsonl"
    )


def flush_buffer_to_minio(
    client: Minio,
    buffer: list[dict],
    event_date,
    file_index: int,
) -> tuple[int, bool]:
    if not buffer:
        return file_index, True

    object_name = build_object_name(event_date, file_index)

    logger.info(
        "Flushing %s records to s3://%s/%s",
        len(buffer),
        MINIO_BUCKET,
        object_name,
    )

    lines = [json.dumps(rec, ensure_ascii=False) for rec in buffer]
    jsonl_str = "\n".join(lines) + "\n"
    data_bytes = jsonl_str.encode("utf-8")
    data_stream = io.BytesIO(data_bytes)

    try:
        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            data=data_stream,
            length=len(data_bytes),
            content_type="application/x-ndjson",
        )

        logger.info(
            "Wrote MinIO object successfully: s3://%s/%s size=%s bytes",
            MINIO_BUCKET,
            object_name,
            len(data_bytes),
        )

        return file_index + 1, True

    except S3Error as e:
        logger.error("MinIO write error: %s", e)
        return file_index, False

    except Exception as e:
        logger.exception("Unexpected MinIO write error: %s", e)
        return file_index, False


def create_consumer() -> KafkaConsumer:
    logger.info(
        "Connecting Kafka bootstrap_servers=%s topic=%s group_id=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        KAFKA_GROUP_ID,
    )

    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        auto_offset_reset="earliest",
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
    )


def run() -> None:
    client = create_minio_client()
    consumer = create_consumer()

    buffer: list[dict] = []
    current_date = None
    file_index_for_date = 0
    seen_ids_for_date: set = set()

    logger.info(
        "Starting Kafka to Bronze consumer source=%s category=%s topic=%s bucket=%s prefix=%s max_records_per_file=%s",
        SOURCE_NAME,
        CATEGORY,
        KAFKA_TOPIC,
        MINIO_BUCKET,
        BRONZE_PREFIX,
        MAX_RECORDS_PER_FILE,
    )

    try:
        global _running

        while _running:
            for msg in consumer:
                if not _running:
                    break

                record = msg.value
                event_date = get_event_date(record)

                if current_date is None:
                    current_date = event_date
                    file_index_for_date = 0

                if event_date != current_date:
                    logger.info(
                        "Date changed from %s to %s. Flushing old buffer...",
                        current_date,
                        event_date,
                    )

                    file_index_for_date, ok = flush_buffer_to_minio(
                        client=client,
                        buffer=buffer,
                        event_date=current_date,
                        file_index=file_index_for_date,
                    )

                    if ok:
                        buffer.clear()
                        consumer.commit()
                        seen_ids_for_date.clear()
                        current_date = event_date
                        file_index_for_date = 0
                    else:
                        logger.error("Flush failed. Not committing Kafka offsets.")
                        continue

                record_id = get_record_id(record)

                if record_id is not None:
                    if record_id in seen_ids_for_date:
                        logger.debug("Skip duplicate record_id=%s", record_id)
                        continue

                    seen_ids_for_date.add(record_id)
                else:
                    logger.warning("Record has no clear id. It will still be written.")

                bronze_record = enrich_bronze_record(record)
                buffer.append(bronze_record)

                if len(buffer) % 100 == 0 or len(buffer) <= 20:
                    logger.info(
                        "Buffered records=%s/%s for date=%s",
                        len(buffer),
                        MAX_RECORDS_PER_FILE,
                        current_date,
                    )

                if len(buffer) >= MAX_RECORDS_PER_FILE:
                    logger.info(
                        "Buffer reached max_records_per_file=%s. Flushing...",
                        MAX_RECORDS_PER_FILE,
                    )

                    file_index_for_date, ok = flush_buffer_to_minio(
                        client=client,
                        buffer=buffer,
                        event_date=current_date,
                        file_index=file_index_for_date,
                    )

                    if ok:
                        buffer.clear()
                        consumer.commit()
                    else:
                        logger.error("Flush failed. Not committing Kafka offsets.")

    finally:
        try:
            if buffer and current_date is not None:
                logger.info(
                    "Shutdown flush: %s records for date=%s",
                    len(buffer),
                    current_date,
                )

                _, ok = flush_buffer_to_minio(
                    client=client,
                    buffer=buffer,
                    event_date=current_date,
                    file_index=file_index_for_date,
                )

                if ok:
                    buffer.clear()
                    consumer.commit()

        finally:
            try:
                consumer.close()
            except Exception:
                pass

            logger.info("Kafka consumer closed")


if __name__ == "__main__":
    run()