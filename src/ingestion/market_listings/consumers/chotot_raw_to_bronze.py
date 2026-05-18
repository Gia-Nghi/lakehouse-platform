import os
from datetime import datetime, timezone

from common.io.kafka_consumer import create_kafka_consumer
from common.io.minio_client import create_minio_client, ensure_bucket, upload_json_bytes
from common.utils.logger import get_logger

logger = get_logger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "chotot_raw")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "chotot-raw-to-bronze")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "20"))
CONSUMER_MAX_RECORDS = int(os.getenv("CONSUMER_MAX_RECORDS", "20"))


def build_object_name(now: datetime) -> str:
    dt = now.strftime("%Y-%m-%d")
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    return (
        f"bronze/market_listings/chotot/listings/"
        f"dt={dt}/chotot_listings_raw_{ts}.json"
    )


def build_bronze_payload(records: list[dict], now: datetime) -> dict:
    return {
        "source": "chotot",
        "domain": "market_listings",
        "entity_type": "listings",
        "target_region": "Ho Chi Minh City",
        "collected_at_ms": int(now.timestamp() * 1000),
        "record_count": len(records),
        "payload": records,
    }


def flush_to_minio(minio_client, records: list[dict]) -> None:
    if not records:
        return

    now = datetime.now(timezone.utc)
    object_name = build_object_name(now)
    payload = build_bronze_payload(records, now)

    upload_json_bytes(minio_client, MINIO_BUCKET, object_name, payload)
    logger.info(
        "Uploaded %s records to s3://%s/%s",
        len(records),
        MINIO_BUCKET,
        object_name,
    )


def main() -> None:
    consumer = create_kafka_consumer(KAFKA_TOPIC, KAFKA_GROUP_ID)
    minio_client = create_minio_client()
    ensure_bucket(minio_client, MINIO_BUCKET)

    buffer: list[dict] = []
    consumed = 0

    logger.info("Started consumer topic=%s bucket=%s", KAFKA_TOPIC, MINIO_BUCKET)

    for msg in consumer:
        buffer.append(msg.value)
        consumed += 1

        if len(buffer) >= FLUSH_EVERY:
            flush_to_minio(minio_client, buffer)
            buffer = []

        if consumed >= CONSUMER_MAX_RECORDS:
            break

    flush_to_minio(minio_client, buffer)
    consumer.close()

    logger.info("Consumer completed. Consumed=%s", consumed)


if __name__ == "__main__":
    main()