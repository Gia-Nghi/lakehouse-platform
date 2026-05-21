import os
import io
import json
import signal
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error

from src.ingestion.market_listings.chotot.config import load_chotot_config
from src.ingestion.market_listings.chotot.parser import get_record_id

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

MAX_RECORDS_PER_FILE = int(os.getenv("MAX_RECORDS_PER_FILE", "1000"))

TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def create_minio_client(bucket_name: str) -> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    return client


def get_event_date(record: dict):
    ts_ms = record.get("crawled_at")

    if ts_ms is None:
        return datetime.now(TZ).date()

    return datetime.fromtimestamp(ts_ms / 1000.0, tz=TZ).date()


def build_object_name(
    source: str,
    bronze_prefix: str,
    event_date,
    file_index: int,
) -> str:
    date_str = event_date.strftime("%Y-%m-%d")
    compact_date = event_date.strftime("%Y%m%d")

    return (
        f"{bronze_prefix}/"
        f"date={date_str}/"
        f"{source}_{compact_date}_{file_index:04d}.jsonl"
    )


def flush_buffer_to_minio(
    client: Minio,
    bucket_name: str,
    object_name: str,
    buffer: list[dict],
) -> bool:
    if not buffer:
        return True

    payload = "\n".join(
        json.dumps(record, ensure_ascii=False)
        for record in buffer
    ) + "\n"

    data = payload.encode("utf-8")

    try:
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )

        logger.info(
            "Flushed %s records to s3://%s/%s",
            len(buffer),
            bucket_name,
            object_name,
        )

        return True

    except S3Error as e:
        logger.error("MinIO write error: %s", e)
        return False


def flush_current_buffer(
    client: Minio,
    consumer: KafkaConsumer,
    bucket_name: str,
    source: str,
    bronze_prefix: str,
    event_date,
    file_index: int,
    buffer: list[dict],
) -> bool:
    if not buffer or event_date is None:
        return True

    object_name = build_object_name(
        source=source,
        bronze_prefix=bronze_prefix,
        event_date=event_date,
        file_index=file_index,
    )

    ok = flush_buffer_to_minio(
        client=client,
        bucket_name=bucket_name,
        object_name=object_name,
        buffer=buffer,
    )

    if ok:
        buffer.clear()
        consumer.commit()

    return ok


def _handle_shutdown_signal(signum, frame):
    raise KeyboardInterrupt()


signal.signal(signal.SIGTERM, _handle_shutdown_signal)
signal.signal(signal.SIGINT, _handle_shutdown_signal)


def run():
    config = load_chotot_config()

    source = config["source"]

    kafka_cfg = config["kafka"]
    storage_cfg = config["storage"]
    runtime_cfg = config["runtime"]

    kafka_topic = kafka_cfg["topic"]
    kafka_bootstrap_servers = kafka_cfg["bootstrap_servers"]

    kafka_group_id = os.getenv(
        "KAFKA_GROUP_ID_CHOTOT_BRONZE",
        "chotot_raw_to_bronze",
    )

    bronze_bucket = storage_cfg["bronze_bucket"]

    bronze_prefix = storage_cfg.get(
        "bronze_prefix",
        source,
    )

    deduplicate_key = runtime_cfg.get(
        "deduplicate_key",
        "list_id",
    )

    client = create_minio_client(bronze_bucket)

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers.split(","),
        group_id=kafka_group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
    )

    logger.info(
        (
            "Start consuming "
            "topic=%s "
            "group_id=%s "
            "bucket=%s "
            "prefix=%s"
        ),
        kafka_topic,
        kafka_group_id,
        bronze_bucket,
        bronze_prefix,
    )

    buffer: list[dict] = []

    current_date = None

    file_index_for_date = 0

    seen_ids_for_date: set = set()

    try:
        while True:

            records = consumer.poll(timeout_ms=1000)

            for _, messages in records.items():

                for msg in messages:

                    record = msg.value

                    event_date = get_event_date(record)

                    if current_date is None:
                        current_date = event_date

                    # Sang ngày mới -> flush ngày cũ
                    if event_date != current_date:

                        ok = flush_current_buffer(
                            client=client,
                            consumer=consumer,
                            bucket_name=bronze_bucket,
                            source=source,
                            bronze_prefix=bronze_prefix,
                            event_date=current_date,
                            file_index=file_index_for_date,
                            buffer=buffer,
                        )

                        if ok:
                            file_index_for_date += 1

                        seen_ids_for_date.clear()

                        current_date = event_date

                        file_index_for_date = 0

                    record_id = get_record_id(
                        record,
                        deduplicate_key=deduplicate_key,
                    )

                    if record_id is not None:

                        if record_id in seen_ids_for_date:
                            continue

                        seen_ids_for_date.add(record_id)

                    buffer.append(record)

                    # Đủ số records -> flush
                    if len(buffer) >= MAX_RECORDS_PER_FILE:

                        ok = flush_current_buffer(
                            client=client,
                            consumer=consumer,
                            bucket_name=bronze_bucket,
                            source=source,
                            bronze_prefix=bronze_prefix,
                            event_date=current_date,
                            file_index=file_index_for_date,
                            buffer=buffer,
                        )

                        if ok:
                            file_index_for_date += 1

            # Không có record mới nhưng đã sang ngày
            today = datetime.now(TZ).date()

            if (
                buffer
                and current_date is not None
                and today != current_date
            ):

                logger.info(
                    (
                        "Day changed without new records. "
                        "Flushing previous day buffer date=%s"
                    ),
                    current_date,
                )

                ok = flush_current_buffer(
                    client=client,
                    consumer=consumer,
                    bucket_name=bronze_bucket,
                    source=source,
                    bronze_prefix=bronze_prefix,
                    event_date=current_date,
                    file_index=file_index_for_date,
                    buffer=buffer,
                )

                if ok:

                    seen_ids_for_date.clear()

                    current_date = None

                    file_index_for_date = 0

    except KeyboardInterrupt:

        logger.info("Shutdown signal received")

    finally:

        try:

            # Flush buffer cuối cùng khi shutdown
            if current_date is not None and buffer:

                flush_current_buffer(
                    client=client,
                    consumer=consumer,
                    bucket_name=bronze_bucket,
                    source=source,
                    bronze_prefix=bronze_prefix,
                    event_date=current_date,
                    file_index=file_index_for_date,
                    buffer=buffer,
                )

        finally:

            consumer.close()

            logger.info("Consumer stopped")


if __name__ == "__main__":
    run()