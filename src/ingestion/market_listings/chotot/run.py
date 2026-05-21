import os
import json
import time
import random
import logging

from kafka import KafkaProducer

from src.ingestion.market_listings.chotot.client import ChototClient
from src.ingestion.market_listings.chotot.config import load_chotot_config
from src.ingestion.market_listings.chotot.parser import build_raw_record

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: str, retries: int) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
        retries=retries,
        acks="all",
    )


def create_client(config: dict) -> ChototClient:
    request_cfg = config["request"]
    api_cfg = config["api"]

    headers = {
        "User-Agent": request_cfg["user_agent"]
    }

    return ChototClient(
        list_url=api_cfg["list_url"],
        detail_url=api_cfg["detail_url"],
        params=api_cfg.get("params", {}),
        headers=headers,
        retries=request_cfg.get("retries", 3),
        timeout=request_cfg.get("timeout", 10),
        sleep_base=request_cfg.get("sleep_base", 1.0),
        backoff_factor=request_cfg.get("backoff_factor", 2),
    )


def crawl_once_and_send(
    config: dict,
    client: ChototClient,
    producer: KafkaProducer,
) -> int:
    source = config["source"]
    category = config["category"]

    kafka_topic = config["kafka"]["topic"]
    runtime_cfg = config["runtime"]

    max_records = runtime_cfg.get(
        "max_records_per_poll",
        config["api"]["params"].get("limit", 50),
    )

    ads = client.get_current_ads(limit=max_records)
    sent_count = 0

    for ad in ads:
        list_id = ad.get("list_id")
        if not list_id:
            continue

        detail = client.get_detail(list_id)
        if not detail:
            continue

        record = build_raw_record(
            ad=ad,
            detail=detail,
            source=source,
            category=category,
        )

        producer.send(
            kafka_topic,
            key=list_id,
            value=record,
        )

        sent_count += 1
        time.sleep(random.uniform(0.2, 0.6))

    producer.flush()
    logger.info("Sent %s records to Kafka topic=%s", sent_count, kafka_topic)

    return sent_count


def run_loop():
    config = load_chotot_config()

    runtime_cfg = config["runtime"]
    kafka_cfg = config["kafka"]
    request_cfg = config["request"]

    mode = runtime_cfg.get("mode", "streaming")
    poll_interval_seconds = runtime_cfg.get("poll_interval_seconds", 300)
    stop_after_polls = runtime_cfg.get("stop_after_polls")

    client = create_client(config)

    producer = create_producer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        retries=request_cfg.get("retries", 3),
    )

    logger.info(
        "Start Chotot crawler mode=%s poll_interval=%ss topic=%s",
        mode,
        poll_interval_seconds,
        kafka_cfg["topic"],
    )

    poll_count = 0

    while True:
        try:
            crawl_once_and_send(config, client, producer)
            poll_count += 1
        except Exception as e:
            logger.exception("Crawler loop error: %s", e)

        if mode == "batch":
            break

        if stop_after_polls is not None and poll_count >= stop_after_polls:
            break

        time.sleep(poll_interval_seconds)


def send_mock_message():
    config = load_chotot_config()

    producer = create_producer(
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        retries=config["request"].get("retries", 3),
    )

    producer.send(
        config["kafka"]["topic"],
        key="mock",
        value={
            "source": config["source"],
            "category": config["category"],
            "type": "mock_test",
            "message": "hello from chotot crawler",
            "crawled_at": int(time.time() * 1000),
        },
    )
    producer.flush()
    logger.info("Mock message sent")


if __name__ == "__main__":
    mode = os.getenv("MODE", "run")

    if mode == "mock":
        send_mock_message()
    else:
        run_loop()