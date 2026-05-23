import os
import json
import time
import random
import logging
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode

import requests
import yaml
from kafka import KafkaProducer

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


# =========================
# CONFIG FROM YAML
# =========================

SOURCE_NAME = config.get("source", "chotot")
CATEGORY = config.get("category", "market_listings")

API_CONFIG = config.get("api", {})
REQUEST_CONFIG = config.get("request", {})
KAFKA_CONFIG = config.get("kafka", {})
RUNTIME_CONFIG = config.get("runtime", {})

CHOTOT_LIST_URL = API_CONFIG["list_url"]
CHOTOT_DETAIL_URL = API_CONFIG["detail_url"]
API_PARAMS = API_CONFIG.get("params", {})

REQUEST_TIMEOUT = int(REQUEST_CONFIG.get("timeout", 10))
REQUEST_RETRIES = int(REQUEST_CONFIG.get("retries", 3))
REQUEST_SLEEP_BASE = float(REQUEST_CONFIG.get("sleep_base", 1.0))
REQUEST_BACKOFF_FACTOR = float(REQUEST_CONFIG.get("backoff_factor", 2.0))

HEADERS = {
    "User-Agent": REQUEST_CONFIG.get(
        "user_agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    )
}

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    KAFKA_CONFIG.get("bootstrap_servers", "kafka:29092"),
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    KAFKA_CONFIG.get("topic", "chotot_raw"),
)

BATCH_SIZE = int(os.getenv(
    "BATCH_SIZE",
    RUNTIME_CONFIG.get("max_records_per_poll", API_PARAMS.get("limit", 50)),
))

LOOP_SLEEP_SECONDS = int(os.getenv(
    "LOOP_SLEEP_SECONDS",
    RUNTIME_CONFIG.get("poll_interval_seconds", 300),
))

DETAIL_SLEEP_MIN = float(RUNTIME_CONFIG.get("detail_sleep_min", 0.2))
DETAIL_SLEEP_MAX = float(RUNTIME_CONFIG.get("detail_sleep_max", 0.6))

DEDUPLICATE_KEY = RUNTIME_CONFIG.get("deduplicate_key", "list_id")
STOP_AFTER_POLLS = RUNTIME_CONFIG.get("stop_after_polls")

MODE = os.getenv(
    "MODE",
    RUNTIME_CONFIG.get("mode", "streaming"),
)


# =========================
# LOGGING
# =========================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


# =========================
# URL BUILDER
# =========================

def build_list_url(limit: int) -> str:
    params = dict(API_PARAMS)
    params["limit"] = limit

    return f"{CHOTOT_LIST_URL}?{urlencode(params)}"


def build_detail_url(list_id: int) -> str:
    return CHOTOT_DETAIL_URL.format(list_id=list_id)


# =========================
# REQUEST WITH RETRY
# =========================

def request_with_retry(
    url: str,
    headers: Dict[str, str],
    retries: int = REQUEST_RETRIES,
    timeout: int = REQUEST_TIMEOUT,
    sleep_base: float = REQUEST_SLEEP_BASE,
    backoff_factor: float = REQUEST_BACKOFF_FACTOR,
) -> Optional[requests.Response]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)

            if resp.status_code == 200:
                return resp

            preview = resp.text[:200].replace("\n", " ")

            logger.warning(
                "Request failed url=%s status=%s attempt=%s/%s body=%s",
                url,
                resp.status_code,
                attempt,
                retries,
                preview,
            )

        except Exception as e:
            logger.error(
                "Network error url=%s attempt=%s/%s error=%s",
                url,
                attempt,
                retries,
                e,
            )

        sleep_seconds = sleep_base * (backoff_factor ** (attempt - 1))
        sleep_seconds += random.uniform(0, 0.5)
        time.sleep(sleep_seconds)

    return None


# =========================
# CHOTOT API
# =========================

def get_current_ads(limit: int) -> List[Dict[str, Any]]:
    url = build_list_url(limit)

    logger.info("Calling Chotot LIST API: %s", url)

    resp = request_with_retry(
        url=url,
        headers=HEADERS,
    )

    if not resp:
        logger.error("Cannot fetch Chotot listing API")
        return []

    try:
        data = resp.json()
        ads = data.get("ads", [])

        logger.info("Fetched %s ads from listing API", len(ads))

        return ads

    except Exception as e:
        logger.error("Cannot parse listing JSON: %s", e)
        return []


def get_detail(list_id: int) -> Optional[Dict[str, Any]]:
    url = build_detail_url(list_id)

    logger.debug("Calling Chotot DETAIL API: %s", url)

    resp = request_with_retry(
        url=url,
        headers=HEADERS,
    )

    if not resp:
        logger.warning("Cannot fetch detail for list_id=%s", list_id)
        return None

    try:
        return resp.json()

    except Exception as e:
        logger.error("Cannot parse detail JSON list_id=%s error=%s", list_id, e)
        return None


# =========================
# KAFKA
# =========================

def create_kafka_producer() -> KafkaProducer:
    logger.info("Connecting Kafka bootstrap_servers=%s", KAFKA_BOOTSTRAP_SERVERS)

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
        retries=REQUEST_RETRIES,
        linger_ms=100,
    )


def send_record(
    producer: KafkaProducer,
    topic: str,
    record: Dict[str, Any],
    key: Optional[Any] = None,
) -> bool:
    try:
        future = producer.send(topic, key=key, value=record)
        metadata = future.get(timeout=10)

        logger.info(
            "Sent record topic=%s partition=%s offset=%s key=%s",
            metadata.topic,
            metadata.partition,
            metadata.offset,
            key,
        )

        return True

    except Exception as e:
        logger.error("Cannot send record to Kafka: %s", e)
        return False


# =========================
# RECORD BUILDER
# =========================

def extract_record_id(
    ad: Dict[str, Any],
    detail: Optional[Dict[str, Any]] = None,
) -> Optional[Any]:
    detail = detail or {}

    return (
        ad.get(DEDUPLICATE_KEY)
        or ad.get("list_id")
        or detail.get(DEDUPLICATE_KEY)
        or detail.get("list_id")
        or detail.get("ad_id")
    )


def build_raw_record(ad: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    record_id = extract_record_id(ad, detail)

    base_metadata = build_ingestion_metadata(
        source=SOURCE_NAME,
        category=CATEGORY,
        entity="market_listing",
        ingestion_type="api_crawl",
        pipeline_name="chotot_crawler",
        extra={
            "raw_topic": KAFKA_TOPIC,
            "deduplicate_key": DEDUPLICATE_KEY,
        },
    )

    return {
        **base_metadata,
        "record_id": record_id,
        "list": ad,
        "detail": detail,
        "crawled_at": now_ms,
        "ingested_at": now_ms,
    }


# =========================
# CRAWLER
# =========================

def crawl_once_and_send(producer: KafkaProducer) -> int:
    ads = get_current_ads(limit=BATCH_SIZE)

    success_count = 0
    seen_ids = set()

    for ad in ads:
        list_id = extract_record_id(ad)

        if not list_id:
            logger.warning("Skip ad without id. deduplicate_key=%s", DEDUPLICATE_KEY)
            continue

        if list_id in seen_ids:
            logger.debug("Skip duplicated id in current batch: %s", list_id)
            continue

        seen_ids.add(list_id)

        logger.info("Fetching detail list_id=%s", list_id)

        detail = get_detail(list_id)

        if not detail:
            continue

        record = build_raw_record(ad, detail)

        ok = send_record(
            producer=producer,
            topic=KAFKA_TOPIC,
            record=record,
            key=list_id,
        )

        if ok:
            success_count += 1

        time.sleep(random.uniform(DETAIL_SLEEP_MIN, DETAIL_SLEEP_MAX))

    logger.info("Crawl batch done. Sent %s records", success_count)

    return success_count


def send_mock_message() -> None:
    producer = create_kafka_producer()

    now_ms = int(time.time() * 1000)

    base_metadata = build_ingestion_metadata(
        source=SOURCE_NAME,
        category=CATEGORY,
        entity="market_listing",
        ingestion_type="mock_test",
        pipeline_name="chotot_crawler",
        extra={
            "raw_topic": KAFKA_TOPIC,
        },
    )

    mock = {
        **base_metadata,
        "record_id": "mock",
        "type": "mock_test",
        "msg": "Hello from chotot crawler",
        "crawled_at": now_ms,
        "ingested_at": now_ms,
    }

    send_record(
        producer=producer,
        topic=KAFKA_TOPIC,
        record=mock,
        key="mock",
    )

    producer.flush()
    producer.close()

    logger.info("Mock message sent")


def run_loop() -> None:
    producer = create_kafka_producer()

    logger.info(
        "Starting Chotot crawler loop source=%s category=%s batch_size=%s sleep=%ss topic=%s",
        SOURCE_NAME,
        CATEGORY,
        BATCH_SIZE,
        LOOP_SLEEP_SECONDS,
        KAFKA_TOPIC,
    )

    poll_count = 0

    try:
        while True:
            try:
                poll_count += 1

                logger.info("Starting poll #%s", poll_count)

                sent = crawl_once_and_send(producer)

                producer.flush()

                logger.info("Kafka flush done. poll=%s sent=%s", poll_count, sent)

            except Exception as e:
                logger.exception("Crawler loop error: %s", e)

            if STOP_AFTER_POLLS is not None and poll_count >= int(STOP_AFTER_POLLS):
                logger.info("Reached stop_after_polls=%s. Stopping crawler.", STOP_AFTER_POLLS)
                break

            time.sleep(LOOP_SLEEP_SECONDS)

    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed")


if __name__ == "__main__":
    if MODE == "mock":
        send_mock_message()
    else:
        run_loop()