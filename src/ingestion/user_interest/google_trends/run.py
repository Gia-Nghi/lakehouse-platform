import io
import json
import logging
import os

import pendulum
from datetime import timedelta

from src.common.minio import (
    create_minio_client,
    has_objects,
    put_jsonl_object,
)
from src.ingestion.base import BaseIngestionJob
from src.ingestion.user_interest.google_trends.collector import (
    GoogleTrendsCollector,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
BRONZE_PREFIX = "bronze/user_interest/google_trends"


class GoogleTrendsIngestionJob(BaseIngestionJob):
    source_name = "google_trends"

    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.minio_client = create_minio_client()

    def get_load_plan(self):
        has_data = has_objects(
            client=self.minio_client,
            bucket=MINIO_BUCKET,
            prefix=BRONZE_PREFIX,
        )

        today = pendulum.now(TZ).date()

        if has_data:
            daily_lookback_days = int(
                self.config.get("daily_lookback_days", 7)
            )

            start_date = today - timedelta(days=daily_lookback_days)
            end_date = today

            return {
                "load_type": "daily",
                "start_date": start_date,
                "end_date": end_date,
                "window_days": int(
                    self.config.get("daily_window_days", 30)
                ),
            }

        initial_months_back = int(
            self.config.get("initial_months_back", 12)
        )

        start_date = today - timedelta(days=initial_months_back * 30)
        end_date = today

        return {
            "load_type": "initial",
            "start_date": start_date,
            "end_date": end_date,
            "window_days": int(
                self.config.get("initial_window_days", 30)
            ),
        }
    def fetch(self):
        load_plan = self.get_load_plan()

        logger.info(
            "Google Trends load_type=%s start_date=%s end_date=%s window_days=%s",
            load_plan["load_type"],
            load_plan["start_date"],
            load_plan["end_date"],
            load_plan["window_days"],
        )

        collector = GoogleTrendsCollector(
            geo=self.config.get("geo", "VN"),
            target_region=self.config.get(
                "target_region",
                "Vietnam",
            ),
            start_date=load_plan["start_date"],
            end_date=load_plan["end_date"],
            window_days=load_plan["window_days"],
            load_type=load_plan["load_type"],
        )

        all_records = []

        keyword_groups = self.config.get(
            "keyword_groups",
            {},
        )

        if not keyword_groups:
            logger.warning(
                "No keyword_groups found in config."
            )
            return all_records

        for keyword_group, keywords in keyword_groups.items():
            logger.info(
                "Collecting keyword_group=%s keywords=%s",
                keyword_group,
                keywords,
            )

            records = collector.collect(
                keyword_group=keyword_group,
                keywords=keywords,
            )

            all_records.extend(records)

        logger.info(
            "Collected %s Google Trends records.",
            len(all_records),
        )

        return all_records

    def parse(self, raw_data):
        return raw_data

    def save(self, records):
        if not records:
            logger.warning(
                "No Google Trends records to save."
            )
            return None

        now = pendulum.now(TZ)

        date_str = now.strftime("%Y-%m-%d")
        file_date = now.strftime("%Y%m%d")

        base_prefix = (
            f"{BRONZE_PREFIX}/"
            f"date={date_str}"
        )

        # =========================
        # DATA FILE
        # =========================
        object_name = (
            f"{base_prefix}/"
            f"google_trends_{file_date}_0000.jsonl"
        )

        path = put_jsonl_object(
            client=self.minio_client,
            bucket=MINIO_BUCKET,
            object_name=object_name,
            records=records,
        )

        # =========================
        # METADATA FILE
        # =========================
        dates = [record["date"] for record in records if record.get("date")]

        metadata = {
            "source": "google_trends",
            "domain": "user_interest",
            "entity_type": (
                "search_interest_timeseries"
            ),
            "load_type": records[0].get(
                "load_type",
                "unknown",
            ),
            "target_region": self.config.get(
                "target_region"
            ),
            "geo": self.config.get("geo"),
            "min_record_date": min(dates) if dates else None,
            "max_record_date": max(dates) if dates else None,
            "initial_months_back": self.config.get("initial_months_back"),
            "initial_window_days": self.config.get("initial_window_days"),
            "daily_lookback_days": self.config.get("daily_lookback_days"),
            "daily_window_days": self.config.get("daily_window_days"),
            "keyword_groups": self.config.get(
                "keyword_groups"
            ),
            "record_count": len(records),
            "saved_at": (
                now.to_iso8601_string()
            ),
        }

        metadata_object_name = (
            f"{base_prefix}/"
            f"metadata_{file_date}.json"
        )

        metadata_bytes = json.dumps(
            metadata,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")

        self.minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=metadata_object_name,
            data=io.BytesIO(metadata_bytes),
            length=len(metadata_bytes),
            content_type="application/json",
        )

        logger.info(
            "Saved Google Trends data to %s",
            path,
        )

        logger.info(
            (
                "Saved Google Trends metadata "
                "to s3://%s/%s"
            ),
            MINIO_BUCKET,
            metadata_object_name,
        )

        return {
            "path": path,
            "metadata_path": (
                f"s3://{MINIO_BUCKET}/"
                f"{metadata_object_name}"
            ),
            "record_count": len(records),
        }


def run():
    job = GoogleTrendsIngestionJob(
        "/opt/airflow/config/sources/google_trends.yaml"
    )

    return job.run()


if __name__ == "__main__":
    run()