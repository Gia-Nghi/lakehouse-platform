import io
import json
import logging
import os
import random
import time
from datetime import timedelta
import uuid
import yaml

import pandas as pd
import pendulum
from pytrends.request import TrendReq

from src.common.minio import create_minio_client, has_objects, put_jsonl_object
from src.common.retry import retry

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")


class BaseIngestionJob:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self.load_config(config_path)
        self.batch_id = str(uuid.uuid4())

    def load_config(self, config_path: str) -> dict:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def run(self):
        raw_data = self.fetch()
        records = self.parse(raw_data)
        return self.save(records)


class GoogleTrendsCollector:
    def __init__(
        self,
        geo,
        target_region,
        start_date,
        end_date,
        window_days,
        load_type,
        sleep_min_seconds,
        sleep_max_seconds,
        pytrends_hl,
        pytrends_tz,
        pytrends_timeout_connect,
        pytrends_timeout_read,
    ):
        self.geo = geo
        self.target_region = target_region
        self.start_date = start_date
        self.end_date = end_date
        self.window_days = window_days
        self.load_type = load_type
        self.sleep_min_seconds = sleep_min_seconds
        self.sleep_max_seconds = sleep_max_seconds

        self.client = TrendReq(
            hl=pytrends_hl,
            tz=pytrends_tz,
            timeout=(pytrends_timeout_connect, pytrends_timeout_read),
        )

    def generate_date_windows(self):
        windows = []
        current = self.start_date

        while current < self.end_date:
            window_end = min(
                current + timedelta(days=self.window_days),
                self.end_date,
            )

            windows.append(
                (
                    current.strftime("%Y-%m-%d"),
                    window_end.strftime("%Y-%m-%d"),
                )
            )

            current = window_end

        return windows

    @retry(
        max_attempts=5,
        delay_seconds=60,
        backoff_factor=2,
        max_delay_seconds=600,
    )
    def _fetch_window_with_retry(self, keywords, timeframe):
        self.client.build_payload(
            kw_list=keywords,
            geo=self.geo,
            timeframe=timeframe,
        )
        return self.client.interest_over_time()

    def collect(self, keyword_group, keywords):
        all_frames = []
        windows = self.generate_date_windows()

        for start_date, end_date in windows:
            timeframe = f"{start_date} {end_date}"

            logger.info(
                "Fetching Google Trends group=%s, timeframe=%s, keywords=%s",
                keyword_group,
                timeframe,
                keywords,
            )

            sleep_seconds = random.randint(
                self.sleep_min_seconds,
                self.sleep_max_seconds,
            )
            time.sleep(sleep_seconds)

            try:
                df = self._fetch_window_with_retry(keywords, timeframe)

                if not df.empty:
                    df["timeframe_start"] = start_date
                    df["timeframe_end"] = end_date
                    all_frames.append(df)

            except Exception as e:
                logger.error(
                    "Failed Google Trends window %s for group=%s: %s",
                    timeframe,
                    keyword_group,
                    str(e),
                )
                raise

        if not all_frames:
            return []

        df = pd.concat(all_frames)
        df = df[~df.index.duplicated(keep="last")]
        df = df.sort_index().reset_index()

        collected_at_ms = int(time.time() * 1000)

        records = []

        for row in df.to_dict(orient="records"):
            records.append(
                {
                    "load_type": self.load_type,
                    "date": row["date"].date().isoformat(),
                    "keyword_group": keyword_group,
                    "values": {
                        kw: int(row.get(kw, 0))
                        for kw in keywords
                    },
                    "isPartial": bool(row.get("isPartial", False)),
                    "timeframe_start": row.get("timeframe_start"),
                    "timeframe_end": row.get("timeframe_end"),
                    "collected_at_ms": collected_at_ms,
                }
            )

        return records


class GoogleTrendsIngestionJob(BaseIngestionJob):
    def __init__(self, config_path: str):
        super().__init__(config_path)

        self.minio_client = create_minio_client()

        self.bronze_prefix = self.config.get(
            "bronze_prefix",
            "bronze/user_interest/google_trends",
        )

    def get_load_plan(self):
        has_data = has_objects(
            client=self.minio_client,
            bucket=MINIO_BUCKET,
            prefix=self.bronze_prefix,
        )

        today = pendulum.now(TZ).date()

        if has_data:
            daily_lookback_days = int(
                self.config.get("daily_lookback_days", 7)
            )

            return {
                "load_type": "daily",
                "start_date": today - timedelta(days=daily_lookback_days),
                "end_date": today,
                "window_days": int(self.config.get("daily_window_days", 30)),
            }

        initial_start_date = self.config.get(
            "initial_start_date",
            "2025-11-01",
        )

        return {
            "load_type": "initial",
            "start_date": pendulum.parse(initial_start_date).date(),
            "end_date": today,
            "window_days": int(self.config.get("initial_window_days", 270)),
        }

    def fetch(self):
        load_plan = self.get_load_plan()

        logger.info(
            "Google Trends load plan: load_type=%s, range=%s to %s",
            load_plan["load_type"],
            load_plan["start_date"],
            load_plan["end_date"],
        )

        collector = GoogleTrendsCollector(
            geo=self.config.get("geo", "VN"),
            target_region=self.config.get("target_region", "Vietnam"),
            start_date=load_plan["start_date"],
            end_date=load_plan["end_date"],
            window_days=load_plan["window_days"],
            load_type=load_plan["load_type"],
            sleep_min_seconds=int(self.config.get("sleep_min_seconds", 15)),
            sleep_max_seconds=int(self.config.get("sleep_max_seconds", 30)),
            pytrends_hl=self.config.get("pytrends_hl", "vi-VN"),
            pytrends_tz=int(self.config.get("pytrends_tz", 420)),
            pytrends_timeout_connect=int(
                self.config.get("pytrends_timeout_connect", 10)
            ),
            pytrends_timeout_read=int(
                self.config.get("pytrends_timeout_read", 25)
            ),
        )

        all_records = []

        for keyword_group, keywords in self.config.get("keyword_groups", {}).items():
            records = collector.collect(keyword_group, keywords)
            all_records.extend(records)

        return all_records

    def parse(self, raw_data):
        return raw_data

    def build_metadata(self, records, now):
        record_dates = [record["date"] for record in records]

        return {
            "source": self.config.get("source", "google_trends"),
            "domain": self.config.get("domain", "user_interest"),
            "entity_type": self.config.get(
                "entity_type",
                "search_interest_timeseries",
            ),
            "load_type": records[0]["load_type"],
            "target_region": self.config.get("target_region", "Vietnam"),
            "geo": self.config.get("geo", "VN"),
            "min_record_date": min(record_dates),
            "max_record_date": max(record_dates),
            "initial_window_days": int(
                self.config.get("initial_window_days", 270)
            ),
            "daily_lookback_days": int(
                self.config.get("daily_lookback_days", 7)
            ),
            "daily_window_days": int(
                self.config.get("daily_window_days", 30)
            ),
            "keyword_groups": self.config.get("keyword_groups", {}),
            "record_count": len(records),
            "saved_at": now.to_iso8601_string(),
        }

    def _get_next_sequence_id(self, base_prefix: str, date_str: str) -> str:
        try:
            objects = self.minio_client.list_objects(
                MINIO_BUCKET,
                prefix=base_prefix + "/",
                recursive=True,
            )

            seq_numbers = []

            for obj in objects:
                file_name = os.path.basename(obj.object_name)

                if file_name.startswith("google_trends_") and file_name.endswith(".jsonl"):
                    name_part = file_name.replace("google_trends_", "").replace(".jsonl", "")

                    try:
                        seq_str = name_part.split("_")[-1]
                        seq_numbers.append(int(seq_str))
                    except (ValueError, IndexError):
                        continue

            if not seq_numbers:
                return "0000"

            return f"{max(seq_numbers) + 1:04d}"

        except Exception as e:
            logger.warning(
                "Could not list objects to determine sequence ID, fallback to timestamp. Error: %s",
                e,
            )
            return pendulum.now(TZ).strftime("%H%M%S")

    def save(self, records):
        if not records:
            logger.info("No Google Trends records to save.")
            return None

        now = pendulum.now(TZ)
        date_str = now.strftime("%Y-%m-%d")
        file_date_str = now.strftime("%Y%m%d")

        base_prefix = f"{self.bronze_prefix}/date={date_str}"
        
        # Lấy số thứ tự tự động tăng cho ngày hiện tại
        seq_id = self._get_next_sequence_id(base_prefix, date_str)

        object_name = (
            f"{base_prefix}/google_trends_{file_date_str}_{seq_id}.jsonl"
        )

        path = put_jsonl_object(
            client=self.minio_client,
            bucket=MINIO_BUCKET,
            object_name=object_name,
            records=records,
        )

        metadata = self.build_metadata(records, now)

        metadata_bytes = json.dumps(
            metadata,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")

        metadata_object_name = (
            f"{base_prefix}/metadata_{file_date_str}_{seq_id}.json"
        )

        self.minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=metadata_object_name,
            data=io.BytesIO(metadata_bytes),
            length=len(metadata_bytes),
            content_type="application/json",
        )

        logger.info("Saved Google Trends data to: %s", path)
        logger.info("Saved Google Trends metadata to: %s", metadata_object_name)

        return {
            "path": path,
            "metadata_path": metadata_object_name,
            "record_count": len(records),
        }


def run():
    config_path = os.getenv(
        "GOOGLE_TRENDS_CONFIG_PATH",
        "/opt/airflow/config/sources/google_trends.yaml",
    )

    job = GoogleTrendsIngestionJob(config_path)
    return job.run()

 
if __name__ == "__main__":
    run()