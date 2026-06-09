import io
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from minio import Minio


class GoogleTrendsBronzeWriter:
    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: str = "lakehouse",
        prefix: str = "bronze/user_interest/google_trends",
        secure: bool = False,
    ):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.secure = secure

        if not self.access_key or not self.secret_key:
            raise ValueError(
                "Missing MinIO credentials. "
                "Please set MINIO_ROOT_USER and MINIO_ROOT_PASSWORD."
            )

        self.client = Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def ensure_bucket(self) -> None:
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def has_existing_data(self) -> bool:
        self.ensure_bucket()

        objects = self.client.list_objects(
            bucket_name=self.bucket,
            prefix=self.prefix + "/",
            recursive=True,
        )

        for obj in objects:
            if obj.object_name.endswith(".jsonl"):
                return True

        return False

    def get_next_file_index(self, run_date: str) -> int:
        """
        Tự tìm index tiếp theo trong cùng date partition.

        Ví dụ đã có:
        google_trends_20260525_0000.jsonl
        google_trends_20260525_0001.jsonl

        Thì lần sau trả về 2.
        """
        self.ensure_bucket()

        compact_date = run_date.replace("-", "")
        date_prefix = f"{self.prefix}/date={run_date}/"

        pattern = re.compile(
            rf"google_trends_{compact_date}_(\d{{4}})\.jsonl$"
        )

        max_index = -1

        objects = self.client.list_objects(
            bucket_name=self.bucket,
            prefix=date_prefix,
            recursive=True,
        )

        for obj in objects:
            file_name = obj.object_name.split("/")[-1]
            match = pattern.match(file_name)

            if match:
                index = int(match.group(1))
                max_index = max(max_index, index)

        return max_index + 1

    def write_jsonl(
        self,
        records: List[Dict[str, Any]],
        run_date: Optional[str] = None,
        file_index: Optional[int] = None,
    ) -> str:
        if not records:
            raise ValueError("No records to write to MinIO.")

        self.ensure_bucket()

        if run_date is None:
            run_date = datetime.utcnow().strftime("%Y-%m-%d")

        if file_index is None:
            file_index = self.get_next_file_index(run_date)

        compact_date = run_date.replace("-", "")

        file_name = (
            f"google_trends_{compact_date}_{file_index:04d}.jsonl"
        )

        object_name = (
            f"{self.prefix}/"
            f"date={run_date}/"
            f"{file_name}"
        )

        content = "\n".join(
            json.dumps(record, ensure_ascii=False)
            for record in records
        ) + "\n"

        data = content.encode("utf-8")

        self.client.put_object(
            bucket_name=self.bucket,
            object_name=object_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/x-ndjson",
        )

        return object_name