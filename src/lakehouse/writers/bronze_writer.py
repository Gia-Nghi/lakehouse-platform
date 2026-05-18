import io
import json
import math
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.lakehouse.writers.base_writer import BaseMinioWriter


def sanitize_json(value):
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    if isinstance(value, dict):
        return {k: sanitize_json(v) for k, v in value.items()}

    if isinstance(value, list):
        return [sanitize_json(v) for v in value]

    return value


class BronzeWriter(BaseMinioWriter):
    def __init__(self, bucket: str = "bronze"):
        super().__init__()
        self.bucket = bucket
        self.ensure_bucket(bucket)

    def write_jsonl(
        self,
        table: str,
        records: List[Dict[str, Any]],
        batch_id: str,
    ) -> str:
        now = datetime.now(timezone.utc)
        date = now.strftime("%Y-%m-%d")

        clean_records = [sanitize_json(r) for r in records]

        content = "\n".join(
            json.dumps(r, ensure_ascii=False, allow_nan=False)
            for r in clean_records
        )

        data = content.encode("utf-8")

        object_name = f"table={table}/date={date}/batch_id={batch_id}/data.jsonl"

        self.client.put_object(
            self.bucket,
            object_name,
            io.BytesIO(data),
            length=len(data),
            content_type="application/jsonl",
        )

        return f"s3://{self.bucket}/{object_name}"