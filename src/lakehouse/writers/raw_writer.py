import io
import json
import math
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.lakehouse.writers.base_writer import BaseMinioWriter


def sanitize_json(value: Any) -> Any:
    """đệ quy để chuyển đổi giá trị NaN (float) thành None (null trong JSON)"""
    if isinstance(value, float) and math.isnan(value):
        return None

    if isinstance(value, dict):
        return {k: sanitize_json(v) for k, v in value.items()}

    if isinstance(value, list):
        return [sanitize_json(v) for v in value]

    return value


class RawWriter(BaseMinioWriter):
    def __init__(self, bucket: str = "raw"):
        super().__init__()
        self.bucket = bucket
        self.ensure_bucket(bucket)

    def write_jsonl(
        self,
        source: str,
        records: List[Dict[str, Any]],
        batch_id: str,
    ) -> str:
        ingestion_time = datetime.now(timezone.utc)
        ingestion_date = ingestion_time.strftime("%Y-%m-%d")

        enriched = []
        for record in records:
            enriched.append(
                {
                    **record,
                    "_source": source,
                    "_batch_id": batch_id,
                    "_ingestion_time": ingestion_time.isoformat(),
                    "_ingestion_date": ingestion_date,
                }
            )

        # Xử lý triệt để các giá trị NaN trong danh sách records đã enriched
        enriched = [sanitize_json(r) for r in enriched]

        # Thực hiện dumps chuỗi với cấu hình an toàn chống NaN
        content = "\n".join(
            json.dumps(r, ensure_ascii=False, allow_nan=False) for r in enriched
        )
        data = content.encode("utf-8")

        object_name = (
            f"source={source}/ingestion_date={ingestion_date}/"
            f"batch_id={batch_id}/data.jsonl"
        )

        self.client.put_object(
            self.bucket,
            object_name,
            io.BytesIO(data),
            length=len(data),
            content_type="application/jsonl",
        )

        return f"s3://{self.bucket}/{object_name}"