import json
from typing import Dict, List

from src.lakehouse.writers.base_writer import BaseMinioWriter


class JsonlMinioReader(BaseMinioWriter):
    def read_prefix(self, bucket: str, prefix: str) -> List[Dict]:
        records = []

        objects = self.client.list_objects(
            bucket,
            prefix=prefix,
            recursive=True,
        )

        for obj in objects:
            if not obj.object_name.endswith(".jsonl"):
                continue

            response = self.client.get_object(bucket, obj.object_name)

            try:
                content = response.read().decode("utf-8")

                for line in content.splitlines():
                    if line.strip():
                        records.append(json.loads(line))

            finally:
                response.close()
                response.release_conn()

        return records