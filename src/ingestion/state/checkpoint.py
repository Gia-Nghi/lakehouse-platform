import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


class LocalCheckpointStore:
    def __init__(self, path: str = "/opt/airflow/data/checkpoints"):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def _file(self, source: str) -> Path:
        return self.path / f"{source}.json"

    def read(self, source: str) -> Optional[Dict[str, Any]]:
        file_path = self._file(source)

        if not file_path.exists():
            return None

        return json.loads(file_path.read_text(encoding="utf-8"))

    def write_success(
        self,
        source: str,
        batch_id: str,
        output_path: str,
        records_count: int,
    ) -> None:
        payload = {
            "source": source,
            "batch_id": batch_id,
            "output_path": output_path,
            "records_count": records_count,
            "status": "success",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        self._file(source).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )