import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import yaml

from src.common.logger import get_logger
from src.ingestion.state.checkpoint import LocalCheckpointStore
from src.lakehouse.writers.raw_writer import RawWriter


class BaseIngestionJob(ABC):
    source_name: str

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self.load_config(config_path)
        self.logger = get_logger(self.__class__.__name__)
        self.raw_writer = RawWriter()
        self.checkpoint_store = LocalCheckpointStore()

    def load_config(self, config_path: str) -> Dict[str, Any]:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    @abstractmethod
    def fetch(self) -> Any:
        pass

    @abstractmethod
    def parse(self, raw_data: Any) -> List[Dict[str, Any]]:
        pass

    def run(self) -> Dict[str, Any]:
        batch_id = str(uuid.uuid4())

        self.logger.info(f"Starting ingestion source={self.source_name} batch_id={batch_id}")

        raw_data = self.fetch()
        records = self.parse(raw_data)

        output_path = self.raw_writer.write_jsonl(
            source=self.source_name,
            records=records,
            batch_id=batch_id,
        )

        self.checkpoint_store.write_success(
            source=self.source_name,
            batch_id=batch_id,
            output_path=output_path,
            records_count=len(records),
        )

        self.logger.info(
            f"Finished ingestion source={self.source_name} records={len(records)} path={output_path}"
        )

        return {
            "source": self.source_name,
            "batch_id": batch_id,
            "records_count": len(records),
            "output_path": output_path,
        }