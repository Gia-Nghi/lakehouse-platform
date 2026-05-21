import uuid
import yaml
import logging


logger = logging.getLogger(__name__)


class BaseIngestionJob:
    source_name = "unknown"

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self.load_config(config_path)
        self.batch_id = str(uuid.uuid4())

    def load_config(self, config_path: str) -> dict:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def fetch(self):
        raise NotImplementedError

    def parse(self, raw_data):
        return raw_data

    def save(self, records):
        raise NotImplementedError(
            "Each ingestion job must implement its own save() method."
        )

    def run(self):
        logger.info("Starting ingestion job: %s", self.source_name)

        raw_data = self.fetch()
        records = self.parse(raw_data)
        result = self.save(records)

        logger.info("Finished ingestion job: %s", self.source_name)

        return result