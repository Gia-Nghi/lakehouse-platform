import uuid
from datetime import datetime, timezone

from src.common.logger import get_logger
from src.lakehouse.readers.jsonl_reader import JsonlMinioReader
from src.lakehouse.writers.bronze_writer import BronzeWriter
from src.transformations.bronze.chotot_bronze import transform_chotot_to_bronze
from src.transformations.bronze.google_trends_bronze import transform_google_trends_to_bronze
from src.transformations.bronze.osm_bronze import transform_osm_to_bronze


logger = get_logger(__name__)


TRANSFORMERS = {
    "chotot": transform_chotot_to_bronze,
    "google_trends": transform_google_trends_to_bronze,
    "osm": transform_osm_to_bronze,
}


def run_raw_to_bronze(source_name: str):
    if source_name not in TRANSFORMERS:
        raise ValueError(f"Unknown source: {source_name}")

    batch_id = str(uuid.uuid4())
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    raw_prefix = f"source={source_name}/ingestion_date={today}/"

    logger.info(f"Reading raw source={source_name} prefix={raw_prefix}")

    reader = JsonlMinioReader()
    raw_records = reader.read_prefix(
        bucket="raw",
        prefix=raw_prefix,
    )

    logger.info(f"Read raw source={source_name} records={len(raw_records)}")

    transformer = TRANSFORMERS[source_name]
    bronze_records = transformer(raw_records)

    writer = BronzeWriter(bucket="bronze")
    output_path = writer.write_jsonl(
        table=source_name,
        records=bronze_records,
        batch_id=batch_id,
    )

    logger.info(
        f"Wrote bronze source={source_name} records={len(bronze_records)} path={output_path}"
    )

    return {
        "source": source_name,
        "batch_id": batch_id,
        "records_count": len(bronze_records),
        "output_path": output_path,
    }