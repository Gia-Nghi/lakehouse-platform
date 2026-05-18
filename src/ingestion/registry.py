from src.ingestion.geo_context.osm.runners.run_osm import OSMIngestionJob
from src.ingestion.market_listings.chotot.run import ChototIngestionJob
from src.ingestion.user_interest.google_trends.run import GoogleTrendsIngestionJob


SOURCES = {
    "chotot": {
        "class": ChototIngestionJob,
        "config": "/opt/airflow/config/sources/chotot.yaml",
    },
    "google_trends": {
        "class": GoogleTrendsIngestionJob,
        "config": "/opt/airflow/config/sources/google_trends.yaml",
    },
    "osm": {
        "class": OSMIngestionJob,
        "config": "/opt/airflow/config/sources/osm.yaml",
    },
}


def run_ingestion(source_name: str):
    if source_name not in SOURCES:
        raise ValueError(f"Unknown source: {source_name}")

    source = SOURCES[source_name]
    job = source["class"](source["config"])

    return job.run()