from datetime import datetime

from airflow.decorators import dag, task

from src.ingestion.registry import run_ingestion


@dag(
    dag_id="multi_source_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["lakehouse", "ingestion"],
)
def multi_source_ingestion():

    @task
    def ingest_source(source_name: str):
        return run_ingestion(source_name)

    ingest_source.override(task_id="ingest_chotot")("chotot")
    ingest_source.override(task_id="ingest_google_trends")("google_trends")
    ingest_source.override(task_id="ingest_osm")("osm")


multi_source_ingestion()