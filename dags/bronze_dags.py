from datetime import datetime

from airflow.decorators import dag, task

from src.transformations.bronze.raw_to_bronze import run_raw_to_bronze


@dag(
    dag_id="raw_to_bronze",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "bronze"],
)
def raw_to_bronze_dag():

    @task
    def bronze_source(source_name: str):
        return run_raw_to_bronze(source_name)

    bronze_source.override(task_id="bronze_chotot")("chotot")
    bronze_source.override(task_id="bronze_google_trends")("google_trends")
    bronze_source.override(task_id="bronze_osm")("osm")


raw_to_bronze_dag()