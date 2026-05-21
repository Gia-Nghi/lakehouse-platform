from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="osm_ingestion",
    description="Collect OSM geo context data for HCMC",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * 0",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "osm", "geo", "bronze"],
) as dag:

    ingest_osm = BashOperator(
        task_id="ingest_osm",
        bash_command="""
        cd /opt/airflow && \
        python -m src.ingestion.geo_context.osm.runners.run_osm
        """,
    )

    ingest_osm