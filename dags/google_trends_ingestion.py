from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="google_trends_ingestion",
    description="Daily Google Trends ingestion to MinIO Bronze layer",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=[
        "lakehouse",
        "bronze",
        "google_trends",
        "user_interest",
    ],
) as dag:

    ingest_google_trends = BashOperator(
        task_id="ingest_google_trends",
        bash_command="""
        cd /opt/airflow && \
        python -m src.ingestion.user_interest.google_trends.run
        """,
    )

    ingest_google_trends