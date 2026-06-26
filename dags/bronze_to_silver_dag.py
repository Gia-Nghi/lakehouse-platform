from __future__ import annotations

from datetime import datetime
from pendulum import timezone

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


local_tz = timezone("Asia/Ho_Chi_Minh")

SPARK_SCRIPT = "/opt/airflow/src/etl/etl_bronze_to_silver.py"


with DAG(
    dag_id="bronze_to_silver",
    description="Run Bronze to Silver Spark ETL for Chotot, Google Trends and Metro stations",
    start_date=datetime(2026, 5, 1, tzinfo=local_tz),
    schedule="0 19 * * *",
    catchup=False,
    tags=[
        "lakehouse",
        "bronze",
        "silver",
        "spark",
        "chotot",
        "google_trends",
        "metro",
    ],
) as dag:

    run_bronze_to_silver_daily = SparkSubmitOperator(
        task_id="run_bronze_to_silver_daily",
        application=SPARK_SCRIPT,
        conn_id="spark_default",
        name="bronze_to_silver_etl",
        properties_file="/opt/spark/conf/spark-defaults.conf",
        verbose=True,
        env_vars={
            "PYSPARK_PYTHON": "python3",
            "PYSPARK_DRIVER_PYTHON": "python3",
        },
    )