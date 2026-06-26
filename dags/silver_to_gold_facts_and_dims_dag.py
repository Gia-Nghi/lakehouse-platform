from __future__ import annotations

from datetime import datetime
from pendulum import timezone

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


SPARK_SCRIPT = "/opt/airflow/src/etl/etl_silver_to_gold_facts_and_dims.py"

default_args = {
    "owner": "tisucam",
    "depends_on_past": False,
    "retries": 0,
}

local_tz = timezone("Asia/Ho_Chi_Minh")


with DAG(
    dag_id="silver_to_gold_facts_and_dims_dag",
    description="Build Gold facts and dimensions from Silver Iceberg tables",
    default_args=default_args,
    start_date=datetime(2026, 5, 1, tzinfo=local_tz),
    schedule="0 21 * * *",
    catchup=False,
    tags=[
        "lakehouse",
        "silver",
        "gold",
        "spark",
        "iceberg",
        "dremio",
        "daily",
    ],
) as dag:

    run_silver_to_gold = SparkSubmitOperator(
        task_id="run_silver_to_gold_facts_and_dims",
        application=SPARK_SCRIPT,
        conn_id="spark_default",
        name="silver_to_gold_facts_and_dims_etl",
        properties_file="/opt/spark/conf/spark-defaults.conf",
        deploy_mode="client",
        spark_binary="spark-submit",
        verbose=True,
        env_vars={
            "PYSPARK_PYTHON": "python3",
            "PYSPARK_DRIVER_PYTHON": "python3",
            "SILVER_TABLE": "lakehouse.silver.chotot_cleaned",
            "METRO_SILVER_TABLE": "lakehouse.silver.metro_stations",
            "GGTREND_SILVER_TABLE": "lakehouse.silver.ggtrend_daily",
            "GOLD_NAMESPACE": "lakehouse.gold",
        },
    )