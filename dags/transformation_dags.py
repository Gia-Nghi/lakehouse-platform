from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "lakehouse",
}

with DAG(
    dag_id="lakehouse_transformations",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["transformations"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        spark-submit \
        /opt/airflow/src/transformations/bronze_to_silver/etl_bronze_to_silver.py
        """
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
        spark-submit \
        /opt/airflow/src/transformations/silver_to_gold/etl_silver_to_gold_facts_and_dims.py
        """
    )

    bronze_to_silver >> silver_to_gold