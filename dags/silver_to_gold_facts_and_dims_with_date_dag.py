from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_SCRIPT = "/opt/airflow/src/ETL/etl_silver_to_gold_facts_and_dims.py"


default_args = {
    "owner": "tisucam",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id="silver_to_gold_facts_and_dims_with_date",
    description="Build Gold facts and dimensions from Chotot Silver Iceberg table by Airflow logical date",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "silver", "gold", "spark", "iceberg", "dremio"],
) as dag:

    run_silver_to_gold_with_date = BashOperator(
        task_id="run_silver_to_gold_facts_and_dims_with_date",
        bash_command=f"""
        set -e

        echo "=== CHECK SPARK SCRIPT ==="
        ls -l {SPARK_SCRIPT}

        echo "=== CHECK ENV ==="
        echo "SILVER_TABLE=$SILVER_TABLE"
        echo "METRO_SILVER_TABLE=$METRO_SILVER_TABLE"
        echo "GGTREND_SILVER_TABLE=$GGTREND_SILVER_TABLE"
        echo "GOLD_NAMESPACE=$GOLD_NAMESPACE"
        echo "NESSIE_URI=$NESSIE_URI"
        echo "S3_ENDPOINT=$S3_ENDPOINT"
        echo "ICEBERG_WAREHOUSE=$ICEBERG_WAREHOUSE"
        echo "PYSPARK_PYTHON=$PYSPARK_PYTHON"
        echo "PYSPARK_DRIVER_PYTHON=$PYSPARK_DRIVER_PYTHON"

        echo "=== RUN SILVER TO GOLD SPARK JOB WITH DATE: {{{{ ds }}}} ==="
        spark-submit \
          --master spark://spark-master:7077 \
          {SPARK_SCRIPT} {{{{ ds }}}}
        """,
    )