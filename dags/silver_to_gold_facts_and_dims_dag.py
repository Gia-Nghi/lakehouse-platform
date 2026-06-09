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
    dag_id="silver_to_gold_facts_and_dims_dag",
    description="Build Gold facts and dimensions from Silver Iceberg tables",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "silver", "gold", "spark", "iceberg", "dremio"],
) as dag:

    run_silver_to_gold = BashOperator(
        task_id="run_silver_to_gold_facts_and_dims",
        bash_command=f"""
        set -e

        export PYSPARK_PYTHON=python3
        export PYSPARK_DRIVER_PYTHON=python3

        echo "=== CHECK SPARK SCRIPT ==="
        ls -l {SPARK_SCRIPT}

        echo "=== CHECK PYTHON ENV ==="
        python --version
        which python
        echo "PYSPARK_PYTHON=$PYSPARK_PYTHON"
        echo "PYSPARK_DRIVER_PYTHON=$PYSPARK_DRIVER_PYTHON"

        echo "=== CHECK ENV ==="
        echo "SILVER_TABLE=$SILVER_TABLE"
        echo "METRO_SILVER_TABLE=$METRO_SILVER_TABLE"
        echo "GGTREND_SILVER_TABLE=$GGTREND_SILVER_TABLE"
        echo "GOLD_NAMESPACE=$GOLD_NAMESPACE"
        echo "NESSIE_URI=$NESSIE_URI"
        echo "S3_ENDPOINT=$S3_ENDPOINT"
        echo "ICEBERG_WAREHOUSE=$ICEBERG_WAREHOUSE"

        echo "=== RUN SILVER TO GOLD SPARK JOB ==="
        spark-submit \
          --master spark://spark-master:7077 \
          --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.95.0,org.apache.hadoop:hadoop-aws:3.3.4 \
          --conf spark.pyspark.python=python3 \
          --conf spark.pyspark.driver.python=python3 \
          --conf spark.driverEnv.PYSPARK_PYTHON=python3 \
          --conf spark.executorEnv.PYSPARK_PYTHON=python3 \
          --conf spark.driverEnv.PYTHONPATH=/opt/airflow \
          --conf spark.executorEnv.PYTHONPATH=/opt/airflow \
          --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
          --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
          --conf spark.sql.catalog.lakehouse.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
          --conf spark.sql.catalog.lakehouse.uri=http://nessie:19120/api/v2 \
          --conf spark.sql.catalog.lakehouse.ref=main \
          --conf spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/ \
          --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
          --conf spark.hadoop.fs.s3a.access.key=admin \
          --conf spark.hadoop.fs.s3a.secret.key=password123 \
          --conf spark.hadoop.fs.s3a.path.style.access=true \
          --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
          --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
          {SPARK_SCRIPT}
        """,
    )