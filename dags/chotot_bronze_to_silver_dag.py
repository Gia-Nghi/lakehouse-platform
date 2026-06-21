from __future__ import annotations

from datetime import datetime
# Import thêm timezone để đảm bảo Airflow chạy đúng giờ Việt Nam (UTC+7)
from pendulum import timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_SCRIPT = "/opt/airflow/src/ETL/etl_bronze_to_silver.py"

# Thiết lập múi giờ Hồ Chí Minh để tránh lệch múi giờ UTC mặc định của Airflow
local_tz = timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="chotot_bronze_to_silver_daily",
    description="Run Chotot Bronze to Silver Spark job daily at 7 PM",
    start_date=datetime(2026, 5, 1, tzinfo=local_tz),
    schedule="0 19 * * *",  # Cron expression: 19:00 mỗi ngày
    catchup=False,
    tags=["lakehouse", "chotot", "bronze", "silver", "spark", "daily"],
) as dag:

    run_chotot_bronze_to_silver_daily = BashOperator(
        task_id="run_chotot_bronze_to_silver_daily",
        bash_command=f"""
        export PYSPARK_PYTHON=python3
        export PYSPARK_DRIVER_PYTHON=python3

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