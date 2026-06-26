from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    spark.sql("SET spark.sql.session.timeZone = UTC")

    return spark