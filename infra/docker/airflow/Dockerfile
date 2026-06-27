FROM apache/spark:3.5.2-scala2.12-java17-python3-ubuntu AS spark-base

FROM apache/airflow:2.9.3-python3.11

USER root

COPY --from=spark-base /opt/spark /opt/spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    openjdk-17-jdk \
    curl \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fL --retry 3 -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    && curl -fL --retry 3 -o /opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.108.0.jar \
        https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.108.0/nessie-spark-extensions-3.5_2.12-0.108.0.jar \
    && curl -fL --retry 3 -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fL --retry 3 -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && chmod 644 /opt/spark/jars/*.jar \
    && chown -R airflow:root /opt/spark

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.11.0 \
    osmnx \
    pytrends \
    minio \
    requests \
    pandas \
    pyyaml \
    kafka-python \
    boto3 \
    pyarrow