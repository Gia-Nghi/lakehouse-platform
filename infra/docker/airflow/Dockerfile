FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    openjdk-17-jdk \
    curl \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.2 \
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