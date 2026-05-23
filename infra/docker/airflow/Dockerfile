FROM apache/airflow:2.10.4

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    openjdk-17-jdk \
    curl \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
    | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    osmnx \
    pytrends \
    minio \
    requests \
    pandas \
    pyyaml \
    kafka-python