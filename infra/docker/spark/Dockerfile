FROM apache/spark:3.5.2-scala2.12-java17-python3-ubuntu

USER root

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    software-properties-common \
    python3.11 \
    python3.11-dev \
    python3.11-venv \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3.11 /usr/bin/python3

ENV PYSPARK_PYTHON=python3.11
ENV PYSPARK_DRIVER_PYTHON=python3.11

RUN python3.11 -m pip install --no-cache-dir \
    boto3 \
    pandas \
    pyarrow \
    kafka-python \
    requests \
    pyiceberg \
    pynessie \
    psycopg2-binary \
    jupyterlab

RUN curl -fL --retry 3 -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    && curl -fL --retry 3 -o /opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.108.0.jar \
        https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.108.0/nessie-spark-extensions-3.5_2.12-0.108.0.jar \
    && curl -fL --retry 3 -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fL --retry 3 -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && chmod 644 /opt/spark/jars/*.jar \
    && chown -R spark:spark /opt/spark/jars

WORKDIR /opt/spark

USER spark