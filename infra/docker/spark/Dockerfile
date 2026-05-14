FROM apache/spark:3.5.1

USER root

# Cài đặt Python dependencies
RUN pip install --no-cache-dir jupyterlab pyiceberg pynessie psycopg2-binary boto3

# --- THIẾT LẬP PHIÊN BẢN (Quản lý tập trung ở đây) ---
ENV ICEBERG_VERSION=1.5.0
ENV NESSIE_VERSION=0.95.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_V2_VERSION=2.20.160
ENV AWS_SDK_V1_VERSION=1.12.262
ENV SPARK_JARS_DIR=/opt/spark/jars

# Cài đặt curl và tải các JAR
RUN apt-get update && apt-get install -y curl && \
    # 1. Iceberg & Nessie Runtime
    curl -o $SPARK_JARS_DIR/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar && \
    curl -o $SPARK_JARS_DIR/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar && \
    # 2. Hadoop AWS (Kết nối s3a://)
    curl -o $SPARK_JARS_DIR/hadoop-aws-${HADOOP_AWS_VERSION}.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -o $SPARK_JARS_DIR/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar && \
    # 3. AWS SDK V2 Bundle (Sửa lỗi thiếu Class S3Exception của Iceberg 1.5.0)
    # Thay vì tải từng file lẻ, ta tải bản bundle đầy đủ của V2 để đảm bảo không thiếu Class nào
    curl -o $SPARK_JARS_DIR/bundle-${AWS_SDK_V2_VERSION}.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar && \
    curl -o $SPARK_JARS_DIR/url-connection-client-${AWS_SDK_V2_VERSION}.jar https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWS_SDK_V2_VERSION}/url-connection-client-${AWS_SDK_V2_VERSION}.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark

USER spark