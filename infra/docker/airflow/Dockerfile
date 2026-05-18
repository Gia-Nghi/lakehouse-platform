FROM apache/airflow:2.10.4
# Thay 2.7.1 bằng phiên bản Airflow hiện tại trong file .env của bạn nếu có

USER root
# Nếu osmnx cần các thư viện C++ của hệ thống (như GDAL, PROJ), cài ở đây:
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Cài đặt các thư viện Python bổ sung cho pipeline của bạn
RUN pip install --no-cache-dir \
    osmnx \
    pytrends \
    minio \
    requests \
    pandas \
    pyyaml \
    kafka-python
