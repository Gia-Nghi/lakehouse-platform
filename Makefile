# Nội dung file Makefile tại ~/lakehouse-platform/Makefile

include .env
export

up:
	docker compose --env-file .env -f infra/docker-compose.yml up -d

down:
	docker compose --env-file .env -f infra/docker-compose.yml down

status:
	docker compose --env-file .env -f infra/docker-compose.yml ps

# down và xóa volume để reset hoàn toàn hệ thống (cẩn thận: sẽ mất dữ liệu nếu không dùng volume bền vững)
reset:
	docker compose --env-file .env -f infra/docker-compose.yml down -v

# Mở pyspark nhanh để test code
pyspark:
	docker exec -it lakehouse-spark-master /opt/spark/bin/pyspark

# Xem log của Airflow nếu có lỗi DAG
logs-airflow:
	docker logs -f lakehouse-airflow-web

# Build lại Spark nếu bạn thay đổi Dockerfile/thêm JARs mới
build-spark:
	docker compose -f infra/docker-compose.yml build spark-master spark-worker

# Restart lại Airflow nếu bạn thay đổi code DAGs hoặc cài thêm thư viện mới
restart-airflow:
	docker compose --env-file .env -f infra/docker-compose.yml restart airflow-webserver airflow-scheduler

# Khởi động riêng cụm Airflow
up-airflow:
	docker compose --env-file .env -f infra/docker-compose.yml up -d airflow-webserver airflow-scheduler

# Dừng và xóa các container thuộc cụm Airflow
down-airflow:
	docker compose --env-file .env -f infra/docker-compose.yml rm -fs airflow-webserver airflow-scheduler airflow-init
