#!/bin/bash
set -e

# Kiểm tra nếu biến chưa được gán giá trị thì dừng lại báo lỗi
if [ -z "$POSTGRES_DB_NESSIE" ] || [ -z "$POSTGRES_DB_AIRFLOW" ]; then
  echo "LỖI: Biến môi trường chưa được thiết lập!"
  exit 1
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE "${POSTGRES_DB_NESSIE}";
    CREATE DATABASE "${POSTGRES_DB_AIRFLOW}";
EOSQL