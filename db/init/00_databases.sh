#!/bin/bash
# Runs once, on the first start of an empty PostgreSQL volume.
# Creates the Airflow metadata database and a read-only user for Grafana.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE DATABASE airflow OWNER airflow;
    CREATE USER grafana_reader WITH PASSWORD '${GRAFANA_DB_PASSWORD}';
EOSQL
