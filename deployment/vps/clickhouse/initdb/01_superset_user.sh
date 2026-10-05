#!/bin/bash
# Runs once on an empty data volume: schema + read-only user for Superset.
set -euo pipefail

clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery --queries-file /schema/create_tables.sql

clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
CREATE USER IF NOT EXISTS superset IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_SUPERSET_PASSWORD}';
GRANT SELECT ON default.bybit_api TO superset;
GRANT SELECT ON cdm.* TO superset;
SQL
