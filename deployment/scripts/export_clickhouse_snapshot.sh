#!/usr/bin/env bash
# Exports a deduplicated Crypto Dashboard snapshot from the local ClickHouse.
#
# - raw: default.bybit_api, one row per (symbol, interval, open_time), latest loaded_at wins
# - cdm: rebuilt from the deduplicated raw with deployment/clickhouse/cdm_from_raw.sql
#        inside an isolated `clickhouse local` (the local server is only read)
# - format: ClickHouse Native, plus manifest.tsv with expected counts for restore validation
#
# Usage: CH_PASSWORD=... deployment/scripts/export_clickhouse_snapshot.sh [out_dir]
# Env:   CH_CONTAINER (default some-clickhouse-server), CH_USER (default airflow)
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${1:-$REPO_DIR/deployment/clickhouse/snapshot}"
CH_CONTAINER="${CH_CONTAINER:-some-clickhouse-server}"
CH_USER="${CH_USER:-airflow}"
: "${CH_PASSWORD:?set CH_PASSWORD for the local ClickHouse user}"

TABLES=(
  default.bybit_api
  cdm.bybit_price_timeseries
  cdm.bybit_volume_timeseries
  cdm.bybit_latest_table
  cdm.bybit_waterfall_btc
  cdm.bybit_waterfall_eth
  cdm.bybit_waterfall_sol
  cdm.bybit_boxplot_btc
  cdm.bybit_boxplot_eth
  cdm.bybit_boxplot_sol
)

WORK="/tmp/crypto_snapshot_$$"
mkdir -p "$OUT_DIR"
docker exec "$CH_CONTAINER" mkdir -p "$WORK/out"
trap 'docker exec "$CH_CONTAINER" rm -rf "$WORK"' EXIT

# 1. Deduplicated raw from the running server
docker exec "$CH_CONTAINER" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" -q "
  SELECT exchange, category, symbol, interval, open_time, open, high, low, close, volume, turnover, loaded_at
  FROM default.bybit_api
  ORDER BY loaded_at DESC
  LIMIT 1 BY symbol, interval, open_time
  FORMAT Native" > "$OUT_DIR/default.bybit_api.native"
docker cp "$OUT_DIR/default.bybit_api.native" "$CH_CONTAINER:$WORK/raw.native"

# 2. Rebuild CDM in an isolated clickhouse-local and dump every table.
#    One session: clickhouse-local does not persist the `default` database between runs.
{
  cat "$REPO_DIR/sql/create_tables.sql"
  echo "INSERT INTO default.bybit_api SELECT * FROM file('$WORK/raw.native', Native);"
  cat "$REPO_DIR/deployment/clickhouse/cdm_from_raw.sql"
  for t in "${TABLES[@]}"; do
    echo "SELECT * FROM $t INTO OUTFILE '$WORK/out/$t.native' FORMAT Native;"
  done
  echo "SELECT * FROM ("
  for t in "${TABLES[@]}"; do
    echo "  SELECT 1 AS o, '$t' AS table, 'rows' AS key, toString(count()) AS value FROM $t UNION ALL"
  done
  echo "  SELECT 2, 'default.bybit_api', concat('rows_', symbol), toString(count()) FROM default.bybit_api GROUP BY symbol UNION ALL"
  echo "  SELECT 3, 'default.bybit_api', 'max_open_time', toString(max(open_time)) FROM default.bybit_api UNION ALL"
  echo "  SELECT 4, 'default.bybit_api', 'max_loaded_at', toString(max(loaded_at)) FROM default.bybit_api"
  echo ") ORDER BY o, table, key"
  echo "INTO OUTFILE '$WORK/out/manifest.tsv' FORMAT TSVWithNames;"
} > "$OUT_DIR/.build.sql"
docker cp "$OUT_DIR/.build.sql" "$CH_CONTAINER:$WORK/build.sql"
rm "$OUT_DIR/.build.sql"
docker exec "$CH_CONTAINER" clickhouse local --path "$WORK/db" --multiquery --queries-file "$WORK/build.sql"

for t in "${TABLES[@]}"; do
  docker cp "$CH_CONTAINER:$WORK/out/$t.native" "$OUT_DIR/$t.native"
done
docker cp "$CH_CONTAINER:$WORK/out/manifest.tsv" "$OUT_DIR/manifest.tsv"
date -u +"%Y-%m-%dT%H:%M:%SZ" > "$OUT_DIR/exported_at.txt"

echo "Snapshot written to $OUT_DIR"
cat "$OUT_DIR/manifest.tsv"
