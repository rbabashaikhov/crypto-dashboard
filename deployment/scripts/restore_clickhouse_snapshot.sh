#!/usr/bin/env bash
# Restores deployment/clickhouse/snapshot into the VPS ClickHouse and validates it
# against manifest.tsv. Idempotent: every table is truncated before loading.
#
# Run from deployment/vps on the server: ../scripts/restore_clickhouse_snapshot.sh
set -euo pipefail

cd "$(dirname "$0")/../vps"
set -a; source .env; set +a

ch() {
  docker compose exec -T clickhouse clickhouse-client \
    --user "$CLICKHOUSE_ADMIN_USER" --password "$CLICKHOUSE_ADMIN_PASSWORD" "$@"
}

ch --multiquery --queries-file /schema/create_tables.sql

tables=$(awk -F'\t' 'NR > 1 && $3 == "rows" {print $2}' ../clickhouse/snapshot/manifest.tsv)
for t in $tables; do
  ch -q "TRUNCATE TABLE $t"
  ch -q "INSERT INTO $t FORMAT Native" < "../clickhouse/snapshot/$t.native"
done

# Validate against the manifest written at export time
actual=$(ch -q "
  SELECT * FROM (
    $(for t in $tables; do echo "SELECT 1 AS o, '$t' AS table, 'rows' AS key, toString(count()) AS value FROM $t UNION ALL"; done)
    SELECT 2, 'default.bybit_api', concat('rows_', symbol), toString(count()) FROM default.bybit_api GROUP BY symbol UNION ALL
    SELECT 3, 'default.bybit_api', 'max_open_time', toString(max(open_time)) FROM default.bybit_api UNION ALL
    SELECT 4, 'default.bybit_api', 'max_loaded_at', toString(max(loaded_at)) FROM default.bybit_api
  ) ORDER BY o, table, key FORMAT TSVWithNames")

if diff <(echo "$actual") ../clickhouse/snapshot/manifest.tsv; then
  echo "RESTORE OK: VPS ClickHouse matches snapshot manifest"
  echo "$actual"
else
  echo "RESTORE MISMATCH (diff above: < VPS, > manifest)" >&2
  exit 1
fi
