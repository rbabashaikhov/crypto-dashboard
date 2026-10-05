#!/usr/bin/env bash
# Regression test: two back-to-back runs of bybit_pipeline over the same ~200-candle
# window must not add logical duplicates to raw or CDM, and must keep the newest
# version of each candle.
#
# Runs against the local docker environment (DAG stays paused: uses `airflow dags test`).
# Env: AIRFLOW_CONTAINER, CH_CONTAINER, CH_USER, CH_PASSWORD
set -euo pipefail

AIRFLOW_CONTAINER="${AIRFLOW_CONTAINER:-airflow-airflow-scheduler-1}"
CH_CONTAINER="${CH_CONTAINER:-some-clickhouse-server}"
CH_USER="${CH_USER:-airflow}"
: "${CH_PASSWORD:?set CH_PASSWORD for the local ClickHouse user}"

ch() { docker exec "$CH_CONTAINER" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" -q "$1"; }
fail=0
check() {  # check <description> <actual> <expected>
  if [ "$2" == "$3" ]; then echo "PASS  $1 ($2)"; else echo "FAIL  $1: got $2, expected $3"; fail=1; fi
}
counts() {
  ch "SELECT symbol, physical, logical, uniq_keys, max_open_time, max_loaded_at
      FROM (SELECT symbol, count() AS physical,
                   uniqExact(exchange, category, symbol, interval, open_time) AS uniq_keys,
                   max(open_time) AS max_open_time, max(loaded_at) AS max_loaded_at
            FROM default.bybit_api GROUP BY symbol) AS r
      JOIN (SELECT symbol, count() AS logical FROM default.bybit_api FINAL GROUP BY symbol) AS f USING symbol
      ORDER BY symbol FORMAT PrettyCompactMonoBlock"
}
run_dag() {
  docker exec "$AIRFLOW_CONTAINER" airflow dags test bybit_pipeline 2>&1 \
    | grep -E 'Loaded [0-9]+ rows|DagRun Finished' | sed -E 's/.*(\[INFO\] Loaded|DagRun Finished)/\1/; s/, execution_date.*(state=[a-z]+).*/ \1/'
}

echo "== before"; counts
logical0=$(ch "SELECT count() FROM default.bybit_api FINAL")

echo "== run 1"; run_dag
counts
logical1=$(ch "SELECT count() FROM default.bybit_api FINAL")
max1=$(ch "SELECT max(open_time) FROM default.bybit_api")
daily_vol1=$(ch "SELECT toDate(open_time) d, symbol, round(sum(volume), 6) FROM cdm.bybit_volume_timeseries
                 WHERE d < (SELECT toDate(max(open_time)) FROM default.bybit_api) GROUP BY d, symbol ORDER BY d, symbol FORMAT TSV")

echo "== run 2 (same window)"; run_dag
counts
logical2=$(ch "SELECT count() FROM default.bybit_api FINAL")
# A new hourly candle may legitimately open between runs; only that may add rows.
new_candles=$(ch "SELECT count() FROM default.bybit_api FINAL WHERE open_time > toDateTime('$max1')")
daily_vol2=$(ch "SELECT toDate(open_time) d, symbol, round(sum(volume), 6) FROM cdm.bybit_volume_timeseries
                 WHERE d < (SELECT toDate(max(open_time)) FROM default.bybit_api) GROUP BY d, symbol ORDER BY d, symbol FORMAT TSV")

echo "== checks"
echo "logical candles: before=$logical0 run1=$logical1 run2=$logical2 (new hourly candles between runs: $new_candles)"
check "run 2 adds no logical candles beyond newly opened hours" "$((logical2 - logical1))" "$new_candles"
check "logical rows == unique keys" "$logical2" "$(ch "SELECT uniqExact(exchange, category, symbol, interval, open_time) FROM default.bybit_api")"
check "keys with >1 row after FINAL" "$(ch "SELECT count() FROM (SELECT 1 FROM default.bybit_api FINAL GROUP BY exchange, category, symbol, interval, open_time HAVING count() > 1)")" "0"
for s in BTCUSDT ETHUSDT SOLUSDT; do
  check "$s raw logical == unique open_time" \
    "$(ch "SELECT count() FROM default.bybit_api FINAL WHERE symbol = '$s'")" \
    "$(ch "SELECT uniqExact(open_time) FROM default.bybit_api WHERE symbol = '$s'")"
  check "$s latest candle version is from run 2" \
    "$(ch "SELECT argMax(loaded_at, open_time) FROM default.bybit_api FINAL WHERE symbol = '$s'")" \
    "$(ch "SELECT max(loaded_at) FROM default.bybit_api WHERE symbol = '$s'")"
done

check "cdm.bybit_price_timeseries rows == raw logical" "$(ch "SELECT count() FROM cdm.bybit_price_timeseries")" "$logical2"
check "cdm.bybit_volume_timeseries rows == raw logical" "$(ch "SELECT count() FROM cdm.bybit_volume_timeseries")" "$logical2"
check "cdm volume duplicate (symbol, open_time)" "$(ch "SELECT count() FROM (SELECT 1 FROM cdm.bybit_volume_timeseries GROUP BY symbol, open_time HAVING count() > 1)")" "0"
check "cdm.bybit_latest_table rows" "$(ch "SELECT count() FROM cdm.bybit_latest_table")" "3"
check "boxplot rows == raw logical" \
  "$(ch "SELECT (SELECT count() FROM cdm.bybit_boxplot_btc) + (SELECT count() FROM cdm.bybit_boxplot_eth) + (SELECT count() FROM cdm.bybit_boxplot_sol)")" "$logical2"
check "Volume Trend source: SUM(volume) cdm == raw FINAL" \
  "$(ch "SELECT round(sum(volume), 4) FROM cdm.bybit_volume_timeseries")" \
  "$(ch "SELECT round(sum(volume), 4) FROM default.bybit_api FINAL")"
check "Volume Trend daily SUM(volume) unchanged by re-run (closed days)" "$([ "$daily_vol1" == "$daily_vol2" ] && echo same || echo changed)" "same"

[ "$fail" -eq 0 ] && echo "ALL CHECKS PASSED" || { echo "SOME CHECKS FAILED"; exit 1; }
