#!/usr/bin/env bash
# Regression test for the waterfall CDM: the first day must not appear as a delta
# from a zero/default previous value (lagInFrame on non-Nullable returns 0).
#
# Runs the waterfall SQL from BOTH dags/bybit_pipeline.py and
# deployment/clickhouse/cdm_from_raw.sql on synthetic data inside an isolated
# `clickhouse local` (no server tables are touched).
# Env: CH_CONTAINER (default some-clickhouse-server)
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CH_CONTAINER="${CH_CONTAINER:-some-clickhouse-server}"

# Daily average close per symbol: day1 = 100*k, day2 = 110*k, day3 = 105*k
# Expected waterfall: 2026-01-02 -> +10*k, 2026-01-03 -> -5*k; no 2026-01-01 row.
FIXTURE=$(cat <<'SQL'
INSERT INTO default.bybit_api
SELECT 'bybit', 'spot', s.1, '60', toDateTime('2026-01-01 00:00:00') + INTERVAL d DAY + INTERVAL h HOUR,
       0, 0, 0, s.2 * [100, 110, 105][d + 1] + (h - 0.5) * s.2, 1, 1, now()
FROM (SELECT arrayJoin([('BTCUSDT', 1.0), ('ETHUSDT', 0.1), ('SOLUSDT', 0.01)]) AS s) AS syms
CROSS JOIN (SELECT number AS d FROM numbers(3)) AS days
CROSS JOIN (SELECT number AS h FROM numbers(2)) AS hours;
SQL
)

CHECK=$(cat <<'SQL'
SELECT t, k, rows, first_step, first_delta, second_delta,
       throwIf(rows != 2, 'waterfall: expected 2 deltas for 3 days') AS c1,
       throwIf(first_step != '2026-01-02', 'waterfall: first day leaked in as a delta from 0') AS c2,
       throwIf(abs(first_delta - 10 * k) > 1e-9 OR abs(second_delta + 5 * k) > 1e-9, 'waterfall: wrong deltas') AS c3
FROM (
    SELECT 'btc' AS t, 1.0 AS k, count() AS rows, min(step) AS first_step,
           argMin(delta, step) AS first_delta, argMax(delta, step) AS second_delta FROM cdm.bybit_waterfall_btc
    UNION ALL
    SELECT 'eth', 0.1, count(), min(step), argMin(delta, step), argMax(delta, step) FROM cdm.bybit_waterfall_eth
    UNION ALL
    SELECT 'sol', 0.01, count(), min(step), argMin(delta, step), argMax(delta, step) FROM cdm.bybit_waterfall_sol
) ORDER BY t FORMAT TSV;
SQL
)

# Waterfall INSERT statements exactly as the DAG executes them
DAG_SQL=$(python3 - "$REPO_DIR/dags/bybit_pipeline.py" <<'PY'
import re, sys
src = open(sys.argv[1]).read()
blocks = [b for b in re.findall(r'client\.execute\("""(.*?)"""\)', src, re.S) if "cdm.bybit_waterfall_" in b]
assert len(blocks) == 3, f"expected 3 waterfall queries in DAG, found {len(blocks)}"
print(";\n".join(blocks) + ";")
PY
)
MIRROR_SQL=$(awk '/^INSERT INTO cdm.bybit_waterfall_/,/;$/' "$REPO_DIR/deployment/clickhouse/cdm_from_raw.sql")

run_case() {  # run_case <label> <waterfall sql>
  local sql
  sql="$(cat "$REPO_DIR/sql/create_tables.sql")
$FIXTURE
$2
$CHECK"
  echo "== $1"
  echo "$sql" | docker exec -i "$CH_CONTAINER" bash -c \
    'd=$(mktemp -d); clickhouse local --path "$d" --multiquery; rc=$?; rm -rf "$d"; exit $rc' \
    | LC_ALL=C awk -F'\t' '{printf "PASS  %s: %s deltas, first step %s = %+g, then %+g\n", $1, $3, $4, $5, $6}'
}

run_case "DAG refresh_cdm waterfall" "$DAG_SQL"
run_case "deployment cdm_from_raw.sql waterfall" "$MIRROR_SQL"
echo "ALL WATERFALL CHECKS PASSED"
