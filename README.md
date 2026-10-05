# Crypto Dashboard ETL Pipeline

Portfolio project: market data pipeline for crypto analytics.

## Canonical pipeline

Bybit API -> Airflow DAG -> ClickHouse raw table -> ClickHouse CDM tables -> BI-ready datasets

## Project structure

- `dags/bybit_pipeline.py` - canonical Airflow DAG (ingest + transform)
- `sql/create_tables.sql` - DDL for raw and CDM tables used by the DAG
- `sql/migrations/` - one-off migrations for existing ClickHouse installs
- `airflow/` - Airflow image with the DAG's Python dependencies (`Dockerfile`, `requirements.txt`)
- `tests/check_idempotency.sh` - regression test: two back-to-back DAG runs add no duplicates
- `archive/standalone_etl/bybit_to_clickhouse.py` - early standalone ETL prototype kept for reference
- `requirements.txt` - Python dependencies
- `.env.example` - environment variable template
- `deployment/` - static demo deployment: ClickHouse snapshot + Superset dashboard on a VPS (see `deployment/README.md`)

## What the DAG does

1. Loads hourly klines from Bybit for:
   - `BTCUSDT`
   - `ETHUSDT`
   - `SOLUSDT`
   Each run fetches the latest 200 hourly candles (~8 days), overlapping previous runs.
2. Inserts rows into raw table `default.bybit_api`. The table is a
   `ReplacingMergeTree(loaded_at)` keyed by `(exchange, category, symbol, interval, open_time)`,
   so re-ingesting an overlapping window is idempotent: each candle keeps its newest
   version (the still-open candle's volume/close get updated on the next run).
3. Rebuilds CDM tables in `cdm` from the deduplicated raw (`FROM default.bybit_api FINAL`):
   - `bybit_price_timeseries`
   - `bybit_volume_timeseries`
   - `bybit_latest_table`
   - `bybit_waterfall_btc`, `bybit_waterfall_eth`, `bybit_waterfall_sol`
   - `bybit_boxplot_btc`, `bybit_boxplot_eth`, `bybit_boxplot_sol`

## Setup

1. Build the Airflow image with the DAG dependencies (`clickhouse-driver`) and point the
   official Airflow `docker-compose.yaml` at it through its `AIRFLOW_IMAGE_NAME` variable:
   - `docker build -t crypto-dashboard-airflow:2.10.0 airflow/`
   - add `AIRFLOW_IMAGE_NAME=crypto-dashboard-airflow:2.10.0` to the `.env` next to the
     Airflow `docker-compose.yaml`, then `docker compose up -d` there
2. Apply DDL in ClickHouse:
   - run `sql/create_tables.sql`
   - if `default.bybit_api` already exists as a plain `MergeTree` (created before the
     idempotent schema), run `sql/migrations/001_bybit_api_replacing_merge_tree.sql` once as
     an admin user; it keeps the old table as `default.bybit_api_mergetree_backup`
3. Copy `dags/bybit_pipeline.py` into your Airflow `dags` directory and enable `bybit_pipeline`
   (or run it once with `airflow dags test bybit_pipeline`).
4. Optional regression check: `CH_PASSWORD=... tests/check_idempotency.sh`

`requirements.txt` and `.env.example` in the repository root are for running the notebook /
archived standalone ETL outside Airflow (`pip install -r requirements.txt`, `cp .env.example .env`).

## Limitations

- Ingestion is an incremental, rolling refresh of the latest 200 hourly candles.
  Full historical backfill is not implemented: if the DAG does not run for more than
  ~8 days, the missing period stays as a gap (currently 2026-04-24 .. 2026-09-27).
- `open_time` is UTC (as returned by Bybit); `loaded_at` is UTC+3.
- Raw readers must use `FINAL` (or `argMax(..., loaded_at)`): between background
  merges the table can physically hold several versions of the same candle.

## Notes

- The archived standalone ETL script is intentionally preserved as an earlier development step.
- Real `.env` files are excluded from git.
