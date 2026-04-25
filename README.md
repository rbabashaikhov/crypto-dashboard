# Crypto Dashboard ETL Pipeline

Portfolio project: market data pipeline for crypto analytics.

## Canonical pipeline

Bybit API -> Airflow DAG -> ClickHouse raw table -> ClickHouse CDM tables -> BI-ready datasets

## Project structure

- `dags/bybit_pipeline.py` - canonical Airflow DAG (ingest + transform)
- `sql/create_tables.sql` - DDL for raw and CDM tables used by the DAG
- `archive/standalone_etl/bybit_to_clickhouse.py` - early standalone ETL prototype kept for reference
- `requirements.txt` - Python dependencies
- `.env.example` - environment variable template

## What the DAG does

1. Loads hourly klines from Bybit for:
   - `BTCUSDT`
   - `ETHUSDT`
   - `SOLUSDT`
2. Inserts rows into raw table `default.bybit_api`.
3. Rebuilds CDM tables in `cdm`:
   - `bybit_price_timeseries`
   - `bybit_volume_timeseries`
   - `bybit_latest_table`
   - `bybit_waterfall_btc`, `bybit_waterfall_eth`, `bybit_waterfall_sol`
   - `bybit_boxplot_btc`, `bybit_boxplot_eth`, `bybit_boxplot_sol`

## Setup

1. Install dependencies:
   - `pip install -r requirements.txt`
2. Create local env file from template:
   - `cp .env.example .env`
3. Apply DDL in ClickHouse:
   - run `sql/create_tables.sql`
4. Place DAG into your Airflow `dags` directory and enable `bybit_pipeline`.

## Notes

- The archived standalone ETL script is intentionally preserved as an earlier development step.
- Real `.env` files are excluded from git.
