-- Builds CDM tables from default.bybit_api.
-- Mirror of refresh_cdm() in dags/bybit_pipeline.py (without TRUNCATE):
-- used only to build a snapshot into empty tables. Keep in sync with the DAG.

INSERT INTO cdm.bybit_price_timeseries
SELECT symbol, open_time, close, loaded_at
FROM default.bybit_api;

INSERT INTO cdm.bybit_volume_timeseries
SELECT symbol, open_time, volume, turnover, loaded_at
FROM default.bybit_api;

INSERT INTO cdm.bybit_latest_table
SELECT
    symbol,
    max(open_time) AS last_open_time,
    argMax(close, open_time) AS last_close,
    argMax(volume, open_time) AS last_volume,
    argMax(turnover, open_time) AS last_turnover,
    max(loaded_at) AS loaded_at
FROM default.bybit_api
GROUP BY symbol;

INSERT INTO cdm.bybit_waterfall_btc
WITH daily AS (
    SELECT toDate(open_time) AS dt, avg(close) AS close
    FROM default.bybit_api
    WHERE symbol = 'BTCUSDT'
    GROUP BY dt
),
changes AS (
    SELECT dt, close - lagInFrame(close) OVER (ORDER BY dt ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS delta
    FROM daily
)
SELECT toString(dt) AS step, delta FROM changes WHERE delta IS NOT NULL ORDER BY dt;

INSERT INTO cdm.bybit_waterfall_eth
WITH daily AS (
    SELECT toDate(open_time) AS dt, avg(close) AS close
    FROM default.bybit_api
    WHERE symbol = 'ETHUSDT'
    GROUP BY dt
),
changes AS (
    SELECT dt, close - lagInFrame(close) OVER (ORDER BY dt ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS delta
    FROM daily
)
SELECT toString(dt) AS step, delta FROM changes WHERE delta IS NOT NULL ORDER BY dt;

INSERT INTO cdm.bybit_waterfall_sol
WITH daily AS (
    SELECT toDate(open_time) AS dt, avg(close) AS close
    FROM default.bybit_api
    WHERE symbol = 'SOLUSDT'
    GROUP BY dt
),
changes AS (
    SELECT dt, close - lagInFrame(close) OVER (ORDER BY dt ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS delta
    FROM daily
)
SELECT toString(dt) AS step, delta FROM changes WHERE delta IS NOT NULL ORDER BY dt;

INSERT INTO cdm.bybit_boxplot_btc
SELECT open_time, (close - open) / nullIf(open, 0) AS return, (high - low) / nullIf(close, 0) AS candle_volatility
FROM default.bybit_api WHERE symbol = 'BTCUSDT' ORDER BY open_time;

INSERT INTO cdm.bybit_boxplot_eth
SELECT open_time, (close - open) / nullIf(open, 0) AS return, (high - low) / nullIf(close, 0) AS candle_volatility
FROM default.bybit_api WHERE symbol = 'ETHUSDT' ORDER BY open_time;

INSERT INTO cdm.bybit_boxplot_sol
SELECT open_time, (close - open) / nullIf(open, 0) AS return, (high - low) / nullIf(close, 0) AS candle_volatility
FROM default.bybit_api WHERE symbol = 'SOLUSDT' ORDER BY open_time;
