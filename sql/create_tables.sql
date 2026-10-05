CREATE DATABASE IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS default.bybit_api
(
    exchange String,
    category String,
    symbol String,
    interval String,
    open_time DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    turnover Float64,
    loaded_at DateTime DEFAULT now()
)
-- One logical candle per (exchange, category, symbol, interval, open_time).
-- Re-ingesting an overlapping window adds a newer version; the row with the
-- latest loaded_at wins (e.g. the still-open candle's updated volume).
-- Merges are eventual, so readers must use FINAL.
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (exchange, category, symbol, interval, open_time);

CREATE TABLE IF NOT EXISTS cdm.bybit_price_timeseries
(
    symbol String,
    open_time DateTime,
    close Float64,
    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (symbol, open_time);

CREATE TABLE IF NOT EXISTS cdm.bybit_volume_timeseries
(
    symbol String,
    open_time DateTime,
    volume Float64,
    turnover Float64,
    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY (symbol, open_time);

CREATE TABLE IF NOT EXISTS cdm.bybit_latest_table
(
    symbol String,
    last_open_time DateTime,
    last_close Float64,
    last_volume Float64,
    last_turnover Float64,
    loaded_at DateTime
)
ENGINE = MergeTree
ORDER BY symbol;

CREATE TABLE IF NOT EXISTS cdm.bybit_waterfall_btc
(
    step String,
    delta Float64
)
ENGINE = MergeTree
ORDER BY step;

CREATE TABLE IF NOT EXISTS cdm.bybit_waterfall_eth
(
    step String,
    delta Float64
)
ENGINE = MergeTree
ORDER BY step;

CREATE TABLE IF NOT EXISTS cdm.bybit_waterfall_sol
(
    step String,
    delta Float64
)
ENGINE = MergeTree
ORDER BY step;

CREATE TABLE IF NOT EXISTS cdm.bybit_boxplot_btc
(
    open_time DateTime,
    return Float64,
    candle_volatility Float64
)
ENGINE = MergeTree
ORDER BY open_time;

CREATE TABLE IF NOT EXISTS cdm.bybit_boxplot_eth
(
    open_time DateTime,
    return Float64,
    candle_volatility Float64
)
ENGINE = MergeTree
ORDER BY open_time;

CREATE TABLE IF NOT EXISTS cdm.bybit_boxplot_sol
(
    open_time DateTime,
    return Float64,
    candle_volatility Float64
)
ENGINE = MergeTree
ORDER BY open_time;
