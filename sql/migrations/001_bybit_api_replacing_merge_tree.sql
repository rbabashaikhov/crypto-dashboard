-- Migrates an existing default.bybit_api (MergeTree, duplicates on re-ingest)
-- to the idempotent ReplacingMergeTree defined in sql/create_tables.sql.
-- The old table is kept as default.bybit_api_mergetree_backup; drop it once verified.
-- Run once with: clickhouse-client --multiquery < sql/migrations/001_bybit_api_replacing_merge_tree.sql

CREATE TABLE default.bybit_api_new
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
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (exchange, category, symbol, interval, open_time);

INSERT INTO default.bybit_api_new SELECT * FROM default.bybit_api;

-- Collapse the copied duplicates now instead of waiting for background merges
OPTIMIZE TABLE default.bybit_api_new FINAL;

RENAME TABLE default.bybit_api TO default.bybit_api_mergetree_backup,
             default.bybit_api_new TO default.bybit_api;
