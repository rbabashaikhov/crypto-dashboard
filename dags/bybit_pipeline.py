from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
from clickhouse_driver import Client


# =========================================================
# CONFIG
# =========================================================

BYBIT_URL = "https://api.bybit.com/v5/market/kline"

CLICKHOUSE_HOST = "some-clickhouse-server"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "airflow"
CLICKHOUSE_PASSWORD = "airflow"

RAW_DB = "default"
RAW_TABLE = "bybit_api"

UTC_PLUS_3 = timezone(timedelta(hours=3))


# =========================================================
# AIRFLOW CONFIG
# =========================================================

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 1),
    # Transient Bybit/DNS failures happen; re-running a task is safe (idempotent raw).
    "retries": 2,
    "retry_delay": timedelta(seconds=45),
}

dag = DAG(
    dag_id="bybit_pipeline",
    default_args=default_args,
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "bybit", "etl"],
)


# =========================================================
# CLICKHOUSE CLIENT
# =========================================================
def get_clickhouse_client():
    return Client(
        CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        port=CLICKHOUSE_PORT,
        database=RAW_DB,
    )


# =========================================================
# FETCH DATA
# =========================================================
def fetch_klines(symbol, category="spot", interval="60", limit=200):
    params = {
        "category": category,
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    response = requests.get(BYBIT_URL, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()

    if payload.get("retCode") != 0:
        raise RuntimeError(payload)

    rows = payload["result"]["list"]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "open_time_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover"
    ])

    df["open_time"] = pd.to_datetime(df["open_time_ms"].astype("int64"), unit="ms")

    for col in ["open", "high", "low", "close", "volume", "turnover"]:
        df[col] = df[col].astype(float)

    df["exchange"] = "bybit"
    df["category"] = category
    df["symbol"] = symbol
    df["interval"] = interval

    df["loaded_at"] = datetime.now(UTC_PLUS_3).replace(tzinfo=None)

    df = df.sort_values("open_time").reset_index(drop=True)

    return df[
        [
            "exchange",
            "category",
            "symbol",
            "interval",
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "loaded_at"
        ]
    ]


# =========================================================
# INSERT RAW
# =========================================================
def insert_raw(df):
    if df.empty:
        return 0

    client = get_clickhouse_client()

    df = df.copy()
    df["open_time"] = [x.to_pydatetime() for x in df["open_time"]]

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    query = f"""
    INSERT INTO {RAW_DB}.{RAW_TABLE}
    (
        exchange,
        category,
        symbol,
        interval,
        open_time,
        open,
        high,
        low,
        close,
        volume,
        turnover,
        loaded_at
    )
    VALUES
    """

    client.execute(query, rows)

    return len(rows)


# =========================================================
# INGEST TASK
# =========================================================
def load_symbol(symbol):
    df = fetch_klines(symbol)
    rows = insert_raw(df)

    print(f"[INFO] Loaded {rows} rows for {symbol}")


# =========================================================
# TRANSFORM (CDM)
# =========================================================
def refresh_cdm():
    # Raw is a ReplacingMergeTree: overlapping ingests leave several versions of a
    # candle until a background merge. FINAL reads only the latest version.
    client = get_clickhouse_client()

    # -----------------------------
    # PRICE TIMESERIES
    # -----------------------------
    client.execute("TRUNCATE TABLE cdm.bybit_price_timeseries")
    client.execute("""
        INSERT INTO cdm.bybit_price_timeseries
        SELECT
            symbol,
            open_time,
            close,
            loaded_at
        FROM default.bybit_api FINAL
    """)

    # -----------------------------
    # VOLUME TIMESERIES
    # -----------------------------
    client.execute("TRUNCATE TABLE cdm.bybit_volume_timeseries")
    client.execute("""
        INSERT INTO cdm.bybit_volume_timeseries
        SELECT
            symbol,
            open_time,
            volume,
            turnover,
            loaded_at
        FROM default.bybit_api FINAL
    """)

    # -----------------------------
    # LATEST TABLE
    # -----------------------------
    client.execute("TRUNCATE TABLE cdm.bybit_latest_table")
    client.execute("""
        INSERT INTO cdm.bybit_latest_table
        SELECT
            symbol,
            max(open_time) AS last_open_time,
            argMax(close, open_time) AS last_close,
            argMax(volume, open_time) AS last_volume,
            argMax(turnover, open_time) AS last_turnover,
            max(loaded_at) AS loaded_at
        FROM default.bybit_api FINAL
        GROUP BY symbol
    """)

    # -----------------------------
    # WATERFALL BTC
    # -----------------------------
    # lagInFrame on a non-Nullable column returns 0 for the first row; toNullable
    # makes it NULL, so the first day is dropped instead of becoming a full-price delta.
    client.execute("TRUNCATE TABLE cdm.bybit_waterfall_btc")
    client.execute("""
        INSERT INTO cdm.bybit_waterfall_btc
        WITH daily AS (
            SELECT
                toDate(open_time) AS dt,
                avg(close) AS close
            FROM default.bybit_api FINAL
            WHERE symbol = 'BTCUSDT'
            GROUP BY dt
        ),
        changes AS (
            SELECT
                dt,
                close - lagInFrame(toNullable(close)) OVER (
                    ORDER BY dt
                    ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
                ) AS delta
            FROM daily
        )
        SELECT
            toString(dt) AS step,
            delta
        FROM changes
        WHERE delta IS NOT NULL
        ORDER BY dt
    """)

    # -----------------------------
    # WATERFALL ETH
    # -----------------------------
    client.execute("TRUNCATE TABLE cdm.bybit_waterfall_eth")
    client.execute("""
        INSERT INTO cdm.bybit_waterfall_eth
        WITH daily AS (
            SELECT
                toDate(open_time) AS dt,
                avg(close) AS close
            FROM default.bybit_api FINAL
            WHERE symbol = 'ETHUSDT'
            GROUP BY dt
        ),
        changes AS (
            SELECT
                dt,
                close - lagInFrame(toNullable(close)) OVER (
                    ORDER BY dt
                    ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
                ) AS delta
            FROM daily
        )
        SELECT
            toString(dt) AS step,
            delta
        FROM changes
        WHERE delta IS NOT NULL
        ORDER BY dt
    """)

    # -----------------------------
    # WATERFALL SOL
    # -----------------------------
    client.execute("TRUNCATE TABLE cdm.bybit_waterfall_sol")
    client.execute("""
        INSERT INTO cdm.bybit_waterfall_sol
        WITH daily AS (
            SELECT
                toDate(open_time) AS dt,
                avg(close) AS close
            FROM default.bybit_api FINAL
            WHERE symbol = 'SOLUSDT'
            GROUP BY dt
        ),
        changes AS (
            SELECT
                dt,
                close - lagInFrame(toNullable(close)) OVER (
                    ORDER BY dt
                    ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
                ) AS delta
            FROM daily
        )
        SELECT
            toString(dt) AS step,
            delta
        FROM changes
        WHERE delta IS NOT NULL
        ORDER BY dt
    """)

    # -----------------------------
    # BOXPLOT BTC
    # -----------------------------
    client.execute("TRUNCATE TABLE cdm.bybit_boxplot_btc")
    client.execute("""
        INSERT INTO cdm.bybit_boxplot_btc
        SELECT
            open_time,
            (close - open) / nullIf(open, 0) AS return,
            (high - low) / nullIf(close, 0) AS candle_volatility
        FROM default.bybit_api FINAL
        WHERE symbol = 'BTCUSDT'
        ORDER BY open_time
    """)

    # -----------------------------
    # BOXPLOT ETH
    # -----------------------------
    client.execute("TRUNCATE TABLE cdm.bybit_boxplot_eth")
    client.execute("""
        INSERT INTO cdm.bybit_boxplot_eth
        SELECT
            open_time,
            (close - open) / nullIf(open, 0) AS return,
            (high - low) / nullIf(close, 0) AS candle_volatility
        FROM default.bybit_api FINAL
        WHERE symbol = 'ETHUSDT'
        ORDER BY open_time
    """)

    # -----------------------------
    # BOXPLOT SOL
    # -----------------------------
    client.execute("TRUNCATE TABLE cdm.bybit_boxplot_sol")
    client.execute("""
        INSERT INTO cdm.bybit_boxplot_sol
        SELECT
            open_time,
            (close - open) / nullIf(open, 0) AS return,
            (high - low) / nullIf(close, 0) AS candle_volatility
        FROM default.bybit_api FINAL
        WHERE symbol = 'SOLUSDT'
        ORDER BY open_time
    """)

    print("[INFO] CDM tables refreshed: price, volume, latest, waterfall, boxplot")


# =========================================================
# TASKS
# =========================================================

btc = PythonOperator(
    task_id="load_btc",
    python_callable=lambda: load_symbol("BTCUSDT"),
    dag=dag,
)

eth = PythonOperator(
    task_id="load_eth",
    python_callable=lambda: load_symbol("ETHUSDT"),
    dag=dag,
)

sol = PythonOperator(
    task_id="load_sol",
    python_callable=lambda: load_symbol("SOLUSDT"),
    dag=dag,
)

transform = PythonOperator(
    task_id="refresh_cdm",
    python_callable=refresh_cdm,
    dag=dag,
)

# =========================================================
# DEPENDENCIES
# =========================================================

[btc, eth, sol] >> transform
