import requests
import pandas as pd

from clickhouse_driver import Client
from datetime import datetime, timezone, timedelta



# =========================================================
# CONFIG
# =========================================================

# Публичный endpoint Bybit для свечей
BYBIT_URL = "https://api.bybit.com/v5/market/kline"

# SOCKS5-прокси для requests
# ВАЖНО:
# - socks5h лучше socks5, потому что DNS тоже пойдет через прокси
PROXIES = {
    "http": "socks5h://127.0.0.1:1080",
    "https": "socks5h://127.0.0.1:1080"
}

# Настройки ClickHouse
# ВАЖНО:
# clickhouse_driver работает по TCP, обычно это порт 9000

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 19000
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "default"
CLICKHOUSE_DATABASE = "cdm"
CLICKHOUSE_TABLE = "bybit_klines"


# =========================================================
# 1. Получение клиента ClickHouse
# =========================================================
def get_clickhouse_client() -> Client:
    """
    Создает и возвращает клиент ClickHouse.

    Здесь используется clickhouse_driver, а не HTTP-клиент.
    Поэтому:
    - порт обычно 9000
    - метод вставки будет через client.execute(..., rows)
    """
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )
    return client


# =========================================================
# 2. Получение свечей из Bybit API
# =========================================================
def fetch_klines(
    symbol: str,
    category: str = "spot",
    interval: str = "60",
    limit: int = 200
) -> pd.DataFrame:
    """
    Загружает свечи из Bybit и возвращает pandas DataFrame.

    Параметры:
    - symbol: например BTCUSDT
    - category: для старта используем spot
    - interval: 60 = 1 час
    - limit: сколько свечей брать за один запрос
    """

    params = {
        "category": category,
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    response = requests.get(
        BYBIT_URL,
        params=params,
        proxies=PROXIES,
        timeout=30
    )
    response.raise_for_status()

    payload = response.json()

    # Bybit возвращает retCode = 0, если все ок
    if payload.get("retCode") != 0:
        raise RuntimeError(f"Bybit API error for {symbol}: {payload}")

    rows = payload["result"]["list"]

    # Если API ничего не вернул — отдаем пустой DataFrame,
    # а выше по пайплайну уже решим, что с этим делать
    if not rows:
        return pd.DataFrame()

    # ВАЖНО:
    # Bybit отдает массив массивов, а не список словарей.
    # Порядок колонок нужно задать вручную.
    df = pd.DataFrame(rows, columns=[
        "open_time_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover"
    ])

    # Timestamp приходит в миллисекундах
    df["open_time"] = pd.to_datetime(df["open_time_ms"].astype("int64"), unit="ms")

    # Числа Bybit отдает строками -> приводим к float
    numeric_cols = ["open", "high", "low", "close", "volume", "turnover"]
    for col in numeric_cols:
        df[col] = df[col].astype(float)

    # Технические поля для аналитической таблицы
    df["exchange"] = "bybit"
    df["category"] = category
    df["symbol"] = symbol
    df["interval"] = interval

    # loaded_at — время, когда мы загрузили данные в пайплайн

    UTC_PLUS_3 = timezone(timedelta(hours=3))
    loaded_at = datetime.now(UTC_PLUS_3).replace(tzinfo=None)

    df["loaded_at"] = loaded_at

    # У Bybit свечи часто идут от новых к старым.
    # Для хранения и отладки удобнее отсортировать по времени вверх.
    df = df.sort_values("open_time").reset_index(drop=True)

    # Оставляем только нужные колонки в правильном порядке
    df = df[
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

    return df


# =========================================================
# 3. Удаление старых строк по диапазону времени
# =========================================================
def delete_existing_rows(
    client: Client,
    symbol: str,
    interval: str,
    min_time: datetime,
    max_time: datetime
) -> None:
    """
    Удаляет существующие строки по конкретной монете, интервалу и временному диапазону.

    Зачем это нужно:
    - если ты каждый раз загружаешь последние 200 свечей,
      новый набор будет пересекаться со старым
    - без удаления будут копиться дубли
    - для MVP стратегия delete + insert — нормальный и понятный вариант
    """

    query = f"""
    ALTER TABLE {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
    DELETE WHERE symbol = '{symbol}'
      AND interval = '{interval}'
      AND open_time >= toDateTime('{min_time.strftime("%Y-%m-%d %H:%M:%S")}')
      AND open_time <= toDateTime('{max_time.strftime("%Y-%m-%d %H:%M:%S")}')
    """

    client.execute(query)


# =========================================================
# 4. Вставка DataFrame в ClickHouse
# =========================================================
def insert_dataframe(client: Client, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    df = df.copy()

    # open_time остается pandas datetime, его переводим в обычный python datetime
    df["open_time"] = [ts.to_pydatetime() for ts in df["open_time"]]

    # loaded_at уже обычный python datetime, его трогать не нужно

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    insert_query = f"""
    INSERT INTO {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
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

    client.execute(insert_query, rows)

    return len(rows)


# =========================================================
# 5. Главная функция загрузки одной монеты
# =========================================================
def load_symbol_to_clickhouse(
    symbol: str,
    category: str = "spot",
    interval: str = "60",
    limit: int = 200
) -> int:
    """
    Полный цикл для одной монеты:
    1. забираем свечи из Bybit
    2. если пусто -> выходим
    3. вычисляем временной диапазон
    4. удаляем старые строки за этот диапазон
    5. вставляем новые строки

    Возвращаем количество загруженных строк.
    """

    client = get_clickhouse_client()

    df = fetch_klines(
        symbol=symbol,
        category=category,
        interval=interval,
        limit=limit
    )

    # ВАЖНО:
    # если ответ пустой, не надо пытаться что-то удалять/вставлять
    if df.empty:
        print(f"[INFO] No data returned for {symbol}")
        return 0

    # Этот диапазон нужен именно для delete + insert
    min_time = df["open_time"].min().to_pydatetime()
    max_time = df["open_time"].max().to_pydatetime()

    delete_existing_rows(
        client=client,
        symbol=symbol,
        interval=interval,
        min_time=min_time,
        max_time=max_time
    )

    inserted_rows = insert_dataframe(client, df)

    print(
        f"[INFO] Loaded {inserted_rows} rows for {symbol} "
        f"({category}, interval={interval}, range={min_time} -> {max_time})"
    )

    return inserted_rows


# =========================================================
# 6. Проверка, что все запускается как обычный скрипт
# =========================================================
if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    for symbol in symbols:
        load_symbol_to_clickhouse(
            symbol=symbol,
            category="spot",
            interval="60",
            limit=200
        )
