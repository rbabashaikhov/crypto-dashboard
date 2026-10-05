"""Backfill historical Bybit klines into default.bybit_api.

Separate from the hourly DAG on purpose: the DAG only refreshes the latest
200 candles, this tool pages through an explicit [start, end] range.

Rows use the same schema and mapping as dags/bybit_pipeline.py. The raw table
is a ReplacingMergeTree keyed by (exchange, category, symbol, interval, open_time),
so re-running the same range does not add logical candles.

Examples (times are UTC, inclusive, whole hours):
    python tools/backfill_bybit.py --all --start 2026-03-23T21:00 --end 2026-09-27T02:00 --dry-run
    python tools/backfill_bybit.py --symbol BTCUSDT --start 2026-04-01 --end 2026-04-02T23:00

ClickHouse connection: CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
CLICKHOUSE_PASSWORD (defaults match the DAG).

After a backfill, rebuild CDM (run the DAG once, or its refresh_cdm task).
"""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

BYBIT_URL = "https://api.bybit.com/v5/market/kline"
ALL_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
CATEGORY = "spot"
INTERVAL = "60"
STEP = timedelta(hours=1)
PAGE_LIMIT = 1000  # Bybit V5 kline maximum
UTC_PLUS_3 = timezone(timedelta(hours=3))

RAW_TABLE = "default.bybit_api"
COLUMNS = (
    "exchange", "category", "symbol", "interval", "open_time",
    "open", "high", "low", "close", "volume", "turnover", "loaded_at",
)

RETRYABLE = (requests.ConnectionError, requests.Timeout)
RETRYABLE_RET_CODES = {10002, 10006}  # request expired / rate limit


class BybitError(RuntimeError):
    def __init__(self, message, retryable=False):
        super().__init__(message)
        self.retryable = retryable


def parse_hour(value):
    """Parse an ISO date/datetime as naive UTC; must fall on a whole hour."""
    try:
        dt = datetime.fromisoformat(value.replace(" ", "T"))
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD[THH:MM]")
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    if dt.minute or dt.second or dt.microsecond:
        raise argparse.ArgumentTypeError(f"{value!r} is not a whole hour")
    return dt


def to_ms(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def iter_windows(start, end, limit=PAGE_LIMIT):
    """Yield inclusive [window_start, window_end] pages covering [start, end].

    Bybit treats both start and end as inclusive, so consecutive windows are
    contiguous without overlapping: next start = previous end + 1 hour.
    """
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    cursor = start
    while cursor <= end:
        window_end = min(cursor + STEP * (limit - 1), end)
        yield cursor, window_end
        cursor = window_end + STEP


def rows_from_payload(payload, symbol, window_start, window_end, loaded_at):
    """Map a Bybit kline response to raw rows (ascending, unique, inside the window)."""
    if payload.get("retCode") != 0:
        raise BybitError(f"Bybit retCode={payload.get('retCode')}: {payload.get('retMsg')}")
    by_time = {}
    for item in payload["result"]["list"]:
        open_time = datetime.fromtimestamp(int(item[0]) / 1000, timezone.utc).replace(tzinfo=None)
        if window_start <= open_time <= window_end:
            by_time[open_time] = item
    return [
        (
            "bybit", CATEGORY, symbol, INTERVAL, open_time,
            float(item[1]), float(item[2]), float(item[3]), float(item[4]),
            float(item[5]), float(item[6]), loaded_at,
        )
        for open_time, item in sorted(by_time.items())
    ]


def _is_retryable(exc):
    if isinstance(exc, RETRYABLE):
        return True
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else 0
        return status == 429 or status >= 500
    return isinstance(exc, BybitError) and exc.retryable


def get_with_retry(get, params, attempts=3, backoff=2.0, sleep=time.sleep):
    """GET the kline endpoint, retrying DNS/connection errors, timeouts, 429/5xx and rate limits."""
    for attempt in range(1, attempts + 1):
        try:
            response = get(BYBIT_URL, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            if payload.get("retCode") in RETRYABLE_RET_CODES:
                raise BybitError(f"Bybit retCode={payload['retCode']}: {payload.get('retMsg')}", retryable=True)
            return payload
        except (*RETRYABLE, requests.HTTPError, BybitError) as exc:
            if not _is_retryable(exc) or attempt == attempts:
                raise
            print(f"  retry {attempt}/{attempts - 1} after {type(exc).__name__}: {exc}", file=sys.stderr)
            sleep(backoff * attempt)


def fetch_range(symbol, start, end, get=requests.get, sleep=time.sleep):
    """Download all candles for symbol in [start, end]. Returns (rows, api_calls)."""
    loaded_at = datetime.now(UTC_PLUS_3).replace(tzinfo=None)
    rows, calls = [], 0
    for window_start, window_end in iter_windows(start, end):
        params = {
            "category": CATEGORY,
            "symbol": symbol,
            "interval": INTERVAL,
            "start": to_ms(window_start),
            "end": to_ms(window_end),
            "limit": PAGE_LIMIT,
        }
        payload = get_with_retry(get, params, sleep=sleep)
        calls += 1
        rows.extend(rows_from_payload(payload, symbol, window_start, window_end, loaded_at))
    return rows, calls


def logical_count(client, symbol, start, end, table=RAW_TABLE):
    return client.execute(
        f"SELECT count() FROM {table} FINAL WHERE exchange = 'bybit' AND category = %(c)s "
        "AND symbol = %(s)s AND interval = %(i)s AND open_time BETWEEN %(a)s AND %(b)s",
        {"c": CATEGORY, "s": symbol, "i": INTERVAL, "a": start, "b": end},
    )[0][0]


def insert_rows(client, rows, table=RAW_TABLE):
    if rows:
        client.execute(f"INSERT INTO {table} ({', '.join(COLUMNS)}) VALUES", rows)


def clickhouse_client():
    from clickhouse_driver import Client

    return Client(
        os.environ.get("CLICKHOUSE_HOST", "some-clickhouse-server"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "9000")),
        user=os.environ.get("CLICKHOUSE_USER", "airflow"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database="default",
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--symbol", choices=ALL_SYMBOLS)
    target.add_argument("--all", action="store_true", help="BTCUSDT, ETHUSDT, SOLUSDT")
    parser.add_argument("--start", required=True, type=parse_hour, help="UTC, inclusive")
    parser.add_argument("--end", required=True, type=parse_hour, help="UTC, inclusive")
    parser.add_argument("--dry-run", action="store_true", help="download and report, do not write")
    args = parser.parse_args(argv)

    if args.start > args.end:
        parser.error(f"--start {args.start} is after --end {args.end}")
    if args.end > datetime.now(timezone.utc).replace(tzinfo=None):
        parser.error(f"--end {args.end} is in the future")

    symbols = ALL_SYMBOLS if args.all else (args.symbol,)
    expected = int((args.end - args.start) / STEP) + 1
    client = None if args.dry_run else clickhouse_client()
    total_calls = total_rows = total_added = 0

    for symbol in symbols:
        rows, calls = fetch_range(symbol, args.start, args.end)
        total_calls += calls
        total_rows += len(rows)
        line = f"{symbol}: {calls} API calls, {len(rows)}/{expected} candles from source"
        if rows:
            line += f" ({rows[0][4]} .. {rows[-1][4]})"
        if client is None:
            print(line + " [dry-run, nothing written]")
            continue
        before = logical_count(client, symbol, args.start, args.end)
        insert_rows(client, rows)
        after = logical_count(client, symbol, args.start, args.end)
        total_added += after - before
        print(line + f", logical candles in range {before} -> {after} (+{after - before})")

    summary = f"TOTAL: {total_calls} API calls, {total_rows} candles downloaded"
    if client is not None:
        summary += f", {total_added} candles logically added"
    print(summary)


if __name__ == "__main__":
    main()
