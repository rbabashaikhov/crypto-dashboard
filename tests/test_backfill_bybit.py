"""Tests for tools/backfill_bybit.py.

Unit tests use a fake Bybit API (no network). Two optional groups:
- DAG mapping parity: runs where apache-airflow is importable (the Airflow image)
- ClickHouse rerun/duplicate test: runs when CLICKHOUSE_PASSWORD is set; uses a
  session TEMPORARY table with the raw table's engine, so nothing persistent is written

Run in the Airflow image (has all dependencies):
    docker run --rm --network airflow_default -v "$PWD":/repo -w /repo \
      -e CLICKHOUSE_PASSWORD=... --entrypoint python crypto-dashboard-airflow:2.10.0 \
      -m unittest discover -s tests -v
"""
import importlib.util
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import backfill_bybit as bf  # noqa: E402

H = timedelta(hours=1)


def ms(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def candle(dt, price=100.0):
    return [str(ms(dt)), str(price), str(price + 1), str(price - 1), str(price + 0.5), "10.5", "1050.25"]


class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self):
        return self._payload


class FakeBybit:
    """Mimics V5 kline: inclusive start/end, newest first, capped at limit."""

    def __init__(self, missing=()):
        self.calls = []
        self.missing = set(missing)

    def __call__(self, url, params, timeout):
        self.calls.append(dict(params))
        start = datetime.fromtimestamp(params["start"] / 1000, timezone.utc).replace(tzinfo=None)
        end = datetime.fromtimestamp(params["end"] / 1000, timezone.utc).replace(tzinfo=None)
        items, t = [], start
        while t <= end:
            if t not in self.missing:
                items.append(candle(t))
            t += H
        items = list(reversed(items))[: params["limit"]]
        return FakeResponse({"retCode": 0, "retMsg": "OK", "result": {"list": items}})


class WindowTests(unittest.TestCase):
    def test_windows_are_contiguous_without_overlap(self):
        start = datetime(2026, 4, 24, 21)
        end = start + H * 3725  # 3726 hours, the real gap size
        windows = list(bf.iter_windows(start, end))
        self.assertEqual(windows[0][0], start)
        self.assertEqual(windows[-1][1], end)
        for (a1, b1), (a2, _) in zip(windows, windows[1:]):
            self.assertEqual(a2, b1 + H)  # no gap, no overlap
        for a, b in windows:
            self.assertLessEqual(int((b - a) / H) + 1, bf.PAGE_LIMIT)
        self.assertEqual(sum(int((b - a) / H) + 1 for a, b in windows), 3726)
        self.assertEqual(len(windows), 4)

    def test_exact_page_sizes(self):
        start = datetime(2026, 1, 1)
        self.assertEqual(list(bf.iter_windows(start, start)), [(start, start)])
        self.assertEqual(len(list(bf.iter_windows(start, start + H * 999))), 1)
        self.assertEqual(len(list(bf.iter_windows(start, start + H * 1000))), 2)

    def test_start_after_end_rejected(self):
        with self.assertRaises(ValueError):
            list(bf.iter_windows(datetime(2026, 1, 2), datetime(2026, 1, 1)))


class PayloadTests(unittest.TestCase):
    start, end = datetime(2026, 4, 1, 0), datetime(2026, 4, 1, 3)
    loaded_at = datetime(2026, 10, 5, 12)

    def payload(self, items):
        return {"retCode": 0, "result": {"list": items}}

    def test_reverse_order_sorted_ascending(self):
        items = [candle(self.start + H * i) for i in range(4)][::-1]
        rows = bf.rows_from_payload(self.payload(items), "BTCUSDT", self.start, self.end, self.loaded_at)
        self.assertEqual([r[4] for r in rows], [self.start + H * i for i in range(4)])

    def test_mapping(self):
        rows = bf.rows_from_payload(self.payload([candle(self.start)]), "ETHUSDT", self.start, self.end, self.loaded_at)
        self.assertEqual(
            rows[0],
            ("bybit", "spot", "ETHUSDT", "60", self.start, 100.0, 101.0, 99.0, 100.5, 10.5, 1050.25, self.loaded_at),
        )

    def test_boundary_duplicates_and_out_of_window_dropped(self):
        items = [
            candle(self.start - H),  # before window
            candle(self.start), candle(self.start),  # duplicated boundary candle
            candle(self.end),
            candle(self.end + H),  # after window
        ]
        rows = bf.rows_from_payload(self.payload(items), "SOLUSDT", self.start, self.end, self.loaded_at)
        self.assertEqual([r[4] for r in rows], [self.start, self.end])

    def test_empty_response(self):
        self.assertEqual(bf.rows_from_payload(self.payload([]), "BTCUSDT", self.start, self.end, self.loaded_at), [])

    def test_ret_code_error_raises(self):
        with self.assertRaises(bf.BybitError):
            bf.rows_from_payload({"retCode": 10001, "retMsg": "params error"}, "BTCUSDT", self.start, self.end, self.loaded_at)


class PaginationTests(unittest.TestCase):
    def test_full_range_no_missing_no_duplicates(self):
        start = datetime(2026, 3, 23, 21)
        end = datetime(2026, 9, 27, 2)
        api = FakeBybit()
        rows, calls = bf.fetch_range("BTCUSDT", start, end, get=api)
        expected = int((end - start) / H) + 1
        times = [r[4] for r in rows]
        self.assertEqual(len(times), expected)
        self.assertEqual(len(set(times)), expected)
        self.assertEqual(times, sorted(times))
        self.assertEqual((times[0], times[-1]), (start, end))
        self.assertEqual(calls, len(api.calls))
        self.assertEqual(calls, -(-expected // bf.PAGE_LIMIT))
        self.assertTrue(all(c["limit"] == bf.PAGE_LIMIT for c in api.calls))

    def test_source_side_gap_is_not_filled(self):
        start, end = datetime(2026, 4, 1), datetime(2026, 4, 1, 5)
        api = FakeBybit(missing={datetime(2026, 4, 1, 2)})
        rows, _ = bf.fetch_range("BTCUSDT", start, end, get=api)
        self.assertEqual(len(rows), 5)
        self.assertNotIn(datetime(2026, 4, 1, 2), [r[4] for r in rows])


class RetryTests(unittest.TestCase):
    params = {"symbol": "BTCUSDT"}
    ok = FakeResponse({"retCode": 0, "result": {"list": []}})

    def test_dns_failure_retried_then_succeeds(self):
        get = mock.Mock(side_effect=[requests.ConnectionError("Failed to resolve 'api.bybit.com'"), requests.Timeout(), self.ok])
        sleep = mock.Mock()
        self.assertEqual(bf.get_with_retry(get, self.params, sleep=sleep)["retCode"], 0)
        self.assertEqual(get.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_gives_up_after_attempts(self):
        get = mock.Mock(side_effect=requests.ConnectionError("down"))
        with self.assertRaises(requests.ConnectionError):
            bf.get_with_retry(get, self.params, attempts=3, sleep=mock.Mock())
        self.assertEqual(get.call_count, 3)

    def test_rate_limit_and_5xx_retried(self):
        get = mock.Mock(side_effect=[
            FakeResponse({"retCode": 10006, "retMsg": "Too many visits"}),
            FakeResponse({}, status=503),
            self.ok,
        ])
        self.assertEqual(bf.get_with_retry(get, self.params, sleep=mock.Mock())["retCode"], 0)

    def test_client_error_not_retried(self):
        get = mock.Mock(return_value=FakeResponse({}, status=400))
        with self.assertRaises(requests.HTTPError):
            bf.get_with_retry(get, self.params, sleep=mock.Mock())
        self.assertEqual(get.call_count, 1)


class CliValidationTests(unittest.TestCase):
    def run_cli(self, *args):
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit) as ctx:
            bf.main(list(args))
        return ctx.exception.code

    def test_invalid_date_format(self):
        self.assertEqual(self.run_cli("--all", "--start", "2026-13-01", "--end", "2026-04-01", "--dry-run"), 2)

    def test_not_whole_hour(self):
        self.assertEqual(self.run_cli("--all", "--start", "2026-04-01T10:30", "--end", "2026-04-02", "--dry-run"), 2)

    def test_start_after_end(self):
        self.assertEqual(self.run_cli("--all", "--start", "2026-04-02", "--end", "2026-04-01", "--dry-run"), 2)

    def test_end_in_future(self):
        future = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")
        self.assertEqual(self.run_cli("--all", "--start", "2026-04-01", "--end", future, "--dry-run"), 2)

    def test_symbol_and_all_exclusive(self):
        self.assertEqual(self.run_cli("--all", "--symbol", "BTCUSDT", "--start", "2026-04-01", "--end", "2026-04-02", "--dry-run"), 2)

    def test_unknown_symbol(self):
        self.assertEqual(self.run_cli("--symbol", "DOGEUSDT", "--start", "2026-04-01", "--end", "2026-04-02", "--dry-run"), 2)


@unittest.skipUnless(importlib.util.find_spec("airflow"), "apache-airflow not installed")
class DagParityTests(unittest.TestCase):
    """Backfill rows must match what the DAG's fetch_klines() inserts."""

    def test_same_columns_and_values_as_dag(self):
        spec = importlib.util.spec_from_file_location("bybit_pipeline", ROOT / "dags" / "bybit_pipeline.py")
        dag = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(dag)

        start = datetime(2026, 4, 1)
        items = [candle(start + H * i, price=100 + i) for i in range(3)][::-1]
        payload = {"retCode": 0, "result": {"list": items}}
        with mock.patch.object(dag.requests, "get", return_value=FakeResponse(payload)):
            df = dag.fetch_klines("BTCUSDT")
        dag_rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
        tool_rows = bf.rows_from_payload(payload, "BTCUSDT", start, start + H * 2, loaded_at=None)

        self.assertEqual(list(df.columns), list(bf.COLUMNS))
        for d, t in zip(dag_rows, tool_rows):
            self.assertEqual(d[:4], t[:4])
            self.assertEqual(d[4].to_pydatetime(), t[4])
            self.assertEqual(d[5:11], t[5:11])


@unittest.skipUnless(os.environ.get("CLICKHOUSE_PASSWORD"), "CLICKHOUSE_PASSWORD not set")
class ClickHouseRerunTests(unittest.TestCase):
    """Re-inserting the same range must not add logical candles; newest version wins."""

    TABLE = "backfill_rerun_test"

    def setUp(self):
        self.client = bf.clickhouse_client()
        ddl = self.client.execute("SHOW CREATE TABLE default.bybit_api")[0][0]
        engine = ddl[ddl.index("ENGINE"):]
        self.client.execute(f"CREATE TEMPORARY TABLE {self.TABLE} AS default.bybit_api {engine}")

    def tearDown(self):
        self.client.disconnect()  # drops the session TEMPORARY table

    def test_rerun_same_range(self):
        start, end = datetime(2026, 4, 1), datetime(2026, 4, 2, 23)  # 48 candles, 1 page
        first, _ = bf.fetch_range("BTCUSDT", start, end, get=FakeBybit())
        bf.insert_rows(self.client, first, table=self.TABLE)
        self.assertEqual(bf.logical_count(self.client, "BTCUSDT", start, end, table=self.TABLE), 48)

        # Second run: same range, later loaded_at, updated volume on the last candle
        second = [r[:11] + (r[11] + timedelta(minutes=5),) for r in first]
        second[-1] = second[-1][:9] + (999.0,) + second[-1][10:]
        bf.insert_rows(self.client, second, table=self.TABLE)

        self.assertEqual(bf.logical_count(self.client, "BTCUSDT", start, end, table=self.TABLE), 48)
        dups = self.client.execute(
            f"SELECT count() FROM (SELECT 1 FROM {self.TABLE} FINAL "
            "GROUP BY exchange, category, symbol, interval, open_time HAVING count() > 1)"
        )[0][0]
        self.assertEqual(dups, 0)
        latest_volume = self.client.execute(
            f"SELECT volume FROM {self.TABLE} FINAL WHERE open_time = %(t)s", {"t": end}
        )[0][0]
        self.assertEqual(latest_volume, 999.0)


if __name__ == "__main__":
    unittest.main()
