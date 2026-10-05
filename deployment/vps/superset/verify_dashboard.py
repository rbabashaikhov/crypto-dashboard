"""Checks the BYBIT dashboard end to end through Superset's query engine.

- dashboard, charts and datasets exist and every dataset resolves to a ClickHouse table
- every chart's saved query runs without error and returns rows
- native filters apply to exactly the expected charts and exclude the rest:
    Symbol -> Price trend, Volume trend (excluded: Last day table, BTC/ETH/SOL Waterfall)
    Period -> Price trend, Volume trend, BTC/ETH/SOL Waterfall (excluded: Last day table)
- Period has the fixed saved default PERIOD_DEFAULT, Symbol has no default
- Symbol narrows its charts to the chosen symbol
- Period (saved default, "Last month", a custom range) limits every chart in scope
  to that range, and Last day table is unaffected

Native filter values are applied the way the Superset 4.1.4 dashboard frontend
sends them: a time filter sets extra_form_data.time_range and query.time_range
(the chart's own TEMPORAL_RANGE filter stays "No filter"); a select adds an IN filter.

Run: docker compose exec superset python /app/deploy/verify_dashboard.py
Exit code 1 if anything fails.
"""
import copy
import json
import os
import sys

import pandas as pd

from superset.app import create_app

DASHBOARD_UUID = "63182cb4-866e-4ac4-8eca-5d283e827b9f"
WATERFALLS = {"BTC Waterfall", "ETH Waterfall", "SOL Waterfall"}
EXPECTED_SCOPE = {
    "Symbol": {"Price trend", "Volume trend"},
    "Period": {"Price trend", "Volume trend"} | WATERFALLS,
}
EXPECTED_EXCLUDED = {
    "Symbol": {"Last day table"} | WATERFALLS,
    "Period": {"Last day table"},
}
# Fixed default saved in the dashboard: 2026-09-05 00:00 <= time < 2026-10-05 00:00
PERIOD_DEFAULT = 'DATEADD(DATETIME("2026-10-05T00:00:00"), -1, month) : 2026-10-05T00:00:00'
EXPECTED_DEFAULT = {"Symbol": None, "Period": PERIOD_DEFAULT}
CUSTOM_RANGE = "2026-05-01T00:00:00 : 2026-05-22T00:00:00"

app = create_app()
with app.app_context():
    from superset import db, security_manager
    from superset.charts.schemas import ChartDataQueryContextSchema
    from superset.commands.chart.data.get_data_command import ChartDataCommand
    from superset.models.dashboard import Dashboard
    from superset.utils.core import override_user
    from superset.utils.date_parser import get_since_until

    failures = []

    def fail(msg):
        failures.append(msg)
        print(f"  FAIL {msg}")

    def run(slc, time_range=None, symbols=None):
        qc = copy.deepcopy(json.loads(slc.query_context))
        qc["force"] = True
        if time_range:
            qc["form_data"]["extra_form_data"] = {"time_range": time_range}
        for q in qc["queries"]:
            if time_range:
                q["time_range"] = time_range
            if symbols:
                q["filters"].append({"col": "symbol", "op": "IN", "val": symbols})
        command = ChartDataCommand(ChartDataQueryContextSchema().load(qc))
        command.validate()
        return command.run()["queries"][0]

    def x_values(q):
        df = pd.DataFrame(q["data"])
        col = next((c for c in ("open_time", "step_date", "step", "last_open_time") if c in df), None)
        return pd.to_datetime(df[col]) if col else pd.Series(dtype="datetime64[ns]")

    with override_user(security_manager.find_user(os.environ.get("SUPERSET_ADMIN_USER", "admin"))):
        dash = db.session.query(Dashboard).filter_by(uuid=DASHBOARD_UUID).one()
        charts = {s.slice_name: s for s in dash.slices}
        names = {s.id: s.slice_name for s in dash.slices}
        print(f"dashboard: id={dash.id} title={dash.dashboard_title!r} charts={len(charts)}")

        for ds in {s.datasource for s in dash.slices}:
            with ds.database.get_sqla_engine() as engine:
                n = engine.execute(f"SELECT count() FROM {ds.schema}.{ds.table_name}").scalar()
            temporal = [c.column_name for c in ds.columns if c.is_dttm]
            print(f"dataset: {ds.schema}.{ds.table_name} rows={n} temporal={temporal}")
            if not n:
                fail(f"dataset {ds.table_name} is empty")

        # No duplicates of the dashboard, its charts or datasets
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.slice import Slice

        dups = {
            "dashboards": db.session.query(Dashboard).filter_by(dashboard_title=dash.dashboard_title).count() - 1,
            "charts": sum(db.session.query(Slice).filter_by(slice_name=n).count() - 1 for n in charts),
            "datasets": sum(
                db.session.query(SqlaTable).filter_by(schema=s.datasource.schema, table_name=s.datasource.table_name).count() - 1
                for s in dash.slices
            ),
        }
        print(f"duplicates: {dups}")
        if any(dups.values()):
            fail(f"duplicates found: {dups}")

        # Waterfall charts: step_date axis, no time grain (Superset 4.1.4 waterfall
        # orders by the raw axis, so a grain breaks GROUP BY on ClickHouse), and the
        # only temporal filter is the step_date one the Period filter overrides
        for name, slc in sorted(charts.items()):
            if slc.viz_type != "waterfall":
                continue
            p, qc = json.loads(slc.params), json.loads(slc.query_context)
            temporal = [(f["subject"], f["comparator"]) for f in p.get("adhoc_filters", []) if f.get("operator") == "TEMPORAL_RANGE"]
            q = qc["queries"][0]
            ok = (p.get("x_axis") == "step_date" and not p.get("time_grain_sqla")
                  and temporal == [("step_date", "No filter")]
                  and [c["sqlExpression"] for c in q["columns"]] == ["step_date"]
                  and not any("timeGrain" in c for c in q["columns"])
                  and [f["col"] for f in q["filters"] if f["op"] == "TEMPORAL_RANGE"] == ["step_date"])
            print(f"waterfall {name!r}: x_axis={p.get('x_axis')} time_grain={p.get('time_grain_sqla')} temporal_filters={temporal} {'PASS' if ok else 'FAIL'}")
            if not ok:
                fail(f"waterfall config of {name}")

        # Native filter configuration
        filters = {f["name"]: f for f in json.loads(dash.json_metadata or "{}").get("native_filter_configuration", [])}
        for name, expected in EXPECTED_SCOPE.items():
            flt = filters.get(name)
            if not flt:
                fail(f"native filter {name!r} missing")
                continue
            applied = {names.get(i, f"<missing chart {i}>") for i in flt.get("chartsInScope", [])}
            excluded = {names.get(i, f"<missing chart {i}>") for i in flt.get("scope", {}).get("excluded", [])}
            mask = flt.get("defaultDataMask", {})
            default = mask.get("filterState", {}).get("value")
            default_sent = mask.get("extraFormData", {}).get("time_range")
            print(f"filter {name!r} ({flt['filterType']}): applied={sorted(applied)} excluded={sorted(excluded)} default={default!r}")
            if applied != expected:
                fail(f"filter {name!r} applies to {sorted(applied)}, expected {sorted(expected)}")
            if excluded != EXPECTED_EXCLUDED[name]:
                fail(f"filter {name!r} excludes {sorted(excluded)}, expected {sorted(EXPECTED_EXCLUDED[name])}")
            if default != EXPECTED_DEFAULT[name] or (flt["filterType"] == "filter_time" and default_sent != EXPECTED_DEFAULT[name]):
                fail(f"filter {name!r} default {default!r} / extraFormData {default_sent!r}, expected {EXPECTED_DEFAULT[name]!r}")

        # 1. No filters: every chart runs
        baseline = {}
        for name, slc in sorted(charts.items()):
            try:
                q = run(slc)
            except Exception as e:  # noqa: BLE001
                fail(f"chart {name}: {type(e).__name__}: {e}")
                continue
            xs = x_values(q)
            baseline[name] = q
            span = f" {xs.min().date()}..{xs.max().date()}" if len(xs) else ""
            print(f"chart {name!r} ({slc.viz_type}): rows={q['rowcount']}{span}")
            if q.get("error") or not q["rowcount"]:
                fail(f"chart {name}: {q.get('error') or 'no rows'}")

        # 2. Period: the saved default (what the dashboard opens with), Last month, a custom range
        for time_range in (PERIOD_DEFAULT, "Last month", CUSTOM_RANGE):
            since, until = get_since_until(time_range=time_range)
            print(f"period {time_range!r} -> [{since}, {until})")
            for name in sorted(EXPECTED_SCOPE["Period"]):
                q = run(charts[name], time_range=time_range)
                xs = x_values(q)
                ok = not q.get("error") and len(xs) and xs.min() >= pd.Timestamp(since).normalize() and xs.max() < pd.Timestamp(until)
                print(f"  {name!r}: rows={q['rowcount']} {xs.min().date() if len(xs) else '-'}..{xs.max().date() if len(xs) else '-'} {'PASS' if ok else 'FAIL'}")
                if not ok:
                    fail(f"period {time_range!r} on {name}: rows={q['rowcount']} error={q.get('error')}")
            # Out-of-scope chart: the dashboard does not send Period to it
            excluded = [n for n in charts if n not in EXPECTED_SCOPE["Period"]]
            for name in excluded:
                q = run(charts[name])
                same = q["data"] == baseline[name]["data"]
                print(f"  {name!r} (not in scope): unchanged={'PASS' if same else 'FAIL'}")
                if not same:
                    fail(f"{name} changed under period {time_range!r}")

        # 3. Symbol narrows its charts
        for name in sorted(EXPECTED_SCOPE["Symbol"]):
            for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
                q = run(charts[name], symbols=[sym])
                series = [c for c in q["colnames"] if c not in ("open_time", "__timestamp")]
                ok = not q.get("error") and series == [sym]
                print(f"symbol {sym} on {name!r}: rows={q['rowcount']} series={series} {'PASS' if ok else 'FAIL'}")
                if not ok:
                    fail(f"symbol {sym} on {name}: series={series} error={q.get('error')}")

    if failures:
        print("FAILED:", *failures, sep="\n  ")
        sys.exit(1)
    print("ALL CHECKS PASSED")
