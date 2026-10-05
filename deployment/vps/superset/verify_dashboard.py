"""Checks the imported BYBIT dashboard end to end through Superset's query engine.

- dashboard, charts and datasets exist and every dataset resolves to a ClickHouse table
- every chart's saved query runs without error and returns rows
- the native "Symbol" filter has values and narrows the charts in its scope

Run: docker compose exec superset python /app/deploy/verify_dashboard.py
Exit code 1 if anything fails.
"""
import json
import os
import sys

from superset.app import create_app

DASHBOARD_UUID = "63182cb4-866e-4ac4-8eca-5d283e827b9f"

app = create_app()
with app.app_context():
    from superset import db, security_manager
    from superset.commands.chart.data.get_data_command import ChartDataCommand
    from superset.models.dashboard import Dashboard
    from superset.utils.core import override_user

    failures = []
    with override_user(security_manager.find_user(os.environ.get("SUPERSET_ADMIN_USER", "admin"))):
        dash = db.session.query(Dashboard).filter_by(uuid=DASHBOARD_UUID).one()
        print(f"dashboard: id={dash.id} title={dash.dashboard_title!r} charts={len(dash.slices)}")

        for ds in {s.datasource for s in dash.slices}:
            with ds.database.get_sqla_engine() as engine:
                n = engine.execute(f"SELECT count() FROM {ds.schema}.{ds.table_name}").scalar()
            print(f"dataset: {ds.schema}.{ds.table_name} db={ds.database.database_name!r} rows={n}")
            if not n:
                failures.append(f"dataset {ds.table_name} is empty")

        def run(slc, extra_filters=None):
            qc = slc.get_query_context()
            if extra_filters:
                for q in qc.queries:
                    q.filter += extra_filters
            return ChartDataCommand(qc).run()["queries"][0]

        for slc in sorted(dash.slices, key=lambda s: s.id):
            try:
                q = run(slc)
                last = (q.get("data") or [None])[-1]
                print(f"chart {slc.id} {slc.slice_name!r} ({slc.viz_type}): rows={q['rowcount']} last={str(last)[:120]}")
                if q.get("error") or not q["rowcount"]:
                    failures.append(f"chart {slc.slice_name}: {q.get('error') or 'no rows'}")
            except Exception as e:  # noqa: BLE001
                failures.append(f"chart {slc.slice_name}: {type(e).__name__}: {e}")
                print(f"chart {slc.id} {slc.slice_name!r}: FAILED {e}")

        # Native filter: values exist and filtering works on the charts in scope
        meta = json.loads(dash.json_metadata or "{}")
        for flt in meta.get("native_filter_configuration", []):
            col = flt["targets"][0]["column"]["name"]
            in_scope = [s for s in dash.slices if s.id in flt.get("chartsInScope", [])]
            if len(in_scope) != len(flt.get("chartsInScope", [])) or not in_scope:
                failures.append(f"filter {flt['name']}: chartsInScope {flt.get('chartsInScope')} not on dashboard")
                continue
            ds = in_scope[0].datasource
            with ds.database.get_sqla_engine() as engine:
                values = [r[0] for r in engine.execute(f"SELECT DISTINCT {col} FROM {ds.schema}.{ds.table_name} ORDER BY 1")]
            print(f"filter {flt['name']!r} on {col}: values={values} scope={[s.slice_name for s in in_scope]}")
            for slc in in_scope:
                q = run(slc, [{"col": col, "op": "IN", "val": [values[0]]}])
                cols = [c for c in q["colnames"] if c not in ("open_time", "__timestamp")]
                print(f"  {slc.slice_name!r} filtered to {values[0]}: rows={q['rowcount']} series={cols}")
                if q.get("error") or cols != [values[0]]:
                    failures.append(f"filter on {slc.slice_name} did not narrow to {values[0]}: {cols}")

    if failures:
        print("FAILED:", *failures, sep="\n  ")
        sys.exit(1)
    print("ALL CHECKS PASSED")
