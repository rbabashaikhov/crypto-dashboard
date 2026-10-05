"""Imports deployment/superset/dashboard_export into this Superset.

Uses Superset's own ImportDashboardsCommand, so dashboard/chart/dataset/database
UUIDs from the export are preserved. The exported database YAML has a masked
password; the real one is passed in from CLICKHOUSE_SUPERSET_PASSWORD.

Run: docker compose exec superset python /app/deploy/import_dashboard.py
"""
import os
from pathlib import Path

from superset.app import create_app

BUNDLE = Path("/app/bundle/dashboard_export")
ADMIN = os.environ.get("SUPERSET_ADMIN_USER", "admin")

app = create_app()
with app.app_context():
    from superset import security_manager
    from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
    from superset.utils.core import override_user

    contents = {
        str(p.relative_to(BUNDLE)): p.read_text()
        for p in BUNDLE.rglob("*.yaml")
    }
    passwords = {
        name: os.environ["CLICKHOUSE_SUPERSET_PASSWORD"]
        for name in contents
        if name.startswith("databases/")
    }
    with override_user(security_manager.find_user(ADMIN)):
        ImportDashboardsCommand(contents, passwords=passwords, overwrite=True).run()
    print(f"Imported {len(contents)} files from {BUNDLE}")

    # Superset 4.1 import remaps native filter targets and scope.excluded to the new
    # chart ids but leaves chartsInScope with the source ids. Recompute it from the
    # scope the same way the dashboard UI does when a filter is saved.
    import json

    import yaml
    from superset import db
    from superset.models.dashboard import Dashboard

    for name, text in contents.items():
        if not name.startswith("dashboards/"):
            continue
        dash = db.session.query(Dashboard).filter_by(uuid=yaml.safe_load(text)["uuid"]).one()
        meta = json.loads(dash.json_metadata or "{}")
        chart_ids = sorted(s.id for s in dash.slices)
        for flt in meta.get("native_filter_configuration", []):
            excluded = set(flt.get("scope", {}).get("excluded", []))
            flt["chartsInScope"] = [i for i in chart_ids if i not in excluded]
        dash.json_metadata = json.dumps(meta)
        db.session.commit()
        print(f"Fixed native filter scope for dashboard {dash.id} {dash.dashboard_title!r}")
