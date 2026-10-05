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
    import yaml
    from superset import security_manager
    from superset.commands.chart.importers.dispatcher import ImportChartsCommand
    from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
    from superset.commands.dataset.importers.dispatcher import ImportDatasetsCommand
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

    def subset(model_type, *prefixes):
        """Bundle files for one import command, with metadata.yaml typed for it."""
        metadata = {**yaml.safe_load(contents["metadata.yaml"]), "type": model_type}
        files = {k: v for k, v in contents.items() if k.startswith(prefixes)}
        return {"metadata.yaml": yaml.safe_dump(metadata), **files}

    # Superset 4.1 ImportDashboardsCommand imports datasets and charts with a
    # hard-coded overwrite=False, so existing ones would keep their old config.
    # Update them with their own import commands first (databases are never
    # overwritten, so the server's ClickHouse password stays as configured).
    with override_user(security_manager.find_user(ADMIN)):
        ImportDatasetsCommand(subset("SqlaTable", "databases/", "datasets/"), passwords=passwords, overwrite=True).run()
        ImportChartsCommand(subset("Slice", "databases/", "datasets/", "charts/"), passwords=passwords, overwrite=True).run()
        ImportDashboardsCommand(contents, passwords=passwords, overwrite=True).run()
    print(f"Imported {len(contents)} files from {BUNDLE} (datasets, charts, dashboard overwritten by uuid)")

    # Superset 4.1 import leaves native filter chartsInScope with the source
    # instance's chart ids. The applied list is not derivable from scope.excluded
    # (e.g. Symbol has excluded=[Last day table] but applies only to the charts
    # whose dataset has `symbol`), so restore the exported lists exactly:
    # source chart id -> chart uuid (export layout) -> chart id on this instance.
    import json

    from superset import db
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    for name, text in contents.items():
        if not name.startswith("dashboards/"):
            continue
        exported = yaml.safe_load(text)
        source_uuid = {
            node["meta"]["chartId"]: node["meta"]["uuid"]
            for node in exported["position"].values()
            if isinstance(node, dict) and node.get("type") == "CHART"
        }
        local_id = {str(s.uuid): s.id for s in db.session.query(Slice).filter(Slice.uuid.in_(source_uuid.values()))}

        def remap(ids, what):
            missing = [i for i in ids if source_uuid.get(i) not in local_id]
            if missing:
                raise SystemExit(f"{what}: chart ids {missing} have no uuid match after import")
            return [local_id[source_uuid[i]] for i in ids]

        dash = db.session.query(Dashboard).filter_by(uuid=exported["uuid"]).one()
        meta = json.loads(dash.json_metadata or "{}")
        exported_filters = {f["id"]: f for f in exported["metadata"].get("native_filter_configuration", [])}
        for flt in meta.get("native_filter_configuration", []):
            src = exported_filters[flt["id"]]
            flt["chartsInScope"] = remap(src.get("chartsInScope", []), f"{flt['name']}.chartsInScope")
            flt["scope"]["excluded"] = remap(src.get("scope", {}).get("excluded", []), f"{flt['name']}.scope.excluded")
            names = {s.id: s.slice_name for s in dash.slices}
            print(f"Native filter {flt['name']!r}: applied={[names[i] for i in flt['chartsInScope']]} "
                  f"excluded={[names[i] for i in flt['scope']['excluded']]}")
        dash.json_metadata = json.dumps(meta)
        db.session.commit()
