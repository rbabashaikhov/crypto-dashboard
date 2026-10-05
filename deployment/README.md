# Static demo deployment (ClickHouse + Superset on a VPS)

```
Local Airflow -> Local ClickHouse -> snapshot (Native) -> VPS ClickHouse -> VPS Superset
```

Airflow stays local. The VPS runs only ClickHouse and a single-process Superset;
data on the VPS changes only when a new snapshot is exported and restored.

## Layout

| Path | What |
|---|---|
| `vps/compose.yaml` | Compose project `crypto-dashboard`: `clickhouse` + `superset` |
| `vps/.env.example` | Variables for the server-side `vps/.env` (never committed) |
| `vps/clickhouse/config.d/low_memory.xml` | Small caches, no system log tables |
| `vps/clickhouse/initdb/` | First start: applies `sql/create_tables.sql`, creates read-only `superset` user |
| `vps/superset/` | Image (official `apache/superset:4.1.4` + `clickhouse-connect`), config, import/verify scripts |
| `superset/dashboard_export/` | Superset export of **BYBIT Dashboard** (6 charts, 6 datasets, DB connection with masked password) |
| `clickhouse/cdm_from_raw.sql` | CDM build SQL, mirror of `refresh_cdm()` in the DAG — keep in sync |
| `scripts/export_clickhouse_snapshot.sh` | Local: exports a deduplicated snapshot + `manifest.tsv` |
| `scripts/restore_clickhouse_snapshot.sh` | VPS: loads the snapshot and diffs counts against `manifest.tsv` |

Footprint: ClickHouse `mem_limit 768m`, Superset `mem_limit 1g` (1 gunicorn worker,
SQLite metadata, in-process cache, no Redis/Celery). Idle usage is ~80 MB + ~190 MB.

Nothing is published to the internet: ClickHouse has no host ports, Superset binds
`127.0.0.1:${SUPERSET_LOCAL_PORT}`.

## Snapshot format

Data is exported in ClickHouse **Native** format, one file per table, with the DDL
kept separately in `sql/create_tables.sql`. Native is ClickHouse's own columnar
binary format: types (`DateTime`, `Float64`) round-trip exactly, with no text
parsing or timezone conversion, files are small, and `INSERT ... FORMAT Native`
loads them into a clean server. `manifest.tsv` records row counts per table and
symbol plus the latest `open_time` / `loaded_at`, and the restore step diffs against it.

The snapshot is deduplicated at export time (see Known issues): raw keeps one row per
`(symbol, interval, open_time)` with the latest `loaded_at`, and CDM is rebuilt from
that raw inside an isolated `clickhouse local`. The local ClickHouse is only read.

Snapshot files are generated artifacts and are git-ignored.

## Refresh the demo

Local (Airflow + ClickHouse running):

```bash
docker exec airflow-airflow-scheduler-1 airflow dags test bybit_pipeline   # one run, DAG stays paused
CH_PASSWORD=... deployment/scripts/export_clickhouse_snapshot.sh
rsync -az deployment/clickhouse/snapshot/ <vps>:<deploy_dir>/deployment/clickhouse/snapshot/
```

VPS:

```bash
cd <deploy_dir>/deployment/vps
../scripts/restore_clickhouse_snapshot.sh            # prints RESTORE OK or a diff
docker compose exec superset python /app/deploy/verify_dashboard.py
```

## First-time setup on the VPS

```bash
rsync -az --exclude .env --include 'sql/***' --include 'deployment/***' --include README.md --exclude '*' \
  ./ <vps>:<deploy_dir>/
cd <deploy_dir>/deployment/vps
cp .env.example .env && chmod 600 .env    # fill with generated secrets
docker compose pull clickhouse && docker compose build superset
docker compose up -d clickhouse
../scripts/restore_clickhouse_snapshot.sh
set -a; . ./.env; set +a
docker compose run --rm --no-deps -e SUPERSET_ADMIN_PASSWORD superset bash -c '
  superset db upgrade &&
  superset fab create-admin --username "$SUPERSET_ADMIN_USER" --firstname Admin --lastname Demo \
    --email admin@localhost --password "$SUPERSET_ADMIN_PASSWORD" &&
  superset init'
docker compose up -d superset
docker compose exec superset python /app/deploy/import_dashboard.py
docker compose exec superset python /app/deploy/verify_dashboard.py
```

View it through an SSH tunnel: `ssh -N -L 18088:127.0.0.1:8088 <vps>`, then open
http://127.0.0.1:18088.

## Re-exporting the dashboard

Use Superset's export (UI: Dashboards -> Export, or `ExportDashboardsCommand`), unzip
into `superset/dashboard_export/` and point the database URI host at `clickhouse:8123`.
Superset masks the password as `XXXXXXXXXX`; `import_dashboard.py` supplies the real
one from `CLICKHOUSE_SUPERSET_PASSWORD`.

`import_dashboard.py` also rewrites native filter `chartsInScope` after import:
Superset 4.1 remaps filter targets and `scope.excluded` to the new chart ids but keeps
the source ids in `chartsInScope`, which would detach the Symbol filter from its charts.

## Known issues (not fixed in this stage)

- **Duplicate raw rows.** Each DAG run appends the latest 200 hourly candles without
  deleting overlapping ones, so `default.bybit_api` and the CDM tables repeat rows
  (locally ~2.7 rows per key on 2026-10-05). *Volume trend* uses `SUM(volume)`, so
  local volumes are inflated. The VPS snapshot is deduplicated; the DAG is unchanged.
- **Gap in history.** The DAG loads only the last 200 candles (~8 days), so the series
  has no data between 2026-04-24 and 2026-09-27.
- **Time zones.** `open_time` is UTC (from Bybit), `loaded_at` is UTC+3.
- **Local Airflow image lacks `clickhouse-driver`.** On 2026-10-05 it was installed into
  the running containers with pip and will be lost when they are recreated; add it to
  `_PIP_ADDITIONAL_REQUIREMENTS` or a custom image.
- `cdm.bybit_boxplot_daily` exists in the local ClickHouse but is not created or
  refreshed by this repository; the dashboard does not use it.
