# Static demo deployment (ClickHouse + Superset on a VPS)

```
Local Airflow -> Local ClickHouse -> snapshot (Native) -> VPS ClickHouse -> VPS Superset
```

Airflow stays local. The VPS runs only ClickHouse and a single-process Superset;
data on the VPS changes only when a new snapshot is exported and restored.

## Layout

| Path | What |
|---|---|
| `vps/compose.yaml` | Compose project `crypto-dashboard`: `clickhouse` + `superset` + `embed-token` |
| `vps/.env.example` | Variables for the server-side `vps/.env` (never committed) |
| `vps/clickhouse/config.d/low_memory.xml` | Small caches, no system log tables |
| `vps/clickhouse/initdb/` | First start: applies `sql/create_tables.sql`, creates read-only `superset` user |
| `vps/superset/` | Image (official `apache/superset:4.1.4` + `clickhouse-connect`), config, import/verify scripts, embed setup/verify + guest checks |
| `vps/token-service/` | Guest token service for the public embed (Python stdlib, no packages) |
| `superset/dashboard_export/` | Superset export of **BYBIT Dashboard** (6 charts, 6 datasets, DB connection with masked password) |
| `clickhouse/cdm_from_raw.sql` | CDM build SQL, mirror of `refresh_cdm()` in the DAG — keep in sync |
| `scripts/export_clickhouse_snapshot.sh` | Local: exports a deduplicated snapshot + `manifest.tsv` |
| `scripts/restore_clickhouse_snapshot.sh` | VPS: loads the snapshot and diffs counts against `manifest.tsv` |

Footprint: ClickHouse `mem_limit 768m`, Superset `mem_limit 1g` (1 gunicorn worker,
SQLite metadata, in-process cache, no Redis/Celery), embed-token `mem_limit 64m`.
Idle usage is ~90 MB + ~240 MB + ~16 MB.

ClickHouse has no host ports and is only on the project network. Superset binds
`127.0.0.1:${SUPERSET_LOCAL_PORT}` (admin through an SSH tunnel); the only public entry
is the read-only embed below.

## Snapshot format

Data is exported in ClickHouse **Native** format, one file per table, with the DDL
kept separately in `sql/create_tables.sql`. Native is ClickHouse's own columnar
binary format: types (`DateTime`, `Float64`) round-trip exactly, with no text
parsing or timezone conversion, files are small, and `INSERT ... FORMAT Native`
loads them into a clean server. `manifest.tsv` records row counts per table and
symbol plus the latest `open_time` / `loaded_at`, and the restore step diffs against it.

The snapshot is deduplicated at export time: raw keeps one row per
`(exchange, category, symbol, interval, open_time)` with the latest `loaded_at`, and CDM
is rebuilt from that raw inside an isolated `clickhouse local`. The local ClickHouse is
only read. Since raw became a `ReplacingMergeTree` this is a no-op for new data, but it
also keeps exports correct from a raw table that has not been migrated yet.

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

## Public embed (bi.apps.leadmeter.ru)

```
leadmeter.ru page --(@superset-ui/embedded-sdk)--> https://bi.apps.leadmeter.ru  (miniapps-edge Caddy, path allowlist)
   GET /guest-token  -> embed-token -> superset:8088  login -> csrf_token -> guest_token
   iframe /embedded/<EMBED_DASHBOARD_UUID> -> superset:8088 -> clickhouse (project network only)
```

- **Guest token**: issued only by `embed-token` with the service account `embed_token_svc`
  (role `EmbedTokenIssuer`: `can_grant_guest_token` + `can_read` on `SecurityRestApi`).
  Resource is fixed to the embedded uuid, no RLS, TTL 300 s, audience
  `crypto-dashboard-embed`, signed with `GUEST_TOKEN_JWT_SECRET` (Superset's default is
  public, never use it). `GUEST_TOKEN_VALIDATOR_HOOK` makes Superset refuse any other
  resource or RLS even for a leaked service password. The service caches one token for
  everyone and answers only `Origin` in `EMBED_ALLOWED_ORIGINS`.
- **Guest role** `EmbeddedGuest` (`GUEST_ROLE_NAME`): `can_read` on Dashboard and Chart,
  `can_time_range` on Api. Charts are read through Superset's embedded guest checks; no
  datasource, database, SQL Lab, Explore, CSV or filter-state permissions. The Public
  (anonymous) role stays empty.
- **Superset 4.1.4 gap, patched in `embed_security.py`** (`CUSTOM_SECURITY_MANAGER`): the
  guest payload check loads the saved chart through `ChartFilter`, which hides it from a
  guest, so changed columns and ad-hoc SQL passed. For guests the saved chart is loaded
  without the base filter, native filter queries are limited to the filter's column, and
  samples, SQL in `extras` and filters on SQL expressions are refused. Re-check after any
  Superset upgrade (`verify_embedding.py` covers it).
- **Framing**: `allowed_domains` = `EMBED_ALLOWED_ORIGINS` (Superset checks the Referer on
  `/embedded/<uuid>`; an empty list would allow every site), CSP `frame-ancestors 'self'`
  + the same origins, no `X-Frame-Options`.
- **Edge**: Caddy block `bi.apps.leadmeter.ru` in `/srv/miniapps/infrastructure/Caddyfile`, copy in `vps/caddy/`
  (wildcard DNS `*.apps.leadmeter.ru`, no DNS change). Routed: `GET /guest-token`;
  `GET /embedded/<uuid>`, `/static/*`, `/api/v1/me/roles/`, `/api/v1/dashboard/1`,
  `/api/v1/dashboard/1/charts`, `/api/v1/dashboard/1/datasets`, `/api/v1/time_range/`;
  `POST /api/v1/chart/data`. `POST /superset/log/` is answered 204 at the edge. Everything
  else (login, admin UI, SQL Lab, Explore, lists, other APIs) is 404. The access log
  drops request/response headers. This list is exactly what the 4.1.4 embedded page
  requests; `superset` and `embed-token` join `miniapps-net` (aliases `crypto-superset`,
  `crypto-embed-token`).

### First-time setup

```bash
cd <deploy_dir>/deployment/vps
# add to .env (chmod 600): GUEST_TOKEN_JWT_SECRET (openssl rand -base64 48),
# SUPERSET_EMBED_SERVICE_USER, SUPERSET_EMBED_SERVICE_PASSWORD, EMBED_ALLOWED_ORIGINS,
# EMBED_DASHBOARD_UUID= (empty for now); then rsync deployment/vps (config is read by exec)
set -a; . ./.env; set +a
docker compose exec -T -e GUEST_TOKEN_JWT_SECRET -e EMBED_ALLOWED_ORIGINS \
  -e SUPERSET_EMBED_SERVICE_USER -e SUPERSET_EMBED_SERVICE_PASSWORD \
  superset python /app/deploy/setup_embedding.py         # prints EMBED_DASHBOARD_UUID=...
# put that uuid into .env, then recreate superset with the new env/network and start the service
docker compose build embed-token && docker compose up -d superset embed-token
docker compose exec superset python /app/deploy/verify_dashboard.py
docker compose exec -e SUPERSET_EMBED_SERVICE_USER -e SUPERSET_EMBED_SERVICE_PASSWORD \
  superset python /app/deploy/verify_embedding.py
```

`setup_embedding.py` is idempotent (keeps the embedded uuid, re-syncs both roles to exactly
their permissions and the service password from `.env`). Then add the Caddy block: write
the Caddyfile **in place** (`cat new > Caddyfile`; it is a single-file bind mount, a new
inode is not seen by the container), `caddy validate`, `caddy reload` inside `miniapps-caddy`.

### Leadmeter page

```html
<div id="bybit-dashboard" style="width:100%;height:1000px"></div>
<script src="https://cdn.jsdelivr.net/npm/@superset-ui/embedded-sdk@0.1.3/bundle/index.js"></script>
<script>
  supersetEmbeddedSdk.embedDashboard({
    id: "<EMBED_DASHBOARD_UUID>",
    supersetDomain: "https://bi.apps.leadmeter.ru",
    mountPoint: document.getElementById("bybit-dashboard"),
    fetchGuestToken: () => fetch("https://bi.apps.leadmeter.ru/guest-token")
      .then(r => { if (!r.ok) throw new Error("guest token " + r.status); return r.json(); })
      .then(j => j.token),
    dashboardUiConfig: { hideTitle: true, filters: { expanded: true } },
  });
</script>
```

The page must keep sending its origin as Referer (default `strict-origin-when-cross-origin`;
`Referrer-Policy: no-referrer` would make Superset answer 403).

### Rollback

- Leadmeter: remove the embed block.
- Edge: restore the Caddyfile backup in place (`cat Caddyfile.bak-crypto-embed-* > Caddyfile`),
  `docker exec miniapps-caddy caddy reload --config /etc/caddy/Caddyfile`.
- Superset: deploy the previous `superset_config.py` and `compose.yaml`,
  `docker compose rm -sf embed-token && docker compose up -d superset`
  (without `EMBEDDED_SUPERSET`, `/embedded/*` returns 404 and guest tokens are ignored).
- Metadata: `setup_embedding.py --teardown` (embedded config, service user, both roles), or
  restore the `superset.db` backup.

## Re-exporting the dashboard

Use Superset's export (UI: Dashboards -> Export, or `ExportDashboardsCommand`), unzip
into `superset/dashboard_export/` and point the database URI host at `clickhouse:8123`.
Superset masks the password as `XXXXXXXXXX`; `import_dashboard.py` supplies the real
one from `CLICKHOUSE_SUPERSET_PASSWORD`.

`import_dashboard.py` works around two Superset 4.1 import gaps:

- `ImportDashboardsCommand` imports datasets and charts with a hard-coded
  `overwrite=False`, so existing ones keep their old config. The script first runs
  `ImportDatasetsCommand` and `ImportChartsCommand` with `overwrite=True` (databases are
  never overwritten, the server's ClickHouse password stays), then the dashboard.
- Native filter `chartsInScope` keeps the source instance's chart ids. The script restores
  the exported applied/excluded chart lists exactly via the export layout
  (source chart id -> chart uuid -> local id); it does not derive them from
  `scope.excluded`, because Symbol applies only to Price/Volume trend even though only
  Last day table is excluded.

Re-imports update objects in place by uuid; no duplicates are created.
`verify_dashboard.py` checks the exact filter targets (Symbol -> Price/Volume trend only,
waterfalls and Last day table excluded; Period -> Price/Volume trend and the three
waterfalls, Last day table excluded), Period's fixed saved default
(2026-09-05 00:00 <= time < 2026-10-05 00:00), Period applied the way the 4.1.4 frontend
sends it (`query.time_range`) for the default, "Last month" and a custom range, Symbol,
the waterfall config, and duplicates.

Waterfall charts use the temporal calculated column `step_date`
(`parseDateTime64BestEffort(step)`) as x-axis **without a time grain**: Superset 4.1.4's
waterfall orders by the raw x-axis, so with a grain ClickHouse rejects the query
(`NOT_AN_AGGREGATE`). The waterfall tables already hold one row per day.

## Known issues

- **Embed and Superset upgrades.** `embed_security.py` patches a Superset 4.1.4 guest check and
  the Caddy allowlist matches the 4.1.4 embedded frontend; after an upgrade run
  `verify_embedding.py` and a browser check before trusting either.

- **Gap in the VPS snapshot.** The current VPS snapshot predates the backfill and has no
  data for 2026-03-23 21:00 .. 2026-04-16 12:00 and 2026-04-24 21:00 .. 2026-09-27 02:00.
  The local dataset is continuous now; export and restore a new snapshot to update the VPS.
- **Time zones.** `open_time` is UTC (from Bybit), `loaded_at` is UTC+3.
- **VPS raw table engine.** The VPS ClickHouse was created before raw became a
  `ReplacingMergeTree` and still has the old `MergeTree` raw table. Its data is the
  deduplicated snapshot and the dashboard reads only CDM, so it is correct; restoring a
  snapshot keeps it that way. Apply `sql/migrations/001_bybit_api_replacing_merge_tree.sql`
  there when convenient.
- `cdm.bybit_boxplot_daily` exists in the local ClickHouse but is not created or
  refreshed by this repository; the dashboard does not use it.
