"""Checks the public read-only embed of BYBIT Dashboard from inside the superset container.

- config: EMBEDDED_SUPERSET on, non-default guest JWT secret, guest role, fixed audience,
  validator hook, framing only from EMBED_ALLOWED_ORIGINS, Public role empty
- roles EmbeddedGuest / EmbedTokenIssuer have exactly their permissions, the service
  account has only EmbedTokenIssuer, the embedded config allows exactly the origins
- embed-token: token for allowed origins only, scoped to the embedded dashboard, no RLS, 300 s
- with a guest token: the dashboard, its charts and filters (Period, Symbol) work over HTTP;
  lists, database/dataset/SQL Lab/Explore/query APIs and changed chart or filter payloads
  (extra columns, ad-hoc SQL, samples) are refused;
  forged, expired and wrong-audience tokens are rejected
- the service account cannot get a token for anything else (dashboard id, dashboard uuid,
  unknown uuid, extra resource, RLS) and cannot read Superset objects
- /embedded/<uuid> answers only with an allowed Referer and is frameable only by the origins

Run: docker compose exec -e SUPERSET_EMBED_SERVICE_USER -e SUPERSET_EMBED_SERVICE_PASSWORD \
       superset python /app/deploy/verify_embedding.py
Exit code 1 if anything fails.
"""
import copy
import json
import os
import sys
import time
import uuid

import jwt
import requests

import embedding_spec as spec
from superset.app import create_app

DASHBOARD_UUID = "63182cb4-866e-4ac4-8eca-5d283e827b9f"
SUPERSET = "http://127.0.0.1:8088"
TOKEN_SERVICE = "http://embed-token:8080/guest-token"
DEFAULT_SECRET = "test-guest-secret-change-me"
DENIED = {401, 403, 404, 405}

app = create_app()
with app.app_context():
    from superset import db, security_manager as sm
    from superset.extensions import feature_flag_manager
    from superset.models.dashboard import Dashboard
    from superset.models.embedded_dashboard import EmbeddedDashboard

    c = app.config
    failures = []

    def check(ok, msg):
        print(f"  {'PASS' if ok else 'FAIL'} {msg}")
        if not ok:
            failures.append(msg)

    origins = c["EMBED_ALLOWED_ORIGINS"]
    embed_uuid = c["EMBED_DASHBOARD_UUID"]
    dash = db.session.query(Dashboard).filter_by(uuid=DASHBOARD_UUID).one()

    print("config")
    check(feature_flag_manager.is_feature_enabled("EMBEDDED_SUPERSET"), "EMBEDDED_SUPERSET enabled")
    secret = c["GUEST_TOKEN_JWT_SECRET"]
    check(secret != DEFAULT_SECRET and len(secret) >= 32, "GUEST_TOKEN_JWT_SECRET is not the default and >= 32 chars")
    check(c["GUEST_ROLE_NAME"] == spec.GUEST_ROLE, f"GUEST_ROLE_NAME={c['GUEST_ROLE_NAME']}")
    check(c["GUEST_TOKEN_JWT_EXP_SECONDS"] == 300, f"GUEST_TOKEN_JWT_EXP_SECONDS={c['GUEST_TOKEN_JWT_EXP_SECONDS']}")
    check(bool(c["GUEST_TOKEN_JWT_AUDIENCE"]), f"GUEST_TOKEN_JWT_AUDIENCE={c['GUEST_TOKEN_JWT_AUDIENCE']!r}")
    check(callable(c["GUEST_TOKEN_VALIDATOR_HOOK"]), "GUEST_TOKEN_VALIDATOR_HOOK set")
    check(origins == ["https://leadmeter.ru", "https://www.leadmeter.ru"], f"EMBED_ALLOWED_ORIGINS={origins}")
    check(c["TALISMAN_CONFIG"].get("frame_options") is None, "no X-Frame-Options (CSP frame-ancestors instead)")
    check(c["TALISMAN_CONFIG"]["content_security_policy"]["frame-ancestors"] == ["'self'", *origins], "frame-ancestors = 'self' + origins")
    public = sm.find_role(c["AUTH_ROLE_PUBLIC"])
    check(not public.permissions and not c.get("PUBLIC_ROLE_LIKE"), f"anonymous role {public.name} has no permissions")

    print("metadata")
    for name, wanted in ((spec.GUEST_ROLE, spec.GUEST_PERMISSIONS), (spec.ISSUER_ROLE, spec.ISSUER_PERMISSIONS)):
        role = sm.find_role(name)
        actual = {(p.permission.name, p.view_menu.name) for p in role.permissions} if role else None
        check(actual == wanted, f"role {name} permissions exactly {sorted(wanted)}" + ("" if actual == wanted else f", got {actual}"))
    svc_name = os.environ["SUPERSET_EMBED_SERVICE_USER"]
    svc = sm.find_user(svc_name)
    check(svc and [r.name for r in svc.roles] == [spec.ISSUER_ROLE], f"user {svc_name} has only {spec.ISSUER_ROLE}")
    embedded = db.session.query(EmbeddedDashboard).all()
    check([(str(e.uuid), e.dashboard_id) for e in embedded] == [(embed_uuid, dash.id)], f"only {dash.dashboard_title!r} is embedded, as {embed_uuid}")
    check(embedded and embedded[0].allowed_domains == origins, f"allowed_domains={embedded[0].allowed_domains if embedded else None}")

    print("token service")
    r = requests.get(TOKEN_SERVICE, headers={"Origin": origins[0]}, timeout=30)
    check(r.status_code == 200 and r.headers.get("Access-Control-Allow-Origin") == origins[0]
          and r.headers.get("Cache-Control") == "no-store", f"Origin {origins[0]}: {r.status_code} ACAO={r.headers.get('Access-Control-Allow-Origin')}")
    token = r.json().get("token", "") if r.ok else ""
    claims = jwt.decode(token, secret, algorithms=["HS256"], audience=c["GUEST_TOKEN_JWT_AUDIENCE"]) if token else {}
    check(claims.get("type") == "guest" and claims.get("resources") == [{"type": "dashboard", "id": embed_uuid}]
          and claims.get("rls_rules") == [] and claims.get("exp", 0) - claims.get("iat", 0) == 300,
          f"token: resources={claims.get('resources')} rls={claims.get('rls_rules')} ttl={claims.get('exp', 0) - claims.get('iat', 0)}s")
    for origin in origins[1:]:
        r = requests.get(TOKEN_SERVICE, headers={"Origin": origin}, timeout=30)
        check(r.status_code == 200 and r.headers.get("Access-Control-Allow-Origin") == origin, f"Origin {origin}: {r.status_code}")
    for headers in ({"Origin": "https://evil.example"}, {"Origin": "http://leadmeter.ru"}, {}):
        r = requests.get(TOKEN_SERVICE, headers=headers, timeout=30)
        check(r.status_code == 403 and "Access-Control-Allow-Origin" not in r.headers, f"Origin {headers.get('Origin')}: {r.status_code}")

    print("guest over HTTP")
    s = requests.Session()
    s.headers[c["GUEST_TOKEN_HEADER_NAME"]] = token

    def status(method, path, **kw):
        return s.request(method, SUPERSET + path, allow_redirects=False, timeout=60, **kw).status_code

    r = s.get(f"{SUPERSET}/api/v1/me/roles/", timeout=30)
    check(r.ok and list(r.json()["result"]["roles"]) == [spec.GUEST_ROLE], f"/api/v1/me/roles/: {r.status_code} {r.json().get('result', {}).get('roles', {}).keys() if r.ok else ''}")
    for path in (f"/api/v1/dashboard/{dash.id}", f"/api/v1/dashboard/{dash.id}/charts", f"/api/v1/dashboard/{dash.id}/datasets", "/api/v1/security/csrf_token/"):
        check(status("GET", path) == 200, f"GET {path}")
    r = s.get(f"{SUPERSET}/api/v1/time_range/", params={"q": "'Last month'"}, timeout=30)  # rison string, as the frontend sends it
    check(r.status_code == 200, f"GET /api/v1/time_range/ (Period filter): {r.status_code}")
    csrf = s.get(f"{SUPERSET}/api/v1/security/csrf_token/", timeout=30).json()["result"]
    s.headers.update({"X-CSRFToken": csrf, "Referer": SUPERSET})  # so refused writes are refused by authz, not CSRF
    r = s.post(f"{SUPERSET}/api/v1/dashboard/{dash.id}/filter_state", json={"value": "{}"}, timeout=30)
    check(r.status_code == 201, f"POST filter_state (native filter state): {r.status_code}")

    def chart_data(slc, time_range=None, symbols=None, tamper=None):
        # The dashboard frontend sends slice_id and dashboardId; the guest check needs both
        qc = copy.deepcopy(json.loads(slc.query_context))
        qc["form_data"].update(slice_id=slc.id, dashboardId=dash.id)
        for q in qc["queries"]:
            if time_range:
                q["time_range"] = time_range
            if symbols:
                q["filters"].append({"col": "symbol", "op": "IN", "val": symbols})
        if tamper:
            tamper(qc)
        return s.post(f"{SUPERSET}/api/v1/chart/data", json=qc, timeout=60)

    charts = {x.slice_name: x for x in dash.slices}
    for name, slc in sorted(charts.items()):
        r = chart_data(slc)
        rows = r.json()["result"][0]["rowcount"] if r.ok else None
        check(r.ok and rows, f"chart/data {name!r}: {r.status_code} rows={rows}")
    r = chart_data(charts["Price trend"], time_range="Last month")
    check(r.ok and r.json()["result"][0]["rowcount"], f"Period (time_range) on 'Price trend': {r.status_code}")
    r = chart_data(charts["Price trend"], symbols=["BTCUSDT"])
    series = [x for x in r.json()["result"][0]["colnames"] if x not in ("open_time", "__timestamp")] if r.ok else None
    check(r.ok and series == ["BTCUSDT"], f"Symbol=BTCUSDT on 'Price trend': {r.status_code} series={series}")
    # Changed payloads: Superset 4.1.4 alone lets these through for guests (embed_security.py)
    def q0(fn):
        return lambda qc: fn(qc["queries"][0])

    price = charts["Price trend"]
    waterfall_ds = charts["BTC Waterfall"].datasource
    for label, tamper in (
        ("extra column", q0(lambda q: q["columns"].append("loaded_at"))),
        ("ad-hoc SQL metric under the saved label", q0(lambda q: q.update(metrics=[
            {"expressionType": "SQL", "sqlExpression": "max(high)", "label": "AVG(close)"}]))),
        ("ad-hoc SQL column", q0(lambda q: q["columns"].append(
            {"expressionType": "SQL", "sqlExpression": "version()", "label": "v"}))),
        ("extras.where SQL", q0(lambda q: q["extras"].update(where="1 = 1"))),
        ("filter on a SQL expression", q0(lambda q: q["filters"].append(
            {"col": {"sqlExpression": "currentUser()", "label": "u"}, "op": "IS NOT NULL"}))),
        ("result_type samples", lambda qc: qc.update(result_type="samples")),
        ("no slice_id", lambda qc: qc["form_data"].pop("slice_id")),
        ("another chart's dataset", lambda qc: qc.update(datasource={"id": waterfall_ds.id, "type": "table"})),
    ):
        r = chart_data(price, tamper=tamper)
        check(r.status_code in DENIED, f"guest chart/data with {label} refused: {r.status_code}")

    # Symbol filter options: a NATIVE_FILTER query on the filter's target dataset
    symbol = next(f for f in json.loads(dash.json_metadata)["native_filter_configuration"] if f["name"] == "Symbol")
    target = symbol["targets"][0]
    col = target["column"]["name"]

    def filter_options(**query):
        return s.post(f"{SUPERSET}/api/v1/chart/data", timeout=60, json={
            "datasource": {"id": target["datasetId"], "type": "table"},
            "form_data": {"type": "NATIVE_FILTER", "native_filter_id": symbol["id"], "dashboardId": dash.id,
                          "datasource": f"{target['datasetId']}__table", "viz_type": "filter_select", "groupby": [col]},
            "queries": [{"columns": [col], "metrics": [], "filters": [], "orderby": [[col, True]], "row_limit": 1000,
                         "time_range": "No filter", "extras": {"where": "", "having": ""}, **query}],
            "result_format": "json", "result_type": "full",
        })

    r = filter_options()
    options = sorted(row[col] for row in r.json()["result"][0]["data"]) if r.ok else None
    check(r.ok and options == ["BTCUSDT", "ETHUSDT", "SOLUSDT"], f"Symbol filter options: {r.status_code} {options}")
    r = filter_options(filters=[{"col": col, "op": "ILIKE", "val": "%BTC%"}])
    check(r.ok and [row[col] for row in r.json()["result"][0]["data"]] == ["BTCUSDT"], f"Symbol filter search: {r.status_code}")
    for label, query in (
        ("another column", {"columns": ["close"]}),
        ("a metric", {"metrics": [{"expressionType": "SQL", "sqlExpression": "max(close)", "label": "m"}]}),
        ("an ad-hoc SQL column", {"columns": [{"expressionType": "SQL", "sqlExpression": "hostName()", "label": "symbol"}]}),
        ("extras.where SQL", {"extras": {"where": "1 = 1"}}),
    ):
        r = filter_options(**query)
        check(r.status_code in DENIED, f"Symbol filter query with {label} refused: {r.status_code}")
    r = s.post(f"{SUPERSET}/api/v1/chart/data", timeout=60, json={
        "datasource": {"id": target["datasetId"], "type": "table"},
        "form_data": {"type": "NATIVE_FILTER", "native_filter_id": "NATIVE_FILTER-unknown", "dashboardId": dash.id},
        "queries": [{"columns": ["symbol"], "metrics": [], "row_limit": 10}], "result_format": "json", "result_type": "full",
    })
    check(r.status_code in DENIED, f"query for an unknown native filter refused: {r.status_code}")

    r = s.get(f"{SUPERSET}/api/v1/dashboard/", timeout=30)
    listed = [d["id"] for d in r.json().get("result", [])] if r.ok else []
    check(r.status_code in DENIED or listed in ([], [dash.id]), f"dashboard list shows nothing else: {r.status_code} {listed}")
    r = s.get(f"{SUPERSET}/api/v1/chart/", timeout=30)
    check(r.status_code in DENIED or not r.json().get("result"), f"chart list empty: {r.status_code} count={r.json().get('count') if r.ok else '-'}")
    for method, path in (
        ("GET", "/api/v1/database/"), ("GET", "/api/v1/database/1"), ("GET", "/api/v1/database/1/schemas/"),
        ("GET", "/api/v1/dataset/"), ("GET", "/api/v1/dataset/1"), ("GET", "/api/v1/query/"),
        ("GET", "/api/v1/saved_query/"), ("POST", "/api/v1/sqllab/execute/"), ("GET", "/api/v1/sqllab/"),
        ("POST", "/api/v1/security/guest_token/"), ("GET", "/api/v1/security/roles/"), ("GET", "/api/v1/log/"),
        ("GET", f"/api/v1/dashboard/{dash.id}/export/"), ("PUT", f"/api/v1/dashboard/{dash.id}"),
        ("GET", "/api/v1/explore/"), ("GET", "/superset/sqllab/"), ("GET", "/sqllab/"),
        ("GET", f"/explore/?slice_id={charts['Price trend'].id}"), ("GET", f"/superset/dashboard/{dash.id}/"),
        ("GET", "/users/list/"), ("GET", "/databaseview/list/"), ("GET", "/tablemodelview/list/"),
    ):
        code = status(method, path, json={} if method != "GET" else None)
        check(code in DENIED or code == 302, f"guest {method} {path}: {code}")
    # Superset itself serves these SPA shells to a guest (can_read on Dashboard/Chart); their data
    # APIs show only BYBIT (above). The public host does not route them (Caddy allowlist).
    for path in ("/dashboard/list/", "/chart/list/"):
        print(f"  INFO guest GET {path}: {status('GET', path)} (edge-only: not routed by Caddy)")

    def forged(**over):
        now = int(time.time())
        claims = {"user": {"username": "x"}, "resources": [{"type": "dashboard", "id": embed_uuid}], "rls_rules": [],
                  "iat": now, "exp": now + 300, "aud": c["GUEST_TOKEN_JWT_AUDIENCE"], "type": "guest"}
        key = over.pop("key", secret)
        return jwt.encode({**claims, **over}, key, algorithm="HS256")

    for label, tok in (("default secret", forged(key=DEFAULT_SECRET)), ("expired", forged(iat=0, exp=1)),
                       ("wrong audience", forged(aud="other")), ("dashboard id resource, wrong secret", forged(key="x" * 40, resources=[{"type": "dashboard", "id": str(dash.id)}]))):
        code = requests.get(f"{SUPERSET}/api/v1/me/roles/", headers={c["GUEST_TOKEN_HEADER_NAME"]: tok}, timeout=30).status_code
        check(code == 401, f"forged token ({label}) rejected: {code}")

    print("service account")
    svc_s = requests.Session()
    login = svc_s.post(f"{SUPERSET}/api/v1/security/login", json={
        "username": svc_name, "password": os.environ["SUPERSET_EMBED_SERVICE_PASSWORD"], "provider": "db"}, timeout=30)
    check(login.ok, f"login {svc_name}: {login.status_code}")
    svc_s.headers["Authorization"] = f"Bearer {login.json().get('access_token')}" if login.ok else ""
    svc_csrf = svc_s.get(f"{SUPERSET}/api/v1/security/csrf_token/", timeout=30).json().get("result")

    def grant(resources, rls=()):
        return svc_s.post(f"{SUPERSET}/api/v1/security/guest_token/", headers={"X-CSRFToken": svc_csrf, "Referer": SUPERSET},
                          json={"user": {"username": "probe"}, "resources": resources, "rls": list(rls)}, timeout=30).status_code

    check(grant([{"type": "dashboard", "id": embed_uuid}]) == 200, "guest token for the embedded uuid: 200")
    for label, resources, rls in (
        ("dashboard id", [{"type": "dashboard", "id": str(dash.id)}], ()),
        ("dashboard uuid", [{"type": "dashboard", "id": DASHBOARD_UUID}], ()),
        ("unknown uuid", [{"type": "dashboard", "id": str(uuid.uuid4())}], ()),
        ("extra resource", [{"type": "dashboard", "id": embed_uuid}, {"type": "dashboard", "id": str(dash.id)}], ()),
        ("rls rule", [{"type": "dashboard", "id": embed_uuid}], [{"clause": "1=1"}]),
    ):
        code = grant(resources, rls)
        check(code != 200, f"guest token for {label} refused: {code}")
    for path in ("/api/v1/dashboard/", f"/api/v1/dashboard/{dash.id}", "/api/v1/database/", "/api/v1/dataset/", "/api/v1/chart/"):
        code = svc_s.get(SUPERSET + path, allow_redirects=False, timeout=30).status_code
        check(code in DENIED, f"service account GET {path}: {code}")

    print("embedded page")
    page = f"{SUPERSET}/embedded/{embed_uuid}"
    for referer in (f"{origins[0]}/cases/crypto-analytics-dashboard", f"{origins[-1]}/cases/crypto-analytics-dashboard"):
        r = requests.get(page, headers={"Referer": referer}, timeout=30)
        csp = r.headers.get("Content-Security-Policy", "")
        check(r.status_code == 200 and "X-Frame-Options" not in r.headers
              and f"frame-ancestors 'self' {' '.join(origins)}" in csp, f"Referer {referer}: {r.status_code}, frame-ancestors ok")
    for referer in ("https://evil.example/", "http://leadmeter.ru/", "https://leadmeter.ru.evil.example/", "https://bi.apps.leadmeter.ru/", None):
        code = requests.get(page, headers={"Referer": referer} if referer else {}, timeout=30).status_code
        check(code == 403, f"Referer {referer}: {code}")
    code = requests.get(f"{SUPERSET}/embedded/{uuid.uuid4()}", headers={"Referer": origins[0] + "/"}, timeout=30).status_code
    check(code == 404, f"unknown embedded uuid: {code}")

    if failures:
        print("FAILED:", *failures, sep="\n  ")
        sys.exit(1)
    print("ALL CHECKS PASSED")
