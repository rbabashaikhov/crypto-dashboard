"""Guest token service for the public BYBIT Dashboard embed (stdlib only).

GET /guest-token -> {"token": "<Superset guest JWT>"} for the embedded BYBIT Dashboard.

The resource is fixed here (EMBED_DASHBOARD_UUID, no RLS); the client chooses nothing.
Superset rejects anything else via GUEST_TOKEN_VALIDATOR_HOOK. The service account
(role EmbedTokenIssuer) logs in over the internal compose network:
  POST /api/v1/security/login -> GET /api/v1/security/csrf_token/ -> POST /api/v1/security/guest_token/

One token is shared by all visitors (the guest identity is the same for everyone) and
re-issued when less than REFRESH_MARGIN seconds remain, so page views never hit
Superset's login. The Origin check only keeps other sites' browsers away; anyone can
still get a token with curl, which is fine: it opens only this read-only dashboard.

GET /healthz -> 200 "ok" (does not call Superset).
"""
import base64
import http.cookiejar
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")
SERVICE_USER = os.environ["SUPERSET_EMBED_SERVICE_USER"]
SERVICE_PASSWORD = os.environ["SUPERSET_EMBED_SERVICE_PASSWORD"]
EMBED_UUID = os.environ["EMBED_DASHBOARD_UUID"]
ALLOWED_ORIGINS = {o for o in os.environ["EMBED_ALLOWED_ORIGINS"].split(",") if o}
GUEST_USER = {"username": "leadmeter-public", "first_name": "Leadmeter", "last_name": "Visitor"}
REFRESH_MARGIN = 60
PORT = 8080

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("embed-token")

_lock = threading.Lock()
_cached = {"token": None, "exp": 0}


def _call(opener, method, path, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(SUPERSET_URL + path, data=data, method=method, headers={
        "Content-Type": "application/json", "Referer": SUPERSET_URL + "/", **(headers or {}),
    })
    with opener.open(req, timeout=15) as resp:
        return json.load(resp)


def _issue():
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    login = _call(opener, "POST", "/api/v1/security/login",
                  {"username": SERVICE_USER, "password": SERVICE_PASSWORD, "provider": "db", "refresh": False})
    auth = {"Authorization": f"Bearer {login['access_token']}"}
    csrf = _call(opener, "GET", "/api/v1/security/csrf_token/", headers=auth)["result"]
    token = _call(opener, "POST", "/api/v1/security/guest_token/",
                  {"user": GUEST_USER, "resources": [{"type": "dashboard", "id": EMBED_UUID}], "rls": []},
                  headers={**auth, "X-CSRFToken": csrf})["token"]
    payload = token.split(".")[1]
    exp = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))["exp"]
    log.info("issued guest token, expires in %ds", exp - time.time())
    return token, exp


def guest_token():
    with _lock:
        if _cached["exp"] - time.time() < REFRESH_MARGIN:
            _cached["token"], _cached["exp"] = _issue()
        return _cached["token"]


class Handler(BaseHTTPRequestHandler):
    server_version = "embed-token"
    sys_version = ""

    def _send(self, status, body, origin=None):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json" if not isinstance(body, bytes) else "text/plain")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Vary", "Origin")
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Methods", "GET")
            self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def do_GET(self):
        if self.path == "/healthz":
            return self._send(200, b"ok")
        if self.path != "/guest-token":
            return self._send(404, {"error": "not found"})
        origin = self.headers.get("Origin")
        if origin not in ALLOWED_ORIGINS:
            return self._send(403, {"error": "origin not allowed"})
        try:
            return self._send(200, {"token": guest_token()}, origin)
        except (urllib.error.URLError, KeyError, ValueError) as e:
            detail = f"HTTP {e.code}" if isinstance(e, urllib.error.HTTPError) else type(e).__name__
            log.error("guest token request to Superset failed: %s", detail)
            return self._send(502, {"error": "guest token unavailable"}, origin)

    def do_OPTIONS(self):
        origin = self.headers.get("Origin")
        if self.path != "/guest-token" or origin not in ALLOWED_ORIGINS:
            return self._send(403, {"error": "origin not allowed"})
        return self._send(204, b"", origin)

    def log_message(self, fmt, *args):  # one line per request, no headers or tokens
        log.info("%s %s %s", self.command, self.path.split("?")[0], args[1] if len(args) > 1 else "-")


if __name__ == "__main__":
    if not EMBED_UUID or not ALLOWED_ORIGINS or any("*" in o or not o.startswith("https://") for o in ALLOWED_ORIGINS):
        raise SystemExit("EMBED_DASHBOARD_UUID and explicit https EMBED_ALLOWED_ORIGINS are required")
    log.info("serving on :%d for embedded dashboard %s, origins %s", PORT, EMBED_UUID, sorted(ALLOWED_ORIGINS))
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
