import os

# Single-process demo: SQLite metadata in the superset_home volume, no Redis, no Celery.
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG
FILTER_STATE_CACHE_CONFIG = CACHE_CONFIG
EXPLORE_FORM_DATA_CACHE_CONFIG = CACHE_CONFIG
RATELIMIT_STORAGE_URI = "memory://"

FEATURE_FLAGS = {
    "GLOBAL_ASYNC_QUERIES": False,
    "ALERT_REPORTS": False,
    "EMBEDDED_SUPERSET": True,
}

ROW_LIMIT = 10000
SUPERSET_WEBSERVER_TIMEOUT = 60

# --- Public read-only embed of BYBIT Dashboard (see deployment/README.md) ---
# Guest tokens are requested only by the embed-token service with a service
# account (role EmbedTokenIssuer). Guests get role EmbeddedGuest, never Public.
EMBED_DASHBOARD_UUID = os.environ.get("EMBED_DASHBOARD_UUID", "")
EMBED_ALLOWED_ORIGINS = [o for o in os.environ.get("EMBED_ALLOWED_ORIGINS", "").split(",") if o]
if any(not o.startswith("https://") or "*" in o for o in EMBED_ALLOWED_ORIGINS):
    raise ValueError(f"EMBED_ALLOWED_ORIGINS must be explicit https origins: {EMBED_ALLOWED_ORIGINS}")

GUEST_ROLE_NAME = "EmbeddedGuest"
GUEST_TOKEN_JWT_SECRET = os.environ["GUEST_TOKEN_JWT_SECRET"]
GUEST_TOKEN_JWT_EXP_SECONDS = 300
# Fixed audience: the default is the request host, which differs between the token
# service (http://superset:8088) and the browser (https://bi.apps.leadmeter.ru).
GUEST_TOKEN_JWT_AUDIENCE = "crypto-dashboard-embed"


def _only_embedded_bybit_dashboard(body):
    """Superset 4.1 also accepts a plain dashboard id as a guest resource; allow only
    the embedded BYBIT Dashboard uuid and no RLS rules."""
    return bool(EMBED_DASHBOARD_UUID) and not body.get("rls") and body.get("resources") == [
        {"type": "dashboard", "id": EMBED_DASHBOARD_UUID}
    ]


GUEST_TOKEN_VALIDATOR_HOOK = _only_embedded_bybit_dashboard

# Behind Caddy (X-Forwarded-Proto/Host) for the public host
ENABLE_PROXY_FIX = True

# Superset 4.1.4 defaults, plus framing allowed only from the embedding origins.
# flask-talisman's default X-Frame-Options: SAMEORIGIN would block any cross-origin iframe.
TALISMAN_CONFIG = {
    "content_security_policy": {
        "base-uri": ["'self'"],
        "default-src": ["'self'"],
        "img-src": ["'self'", "blob:", "data:", "https://apachesuperset.gateway.scarf.sh", "https://static.scarf.sh/"],
        "worker-src": ["'self'", "blob:"],
        "connect-src": ["'self'", "https://api.mapbox.com", "https://events.mapbox.com"],
        "object-src": "'none'",
        "style-src": ["'self'", "'unsafe-inline'"],
        "script-src": ["'self'", "'strict-dynamic'"],
        "frame-ancestors": ["'self'", *EMBED_ALLOWED_ORIGINS],
    },
    "content_security_policy_nonce_in": ["script-src"],
    "force_https": False,
    "session_cookie_secure": False,
    "frame_options": None,
}
