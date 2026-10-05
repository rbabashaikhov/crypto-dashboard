"""Roles of the public BYBIT Dashboard embed, shared by setup_embedding.py and verify_embedding.py.

Guests read the dashboard's charts through Superset's embedded guest checks, so
EmbeddedGuest has no datasource, database, SQL Lab, Explore or CSV permissions.
"""
GUEST_ROLE = "EmbeddedGuest"  # = GUEST_ROLE_NAME in superset_config.py
ISSUER_ROLE = "EmbedTokenIssuer"
GUEST_PERMISSIONS = {
    ("can_read", "Dashboard"),  # /api/v1/dashboard/<id>, /charts, /datasets
    ("can_read", "Chart"),  # /api/v1/chart/data
    ("can_read", "DashboardFilterStateRestApi"),  # native filter state
    ("can_write", "DashboardFilterStateRestApi"),
    ("can_time_range", "Api"),  # Period filter: /api/v1/time_range/
    ("can_read", "SecurityRestApi"),  # /api/v1/security/csrf_token/ (frontend client init)
}
ISSUER_PERMISSIONS = {
    ("can_grant_guest_token", "SecurityRestApi"),
    ("can_read", "SecurityRestApi"),  # csrf_token for the guest_token POST
}
