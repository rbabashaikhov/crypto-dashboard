"""Roles of the public BYBIT Dashboard embed, shared by setup_embedding.py and verify_embedding.py.

Guests read the dashboard's charts through Superset's embedded guest checks, so
EmbeddedGuest has no datasource, database, SQL Lab, Explore or CSV permissions.
"""
GUEST_ROLE = "EmbeddedGuest"  # = GUEST_ROLE_NAME in superset_config.py
ISSUER_ROLE = "EmbedTokenIssuer"
# What the 4.1.4 embedded frontend calls (browser smoke): me/roles (no permission),
# dashboard + charts + datasets, chart/data, time_range. It never stores filter state
# for guests ("embedded users can't persist filter combinations") and takes the CSRF
# token from the page, so no filter_state or SecurityRestApi permissions.
GUEST_PERMISSIONS = {
    ("can_read", "Dashboard"),  # /api/v1/dashboard/<id>, /charts, /datasets
    ("can_read", "Chart"),  # /api/v1/chart/data
    ("can_time_range", "Api"),  # Period filter: /api/v1/time_range/
}
ISSUER_PERMISSIONS = {
    ("can_grant_guest_token", "SecurityRestApi"),
    ("can_read", "SecurityRestApi"),  # csrf_token for the guest_token POST
}
