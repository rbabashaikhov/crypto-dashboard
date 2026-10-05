"""Sets up the public read-only embed of BYBIT Dashboard. Idempotent.

- role EmbeddedGuest (GUEST_ROLE_NAME): exactly GUEST_PERMISSIONS
- role EmbedTokenIssuer: exactly ISSUER_PERMISSIONS (request guest tokens, nothing else)
- user SUPERSET_EMBED_SERVICE_USER with only EmbedTokenIssuer; its password is
  (re)set from SUPERSET_EMBED_SERVICE_PASSWORD
- embedded config of BYBIT Dashboard with allowed_domains = EMBED_ALLOWED_ORIGINS;
  its uuid is kept across runs and printed: EMBED_DASHBOARD_UUID for vps/.env,
  the Caddyfile and the Leadmeter page

Run (with vps/.env loaded into the shell; first-time setup: see deployment/README.md):
  docker compose exec -e SUPERSET_EMBED_SERVICE_USER -e SUPERSET_EMBED_SERVICE_PASSWORD \
    superset python /app/deploy/setup_embedding.py [--teardown]
"""
import os
import re
import sys

from embedding_spec import GUEST_PERMISSIONS, GUEST_ROLE, ISSUER_PERMISSIONS, ISSUER_ROLE
from superset.app import create_app

DASHBOARD_UUID = "63182cb4-866e-4ac4-8eca-5d283e827b9f"

app = create_app()
with app.app_context():
    from superset import db, security_manager as sm
    from superset.daos.dashboard import EmbeddedDashboardDAO
    from superset.models.dashboard import Dashboard

    assert app.config["GUEST_ROLE_NAME"] == GUEST_ROLE, app.config["GUEST_ROLE_NAME"]
    user_name = os.environ.get("SUPERSET_EMBED_SERVICE_USER", "embed_token_svc")
    dash = db.session.query(Dashboard).filter_by(uuid=DASHBOARD_UUID).one()

    if "--teardown" in sys.argv:
        dash.embedded = []
        if user := sm.find_user(user_name):
            db.session.delete(user)
        for name in (GUEST_ROLE, ISSUER_ROLE):
            if role := sm.find_role(name):
                db.session.delete(role)
        db.session.commit()
        print(f"Removed embedded config of {dash.dashboard_title!r}, user {user_name}, roles {GUEST_ROLE}, {ISSUER_ROLE}")
        sys.exit(0)

    origins = [o for o in os.environ["EMBED_ALLOWED_ORIGINS"].split(",") if o]
    bad = [o for o in origins if not re.fullmatch(r"https://[a-z0-9-]+(\.[a-z0-9-]+)+", o)]
    if not origins or bad:
        sys.exit(f"EMBED_ALLOWED_ORIGINS must be explicit https origins, got {origins}")

    def sync_role(name, wanted):
        role = sm.add_role(name)
        pvs = []
        for perm, view in sorted(wanted):
            pv = sm.find_permission_view_menu(perm, view)
            if not pv:
                sys.exit(f"permission {perm} on {view} does not exist")
            pvs.append(pv)
        role.permissions = pvs
        print(f"role {name}: {sorted(f'{p} on {v}' for p, v in wanted)}")
        return role

    sync_role(GUEST_ROLE, GUEST_PERMISSIONS)
    issuer = sync_role(ISSUER_ROLE, ISSUER_PERMISSIONS)

    password = os.environ["SUPERSET_EMBED_SERVICE_PASSWORD"]
    user = sm.find_user(user_name)
    if not user:
        user = sm.add_user(user_name, "Embed", "Token Service", f"{user_name}@localhost", [issuer], password)
        if not user:
            sys.exit(f"could not create user {user_name}")
    else:
        user.roles = [issuer]
        user.active = True
        sm.reset_password(user.id, password)
    print(f"user {user_name}: roles={[r.name for r in user.roles]} active={user.active}")

    embedded = EmbeddedDashboardDAO.upsert(dash, origins)
    db.session.commit()
    print(f"dashboard {dash.dashboard_title!r} (id={dash.id}) embedded: allowed_domains={embedded.allowed_domains}")
    print(f"EMBED_DASHBOARD_UUID={embedded.uuid}")
