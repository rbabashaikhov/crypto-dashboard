"""Guest (embedded) chart-data checks missing from Superset 4.1.4.

Superset 4.1.4 refuses guest requests whose payload differs from the saved chart
(query_context_modified), but builds the query context with ChartDAO.find_by_id(),
whose ChartFilter hides the chart from a guest (no datasource permission). The saved
chart is then None, the request is treated like a native filter query, and any
columns or ad-hoc SQL metrics on the dashboard's datasets pass. Native filter
queries are only checked for the target dataset, not for what they select.

For guests only, before Superset's own checks:
- chart queries: load the saved chart without the base filter so Superset compares
  columns/metrics/orderby against it
- native filter queries: only the filter's target columns, no metrics
- all queries: result_type "full" only (no samples/drill), no SQL in extras
  where/having, filters on plain dataset columns (or exactly the saved ones)
"""
import json

from superset.security import SupersetSecurityManager
from superset.security.manager import freeze_value

# Models, DAOs and errors are imported inside the functions: this module is loaded
# from superset_config.py, before the Flask app (and its context) exists.


def _deny(reason):
    from superset.errors import ErrorLevel, SupersetError, SupersetErrorType
    from superset.exceptions import SupersetSecurityException

    raise SupersetSecurityException(SupersetError(
        error_type=SupersetErrorType.DASHBOARD_SECURITY_ACCESS_ERROR,
        message=f"Guest user cannot modify chart payload ({reason})",
        level=ErrorLevel.WARNING,
    ))


def _native_filter_columns(form_data, datasource):
    from superset.models.dashboard import Dashboard

    dashboard = Dashboard.get(str(form_data.get("dashboardId") or ""))
    config = json.loads(dashboard.json_metadata or "{}").get("native_filter_configuration", []) if dashboard else []
    fltr = next((f for f in config if f.get("id") == form_data.get("native_filter_id")), None)
    columns = {
        t["column"]["name"] for t in (fltr or {}).get("targets", [])
        if t.get("datasetId") == datasource.id and t.get("column", {}).get("name")
    }
    if not columns:
        _deny("unknown native filter")
    return columns


def check_guest_query_context(query_context):
    from superset.daos.chart import ChartDAO

    form_data = query_context.form_data or {}
    result_type = getattr(query_context.result_type, "value", query_context.result_type)
    if result_type != "full":
        _deny(f"result_type {result_type}")
    dataset_columns = {c.column_name for c in query_context.datasource.columns}

    if form_data.get("type") == "NATIVE_FILTER":
        allowed = _native_filter_columns(form_data, query_context.datasource)
        for q in query_context.queries:
            if any(not isinstance(c, str) or c not in allowed for c in [*q.columns, *(q.series_columns or [])]):
                _deny("native filter columns")
            if q.metrics or q.series_limit_metric or any(
                not isinstance(o[0], str) or o[0] not in allowed for o in q.orderby or []
            ):
                _deny("native filter metrics")
            if (q.extras or {}).get("where") or (q.extras or {}).get("having"):
                _deny("custom SQL")
            if any(not isinstance(f.get("col"), str) or f["col"] not in allowed for f in q.filter or []):
                _deny("filter column")
        return

    slice_id = form_data.get("slice_id")
    chart = ChartDAO.find_by_id(slice_id, skip_base_filter=True) if slice_id else None
    if chart is None or not chart.query_context:
        _deny("unknown chart")
    query_context.slice_ = chart  # Superset's query_context_modified compares against it
    stored = json.loads(chart.query_context).get("queries") or []
    stored_sql = {(sq.get("extras") or {}).get(k) or "" for sq in stored for k in ("where", "having")}
    stored_filters = {freeze_value(f) for sq in stored for f in sq.get("filters") or []}
    stored_limit_metrics = {freeze_value(sq.get("series_limit_metric")) for sq in stored}
    for q in query_context.queries:
        extras = q.extras or {}
        if (extras.get("where") or "") not in stored_sql or (extras.get("having") or "") not in stored_sql:
            _deny("custom SQL")
        if freeze_value(q.series_limit_metric) not in stored_limit_metrics:
            _deny("series limit metric")
        for f in q.filter or []:
            if freeze_value(f) in stored_filters:
                continue
            if not isinstance(f.get("col"), str) or f["col"] not in dataset_columns:
                _deny("filter column")


class EmbedSecurityManager(SupersetSecurityManager):
    def raise_for_access(self, *args, **kwargs):
        query_context = kwargs.get("query_context")
        if query_context is not None and self.is_guest_user():
            check_guest_query_context(query_context)
        return super().raise_for_access(*args, **kwargs)
