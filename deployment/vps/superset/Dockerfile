# Official production image + ClickHouse driver only (no frontend build).
FROM apache/superset:4.1.4

USER root
RUN pip install --no-cache-dir clickhouse-connect==0.13.0
USER superset
