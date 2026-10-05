# Airflow image with the DAG's Python dependencies baked in.
# Used through the AIRFLOW_IMAGE_NAME hook of the official docker-compose.yaml:
#   docker build -t crypto-dashboard-airflow:2.10.0 airflow/
#   echo AIRFLOW_IMAGE_NAME=crypto-dashboard-airflow:2.10.0 >> <airflow-compose-dir>/.env
ARG AIRFLOW_VERSION=2.10.0
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

COPY requirements.txt /tmp/requirements.txt
# Re-pinning apache-airflow keeps pip from up/downgrading Airflow itself.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
