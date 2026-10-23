FROM apache/airflow:3.0.2-python3.12

COPY --chown=airflow:root pyproject.toml README.md /opt/lakehouse/
COPY --chown=airflow:root src /opt/lakehouse/src

RUN pip install --no-cache-dir /opt/lakehouse
