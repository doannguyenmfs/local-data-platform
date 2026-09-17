FROM apache/airflow:3.3.0-python3.12

# Keep project CLI dependencies out of Airflow's framework environment.  Airflow
# intentionally constrains its own Python packages; an isolated venv prevents a
# dbt/Kafka dependency upgrade from silently breaking the scheduler/task runner.
USER root
COPY dbt/requirements.txt /tmp/dbt-requirements.txt
COPY --chown=airflow:0 kafka/producer/requirements.txt /tmp/kafka-requirements.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/python -m pip install \
        --no-cache-dir \
        --upgrade pip \
    && /opt/dbt-venv/bin/python -m pip install \
        --no-cache-dir \
        --requirement /tmp/dbt-requirements.txt \
    && /opt/dbt-venv/bin/python -m pip install \
        --no-cache-dir \
        --requirement /tmp/kafka-requirements.txt \
    && rm -f /tmp/dbt-requirements.txt /tmp/kafka-requirements.txt \
    && mkdir -p \
        /opt/airflow/dbt \
        /opt/airflow/dbt/logs \
        /opt/airflow/dbt/target \
    && chown -R airflow:0 \
        /opt/dbt-venv \
        /opt/airflow/dbt \
    && chmod -R g=u \
        /opt/dbt-venv \
        /opt/airflow/dbt
ENV PATH="/opt/dbt-venv/bin:${PATH}"

# Runtime tasks execute without root.  The build above grants the image's
# Airflow group write access to dbt target/log directories.
USER airflow
RUN dbt --version
