FROM apache/airflow:3.3.0-python3.12
    
USER root
COPY dbt/requirements.txt /tmp/dbt-requirements.txt
COPY --chown=airflow:0 kafka/producer/requirements.txt /tmp/kafka-requirements.txt
RUN python -m  venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/python -m pip install \
        --no-cache-dir \
        --upgrade pip \
    && /opt/dbt-venv/bin/python -m pip install \
        --no-cache-dir \
        --requirement /tmp/dbt-requirements.txt \
    && rm -f /tmp/dbt-requirements.txt \
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

USER airflow
RUN python -m pip install \
        --no-cache-dir \
        --requirement /tmp/kafka-requirements.txt \
    && dbt --version
