FROM alpine:3.22 AS dependencies

ARG POSTGRES_JDBC_VERSION=42.7.12

RUN apk add --no-cache curl \
    && curl --fail --location --retry 3 \
        --output /postgresql.jar \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

FROM apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu

USER root

COPY --from=dependencies /postgresql.jar /opt/spark/jars/postgresql.jar
COPY --chown=185:0 spark/jobs /opt/spark/jobs
COPY --chown=185:0 spark/tests /opt/spark/tests

RUN mkdir -p /opt/lakehouse/parquet /opt/lakehouse/checkpoints \
    && chown -R 185:0 /opt/lakehouse

USER 185
