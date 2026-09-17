# Resolve immutable binary dependencies in throwaway stages; the runtime image
# receives only jars, not curl/Maven caches or build tools.
FROM alpine:3.22 AS dependencies

ARG POSTGRES_JDBC_VERSION=42.7.12
ARG ICEBERG_VERSION=1.11.0

RUN apk add --no-cache curl \
    && curl --fail --location --retry 3 \
        --output /postgresql.jar \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    && curl --fail --location --retry 3 \
        --output /iceberg-spark-runtime.jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    && curl --fail --location --retry 3 \
        --output /iceberg-aws-bundle.jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

FROM maven:3.9.11-eclipse-temurin-17 AS kafka-dependencies

COPY spark/kafka-pom.xml /tmp/kafka-pom.xml
RUN mvn --batch-mode --file /tmp/kafka-pom.xml \
    dependency:copy-dependencies \
    -DoutputDirectory=/kafka-jars

FROM apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu

USER root

# Every Spark role (master, worker, gateway, batch and stream driver) uses this
# same image, eliminating dependency drift between submitter and executor.
COPY --from=dependencies /postgresql.jar /opt/spark/jars/postgresql.jar
COPY --from=dependencies /iceberg-spark-runtime.jar /opt/spark/jars/iceberg-spark-runtime.jar
COPY --from=dependencies /iceberg-aws-bundle.jar /opt/spark/jars/iceberg-aws-bundle.jar
COPY --from=kafka-dependencies /kafka-jars/*.jar /opt/spark/jars/
COPY --chown=185:0 spark/jobs /opt/spark/jobs
COPY --chown=185:0 spark/tests /opt/spark/tests
COPY --chown=185:0 spark/sql /opt/spark/sql
COPY --chown=185:0 spark/gateway /opt/spark/gateway

# UID 185 is the non-root user provided by the official Spark image.  The shared
# volume needs group-compatible ownership for Parquet output and checkpoints.
RUN mkdir -p /opt/lakehouse/parquet /opt/lakehouse/checkpoints \
    && chown -R 185:0 /opt/lakehouse

USER 185
