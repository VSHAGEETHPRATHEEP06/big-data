# syntax=docker/dockerfile:1
# Spark 4.1 (PySpark, local mode) image for the speed layer.
# The "test" target adds the test tools and runs the whole pytest suite.
FROM eclipse-temurin:17-jre AS jre

FROM python:3.12-slim AS runtime
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/fleet/common:/opt/fleet/spark/jobs \
    CONFIG_DIR=/opt/fleet/config \
    HOME=/tmp \
    TZ=UTC
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
RUN useradd --uid 50000 --gid 0 --no-create-home --home-dir /tmp fleet

COPY spark/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install -r /tmp/requirements.txt

# Kafka connector jars are baked in, so nothing is downloaded at run time.
ARG SPARK_VERSION=4.1.3
ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/${SPARK_VERSION}/spark-sql-kafka-0-10_2.13-${SPARK_VERSION}.jar \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.13-${SPARK_VERSION}.jar \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    ${MAVEN}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar \
    /tmp/jars/
RUN JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && cp /tmp/jars/*.jar "$JARS/" && chmod 644 "$JARS"/*.jar && rm -rf /tmp/jars

COPY common /opt/fleet/common
COPY config /opt/fleet/config
COPY spark /opt/fleet/spark

WORKDIR /opt/fleet/spark/jobs
USER 50000:0
EXPOSE 4040 9108
CMD ["python", "speed_layer.py"]

# ---------------------------------------------------------------- tests
FROM runtime AS test
USER root
RUN --mount=type=cache,target=/root/.cache/pip pip install \
    pytest==9.1.1 confluent-kafka==2.15.1 fastapi==0.142.1 httpx==0.28.1 jinja2==3.1.6
COPY simulators /opt/fleet/simulators
COPY api /opt/fleet/api
COPY airflow/include /opt/fleet/airflow/include
COPY tests /opt/fleet/tests
ENV PYTHONPATH=/opt/fleet/common:/opt/fleet/spark/jobs:/opt/fleet/simulators:/opt/fleet/api:/opt/fleet/airflow/include
WORKDIR /opt/fleet
USER 50000:0
CMD ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider", "tests"]
