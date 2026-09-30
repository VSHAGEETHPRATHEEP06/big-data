# syntax=docker/dockerfile:1
# Airflow 3.3 with a Java runtime and PySpark, so batch-layer tasks can run
# spark-submit in local mode. The same Spark job code as the speed layer is used.
FROM eclipse-temurin:17-jre AS jre

FROM apache/airflow:3.3.2-python3.12
USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    TZ=UTC
USER airflow

COPY airflow/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/home/airflow/.cache/pip,uid=50000,gid=0 \
    pip install "apache-airflow==3.3.2" -r /tmp/requirements.txt

COPY --chown=airflow:root common /opt/fleet/common
COPY --chown=airflow:root config /opt/fleet/config
COPY --chown=airflow:root spark/jobs /opt/fleet/spark/jobs
COPY --chown=airflow:root spark/conf /opt/fleet/spark/conf
COPY --chown=airflow:root airflow/include /opt/fleet/airflow/include
COPY --chown=airflow:root airflow/start-airflow.sh /opt/fleet/airflow/start-airflow.sh
COPY --chown=airflow:root airflow/dags /opt/airflow/dags

ENV PYTHONPATH=/opt/fleet/common:/opt/fleet/spark/jobs:/opt/fleet/airflow/include \
    CONFIG_DIR=/opt/fleet/config
