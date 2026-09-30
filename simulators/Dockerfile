# syntax=docker/dockerfile:1
# Image for the two simulated data sources and the Kafka topic set-up job.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/fleet/common:/opt/fleet/simulators \
    CONFIG_DIR=/opt/fleet/config \
    TZ=UTC

# uid 50000 / gid 0 is shared by every pipeline container so all of them can
# write to the shared data volume.
RUN useradd --uid 50000 --gid 0 --no-create-home --home-dir /tmp fleet

COPY simulators/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install -r /tmp/requirements.txt

COPY common /opt/fleet/common
COPY config /opt/fleet/config
COPY simulators /opt/fleet/simulators

WORKDIR /opt/fleet/simulators
USER 50000:0
CMD ["python", "stream_producer.py"]
