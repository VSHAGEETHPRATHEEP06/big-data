# syntax=docker/dockerfile:1
# Serving API (FastAPI + Uvicorn)
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/fleet/common:/opt/fleet/api \
    CONFIG_DIR=/opt/fleet/config \
    TZ=UTC
RUN useradd --uid 50000 --gid 0 --no-create-home --home-dir /tmp fleet

COPY api/requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install -r /tmp/requirements.txt

COPY common /opt/fleet/common
COPY config /opt/fleet/config
COPY api /opt/fleet/api

WORKDIR /opt/fleet/api
USER 50000:0
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
