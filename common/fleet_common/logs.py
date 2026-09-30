"""Structured (JSON lines) logging used by every pipeline stage.

Each log line is one JSON object with a fixed core schema:

    ts       real UTC time the line was written
    level    INFO / WARNING / ERROR ...
    service  container or job name, e.g. "stream-producer"
    stage    pipeline stage: ingestion | processing | storage | serving | orchestration
    event    short snake_case name of what happened, e.g. "tick_sent"

plus any extra key/value fields passed through ``extra=``, for example
``batch_id``, ``run_id``, ``sim_date``, ``rows`` or ``duration_ms``. Because
every stage uses the same keys, logs from Kafka producers, Spark jobs, Airflow
tasks and the API can be filtered together with a single ``jq`` expression.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone

# Attributes that every LogRecord has. Anything else on the record came from
# ``extra=`` and is copied into the JSON payload.
_RESERVED = set(vars(logging.LogRecord("x", 0, "x", 0, "x", (), None))) | {
    "message",
    "asctime",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, stage: str) -> None:
        super().__init__()
        self.service = service
        self.stage = stage

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "service": self.service,
            "stage": getattr(record, "stage", self.stage),
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and key not in payload and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


def get_logger(service: str, stage: str, level: str | None = None) -> logging.Logger:
    """Return a logger that writes JSON lines to stdout.

    Calling it twice with the same service name returns the same logger
    without adding a second handler.
    """
    logger = logging.getLogger(f"fleet.{service}")
    if not getattr(logger, "_fleet_configured", False):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service, stage))
        logger.addHandler(handler)
        logger.propagate = False
        logger._fleet_configured = True  # type: ignore[attr-defined]
    logger.setLevel((level or os.environ.get("LOG_LEVEL", "INFO")).upper())
    return logger
