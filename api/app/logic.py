"""Small pure functions used by the API (unit tested without a database)."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta


def days_behind(sim_today: date, last_processed: date | None, first_day: date) -> int:
    """Finished simulated days that the batch layer has not processed yet."""
    expected_last = sim_today - timedelta(days=1)
    if expected_last < first_day:
        return 0
    if last_processed is None:
        return (expected_last - first_day).days + 1
    return max(0, (expected_last - last_processed).days)


def _parse_ts(value: str | None) -> datetime | None:
    if not value or value.startswith("0001-01-01"):
        return None  # Alertmanager uses year 1 for "not ended yet"
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def alertmanager_rows(payload: dict) -> list[tuple]:
    """Flatten an Alertmanager webhook payload into ops.alert_notifications rows."""
    rows = []
    for alert in payload.get("alerts", []):
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        rows.append(
            (
                "alertmanager",
                labels.get("alertname"),
                labels.get("severity"),
                alert.get("status"),
                annotations.get("summary") or annotations.get("description"),
                json.dumps(labels),
                _parse_ts(alert.get("startsAt")),
                _parse_ts(alert.get("endsAt")),
                alert.get("fingerprint"),
            )
        )
    return rows


def parse_day(value: str | None) -> date | None:
    """Validate a YYYY-MM-DD string (also protects file paths built from it)."""
    if value is None:
        return None
    return date.fromisoformat(value)
