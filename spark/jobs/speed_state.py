"""Pure-Python part of the speed layer's vehicle state logic.

Kept free of Spark so it can be unit tested quickly. A micro-batch of the
vehicle_state query is small (about 80 events every 5 seconds), so the Spark
job collects it once and these functions reduce it to one summary per vehicle
and merge that summary into the stored state (speed.vehicle_state).
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

STATE_COLUMNS = (
    "vehicle_id",
    "driver_id",
    "status",
    "trip_id",
    "zone",
    "lat",
    "lon",
    "speed_kmh",
    "odometer_km",
    "last_event_ts",
    "idle_since",
)


def summarise_vehicles(events: list[dict], zone_of) -> list[dict]:
    """One summary per vehicle from the valid events of a micro-batch.

    ``events`` are dicts with event_id, vehicle_id, driver_id, trip_id, status,
    lat, lon, speed, odometer_km, event_time and kafka_offset.
    ``zone_of(lat, lon)`` maps a position to a zone name (same grid as Spark).
    Duplicates inside the batch (same event_id) are ignored.
    """
    by_vehicle: dict[str, list[dict]] = defaultdict(list)
    seen: set[str] = set()
    for event in events:
        if event["event_id"] in seen:
            continue
        seen.add(event["event_id"])
        by_vehicle[event["vehicle_id"]].append(event)

    summaries = []
    for vehicle_id, rows in by_vehicle.items():
        rows.sort(key=lambda e: (e["event_time"], e["kafka_offset"]))
        latest = rows[-1]
        last_active = max((e["event_time"] for e in rows if e["status"] != "idle"), default=None)
        idle_after = [
            e["event_time"] for e in rows
            if e["status"] == "idle" and (last_active is None or e["event_time"] > last_active)
        ]
        summaries.append(
            {
                "vehicle_id": vehicle_id,
                "driver_id": latest["driver_id"],
                "status": latest["status"],
                "trip_id": latest["trip_id"],
                "zone": zone_of(latest["lat"], latest["lon"]),
                "lat": latest["lat"],
                "lon": latest["lon"],
                "speed_kmh": latest["speed"],
                "odometer_km": latest["odometer_km"],
                "last_event_ts": latest["event_time"],
                "first_event_ts": rows[0]["event_time"],
                "all_idle": last_active is None,
                "idle_start_candidate": min(idle_after) if idle_after else None,
            }
        )
    return summaries


def merge_vehicle_state(prev: dict | None, new: dict, offline_gap: timedelta) -> dict | None:
    """Return the new state row for one vehicle, or None to keep the old row.

    ``prev``: stored row (or None for a new vehicle) with at least
        status, idle_since, last_event_ts.
    ``new``: micro-batch summary with the latest event's columns plus
        first_event_ts        earliest event of the vehicle in the batch
        all_idle              True when every event in the batch was idle
        idle_start_candidate  first idle event after the last non-idle event

    ``idle_since`` is the start of the current idle streak. A streak continues
    across micro-batches only if the vehicle was idle before, stayed idle in the
    whole batch and did not disappear for longer than ``offline_gap``.
    """
    new_last: datetime = new["last_event_ts"]
    if prev is not None and prev.get("last_event_ts") is not None and new_last < prev["last_event_ts"]:
        return None  # the batch only had late events, the stored state is newer

    idle_since = None
    if new["status"] == "idle":
        continues = (
            prev is not None
            and prev.get("status") == "idle"
            and prev.get("idle_since") is not None
            and new["all_idle"]
            and new["first_event_ts"] - prev["last_event_ts"] <= offline_gap
        )
        idle_since = prev["idle_since"] if continues else new["idle_start_candidate"]

    row = {key: new.get(key) for key in STATE_COLUMNS}
    row["idle_since"] = idle_since
    return row


def idle_minutes(state: dict) -> float | None:
    if state.get("status") != "idle" or state.get("idle_since") is None:
        return None
    return (state["last_event_ts"] - state["idle_since"]).total_seconds() / 60.0
