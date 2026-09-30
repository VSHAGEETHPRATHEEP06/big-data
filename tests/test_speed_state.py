"""Idle-streak logic of the speed layer's vehicle_state query."""
from datetime import datetime, timedelta

from speed_state import idle_minutes, merge_vehicle_state, summarise_vehicles

T0 = datetime(2026, 9, 1, 9, 0)
GAP = timedelta(minutes=30)


def ev(vehicle, minute, status, event_id=None, offset=0):
    return {"event_id": event_id or f"{vehicle}-{minute}-{status}", "vehicle_id": vehicle, "driver_id": "D1",
            "trip_id": None, "status": status, "lat": 6.9, "lon": 79.85, "speed": 0.0, "odometer_km": 100.0,
            "event_time": T0 + timedelta(minutes=minute), "kafka_offset": offset or minute}


def zone_of(_lat, _lon):
    return "Kollupitiya"


def test_summary_finds_start_of_idle_streak():
    events = [ev("V1", 0, "on_trip"), ev("V1", 2, "idle"), ev("V1", 4, "idle"), ev("V1", 4, "idle", "dup")]
    [s] = summarise_vehicles(events + [events[1]], zone_of)  # events[1] repeated = duplicate
    assert s["status"] == "idle"
    assert s["idle_start_candidate"] == T0 + timedelta(minutes=2)
    assert s["all_idle"] is False
    assert s["zone"] == "Kollupitiya"


def test_idle_streak_continues_across_batches():
    prev = {"status": "idle", "idle_since": T0, "last_event_ts": T0 + timedelta(minutes=10)}
    [s] = summarise_vehicles([ev("V1", 12, "idle"), ev("V1", 14, "idle")], zone_of)
    row = merge_vehicle_state(prev, s, GAP)
    assert row["idle_since"] == T0
    assert idle_minutes(row) == 14


def test_idle_streak_restarts_after_work_or_long_gap():
    prev = {"status": "idle", "idle_since": T0, "last_event_ts": T0 + timedelta(minutes=10)}
    [s] = summarise_vehicles([ev("V1", 12, "on_trip"), ev("V1", 14, "idle")], zone_of)
    assert merge_vehicle_state(prev, s, GAP)["idle_since"] == T0 + timedelta(minutes=14)
    [s] = summarise_vehicles([ev("V1", 90, "idle")], zone_of)  # offline for 80 minutes in between
    assert merge_vehicle_state(prev, s, GAP)["idle_since"] == T0 + timedelta(minutes=90)


def test_busy_vehicle_has_no_idle_streak_and_late_batches_are_ignored():
    prev = {"status": "idle", "idle_since": T0, "last_event_ts": T0 + timedelta(minutes=10)}
    [s] = summarise_vehicles([ev("V1", 12, "enroute")], zone_of)
    assert merge_vehicle_state(prev, s, GAP)["idle_since"] is None
    [late] = summarise_vehicles([ev("V1", 4, "on_trip")], zone_of)  # older than stored state
    assert merge_vehicle_state(prev, late, GAP) is None
