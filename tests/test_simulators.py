"""Streaming and batch data sources (no Kafka or database needed)."""
import csv
import dataclasses
import hashlib
import json
import random
from datetime import date, datetime, timedelta

import pytest

from batch_producer import EXPENSE_COLUMNS, build_rows, inject_faults, partner_delay_minutes, write_file
from fleet_model import FleetWorld, trip_fare
from stream_producer import FaultInjector


@pytest.fixture()
def world(registry, zones):
    return FleetWorld(registry, zones, datetime(2026, 9, 1, 6, 0), date(2026, 9, 1), seed=7)


def run(world, minutes: int, step: int = 2):
    events, days = [], []
    t = world.sim_time
    for _ in range(minutes // step):
        t += timedelta(minutes=step)
        e, d = world.advance_to(t)
        events += e
        days += d
    return events, days


def test_world_emits_valid_events(world):
    events, _ = run(world, 180)
    assert events, "vehicles should come online at 06:00"
    required = {"event_id", "event_type", "event_ts", "vehicle_id", "driver_id", "status", "lat", "lon",
                "speed", "odometer_km", "fare"}
    for e in events:
        assert required <= e.keys()
        assert e["status"] in ("idle", "enroute", "on_trip")
        assert e["event_type"] in ("ping", "trip_start", "trip_end")
        assert 5.8 < e["lat"] < 10.0 and 79.5 < e["lon"] < 82.0
        assert e["speed"] >= 0 and e["fare"] >= 0
    assert len({e["event_id"] for e in events}) == len(events)


def test_trip_lifecycle_is_consistent(world):
    events, _ = run(world, 300)
    starts = {e["trip_id"] for e in events if e["event_type"] == "trip_start"}
    ends = [e for e in events if e["event_type"] == "trip_end"]
    assert ends, "some trips should finish within five simulated hours"
    for e in ends:
        assert e["trip_id"] in starts
        assert e["fare"] > 0 and e["trip_distance_km"] > 0 and e["trip_duration_min"] > 0
        assert e["pickup_lat"] is not None
    # odometer never goes down for a vehicle
    last = {}
    for e in events:
        assert e["odometer_km"] >= last.get(e["vehicle_id"], 0)
        last[e["vehicle_id"]] = e["odometer_km"]


def test_day_rollover_writes_ground_truth(world):
    _, days = run(world, 19 * 60, step=10)  # 06:00 -> 01:00 next day
    assert len(days) == 1 and days[0]["sim_date"] == "2026-09-01"
    truth = days[0]["vehicles"]
    assert sum(v["trips"] for v in truth.values()) > 50
    assert all(v["km"] >= 0 for v in truth.values())


def test_world_state_round_trip(world, registry, zones):
    run(world, 120)
    copy = FleetWorld(registry, zones, datetime(2026, 9, 1, 6, 0), date(2026, 9, 1), seed=1)
    copy.restore(json.loads(json.dumps(world.to_dict())))
    assert copy.sim_time == world.sim_time
    assert copy.status_counts() == world.status_counts()


def test_fare_rules():
    assert trip_fare("tuk", 0.1, 1, 12) == 150  # minimum fare
    assert trip_fare("car", 10, 30, 8) > trip_fare("car", 10, 30, 12)  # peak surge
    assert trip_fare("tuk", 10, 30, 8) == trip_fare("tuk", 10, 30, 12)  # no surge for tuk-tuks


def test_fault_injector_corrupts_only_pings(settings):
    faults = FaultInjector(dataclasses.replace(settings, invalid_event_rate=1.0), random.Random(1))
    ping = {"event_id": "a", "event_type": "ping", "event_ts": "2026-09-01T08:00:00", "vehicle_id": "V001",
            "status": "idle", "lat": 6.9, "lon": 79.85, "speed": 0.0, "fare": 0.0}
    payload, kind = faults.payload(ping)
    assert kind is not None
    trip_end = dict(ping, event_type="trip_end", fare=500.0)
    payload, kind = faults.payload(trip_end)
    assert kind is None and json.loads(payload)["fare"] == 500.0


def test_outage_buffers_then_releases_late_events(settings, world):
    faults = FaultInjector(settings, random.Random(3))
    now = datetime(2026, 9, 1, 9, 0)
    faults.outage_until["V001"] = now + timedelta(minutes=30)
    faults.buffers["V001"] = []
    event = {"vehicle_id": "V001", "event_ts": "2026-09-01T09:00:00"}
    assert faults.route(event) == []
    assert faults.release_finished_outages(now + timedelta(minutes=10)) == []
    released = faults.release_finished_outages(now + timedelta(minutes=31))
    assert released == [event]


def test_expense_file_and_manifest(tmp_path, registry, settings):
    truth = {v.vehicle_id: {"km": 100.0} for v in registry}
    rng = random.Random(5)
    rows = build_rows(date(2026, 9, 2), registry, truth, settings, rng)
    assert len(rows) == 30
    anomaly = next(r for r in rows if r["vehicle_id"] == "V026")
    assert float(anomaly["distance_covered"]) > 118  # fuel_anomaly reports 30-40% more km
    clean = len(rows)
    kinds = inject_faults(rows, random.Random(2), rate=1.0)
    assert len(rows) == clean + len(kinds)
    manifest = write_file(tmp_path, date(2026, 9, 2), rows)
    data = (tmp_path / manifest["file"]).read_bytes()
    assert hashlib.sha256(data).hexdigest() == manifest["sha256"]
    with open(tmp_path / manifest["file"]) as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == EXPENSE_COLUMNS
        assert len(list(reader)) == manifest["rows"]


def test_partner_delay_is_deterministic():
    d = date(2026, 9, 3)
    assert partner_delay_minutes(d, 1) == partner_delay_minutes(d, 1)
    assert 20 <= partner_delay_minutes(d, 1) <= 90
