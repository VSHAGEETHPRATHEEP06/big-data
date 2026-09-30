"""Batch layer logic on a small synthetic day with known answers (no database)."""
import dataclasses
import json
from datetime import date, datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")
import batch_layer  # noqa: E402
from fleet_common.simclock import SimClock  # noqa: E402

DAY = date(2026, 9, 2)
CLOCK = SimClock(real_start=1_790_000_000.0, sim_start=datetime(2026, 9, 2, 0, 0), speedup=120)


def kafka_ts(sim_time: datetime) -> datetime:
    """Naive UTC datetime of the real moment the simulated time is reached."""
    return datetime.fromtimestamp(CLOCK.to_real(sim_time), tz=timezone.utc).replace(tzinfo=None)


def event(eid, vehicle, minute, status, etype="ping", fare=0.0, odo=1000.0, lat=6.9115, lon=79.8505, **extra):
    ts = datetime(2026, 9, 2, 8, 0) + timedelta(minutes=minute)
    body = {"schema_version": 1, "event_id": eid, "event_type": etype, "event_ts": ts.isoformat(),
            "vehicle_id": vehicle, "driver_id": "D" + vehicle[1:], "trip_id": extra.pop("trip_id", None),
            "status": status, "lat": lat, "lon": lon, "speed": 20.0 if status != "idle" else 0.0,
            "heading": 0, "odometer_km": odo, "fare": fare, **extra}
    return body, ts


@pytest.fixture()
def lake(tmp_path, spark, settings):
    s = dataclasses.replace(settings, data_dir=tmp_path)
    records, offset = [], 0

    def add(body, ts, delay_min=0.5):
        nonlocal offset
        records.append(("V", json.dumps(body), "fleet.telemetry.raw", 0, offset,
                        kafka_ts(ts + timedelta(minutes=delay_min)), ts.date().isoformat()))
        offset += 1

    # V001 (tuk): idle 08:00-08:58, trip 09:00-09:20 (fare 1000), idle 09:22-10:00
    for i, minute in enumerate(range(0, 122, 2)):
        status = "on_trip" if 60 <= minute <= 80 else "idle"
        body, ts = event(f"v1-{minute}", "V001", minute, status, odo=1000 + i * 0.5,
                         trip_id="T1" if status == "on_trip" else None)
        add(body, ts, delay_min=30 if minute == 100 else 0.5)  # one late event
    trip_end, ts = event("v1-end", "V001", 80, "on_trip", etype="trip_end", fare=1000.0, odo=1020.0,
                         trip_id="T1", trip_distance_km=8.0, trip_duration_min=20.0,
                         pickup_lat=6.9145, pickup_lon=79.8780)
    add(trip_end, ts)
    add(*event("v1-10", "V001", 10, "idle", odo=1002.5))  # duplicate of an earlier ping
    bad, ts = event("v1-bad", "V001", 12, "idle")
    add({**bad, "speed": -5.0}, ts)  # invalid
    # V013 (car): idle for two hours
    for minute in range(0, 122, 2):
        add(*event(f"v13-{minute}", "V013", minute, "idle", odo=5000.0, lat=6.9355, lon=79.848))

    raw = spark.createDataFrame(records, "kafka_key string, raw_value string, kafka_topic string, "
                                         "kafka_partition int, kafka_offset long, kafka_ts timestamp, event_date string")
    raw.write.partitionBy("event_date").parquet(str(s.raw_telemetry_dir))

    csv_path = tmp_path / "expenses.csv"
    rows = [
        "record_id,expense_date,vehicle_id,partner_id,fuel_litres,fuel_cost,maintenance_cost,distance_covered,service_flag,submitted_at",
        "EXP-1,2026-09-02,V001,FP-01,1.0,300.00,200.00,31.0,N,x",
        "EXP-1,2026-09-02,V001,FP-01,1.0,300.00,200.00,31.0,N,x",  # duplicate
        "EXP-2,2026-09-02,V013,FP-02,2.0,600.00,16000.00,0.0,Y,x",
        "EXP-3,2026-09-02,V099,FP-02,2.0,600.00,100.00,10.0,N,x",  # unknown vehicle
        "EXP-4,2026-09-02,V013,FP-02,2.0,N/A,100.00,10.0,N,x",  # bad number
    ]
    csv_path.write_text("\n".join(rows) + "\n")
    return s, str(csv_path)


def registry_df(spark, registry):
    return spark.createDataFrame([(v.vehicle_id, v.driver_id, v.vehicle_type, v.plate_no) for v in registry],
                                 "vehicle_id string, driver_id string, vehicle_type string, plate_no string")


def test_batch_views_are_exact(lake, spark, zones, registry):
    s, csv_path = lake
    reg = registry_df(spark, registry)
    telemetry = batch_layer.telemetry_views(spark, s, zones, reg, DAY, CLOCK)
    stats = telemetry["stats"]
    assert stats["events_invalid"] == 1
    assert stats["duplicates_removed"] == 1
    assert stats["late_events"] == 1

    vd = {r["vehicle_id"]: r for r in telemetry["vehicle_daily"].collect()}
    assert vd["V001"]["trips"] == 1 and vd["V001"]["revenue_lkr"] == 1000.0
    assert vd["V001"]["on_trip_min"] == pytest.approx(22.0)  # 11 pings x 2 minutes
    assert vd["V001"]["gps_distance_km"] == pytest.approx(30.0)
    assert vd["V013"]["max_idle_streak_min"] == pytest.approx(122.0)
    assert vd["V013"]["long_idle_episodes"] == 1

    zone = {(r["zone"], r["hour"]): r for r in telemetry["zone_hourly"].collect()}
    assert zone[("Borella", 9)]["earnings_lkr"] == 1000.0  # fare counted in the pickup zone

    expense = batch_layer.expense_views(spark, csv_path, DAY, [v.vehicle_id for v in registry])
    assert expense["stats"] == {"rows": 5, "valid": 2, "quarantined": 3}
    reasons = {r["record_id"]: r["reasons"] for r in expense["quarantine"].collect()}
    assert "duplicate_record" in reasons["EXP-1"]
    assert "unknown_vehicle" in reasons["EXP-3"]
    assert "bad_number:fuel_cost" in reasons["EXP-4"]

    prof = batch_layer.profitability_view(spark, s, DAY, reg, telemetry["vehicle_daily"], expense["per_vehicle"])
    p = {r["vehicle_id"]: r for r in prof.collect()}
    # V001: 1000 fares - 300 driver share - 300 fuel - 200 maintenance = 200 (20% margin)
    assert p["V001"]["net_profit_lkr"] == pytest.approx(200.0)
    assert p["V001"]["status"] == "PROFITABLE"
    assert p["V001"]["distance_gap_pct"] == pytest.approx(3.33, abs=0.01)
    # V013: no fares and a service bill
    assert p["V013"]["status"] == "UNPROFITABLE"
    assert any("service day" in r for r in p["V013"]["reasons"])
    assert p["V013"]["trend"] == "NEW"
