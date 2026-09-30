"""Shared Spark transformations (used by both the speed and the batch layer)."""
import json
from datetime import datetime

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

import fleet_transforms as ft  # noqa: E402
from fleet_common import rules  # noqa: E402

GOOD = {"schema_version": 1, "event_id": "e1", "event_type": "ping", "event_ts": "2026-09-01T08:15:00",
        "vehicle_id": "V001", "driver_id": "D001", "trip_id": None, "status": "idle", "lat": 6.9115,
        "lon": 79.8505, "speed": 5.0, "heading": 10, "odometer_km": 1000.0, "fare": 0.0}


def raw_df(spark, payloads):
    rows = [
        ("V001", p if isinstance(p, str) else json.dumps(p), "fleet.telemetry.raw", 0, i, datetime(2026, 9, 30, 7, 0))
        for i, p in enumerate(payloads)
    ]
    return spark.createDataFrame(
        rows, "kafka_key string, raw_value string, kafka_topic string, kafka_partition int, "
              "kafka_offset long, kafka_ts timestamp")


def test_validation_flags_each_problem(spark):
    bad = [
        '{"event_id": "x", "vehicle',  # truncated JSON
        {**GOOD, "event_id": "e2", "vehicle_id": None},
        {**GOOD, "event_id": "e3", "lat": 0.0, "lon": 0.0},
        {**GOOD, "event_id": "e4", "speed": -3.0},
        {**GOOD, "event_id": "e5", "status": "parked"},
        {**GOOD, "event_id": "e6", "event_ts": "31/09/2026 25:61"},
        {**GOOD, "event_id": "e7", "event_type": "trip_end", "fare": 0.0},
    ]
    df = ft.validate_events(ft.parse_telemetry(raw_df(spark, [GOOD] + bad)))
    rows = {r["kafka_offset"]: r for r in df.select("kafka_offset", "is_valid", "dq_errors").collect()}
    assert rows[0]["is_valid"] and rows[0]["dq_errors"] == []
    expected = {1: "unparseable_or_missing_event_id", 2: "missing_vehicle_id", 3: "gps_out_of_range",
                4: "bad_speed", 5: "bad_status", 6: "bad_event_ts", 7: "incomplete_trip_end"}
    for offset, reason in expected.items():
        assert not rows[offset]["is_valid"], reason
        assert reason in rows[offset]["dq_errors"]


def test_event_date_partition(spark):
    df = ft.with_event_date(raw_df(spark, [GOOD, "not json", {**GOOD, "event_ts": "garbage"}]))
    assert [r["event_date"] for r in df.orderBy("kafka_offset").collect()] == ["2026-09-01", "unknown", "unknown"]


def test_zone_enrichment_matches_python(spark, zones):
    grid = ft.zone_grid_df(spark, zones)
    points = [(z.lat, z.lon) for z in zones.zones] + [(6.93, 79.853), (6.88, 79.905), (7.29, 80.63)]
    df = spark.createDataFrame(points, "lat double, lon double")
    py_grid = dict(zones.build_grid())
    for r in ft.add_zone(df, grid, zones, "lat", "lon", "zone").collect():
        assert r["zone"] == zones.zone_via_grid(r["lat"], r["lon"], py_grid)


def test_zone_metrics_attribute_fares_to_pickup_zone(spark, zones):
    kollu, borella = zones.by_name("Kollupitiya"), zones.by_name("Borella")
    events = [
        {**GOOD, "event_id": "a", "lat": kollu.lat, "lon": kollu.lon},
        {**GOOD, "event_id": "b", "vehicle_id": "V002", "status": "on_trip", "speed": 20.0,
         "lat": kollu.lat, "lon": kollu.lon, "trip_id": "T1", "fare": 300.0},
        {**GOOD, "event_id": "c", "vehicle_id": "V002", "event_type": "trip_end", "status": "on_trip",
         "trip_id": "T1", "fare": 800.0, "lat": kollu.lat, "lon": kollu.lon,
         "pickup_lat": borella.lat, "pickup_lon": borella.lon},
    ]
    parsed = ft.validate_events(ft.parse_telemetry(raw_df(spark, events)))
    enriched = ft.enrich_zones(parsed.filter("is_valid"), ft.zone_grid_df(spark, zones), zones)
    out = {r["metric_zone"]: r for r in enriched.groupBy("metric_zone").agg(*ft.zone_metric_aggs()).collect()}
    assert out["Kollupitiya"]["pings"] == 2 and out["Kollupitiya"]["on_trip_pings"] == 1
    assert out["Kollupitiya"]["trips_completed"] == 0
    assert out["Borella"]["trips_completed"] == 1 and out["Borella"]["earnings_lkr"] == 800.0


CASES = [
    # net, revenue, has_cost, prev2, prev1, prev_status, service_day
    (-100.0, 5000.0, True, 900.0, 400.0, "AT_RISK", False),
    (-100.0, 5000.0, True, None, 1500.0, "PROFITABLE", False),
    (-9000.0, 5000.0, True, None, 1500.0, "PROFITABLE", True),
    (-9000.0, 5000.0, True, 3000.0, 1500.0, "PROFITABLE", True),
    (500.0, 5000.0, True, 700.0, 600.0, "AT_RISK", False),
    (3000.0, 5000.0, True, 1000.0, 2000.0, "PROFITABLE", False),
    (3000.0, 5000.0, False, None, None, None, False),
    (0.0, 0.0, True, None, None, None, False),
]


def test_spark_rules_match_python_rules(spark, settings):
    """The batch layer (Spark) and the shared Python rules must always agree."""
    df = spark.createDataFrame(
        CASES, "net double, revenue double, has_cost boolean, prev2 double, prev1 double, "
               "prev_status string, service_day boolean")
    status = ft.status_col(F.col("net"), F.col("revenue"), F.col("has_cost"), settings.at_risk_margin_pct)
    df = df.withColumn("status", status).withColumn("trend", ft.trend_col(F.col("prev2"), F.col("prev1"), F.col("net")))
    df = df.withColumn("becoming", ft.becoming_unprofitable_col(F.col("status"), F.col("trend"),
                                                               F.col("prev_status"), F.col("service_day")))
    for row in df.collect():
        py_status = rules.classify(row["net"], row["revenue"], row["has_cost"], settings.at_risk_margin_pct)
        py_trend = rules.trend([row["prev2"], row["prev1"], row["net"]])
        assert row["status"] == py_status
        assert row["trend"] == py_trend
        assert row["becoming"] == rules.becoming_unprofitable(py_status, py_trend, row["prev_status"], row["service_day"])
