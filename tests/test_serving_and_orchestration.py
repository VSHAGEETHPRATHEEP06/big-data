"""API helpers, Airflow task logic and the report renderer."""
import csv
import hashlib
import io
from datetime import date

import pytest

from app.logic import alertmanager_rows, days_behind, parse_day
from fleet_batch_tasks import EXPENSE_COLUMNS, check_expense_file, evaluate_checks, pick_target_day
from fleet_report import heat_style, render_csv, render_html


def test_days_behind():
    first = date(2026, 9, 1)
    assert days_behind(date(2026, 9, 1), None, first) == 0  # day 1 is still running
    assert days_behind(date(2026, 9, 2), None, first) == 1
    assert days_behind(date(2026, 9, 4), date(2026, 9, 3), first) == 0
    assert days_behind(date(2026, 9, 5), date(2026, 9, 2), first) == 2


def test_alertmanager_payload_is_flattened():
    payload = {"alerts": [{"status": "firing", "labels": {"alertname": "SpeedLayerStale", "severity": "critical"},
                           "annotations": {"summary": "stale"}, "startsAt": "2026-09-30T07:00:00Z",
                           "endsAt": "0001-01-01T00:00:00Z", "fingerprint": "abc"}]}
    [row] = alertmanager_rows(payload)
    assert row[0:5] == ("alertmanager", "SpeedLayerStale", "critical", "firing", "stale")
    assert row[7] is None  # "not ended yet"


def test_parse_day_rejects_path_tricks():
    assert parse_day("2026-09-01") == date(2026, 9, 1)
    with pytest.raises(ValueError):
        parse_day("../../etc/passwd")


def test_pick_target_day_takes_oldest_unprocessed():
    first = date(2026, 9, 1)
    processed = {date(2026, 9, 1), date(2026, 9, 3)}
    assert pick_target_day(processed, date(2026, 9, 4), first) == date(2026, 9, 2)
    assert pick_target_day({date(2026, 9, 1)}, date(2026, 9, 1), first) is None


def _csv_bytes(rows: int) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(EXPENSE_COLUMNS)
    for i in range(rows):
        writer.writerow([f"EXP-{i}", "2026-09-01", "V001", "FP-01", 1, 300, 20, 10, "N", "x"])
    return buf.getvalue().encode()


def test_expense_file_checks():
    data = _csv_bytes(3)
    manifest = {"sha256": hashlib.sha256(data).hexdigest(), "rows": 3}
    assert check_expense_file(data, manifest)["rows"] == 3
    with pytest.raises(ValueError, match="checksum"):
        check_expense_file(data + b"x", manifest)
    with pytest.raises(ValueError, match="row count"):
        check_expense_file(data, {"sha256": manifest["sha256"], "rows": 4})


def summary(**overrides):
    base = {"trips": 400, "revenue_lkr": 350000.0, "events_total": 20000, "events_invalid": 90,
            "expense_rows": 32, "expense_rows_quarantined": 2, "no_cost_data_vehicles": 0}
    return {**base, **overrides}


def test_quality_checks():
    ok = evaluate_checks(summary(), [{"metric": "trips_completed", "diff_pct": 1.5,
                                      "batch_value": 400, "speed_value": 394}])
    assert all(c["ok"] for c in ok)
    bad = evaluate_checks(summary(trips=0, events_invalid=5000), [])
    failed = {c["check"] for c in bad if not c["ok"]}
    assert {"trips_recorded", "invalid_event_ratio_below_5pct"} <= failed
    assert evaluate_checks(None, [])[0]["ok"] is False


def test_heat_style_scale():
    assert "255, 255" not in heat_style(10, 10)
    assert heat_style(None, 10).startswith("background:#ffffff")


def test_report_renders_and_csv_has_every_vehicle():
    vehicles = [
        {"vehicle_id": "V013", "plate_no": "WP CAF-3378", "vehicle_type": "car", "driver_id": "D013", "trips": 3,
         "revenue_lkr": 4200.0, "driver_payout_lkr": 1260.0, "fuel_cost_lkr": 3100.0, "maintenance_cost_lkr": 16000.0,
         "total_cost_lkr": 19100.0, "net_profit_lkr": -16160.0, "margin_pct": -384.8, "gps_distance_km": 120.0,
         "reported_distance_km": 121.0, "distance_gap_pct": 0.8, "fuel_cost_per_km": 25.8, "utilization_pct": 30.0,
         "idle_min": 200.0, "service_day": True, "prev_net_profit_lkr": 2000.0, "trend": "NEW",
         "status": "UNPROFITABLE", "becoming_unprofitable": False, "reasons": ["service day: maintenance LKR 16,000"]},
    ]
    context = {
        "day": date(2026, 9, 2), "summary": {
            "trips": 3, "active_vehicles": 1, "revenue_lkr": 4200.0, "driver_payout_lkr": 1260.0,
            "fuel_cost_lkr": 3100.0, "maintenance_cost_lkr": 16000.0, "net_profit_lkr": -16160.0,
            "fleet_utilization_pct": 30.0, "unprofitable_vehicles": 1, "at_risk_vehicles": 0,
            "becoming_unprofitable_vehicles": 0, "no_cost_data_vehicles": 0, "long_idle_episodes": 1,
            "events_total": 1000, "events_invalid": 3, "duplicates_removed": 5, "late_events": 7,
            "expense_rows": 2, "expense_rows_valid": 1, "expense_rows_quarantined": 1,
            "distance_mismatch_vehicles": 0},
        "vehicles": vehicles, "attention": vehicles, "hours": [8, 9],
        "heatmap": [{"zone": "Borella", "total": 4200.0, "trips": 3,
                     "cells": [{"value": 1200.0, "style": heat_style(1200, 3000)}, {"value": 3000.0, "style": heat_style(3000, 3000)}]}],
        "idle": [], "reconciliation": [{"metric": "trips_completed", "speed_value": 3, "batch_value": 3, "diff_pct": 0}],
        "quarantine": [{"row_no": 2, "record_id": "EXP-9", "vehicle_id": "V099", "reasons": ["unknown_vehicle"]}],
        "batch_file": {"file_name": "expenses_2026-09-02.csv", "sha256": "0123456789abcdef"},
        "run_id": "manual__test", "generated_at": "2026-09-30 08:00 UTC", "driver_share_pct": 30,
        "at_risk_margin_pct": 15.0, "idle_alert_minutes": 60,
    }
    html = render_html(context)
    assert "V013" in html and "UNPROFITABLE" in html and "service day" in html
    assert "—" not in html  # no em dash in the report text
    rows = list(csv.DictReader(io.StringIO(render_csv(vehicles, date(2026, 9, 2)))))
    assert rows[0]["vehicle_id"] == "V013" and rows[0]["sim_date"] == "2026-09-02"
