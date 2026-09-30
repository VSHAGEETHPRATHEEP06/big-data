"""Simulated clock, zone lookup, registry and business rules."""
from datetime import date, datetime, timedelta

import pytest

from fleet_common import rules
from fleet_common.simclock import SimClock, fmt_sim


def test_clock_maps_real_seconds_to_simulated_minutes():
    clock = SimClock(real_start=1_000.0, sim_start=datetime(2026, 9, 1, 6, 0), speedup=120)
    assert clock.sim_now(1_000.0) == datetime(2026, 9, 1, 6, 0)
    assert clock.sim_now(1_001.0) == datetime(2026, 9, 1, 6, 2)  # 1 s = 2 min
    assert clock.sim_now(1_030.0) == datetime(2026, 9, 1, 7, 0)  # 30 s = 1 h
    assert clock.real_seconds_per_sim_day == 720
    assert clock.to_real(datetime(2026, 9, 2, 0, 0)) == pytest.approx(1_000.0 + 18 * 3600 / 120)
    assert fmt_sim(datetime(2026, 9, 1, 8, 15, 30, 999)) == "2026-09-01T08:15:30"


def test_every_zone_centre_maps_to_itself(zones):
    grid = dict(zones.build_grid())
    for zone in zones.zones:
        assert zones.nearest_zone(zone.lat, zone.lon) == zone.zone_name
        assert zones.zone_via_grid(zone.lat, zone.lon, grid) == zone.zone_name


def test_far_points_fall_in_the_outer_zone(zones):
    assert zones.nearest_zone(7.2906, 80.6337) == zones.outer_zone  # Kandy
    assert zones.zone_via_grid(6.0535, 80.2210) == zones.outer_zone  # Galle


def test_grid_and_exact_lookup_mostly_agree(zones):
    """The grid (used by Spark) is a close approximation of nearest-centre."""
    grid = dict(zones.build_grid())
    agree = total = 0
    lat = zones.bbox["lat_min"]
    while lat < zones.bbox["lat_max"]:
        lon = zones.bbox["lon_min"]
        while lon < zones.bbox["lon_max"]:
            total += 1
            agree += zones.zone_via_grid(lat, lon, grid) == zones.nearest_zone(lat, lon)
            lon += 0.0033
        lat += 0.0033
    assert agree / total > 0.85


def test_registry_is_consistent(registry, zones):
    names = {z.zone_name for z in zones.zones}
    ids = [v.vehicle_id for v in registry]
    assert len(ids) == len(set(ids)) == 30
    assert all(v.home_zone in names for v in registry)
    assert {v.vehicle_type for v in registry} == {"tuk", "mini", "car"}
    night = [v for v in registry if v.on_shift(3)]
    assert night, "at least one driver must work at night so telemetry never stops"


@pytest.mark.parametrize(
    "net, revenue, has_cost, expected",
    [
        (-10.0, 5000.0, True, rules.UNPROFITABLE),
        (500.0, 5000.0, True, rules.AT_RISK),  # 10% margin < 15%
        (2000.0, 5000.0, True, rules.PROFITABLE),
        (2000.0, 5000.0, False, rules.NO_COST_DATA),
        (0.0, 0.0, True, rules.AT_RISK),  # no fares at all
    ],
)
def test_classify(net, revenue, has_cost, expected):
    assert rules.classify(net, revenue, has_cost, at_risk_margin_pct=15.0) == expected


def test_trend_and_becoming_unprofitable():
    assert rules.trend([3000.0, 1000.0, -500.0]) == rules.DECLINING
    assert rules.trend([-500.0, 1000.0, 3000.0]) == rules.IMPROVING
    assert rules.trend([1000.0, 3000.0, 2000.0]) == rules.STABLE
    assert rules.trend([None, 1000.0, 500.0]) == rules.NEW
    assert rules.becoming_unprofitable(rules.UNPROFITABLE, rules.DECLINING, rules.AT_RISK, False)
    assert rules.becoming_unprofitable(rules.UNPROFITABLE, rules.NEW, rules.PROFITABLE, False)
    # a single expensive service day is not a trend
    assert not rules.becoming_unprofitable(rules.UNPROFITABLE, rules.NEW, rules.PROFITABLE, True)
    assert not rules.becoming_unprofitable(rules.UNPROFITABLE, rules.DECLINING, rules.AT_RISK, True)
    assert not rules.becoming_unprofitable(rules.PROFITABLE, rules.DECLINING, rules.PROFITABLE, False)


def test_net_profit_formula():
    assert rules.net_profit(10000, 2000, 500, 0.30) == pytest.approx(4500)
    assert rules.margin_pct(4500, 10000) == pytest.approx(45.0)
    assert rules.margin_pct(100, 0) is None
