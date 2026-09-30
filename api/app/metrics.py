"""Prometheus metrics of the serving layer.

Two kinds of metrics are exported on /metrics:

* request metrics of the API itself (count and latency per route)
* business and pipeline-health gauges read from PostgreSQL at scrape time.
  Prometheus alert rules use them, for example "vehicle idle too long",
  "speed layer stale" and "batch layer behind".
"""
from __future__ import annotations

from datetime import datetime, timezone

from prometheus_client import Counter, Histogram
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from .logic import days_behind

REQUESTS = Counter("api_requests_total", "HTTP requests served", ["method", "route", "status"])
LATENCY = Histogram(
    "api_request_duration_seconds",
    "HTTP request latency",
    ["route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)


def _float(value, default: float = 0.0) -> float:
    return default if value is None else float(value)


class FleetCollector(Collector):
    def __init__(self, db, settings, logger) -> None:
        self.db = db
        self.s = settings
        self.log = logger

    def collect(self):
        up = GaugeMetricFamily("fleet_api_db_up", "1 when the API could read PostgreSQL during this scrape")
        try:
            metrics = list(self._business()) + list(self._pipeline())
            up.add_metric([], 1)
        except Exception as exc:  # never break the scrape, report the failure instead
            self.log.warning("metrics_query_failed", extra={"error": str(exc)[:500]})
            up.add_metric([], 0)
            metrics = []
        yield up
        yield from metrics

    def _business(self):
        live = self.db.one("SELECT * FROM serving.v_fleet_live") or {}
        g = GaugeMetricFamily("fleet_vehicles", "Vehicles by live status", labels=["status"])
        for status in ("on_trip", "enroute", "idle", "offline"):
            g.add_metric([status], _float(live.get(status)))
        yield g
        yield GaugeMetricFamily("fleet_active_vehicles", "Vehicles online now", value=_float(live.get("active_vehicles")))
        yield GaugeMetricFamily("fleet_idle_ratio_pct", "Share of online vehicles that are idle", value=_float(live.get("idle_ratio_pct")))
        yield GaugeMetricFamily("fleet_utilization_pct", "Share of online vehicles on a trip", value=_float(live.get("utilization_pct")))

        today = self.db.one(
            """
            SELECT coalesce(sum(trips_completed), 0) AS trips, coalesce(sum(revenue_lkr), 0) AS revenue
            FROM speed.vehicle_daily WHERE sim_date = ops.sim_now()::date
            """
        ) or {}
        hour = self.db.one(
            """
            SELECT coalesce(sum(trips_completed), 0) AS trips, coalesce(sum(earnings_lkr), 0) AS earnings
            FROM speed.zone_hourly WHERE window_start = (SELECT max(window_start) FROM speed.zone_hourly)
            """
        ) or {}
        yield GaugeMetricFamily("fleet_trips_today", "Trips completed today (speed layer)", value=_float(today.get("trips")))
        yield GaugeMetricFamily("fleet_earnings_today_lkr", "Fares earned today in LKR (speed layer)", value=_float(today.get("revenue")))
        yield GaugeMetricFamily("fleet_trips_current_sim_hour", "Trips completed in the latest simulated hour window", value=_float(hour.get("trips")))
        yield GaugeMetricFamily("fleet_earnings_current_sim_hour_lkr", "Fares in the current simulated hour", value=_float(hour.get("earnings")))

        idle = GaugeMetricFamily(
            "fleet_vehicle_idle_sim_minutes",
            "Simulated minutes the vehicle has been idle (only vehicles idle 30+ minutes)",
            labels=["vehicle_id", "zone"],
        )
        for row in self.db.query(
            "SELECT vehicle_id, zone, idle_minutes FROM serving.v_vehicle_live "
            "WHERE live_status = 'idle' AND idle_minutes >= 30"
        ):
            idle.add_metric([row["vehicle_id"], row["zone"] or "unknown"], _float(row["idle_minutes"]))
        yield idle
        alerts = self.db.one("SELECT count(*) AS n FROM speed.idle_alerts WHERE status = 'OPEN'") or {}
        yield GaugeMetricFamily("fleet_open_idle_alerts", "Idle alerts currently open", value=_float(alerts.get("n")))

        last = self.db.one("SELECT * FROM batch.daily_summary ORDER BY sim_date DESC LIMIT 1") or {}
        yield GaugeMetricFamily("fleet_unprofitable_vehicles_last_day", "Unprofitable vehicles on the last processed day",
                                value=_float(last.get("unprofitable_vehicles")))
        yield GaugeMetricFamily("fleet_becoming_unprofitable_vehicles_last_day",
                                "Vehicles becoming unprofitable on the last processed day",
                                value=_float(last.get("becoming_unprofitable_vehicles")))
        yield GaugeMetricFamily("fleet_net_profit_last_day_lkr", "Fleet net profit on the last processed day",
                                value=_float(last.get("net_profit_lkr")))
        projected = self.db.one(
            "SELECT count(*) FILTER (WHERE live_status = 'UNPROFITABLE') AS n FROM serving.v_vehicle_profit_live"
        ) or {}
        yield GaugeMetricFamily("fleet_projected_unprofitable_vehicles", "Vehicles projected to lose money today",
                                value=_float(projected.get("n")))

    def _pipeline(self):
        fresh = self.db.one("SELECT * FROM serving.v_pipeline_freshness") or {}
        clock = self.db.one("SELECT sim_now, speedup FROM serving.v_sim_clock") or {}
        if fresh.get("speed_state_age_s") is not None:  # absent until the simulated clock exists
            yield GaugeMetricFamily("pipeline_speed_layer_freshness_seconds",
                                    "Real seconds since the speed layer last wrote vehicle state",
                                    value=_float(fresh.get("speed_state_age_s")))
        if fresh.get("batch_success_age_s") is not None:  # absent until the first batch run
            yield GaugeMetricFamily("pipeline_batch_last_success_age_seconds",
                                    "Real seconds since the last successful batch-layer run",
                                    value=_float(fresh.get("batch_success_age_s")))
        yield GaugeMetricFamily("pipeline_dead_letters_5m", "Invalid events stored in the last 5 real minutes",
                                value=_float(fresh.get("dead_letters_5m")))
        if clock.get("sim_now"):
            sim_now = clock["sim_now"]
            behind = days_behind(sim_now.date(), fresh.get("last_batch_sim_date"), self.s.sim_start_date)
            yield GaugeMetricFamily("pipeline_batch_days_behind", "Finished simulated days not yet processed by the batch layer", value=behind)
            yield GaugeMetricFamily("pipeline_sim_time_seconds", "Simulated time (local wall clock as unix seconds)",
                                    value=_sim_epoch(sim_now))
            yield GaugeMetricFamily("pipeline_sim_speedup", "Simulated seconds per real second", value=_float(clock.get("speedup")))
        diff = GaugeMetricFamily("pipeline_layer_diff_pct",
                                 "Batch minus speed layer difference (percent of batch) for the last processed day",
                                 labels=["metric"])
        for row in self.db.query(
            "SELECT metric, diff_pct FROM ops.layer_reconciliation "
            "WHERE sim_date = (SELECT max(sim_date) FROM ops.layer_reconciliation)"
        ):
            diff.add_metric([row["metric"]], _float(row["diff_pct"]))
        yield diff


def _sim_epoch(ts: datetime) -> float:
    """Naive simulated local time shown as if it were UTC (for Grafana display)."""
    return ts.replace(tzinfo=timezone.utc).timestamp()
