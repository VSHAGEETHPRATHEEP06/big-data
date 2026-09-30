"""Generate the Grafana dashboards (JSON) used by FleetPulse.

Run from the repository root after changing a panel:
    python3 observability/grafana/build_dashboards.py

Three dashboards are written to observability/grafana/dashboards/:
    fleet_live.json       business view, speed layer (right now)
    fleet_daily.json      business view, batch layer (one simulated day)
    pipeline_health.json  observability view (Prometheus metrics, alerts, runs)

Simulated time runs 120x faster than real time, so business panels use SQL
with categorical axes (zone, hour) instead of Grafana's real-time time axis.
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
DB = {"type": "grafana-postgresql-datasource", "uid": "fleetdb"}
TEAL = "#0f766e"
STATUS_COLORS = {"UNPROFITABLE": "red", "AT_RISK": "orange", "PROFITABLE": "green", "NO_COST_DATA": "text"}


class Layout:
    """Places panels left to right on the 24-column Grafana grid."""

    def __init__(self) -> None:
        self.x = 0
        self.y = 0
        self.row_h = 0
        self.next_id = 1

    def place(self, w: int, h: int) -> dict:
        if self.x + w > 24:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        pos = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.row_h = max(self.row_h, h)
        return pos

    def new_row(self) -> None:
        if self.x:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0

    def pid(self) -> int:
        self.next_id += 1
        return self.next_id


def sql(raw: str, ref: str = "A", fmt: str = "table") -> dict:
    return {"refId": ref, "datasource": DB, "rawSql": " ".join(raw.split()), "format": fmt,
            "rawQuery": True, "editorMode": "code"}


def prom(expr: str, legend: str = "", ref: str = "A", instant: bool = False, fmt: str = "time_series") -> dict:
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend, "range": not instant,
            "instant": instant, "editorMode": "code", "format": fmt}


def steps(*pairs) -> dict:
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in pairs]}


def stat(lay: Layout, title: str, target: dict, unit: str = "none", decimals: int | None = None,
         thresholds=None, w: int = 3, h: int = 4, text: bool = False, description: str = "") -> dict:
    return {
        "id": lay.pid(), "type": "stat", "title": title, "description": description, "gridPos": lay.place(w, h),
        "datasource": target["datasource"], "targets": [target],
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/.*/" if text else "", "values": False},
            "orientation": "auto", "textMode": "value", "colorMode": "value", "graphMode": "none",
            "justifyMode": "center", "wideLayout": True, "showPercentChange": False,
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit, "decimals": decimals, "color": {"mode": "thresholds"},
                "thresholds": thresholds or steps((None, TEAL)),
            },
            "overrides": [],
        },
    }


def timeseries(lay: Layout, title: str, targets: list[dict], unit: str = "short", w: int = 12, h: int = 8,
               description: str = "", stack: bool = False) -> dict:
    return {
        "id": lay.pid(), "type": "timeseries", "title": title, "description": description,
        "gridPos": lay.place(w, h), "datasource": targets[0]["datasource"], "targets": targets,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {"lineWidth": 2, "fillOpacity": 12, "showPoints": "never", "spanNulls": True,
                           "stacking": {"mode": "normal" if stack else "none", "group": "A"}},
            },
            "overrides": [],
        },
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def barchart(lay: Layout, title: str, target: dict, x_field: str, unit: str = "short", horizontal: bool = False,
             w: int = 12, h: int = 9, description: str = "", legend: bool = False, stacked: bool = False) -> dict:
    return {
        "id": lay.pid(), "type": "barchart", "title": title, "description": description, "gridPos": lay.place(w, h),
        "datasource": target["datasource"], "targets": [target],
        "options": {
            "orientation": "horizontal" if horizontal else "vertical", "xField": x_field, "showValue": "auto",
            "barWidth": 0.8, "groupWidth": 0.75, "stacking": "normal" if stacked else "none", "fullHighlight": False,
            "xTickLabelRotation": 0, "xTickLabelSpacing": 0,
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": legend},
            "tooltip": {"mode": "multi", "sort": "none"},
        },
        "fieldConfig": {
            "defaults": {"unit": unit, "color": {"mode": "palette-classic" if legend else "fixed", "fixedColor": TEAL},
                         "custom": {"fillOpacity": 85, "lineWidth": 0, "gradientMode": "none"}},
            "overrides": [],
        },
    }


def table(lay: Layout, title: str, target: dict, w: int = 12, h: int = 9, overrides: list | None = None,
          description: str = "", unit: str | None = None) -> dict:
    defaults = {"custom": {"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False}}
    if unit:
        defaults["unit"] = unit
    return {
        "id": lay.pid(), "type": "table", "title": title, "description": description, "gridPos": lay.place(w, h),
        "datasource": target["datasource"], "targets": [target],
        "options": {"showHeader": True, "cellHeight": "sm", "footer": {"show": False, "reducer": ["sum"]}},
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
    }


def widths(**px: int) -> list[dict]:
    """Fixed column widths for a table; the other columns share the rest."""
    return [{"matcher": {"id": "byName", "options": name}, "properties": [{"id": "custom.width", "value": v}]}
            for name, v in px.items()]


def color_text(field: str, mapping: dict[str, str]) -> dict:
    """Colour the cell background of a text column with value mappings."""
    options = {value: {"color": color, "index": i} for i, (value, color) in enumerate(mapping.items())}
    return {
        "matcher": {"id": "byName", "options": field},
        "properties": [
            {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
            {"id": "mappings", "value": [{"type": "value", "options": options}]},
        ],
    }


def color_numbers(field: str, thresholds: dict, unit: str | None = None) -> dict:
    props = [
        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
        {"id": "color", "value": {"mode": "thresholds"}},
        {"id": "thresholds", "value": thresholds},
    ]
    if unit:
        props.append({"id": "unit", "value": unit})
    return {"matcher": {"id": "byName", "options": field}, "properties": props}


def heat_override(scheme: str = "continuous-greens") -> dict:
    return {
        "matcher": {"id": "byType", "options": "number"},
        "properties": [
            {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "gradient"}},
            {"id": "color", "value": {"mode": scheme}},
            {"id": "custom.align", "value": "center"},
            {"id": "custom.width", "value": 46},
        ],
    }


def geomap(lay: Layout, title: str, target: dict, w: int = 12, h: int = 12) -> dict:
    return {
        "id": lay.pid(), "type": "geomap", "title": title, "gridPos": lay.place(w, h),
        "datasource": target["datasource"], "targets": [target],
        "description": "Online vehicles from the speed layer. Green = on trip, blue = going to a pickup, orange = idle.",
        "options": {
            "view": {"id": "coords", "lat": 6.902, "lon": 79.876, "zoom": 12.3, "allLayers": True},
            "controls": {"showZoom": True, "mouseWheelZoom": False, "showAttribution": True},
            "basemap": {"type": "default", "name": "Basemap", "config": {}},
            "layers": [{
                "type": "markers", "name": "Vehicles", "tooltip": True,
                "location": {"mode": "coords", "latitude": "lat", "longitude": "lon"},
                "config": {
                    "showLegend": True,
                    "style": {
                        "size": {"fixed": 7, "min": 4, "max": 12},
                        "color": {"field": "live_status", "fixed": "dark-green"},
                        "opacity": 0.9,
                        "symbol": {"mode": "fixed", "fixed": "img/icons/marker/circle.svg"},
                    },
                },
            }],
            "tooltip": {"mode": "details"},
        },
        "fieldConfig": {
            "defaults": {"color": {"mode": "thresholds"}, "thresholds": steps((None, "green"))},
            "overrides": [{
                "matcher": {"id": "byName", "options": "live_status"},
                "properties": [{"id": "mappings", "value": [{"type": "value", "options": {
                    "on_trip": {"color": "green", "index": 0},
                    "enroute": {"color": "blue", "index": 1},
                    "idle": {"color": "orange", "index": 2}}}]}],
            }],
        },
    }


def text_panel(lay: Layout, content: str, w: int = 24, h: int = 3) -> dict:
    return {"id": lay.pid(), "type": "text", "title": "", "gridPos": lay.place(w, h),
            "options": {"mode": "markdown", "content": content}}


def dashboard(uid: str, title: str, panels: list, refresh: str = "10s", time_from: str = "now-30m",
              templating: list | None = None, description: str = "") -> dict:
    return {
        "uid": uid, "title": title, "description": description, "tags": ["fleetpulse"], "timezone": "browser",
        "editable": False, "graphTooltip": 1, "refresh": refresh, "schemaVersion": 41, "version": 1,
        "time": {"from": time_from, "to": "now"}, "timepicker": {},
        "templating": {"list": templating or []}, "annotations": {"list": []}, "links": [],
        "panels": panels,
    }


# ----------------------------------------------------------------- live view
def fleet_live() -> dict:
    lay = Layout()
    today = "window_start >= ops.sim_now()::date AND window_start < ops.sim_now()::date + 1"
    p = [
        stat(lay, "Sim time", sql("SELECT to_char(ops.sim_now(), 'Dy DD Mon HH24:MI') AS sim_time"), text=True,
             description="Simulated Colombo time (1 real second = 2 simulated minutes)"),
        stat(lay, "Online", sql("SELECT active_vehicles FROM serving.v_fleet_live"), description="Vehicles online now"),
        stat(lay, "On a trip", sql("SELECT utilization_pct FROM serving.v_fleet_live"), unit="percent", decimals=0,
             description="Share of online vehicles carrying a passenger right now"),
        stat(lay, "Idle", sql("SELECT idle_ratio_pct FROM serving.v_fleet_live"), unit="percent", decimals=0,
             thresholds=steps((None, "green"), (50, "orange"), (70, "red")),
             description="Share of online vehicles waiting for a ride"),
        stat(lay, "Trips today", sql("SELECT coalesce(sum(trips_completed), 0) FROM speed.vehicle_daily WHERE sim_date = ops.sim_now()::date")),
        stat(lay, "Fares", sql("SELECT coalesce(sum(revenue_lkr), 0) FROM speed.vehicle_daily WHERE sim_date = ops.sim_now()::date"),
             unit="locale", decimals=0, description="Fares earned today in LKR (speed layer)"),
        stat(lay, "Idle alerts", sql("SELECT count(*) FROM speed.idle_alerts WHERE status = 'OPEN'"),
             thresholds=steps((None, "green"), (1, "orange"), (4, "red")),
             description="Vehicles idle for longer than the threshold right now"),
        stat(lay, "In loss", sql("SELECT count(*) FROM serving.v_vehicle_profit_live WHERE live_status = 'UNPROFITABLE'"),
             thresholds=steps((None, "green"), (1, "red")),
             description="Vehicles projected to lose money today: live fares and km with cost rates from the last batch runs (Lambda merge)"),
    ]
    lay.new_row()
    p.append(geomap(lay, "Vehicles right now", sql(
        "SELECT lat, lon, vehicle_id, live_status, zone, round(speed_kmh::numeric, 0) AS speed_kmh "
        "FROM serving.v_vehicle_live WHERE live_status <> 'offline'")))
    p.append(barchart(lay, "Fares by pickup zone today (LKR)", sql(
        f"SELECT zone, round(sum(earnings_lkr)) AS fares_lkr FROM speed.zone_hourly WHERE {today} "
        "GROUP BY zone ORDER BY 2 DESC"), x_field="zone", horizontal=True, h=12, unit="short"))
    lay.new_row()
    p.append(barchart(lay, "Trips completed per simulated hour today", sql(
        f"SELECT to_char(window_start, 'HH24') || ':00' AS hour, sum(trips_completed) AS trips "
        f"FROM speed.zone_hourly WHERE {today} GROUP BY 1 ORDER BY 1"), x_field="hour", h=9))
    # NULLIF hides empty segments; the colours are the same as on the map
    status_bars = barchart(lay, "Vehicle status by zone right now", sql(
        "SELECT zone, nullif(count(*) FILTER (WHERE live_status = 'on_trip'), 0) AS on_trip, "
        "nullif(count(*) FILTER (WHERE live_status = 'enroute'), 0) AS enroute, "
        "nullif(count(*) FILTER (WHERE live_status = 'idle'), 0) AS idle FROM serving.v_vehicle_live "
        "WHERE live_status <> 'offline' GROUP BY zone ORDER BY count(*) DESC"), x_field="zone", h=9,
        legend=True, horizontal=True, stacked=True)
    status_bars["fieldConfig"]["overrides"] = [
        {"matcher": {"id": "byName", "options": name},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]}
        for name, color in (("on_trip", "green"), ("enroute", "blue"), ("idle", "orange"))]
    p.append(status_bars)
    lay.new_row()
    hours = range(0, 24)
    cols = ", ".join(
        f"round(100.0 * sum(on_trip_pings) FILTER (WHERE extract(hour FROM window_start) = {h}) "
        f"/ nullif(sum(pings) FILTER (WHERE extract(hour FROM window_start) = {h}), 0)) AS \"{h:02d}\""
        for h in hours
    )
    p.append(table(lay, "Utilization % by zone and hour today (share of pings on a trip)", sql(
        f"SELECT zone, {cols} FROM speed.zone_hourly WHERE {today} GROUP BY zone ORDER BY zone"),
        w=24, h=13, overrides=[heat_override()]))
    lay.new_row()
    p.append(table(lay, "Open idle alerts (speed layer)", sql(
        "SELECT a.vehicle_id AS vehicle, r.vehicle_type AS type, a.zone, to_char(a.idle_since, 'HH24:MI') AS idle_since, "
        "a.current_idle_minutes AS minutes FROM speed.idle_alerts a JOIN ref.vehicles r USING (vehicle_id) "
        "WHERE a.status = 'OPEN' ORDER BY a.current_idle_minutes DESC"),
        overrides=[color_numbers("minutes", steps((None, "orange"), (150, "red")))], h=9))
    p.append(table(lay, "Profit projection for today (live fares + batch cost rates)", sql(
        "SELECT vehicle_id AS vehicle, vehicle_type AS type, trips, round(revenue_lkr) AS fares, "
        "round(projected_net_lkr) AS projected_net, live_status AS status FROM serving.v_vehicle_profit_live "
        "ORDER BY projected_net_lkr NULLS LAST LIMIT 12"),
        overrides=[color_text("status", STATUS_COLORS),
                   color_numbers("projected_net", steps((None, "red"), (0, "green")))], h=9))
    return dashboard("fleet-live", "FleetPulse | Fleet operations (live)", p,
                     description="Speed layer: utilization, earnings and idle alerts right now")


# --------------------------------------------------------------- daily view
def fleet_daily() -> dict:
    lay = Layout()
    d = "sim_date = '${sim_date}'"
    p = [
        stat(lay, "Trips", sql(f"SELECT trips FROM batch.daily_summary WHERE {d}"), w=2),
        stat(lay, "Fares (LKR)", sql(f"SELECT revenue_lkr FROM batch.daily_summary WHERE {d}"), decimals=0, unit="locale"),
        stat(lay, "Costs (LKR)", sql(f"SELECT fuel_cost_lkr + maintenance_cost_lkr FROM batch.daily_summary WHERE {d}"),
             decimals=0, unit="locale", description="Fuel and maintenance from the partner file"),
        stat(lay, "Net profit", sql(f"SELECT net_profit_lkr FROM batch.daily_summary WHERE {d}"), decimals=0, unit="locale",
             thresholds=steps((None, "red"), (0, "green")), description="Fleet net profit in LKR"),
        stat(lay, "Utilization", sql(f"SELECT fleet_utilization_pct FROM batch.daily_summary WHERE {d}"), unit="percent",
             decimals=1, description="Minutes on a trip divided by minutes online"),
        stat(lay, "Unprofitable", sql(f"SELECT unprofitable_vehicles FROM batch.daily_summary WHERE {d}"),
             thresholds=steps((None, "green"), (1, "red"))),
        stat(lay, "At risk", sql(f"SELECT at_risk_vehicles FROM batch.daily_summary WHERE {d}"),
             thresholds=steps((None, "green"), (1, "orange"))),
        stat(lay, "Becoming unprofitable", sql(f"SELECT becoming_unprofitable_vehicles FROM batch.daily_summary WHERE {d}"),
             thresholds=steps((None, "green"), (1, "red")), w=4),
    ]
    lay.new_row()
    p.append(table(lay, "Per-vehicle profitability (batch layer, exact)", sql(
        f"SELECT vehicle_id AS vehicle, vehicle_type AS type, trips, round(revenue_lkr) AS fares, "
        f"round(total_cost_lkr) AS costs, round(net_profit_lkr) AS net_profit, round(margin_pct) AS margin, "
        f"lower(trend) AS trend, status, array_to_string(reasons, ' | ') AS reasons "
        f"FROM batch.vehicle_profitability WHERE {d} ORDER BY net_profit_lkr"),
        w=24, h=12,
        overrides=[color_text("status", STATUS_COLORS),
                   color_numbers("net_profit", steps((None, "red"), (0, "green"))),
                   {"matcher": {"id": "byName", "options": "reasons"}, "properties": [{"id": "custom.width", "value": 520}]},
                   {"matcher": {"id": "byName", "options": "status"}, "properties": [{"id": "custom.width", "value": 130}]}]))
    lay.new_row()
    p.append(barchart(lay, "Twelve lowest net profits (LKR)", sql(
        f"SELECT vehicle_id, round(net_profit_lkr) AS net_profit FROM batch.vehicle_profitability WHERE {d} "
        f"ORDER BY net_profit_lkr LIMIT 12"), x_field="vehicle_id", h=9))
    p.append(barchart(lay, "Fares by pickup zone (LKR)", sql(
        f"SELECT zone, round(sum(earnings_lkr)) AS fares FROM batch.zone_hourly WHERE {d} GROUP BY zone ORDER BY 2 DESC"),
        x_field="zone", horizontal=True, h=9))
    lay.new_row()
    cols = ", ".join(
        f"round(sum(earnings_lkr) FILTER (WHERE hour = {h}) / 1000, 1) AS \"{h:02d}\"" for h in range(24)
    )
    p.append(table(lay, "Fares by zone and hour of day (thousand LKR)", sql(
        f"SELECT zone, {cols} FROM batch.zone_hourly WHERE {d} GROUP BY zone ORDER BY sum(earnings_lkr) DESC"),
        w=24, h=13, overrides=[heat_override()]))
    lay.new_row()
    p.append(table(lay, "Speed layer vs batch layer", sql(
        f"SELECT metric, speed_value, batch_value, diff, diff_pct FROM ops.layer_reconciliation WHERE {d} ORDER BY metric"),
        w=8, h=7, description="The batch layer includes late events and exact de-duplication."))
    p.append(table(lay, "Data quality", sql(
        f"SELECT 'telemetry events' AS item, events_total AS value FROM batch.daily_summary WHERE {d} "
        f"UNION ALL SELECT 'invalid events', events_invalid FROM batch.daily_summary WHERE {d} "
        f"UNION ALL SELECT 'duplicates removed', duplicates_removed FROM batch.daily_summary WHERE {d} "
        f"UNION ALL SELECT 'late events recovered', late_events FROM batch.daily_summary WHERE {d} "
        f"UNION ALL SELECT 'partner rows quarantined', expense_rows_quarantined FROM batch.daily_summary WHERE {d} "
        f"UNION ALL SELECT 'distance mismatches', distance_mismatch_vehicles FROM batch.daily_summary WHERE {d}"),
        w=8, h=7))
    p.append(table(lay, "Quarantined partner rows", sql(
        f"SELECT row_no, record_id, vehicle_id, array_to_string(reasons, ', ') AS reasons "
        f"FROM batch.expense_quarantine WHERE {d} ORDER BY row_no"), w=8, h=7))
    lay.new_row()
    p.append(barchart(lay, "Fleet net profit by day (LKR)", sql(
        "SELECT sim_date::text AS day, round(revenue_lkr) AS fares, round(net_profit_lkr) AS net_profit "
        "FROM batch.daily_summary ORDER BY sim_date"), x_field="day", h=8, legend=True))
    p.append(barchart(lay, "Problem vehicles by day", sql(
        "SELECT sim_date::text AS day, unprofitable_vehicles AS unprofitable, at_risk_vehicles AS at_risk, "
        "becoming_unprofitable_vehicles AS becoming_unprofitable FROM batch.daily_summary ORDER BY sim_date"),
        x_field="day", h=8, legend=True))
    variable = {
        "name": "sim_date", "label": "Simulated day", "type": "query", "datasource": DB,
        "query": "SELECT sim_date::text FROM batch.daily_summary ORDER BY sim_date DESC",
        "definition": "SELECT sim_date::text FROM batch.daily_summary ORDER BY sim_date DESC",
        "refresh": 1, "sort": 0, "includeAll": False, "multi": False, "hide": 0, "current": {},
    }
    return dashboard("fleet-daily", "FleetPulse | Daily profitability (batch layer)", p, refresh="30s",
                     templating=[variable], description="Batch layer: exact daily per-vehicle profitability")


# -------------------------------------------------------------- ops view
def pipeline_health() -> dict:
    lay = Layout()
    p = [
        stat(lay, "Targets up", prom("sum(up) / count(up) * 100", instant=True), unit="percent", decimals=0,
             thresholds=steps((None, "red"), (99, "green")), description="Share of scrape targets that answer"),
        stat(lay, "Events/s in", prom("sum(rate(producer_events_sent_total[1m]))", instant=True), decimals=1,
             description="Events per second acknowledged by Kafka"),
        stat(lay, "Freshness", prom("pipeline_speed_layer_freshness_seconds", instant=True), unit="s", decimals=1,
             thresholds=steps((None, "green"), (20, "orange"), (60, "red")),
             description="Seconds since the speed layer last wrote vehicle state"),
        stat(lay, "Latency", prom('max(speed_e2e_latency_seconds{query="vehicle_state"})', instant=True), unit="s", decimals=1,
             thresholds=steps((None, "green"), (10, "orange"), (20, "red")),
             description="Kafka append time to PostgreSQL commit (vehicle_state query)"),
        stat(lay, "Kafka lag", prom("max(speed_kafka_offsets_behind_latest)", instant=True),
             thresholds=steps((None, "green"), (500, "orange"), (2000, "red")),
             description="Largest number of offsets any streaming query is behind"),
        stat(lay, "Dead letters", prom("sum(increase(speed_dead_letter_events_total[5m]))", instant=True), decimals=0,
             thresholds=steps((None, "green"), (20, "orange")), description="Invalid events in the last 5 minutes"),
        stat(lay, "Days behind", prom("pipeline_batch_days_behind", instant=True),
             thresholds=steps((None, "green"), (2, "red")),
             description="Finished simulated days not yet processed by the batch layer"),
        stat(lay, "Firing alerts", prom('count(ALERTS{alertstate="firing"}) or vector(0)', instant=True),
             thresholds=steps((None, "green"), (1, "orange"))),
    ]
    lay.new_row()
    p.append(timeseries(lay, "Ingestion: events/s per Kafka partition", [
        prom("sum by (partition) (rate(producer_partition_events_total[1m]))", "partition {{partition}}")],
        unit="short", stack=True, description="Producer delivery reports (acks=all) per partition"))
    p.append(timeseries(lay, "Ingestion: delivery latency and errors", [
        prom("histogram_quantile(0.95, sum by (le) (rate(producer_delivery_latency_seconds_bucket[1m])))", "p95 delivery latency (s)"),
        prom("sum(rate(producer_send_errors_total[1m]))", "send errors/s", ref="B")], unit="short"))
    lay.new_row()
    p.append(timeseries(lay, "Processing: rows/s read by each streaming query", [
        prom("speed_query_input_rows_per_second", "{{query}}")], unit="short"))
    p.append(timeseries(lay, "Processing: micro-batch duration (trigger 5 s state, 10 s aggregates)", [
        prom("speed_query_batch_duration_seconds", "{{query}}")], unit="s"))
    lay.new_row()
    p.append(timeseries(lay, "End-to-end latency Kafka to PostgreSQL", [
        prom("speed_e2e_latency_seconds", "{{query}}")], unit="s", w=8))
    p.append(timeseries(lay, "Consumer lag (offsets behind latest)", [
        prom("speed_kafka_offsets_behind_latest", "{{query}}")], unit="short", w=8))
    p.append(timeseries(lay, "Late and invalid events (per 5 min)", [
        prom('sum by (query) (increase(speed_rows_dropped_by_watermark_total{query=~"zone_hourly|vehicle_daily"}[5m]))',
             "late, dropped ({{query}})"),
        prom("sum(increase(speed_dead_letter_events_total[5m]))", "invalid (dead letters)", ref="B")],
        unit="short", w=8))
    lay.new_row()
    p.append(timeseries(lay, "Orchestration: Airflow task results (per 30 min)", [
        prom("sum by (state) (increase(airflow_task_finished_total[30m]))", "{{state}}")], unit="short", w=8))
    p.append(timeseries(lay, "Batch layer freshness", [
        prom("pipeline_batch_last_success_age_seconds", "seconds since last batch success"),
        prom("pipeline_batch_days_behind", "days behind", ref="B")], unit="short", w=8))
    p.append(timeseries(lay, "Serving: API requests/s and p95 latency", [
        prom("sum(rate(api_requests_total[1m]))", "requests/s"),
        prom("histogram_quantile(0.95, sum by (le) (rate(api_request_duration_seconds_bucket[1m])))", "p95 latency (s)", ref="B")],
        unit="short", w=8))
    lay.new_row()
    firing = table(lay, "Firing alerts (Prometheus rules)", prom(
        'ALERTS{alertstate="firing"}', instant=True, fmt="table"), w=10, h=8,
        overrides=widths(alertname=220, severity=70, vehicle_id=80, zone=100))
    firing["transformations"] = [{
        "id": "organize",
        "options": {
            "excludeByName": {"Time": True, "Value": True, "__name__": True, "alertstate": True,
                              "instance": True, "project": True},
            "indexByName": {"alertname": 0, "severity": 1, "vehicle_id": 2, "zone": 3, "job": 4, "stage": 5},
            "renameByName": {},
        },
    }]
    p.append(firing)
    # business alerts (they carry a team label) are shown on the fleet dashboard instead
    p.append(table(lay, "Pipeline alert notifications (Alertmanager + Airflow)", sql(
        "SELECT to_char(received_at, 'HH24:MI:SS') AS received_utc, alertname, severity, status, summary "
        "FROM ops.alert_notifications WHERE $__timeFilter(received_at) AND coalesce(labels->>'team', '') = '' "
        "ORDER BY id DESC LIMIT 15"), w=14, h=8,
        overrides=[color_text("severity", {"critical": "red", "warning": "orange", "info": "blue"})]
        + widths(received_utc=105, alertname=165, severity=80, status=80)))
    lay.new_row()
    p.append(table(lay, "Pipeline run audit trail (ops.pipeline_runs)", sql(
        "SELECT run_id, stage, sim_date, status, to_char(started_at, 'HH24:MI:SS') AS started_utc, "
        "round(extract(epoch FROM finished_at - started_at)::numeric, 1) AS seconds, rows_in, rows_out "
        "FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 16"), w=24, h=9,
        overrides=[color_text("status", {"success": "green", "failed": "red"})]))
    for panel in p:
        if panel["type"] == "timeseries":
            panel["fieldConfig"]["defaults"]["custom"]["spanNulls"] = False
    return dashboard("pipeline-health", "FleetPulse | Pipeline health", p,
                     description="Observability: metrics of every stage, alerts and the run audit trail")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, board in (("fleet_live", fleet_live()), ("fleet_daily", fleet_daily()),
                        ("pipeline_health", pipeline_health())):
        (OUT / f"{name}.json").write_text(json.dumps(board, indent=2) + "\n")
        print(f"wrote {name}.json with {len(board['panels'])} panels")


if __name__ == "__main__":
    main()
