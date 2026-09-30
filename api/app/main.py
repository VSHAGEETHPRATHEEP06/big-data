"""FleetPulse serving API (FastAPI).

This is the serving layer of the Lambda architecture. It reads the speed-layer
tables for "right now" questions, the batch-layer tables for finished days,
and the serving views that merge both. Interactive docs: /docs
"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, HTMLResponse
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest

from fleet_common.logs import get_logger
from fleet_common.settings import get_settings

from .db import Database
from .logic import alertmanager_rows, days_behind, parse_day
from .metrics import LATENCY, REQUESTS, FleetCollector

settings = get_settings()
log = get_logger("serving-api", "serving")
db = Database(settings.pg_dsn)
collector = FleetCollector(db, settings, log)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    REGISTRY.register(collector)
    log.info("api_started", extra={"reports_dir": str(settings.reports_dir)})
    yield
    REGISTRY.unregister(collector)
    db.close()


app = FastAPI(
    title="FleetPulse Serving API",
    version="1.0.0",
    description=(
        "Real-time fleet utilization and earnings (speed layer) and daily per-vehicle "
        "profitability (batch layer) for a simulated Colombo ride-hailing fleet. "
        "All simulated times are Sri Lanka local time."
    ),
    lifespan=lifespan,
)


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    started = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", request.url.path)
        elapsed = time.perf_counter() - started
        REQUESTS.labels(request.method, path, str(status)).inc()
        LATENCY.labels(path).observe(elapsed)
        if path not in ("/metrics", "/health"):
            log.info(
                "http_request",
                extra={"method": request.method, "path": request.url.path, "status": status,
                       "duration_ms": round(elapsed * 1000, 1)},
            )


def _day(value: str | None):
    """Parse a YYYY-MM-DD query or path value, answering 422 when it is not a date."""
    try:
        return parse_day(value)
    except ValueError as exc:
        raise HTTPException(422, f"not a date (YYYY-MM-DD): {value}") from exc


def _sim_now():
    row = db.one("SELECT ops.sim_now() AS sim_now")
    if not row or row["sim_now"] is None:
        raise HTTPException(503, "simulated clock not started yet")
    return row["sim_now"]


# ------------------------------------------------------------------ health
@app.get("/health", tags=["health"], summary="Liveness check (no database)")
def health():
    return {"status": "ok"}


@app.get("/ready", tags=["health"], summary="Readiness check (database and clock)")
def ready():
    try:
        clock = db.one("SELECT * FROM serving.v_sim_clock")
    except Exception as exc:
        raise HTTPException(503, f"database not reachable: {exc}") from exc
    return {"status": "ready", "clock": clock}


@app.get("/metrics", include_in_schema=False)
def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


# --------------------------------------------------------------- real time
@app.get("/api/v1/fleet/realtime", tags=["real-time (speed layer)"],
         summary="Fleet utilization and earnings right now")
def fleet_realtime():
    live = db.one("SELECT * FROM serving.v_fleet_live")
    sim_now = live["sim_now"]
    today = db.one(
        "SELECT coalesce(sum(trips_completed), 0) AS trips, coalesce(sum(revenue_lkr), 0) AS earnings_lkr, "
        "coalesce(sum(distance_km), 0) AS distance_km FROM speed.vehicle_daily WHERE sim_date = %s",
        (sim_now.date(),),
    )
    # windows that overlap the last simulated hour (aggregates trail real time by a few seconds)
    last_hour = db.one(
        "SELECT coalesce(sum(trips_completed), 0) AS trips, coalesce(sum(earnings_lkr), 0) AS earnings_lkr "
        "FROM speed.zone_hourly WHERE window_end > %s",
        (sim_now - timedelta(hours=1),),
    )
    zones = db.query(
        """
        SELECT zone, pings, active_vehicles, trips_completed, earnings_lkr, avg_speed_kmh,
               round(100.0 * on_trip_pings / nullif(pings, 0), 1) AS utilization_pct,
               round(100.0 * idle_pings / nullif(pings, 0), 1) AS idle_pct
        FROM speed.zone_hourly WHERE window_start = (SELECT max(window_start) FROM speed.zone_hourly)
        ORDER BY earnings_lkr DESC
        """
    )
    alerts = db.one("SELECT count(*) AS open_idle_alerts FROM speed.idle_alerts WHERE status = 'OPEN'")
    return {
        "sim_now": sim_now,
        "vehicles": {k: live[k] for k in ("active_vehicles", "on_trip", "enroute", "idle", "offline")},
        "idle_ratio_pct": live["idle_ratio_pct"],
        "utilization_pct": live["utilization_pct"],
        "trips_last_sim_hour": last_hour["trips"],
        "earnings_last_sim_hour_lkr": last_hour["earnings_lkr"],
        "trips_today": today["trips"],
        "earnings_today_lkr": today["earnings_lkr"],
        "distance_today_km": today["distance_km"],
        "open_idle_alerts": alerts["open_idle_alerts"],
        "latest_hour_by_zone": zones,
    }


@app.get("/api/v1/zones/realtime", tags=["real-time (speed layer)"],
         summary="Earnings and utilization by zone for the current simulated day")
def zones_realtime():
    sim_now = _sim_now()
    return {
        "sim_now": sim_now,
        "zones": db.query(
            """
            SELECT zone, sum(trips_completed) AS trips, sum(earnings_lkr) AS earnings_lkr,
                   round(100.0 * sum(on_trip_pings) / nullif(sum(pings), 0), 1) AS utilization_pct,
                   round(100.0 * sum(idle_pings) / nullif(sum(pings), 0), 1) AS idle_pct,
                   max(active_vehicles) AS peak_active_vehicles
            FROM speed.zone_hourly
            WHERE window_start >= %s AND window_start < %s
            GROUP BY zone ORDER BY earnings_lkr DESC
            """,
            (sim_now.date(), sim_now.date() + timedelta(days=1)),
        ),
    }


@app.get("/api/v1/zones/hourly", tags=["lambda merge"],
         summary="Zone x hour metrics: batch view for finished days, speed view otherwise")
def zones_hourly(date: str | None = Query(None, description="Simulated date YYYY-MM-DD (default today)")):
    day = _day(date) or _sim_now().date()
    rows = db.query(
        "SELECT * FROM serving.v_zone_hourly WHERE sim_date = %s ORDER BY hour, zone", (day,)
    )
    return {"sim_date": day, "source": rows[0]["source"] if rows else None, "rows": rows}


@app.get("/api/v1/vehicles", tags=["real-time (speed layer)"], summary="Live state of every vehicle")
def vehicles(status: str | None = Query(None, description="idle | enroute | on_trip | offline")):
    sql = "SELECT * FROM serving.v_vehicle_live"
    params: tuple = ()
    if status:
        sql += " WHERE live_status = %s"
        params = (status,)
    return {"vehicles": db.query(sql + " ORDER BY vehicle_id", params)}


@app.get("/api/v1/vehicles/{vehicle_id}", tags=["lambda merge"],
         summary="One vehicle: live state, today so far and recent daily profitability")
def vehicle(vehicle_id: str):
    live = db.one("SELECT * FROM serving.v_vehicle_live WHERE vehicle_id = %s", (vehicle_id,))
    if live is None:
        raise HTTPException(404, f"vehicle {vehicle_id} not seen yet")
    return {
        "live": live,
        "today": db.one("SELECT * FROM serving.v_vehicle_profit_live WHERE vehicle_id = %s", (vehicle_id,)),
        "history": db.query(
            "SELECT sim_date, trips, revenue_lkr, fuel_cost_lkr, maintenance_cost_lkr, net_profit_lkr, "
            "margin_pct, status, trend, reasons FROM batch.vehicle_profitability "
            "WHERE vehicle_id = %s ORDER BY sim_date DESC LIMIT 7",
            (vehicle_id,),
        ),
    }


@app.get("/api/v1/profitability/live", tags=["lambda merge"],
         summary="Projected profit today: live revenue (speed) with cost rates from recent batch runs")
def profitability_live(status: str | None = Query(None, description="UNPROFITABLE | AT_RISK | PROFITABLE | NO_COST_DATA")):
    sql = "SELECT * FROM serving.v_vehicle_profit_live"
    params: tuple = ()
    if status:
        sql += " WHERE live_status = %s"
        params = (status,)
    return {"sim_now": _sim_now(), "vehicles": db.query(sql + " ORDER BY projected_net_lkr NULLS LAST", params)}


# ------------------------------------------------------------------- alerts
@app.get("/api/v1/alerts", tags=["alerts"], summary="Idle-vehicle alerts from the speed layer")
def alerts(status: str = Query("OPEN", pattern="^(OPEN|RESOLVED|ALL)$"), limit: int = Query(50, le=500)):
    sql = "SELECT * FROM speed.idle_alerts"
    params: list = []
    if status != "ALL":
        sql += " WHERE status = %s"
        params.append(status)
    params.append(limit)
    return {"alerts": db.query(sql + " ORDER BY detected_at_sim DESC LIMIT %s", tuple(params))}


@app.get("/api/v1/alerts/notifications", tags=["alerts"],
         summary="Pipeline alerts received from Alertmanager and Airflow")
def notifications(limit: int = Query(50, le=500)):
    return {"notifications": db.query(
        "SELECT * FROM ops.alert_notifications ORDER BY received_at DESC LIMIT %s", (limit,)
    )}


@app.post("/api/v1/alerts/webhook", tags=["alerts"], summary="Alertmanager webhook receiver")
async def alert_webhook(request: Request):
    payload = await request.json()
    rows = alertmanager_rows(payload)
    for row in rows:
        db.execute(
            "INSERT INTO ops.alert_notifications (source, alertname, severity, status, summary, labels, "
            "starts_at, ends_at, fingerprint) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)",
            row,
        )
        log.warning("alert_notification", extra={"alertname": row[1], "severity": row[2], "status": row[3], "summary": row[4]})
    return {"stored": len(rows)}


# ---------------------------------------------------------------- batch views
@app.get("/api/v1/profitability/daily", tags=["daily report (batch layer)"],
         summary="Per-vehicle profitability for one processed day")
def profitability_daily(date: str | None = Query(None, description="YYYY-MM-DD, default = last processed day")):
    day = _day(date)
    if day is None:
        latest = db.one("SELECT max(sim_date) AS d FROM batch.daily_summary")
        day = latest["d"] if latest else None
    if day is None:
        raise HTTPException(404, "no day processed by the batch layer yet")
    return {
        "sim_date": day,
        "summary": db.one("SELECT * FROM batch.daily_summary WHERE sim_date = %s", (day,)),
        "vehicles": db.query(
            "SELECT * FROM batch.vehicle_profitability WHERE sim_date = %s ORDER BY net_profit_lkr", (day,)
        ),
    }


@app.get("/api/v1/reports", tags=["daily report (batch layer)"], summary="List of daily reports")
def reports():
    return {"reports": db.query(
        """
        SELECT r.sim_date, r.generated_at, r.run_id, s.trips, s.revenue_lkr, s.net_profit_lkr,
               s.unprofitable_vehicles, s.becoming_unprofitable_vehicles,
               '/reports/' || r.sim_date || '.html' AS html_url, '/reports/' || r.sim_date || '.csv' AS csv_url
        FROM serving.report_index r LEFT JOIN batch.daily_summary s USING (sim_date)
        ORDER BY r.sim_date DESC
        """
    )}


@app.get("/api/v1/reports/{sim_date}", tags=["daily report (batch layer)"],
         summary="Daily summary, data quality and speed-vs-batch reconciliation")
def report(sim_date: str):
    day = _day(sim_date)
    summary = db.one("SELECT * FROM batch.daily_summary WHERE sim_date = %s", (day,))
    if summary is None:
        raise HTTPException(404, f"no batch results for {day}")
    return {
        "summary": summary,
        "reconciliation": db.query("SELECT * FROM ops.layer_reconciliation WHERE sim_date = %s", (day,)),
        "quarantined_expense_rows": db.query(
            "SELECT row_no, record_id, vehicle_id, reasons FROM batch.expense_quarantine WHERE sim_date = %s", (day,)
        ),
        "becoming_unprofitable": db.query(
            "SELECT vehicle_id, vehicle_type, net_profit_lkr, prev_net_profit_lkr, trend, status, reasons "
            "FROM batch.vehicle_profitability WHERE sim_date = %s AND becoming_unprofitable", (day,)
        ),
    }


def _report_file(sim_date: str, ext: str) -> Path:
    day = _day(sim_date)
    path = settings.reports_dir / f"daily_report_{day.isoformat()}.{ext}"
    if not path.exists():
        raise HTTPException(404, f"report for {day} not generated yet")
    return path


@app.get("/reports/{sim_date}.html", tags=["daily report (batch layer)"], response_class=FileResponse)
def report_html(sim_date: str):
    return FileResponse(_report_file(sim_date, "html"), media_type="text/html")


@app.get("/reports/{sim_date}.csv", tags=["daily report (batch layer)"], response_class=FileResponse)
def report_csv(sim_date: str):
    return FileResponse(_report_file(sim_date, "csv"), media_type="text/csv", filename=f"fleet_profitability_{sim_date}.csv")


# -------------------------------------------------------------- pipeline ops
@app.get("/api/v1/pipeline/status", tags=["observability"],
         summary="Freshness of every layer, recent runs and data quality counters")
def pipeline_status():
    fresh = db.one("SELECT * FROM serving.v_pipeline_freshness")
    clock = db.one("SELECT * FROM serving.v_sim_clock")
    behind = None
    if clock:
        behind = days_behind(clock["sim_now"].date(), fresh["last_batch_sim_date"], settings.sim_start_date)
    return {
        "clock": clock,
        "freshness": fresh,
        "batch_days_behind": behind,
        "recent_runs": db.query(
            "SELECT run_id, stage, sim_date, status, started_at, finished_at, rows_in, rows_out "
            "FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 10"
        ),
        "speed_batches_last_minute": db.query(
            "SELECT query_name, count(*) AS batches, sum(rows_in) AS rows FROM ops.speed_batches "
            "WHERE committed_at > now() - interval '1 minute' GROUP BY query_name ORDER BY query_name"
        ),
        "recent_dead_letters": db.query(
            "SELECT received_at, kafka_partition, kafka_offset, errors, left(raw_value, 160) AS raw_value "
            "FROM ops.dead_letters ORDER BY id DESC LIMIT 5"
        ),
    }


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def index():
    return """<!doctype html><html><head><meta charset="utf-8"><title>FleetPulse API</title>
<style>body{font-family:system-ui,sans-serif;max-width:760px;margin:40px auto;padding:0 16px;line-height:1.5}
code{background:#f3f4f6;padding:2px 4px;border-radius:4px}</style></head><body>
<h1>FleetPulse serving API</h1>
<p>Ride-hailing fleet analytics on a Lambda architecture (EC8203 mini project).</p>
<ul>
<li><a href="/docs">Interactive API docs (Swagger)</a></li>
<li><a href="/api/v1/fleet/realtime">Real-time fleet metrics</a></li>
<li><a href="/api/v1/profitability/live">Live profitability projection</a></li>
<li><a href="/api/v1/alerts">Open idle alerts</a></li>
<li><a href="/api/v1/reports">Daily reports</a></li>
<li><a href="/api/v1/pipeline/status">Pipeline status</a></li>
<li><a href="/metrics">Prometheus metrics</a></li>
</ul></body></html>"""
