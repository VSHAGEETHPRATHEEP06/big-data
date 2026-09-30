"""Daily fleet report (HTML for managers, CSV for spreadsheets).

Built by the Airflow DAG after the batch layer has published a day. Every
number comes from the batch views, so the report shows the exact results
recomputed from the raw master dataset, not the approximate speed layer.
"""
from __future__ import annotations

import csv
import io
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from psycopg2.extras import RealDictCursor

from fleet_batch_tasks import record_stage
from fleet_common.db import connect, transaction
from fleet_common.logs import get_logger
from fleet_common.settings import get_settings

log = get_logger("airflow-report", "serving")
TEMPLATE_DIR = Path(__file__).parent / "templates"

CSV_COLUMNS = [
    "sim_date", "vehicle_id", "plate_no", "vehicle_type", "driver_id", "trips", "revenue_lkr",
    "driver_payout_lkr", "fuel_cost_lkr", "maintenance_cost_lkr", "net_profit_lkr", "margin_pct",
    "gps_distance_km", "reported_distance_km", "distance_gap_pct", "fuel_cost_per_km", "utilization_pct",
    "idle_min", "service_day", "prev_net_profit_lkr", "trend", "status", "becoming_unprofitable", "reasons",
]


def _plain(value):
    return float(value) if isinstance(value, Decimal) else value


def _fetch(cur, sql: str, params=None) -> list[dict]:
    cur.execute(sql, params)
    return [{k: _plain(v) for k, v in row.items()} for row in cur.fetchall()]


def heat_style(value: float | None, top: float) -> str:
    """Background colour from white to teal, proportional to value / top."""
    if not value or not top:
        return "background:#ffffff;color:#94a3b8"
    t = max(0.0, min(1.0, value / top))
    start, end = (240, 253, 250), (15, 118, 110)
    rgb = tuple(round(a + (b - a) * t) for a, b in zip(start, end))
    text = "#ffffff" if t > 0.55 else "#0f172a"
    return f"background:rgb{rgb};color:{text}"


def build_context(cur, day: date, gap_limit_pct: float = 15.0) -> dict:
    summary = _fetch(cur, "SELECT * FROM batch.daily_summary WHERE sim_date = %s", (day,))
    if not summary:
        raise ValueError(f"no batch summary for {day}")
    vehicles = _fetch(
        cur,
        "SELECT * FROM batch.vehicle_profitability WHERE sim_date = %s ORDER BY net_profit_lkr, vehicle_id",
        (day,),
    )
    # losing money, thin margin, falling profit, or reported km that do not match GPS (possible fuel fraud)
    attention = [
        v for v in vehicles
        if v["status"] in ("UNPROFITABLE", "AT_RISK") or v["becoming_unprofitable"]
        or (v["distance_gap_pct"] is not None and abs(v["distance_gap_pct"]) > gap_limit_pct)
    ]

    zone_rows = _fetch(cur, "SELECT zone, hour, earnings_lkr, trips_completed, utilization_pct "
                            "FROM batch.zone_hourly WHERE sim_date = %s", (day,))
    hours = sorted({r["hour"] for r in zone_rows})
    totals: dict[str, dict] = {}
    cells: dict[tuple, float] = {}
    for r in zone_rows:
        cells[(r["zone"], r["hour"])] = r["earnings_lkr"]
        t = totals.setdefault(r["zone"], {"zone": r["zone"], "earnings": 0.0, "trips": 0})
        t["earnings"] += r["earnings_lkr"] or 0.0
        t["trips"] += r["trips_completed"] or 0
    zones = sorted(totals.values(), key=lambda z: -z["earnings"])
    top = max(cells.values(), default=0.0)
    heatmap = [
        {"zone": z["zone"], "total": z["earnings"], "trips": z["trips"],
         "cells": [{"value": cells.get((z["zone"], h)), "style": heat_style(cells.get((z["zone"], h)), top)} for h in hours]}
        for z in zones
    ]

    idle = _fetch(
        cur,
        "SELECT vehicle_id, vehicle_type, idle_min, online_min, max_idle_streak_min, long_idle_episodes "
        "FROM batch.vehicle_daily WHERE sim_date = %s AND long_idle_episodes > 0 "
        "ORDER BY max_idle_streak_min DESC",
        (day,),
    )
    reconciliation = _fetch(cur, "SELECT * FROM ops.layer_reconciliation WHERE sim_date = %s ORDER BY metric", (day,))
    quarantine = _fetch(cur, "SELECT row_no, record_id, vehicle_id, reasons FROM batch.expense_quarantine "
                             "WHERE sim_date = %s ORDER BY row_no", (day,))
    batch_file = _fetch(cur, "SELECT * FROM ops.batch_files WHERE sim_date = %s", (day,))
    return {
        "day": day,
        "summary": summary[0],
        "vehicles": vehicles,
        "attention": attention,
        "hours": hours,
        "heatmap": heatmap,
        "idle": idle,
        "reconciliation": reconciliation,
        "quarantine": quarantine,
        "batch_file": batch_file[0] if batch_file else None,
    }


def render_html(context: dict) -> str:
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=select_autoescape(["html", "j2"]))
    env.filters["lkr"] = lambda v: "-" if v is None else f"{v:,.0f}"
    env.filters["num"] = lambda v, d=1: "-" if v is None else f"{v:,.{d}f}"
    return env.get_template("daily_report.html.j2").render(**context)


def render_csv(vehicles: list[dict], day: date) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for v in vehicles:
        row = dict(v, sim_date=day.isoformat(), reasons=" | ".join(v.get("reasons") or []))
        writer.writerow(row)
    return buffer.getvalue()


def _atomic_write(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def build_report(sim_date: str, run_id: str) -> dict:
    s = get_settings()
    day = date.fromisoformat(sim_date)
    conn = connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            context = build_context(cur, day, s.distance_gap_alert_pct)
        conn.commit()
        context.update(
            run_id=run_id,
            generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            driver_share_pct=round(s.driver_share * 100),
            at_risk_margin_pct=s.at_risk_margin_pct,
            idle_alert_minutes=s.idle_alert_minutes,
        )
        s.reports_dir.mkdir(parents=True, exist_ok=True)
        html_path = s.reports_dir / f"daily_report_{sim_date}.html"
        csv_path = s.reports_dir / f"daily_report_{sim_date}.csv"
        _atomic_write(html_path, render_html(context))
        _atomic_write(csv_path, render_csv(context["vehicles"], day))
        with transaction(conn) as cur:
            cur.execute(
                "INSERT INTO serving.report_index (sim_date, html_path, csv_path, run_id) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (sim_date) DO UPDATE SET html_path = EXCLUDED.html_path, csv_path = EXCLUDED.csv_path, "
                "run_id = EXCLUDED.run_id, generated_at = now()",
                (day, str(html_path), str(csv_path), run_id),
            )
    finally:
        conn.close()
    result = {
        "html": str(html_path),
        "csv": str(csv_path),
        "vehicles": len(context["vehicles"]),
        "needs_attention": len(context["attention"]),
    }
    record_stage(run_id, "daily_report", sim_date, "success", result, rows_out=len(context["vehicles"]))
    log.info("daily_report_built", extra={"sim_date": sim_date, "run_id": run_id, **result})
    return result
