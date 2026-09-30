"""Task logic of the daily batch DAG.

The functions are plain Python (no Airflow imports) so they can be tested
without a scheduler. The DAG file only wires them together.
"""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from psycopg2.extras import Json, RealDictCursor

from fleet_common.db import connect, transaction
from fleet_common.logs import get_logger
from fleet_common.settings import get_settings
from fleet_common.simclock import day_start, load_clock

log = get_logger("airflow-batch-dag", "orchestration")

EXPENSE_COLUMNS = [
    "record_id",
    "expense_date",
    "vehicle_id",
    "partner_id",
    "fuel_litres",
    "fuel_cost",
    "maintenance_cost",
    "distance_covered",
    "service_flag",
    "submitted_at",
]
LATE_DATA_GRACE = timedelta(hours=2)  # simulated time to wait after midnight for late events


def record_stage(run_id: str, stage: str, sim_date: str | None, status: str, details: dict | None = None,
                 rows_in: int | None = None, rows_out: int | None = None, started_at: datetime | None = None) -> None:
    conn = connect()
    try:
        with transaction(conn) as cur:
            cur.execute(
                "INSERT INTO ops.pipeline_runs (run_id, stage, sim_date, status, started_at, finished_at, "
                "rows_in, rows_out, details) VALUES (%s, %s, %s, %s, %s, now(), %s, %s, %s) "
                "ON CONFLICT (run_id, stage) DO UPDATE SET status = EXCLUDED.status, finished_at = now(), "
                "rows_in = EXCLUDED.rows_in, rows_out = EXCLUDED.rows_out, details = EXCLUDED.details",
                (run_id, stage, sim_date, status, started_at or datetime.now(timezone.utc),
                 rows_in, rows_out, Json(details or {})),
            )
    finally:
        conn.close()


# --------------------------------------------------------------- scheduling
def pick_target_day(processed: set[date], sim_today: date, first_day: date) -> date | None:
    """Oldest simulated day (up to today) that has no batch result yet."""
    day = first_day
    while day <= sim_today:
        if day not in processed:
            return day
        day += timedelta(days=1)
    return None


def resolve_target_day(requested: str | None = None) -> str | None:
    """Day this run works on: the requested day (manual re-run) or the next
    unprocessed day. When all finished days are done this is today, and the
    sensors simply wait until today's partner file arrives."""
    if requested:
        return date.fromisoformat(requested).isoformat()
    s = get_settings()
    conn = connect()
    try:
        clock = load_clock(conn)
        if clock is None:
            return None
        with conn.cursor() as cur:
            cur.execute("SELECT sim_date FROM batch.daily_summary")
            processed = {r[0] for r in cur.fetchall()}
    finally:
        conn.close()
    day = pick_target_day(processed, clock.sim_now().date(), s.sim_start_date)
    log.info("target_day_resolved", extra={"sim_date": day.isoformat() if day else None,
                                           "sim_now": clock.sim_now().isoformat(timespec="seconds")})
    return day.isoformat() if day else None


# ------------------------------------------------------------------ sensors
def expense_file_ready(sim_date: str) -> bool:
    """True when the partner file and its manifest (completion marker) exist."""
    landing = get_settings().landing_dir
    ready = (landing / f"expenses_{sim_date}.manifest.json").exists() and (landing / f"expenses_{sim_date}.csv").exists()
    if not ready:
        log.info("waiting_for_expense_file", extra={"sim_date": sim_date})
    return ready


def raw_data_ready(sim_date: str) -> bool:
    """True when the day is over (plus a grace period for late events) and the
    master dataset has telemetry files for that day."""
    s = get_settings()
    conn = connect()
    try:
        clock = load_clock(conn)
    finally:
        conn.close()
    day = date.fromisoformat(sim_date)
    partition = s.raw_telemetry_dir / f"event_date={sim_date}"
    has_files = partition.exists() and any(partition.glob("*.parquet"))
    grace_over = clock is not None and clock.sim_now() >= day_start(day + timedelta(days=1)) + LATE_DATA_GRACE
    if not (has_files and grace_over):
        log.info("waiting_for_raw_telemetry", extra={"sim_date": sim_date, "has_files": has_files, "grace_over": grace_over})
    return has_files and grace_over


# ------------------------------------------------------- file validation
def check_expense_file(csv_bytes: bytes, manifest: dict) -> dict:
    """File-level checks: checksum, header and row count against the manifest."""
    digest = hashlib.sha256(csv_bytes).hexdigest()
    if digest != manifest["sha256"]:
        raise ValueError(f"checksum mismatch: file {digest[:12]} manifest {manifest['sha256'][:12]}")
    reader = csv.reader(csv_bytes.decode("utf-8").splitlines())
    header = next(reader, [])
    if header != EXPENSE_COLUMNS:
        raise ValueError(f"unexpected header {header}")
    rows = sum(1 for _ in reader)
    if rows != int(manifest["rows"]):
        raise ValueError(f"row count {rows} does not match manifest {manifest['rows']}")
    return {"sha256": digest, "rows": rows}


def validate_and_archive(sim_date: str, run_id: str) -> dict:
    """Check the landed file, then copy it into the immutable raw zone of the lake."""
    s = get_settings()
    started = datetime.now(timezone.utc)
    name = f"expenses_{sim_date}.csv"
    manifest = json.loads((s.landing_dir / f"expenses_{sim_date}.manifest.json").read_text())
    source = s.landing_dir / name
    result = check_expense_file(source.read_bytes(), manifest)

    target_dir = s.raw_expenses_dir / f"expense_date={sim_date}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / name
    if target.exists() and hashlib.sha256(target.read_bytes()).hexdigest() != result["sha256"]:
        # a corrected file was resubmitted: keep the old version for audit
        shutil.move(str(target), str(target_dir / f"{name}.replaced_{started:%Y%m%dT%H%M%S}"))
    shutil.copy2(source, target)

    conn = connect()
    try:
        with transaction(conn) as cur:
            cur.execute(
                "INSERT INTO ops.batch_files (sim_date, file_name, sha256, rows_declared, archived_path) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (sim_date) DO UPDATE SET file_name = EXCLUDED.file_name, "
                "sha256 = EXCLUDED.sha256, rows_declared = EXCLUDED.rows_declared, "
                "archived_path = EXCLUDED.archived_path, received_at = now()",
                (sim_date, name, result["sha256"], result["rows"], str(target)),
            )
    finally:
        conn.close()
    info = {"file": name, "sha256": result["sha256"], "rows": result["rows"], "archived_path": str(target)}
    record_stage(run_id, "validate_expense_file", sim_date, "success", info, rows_in=result["rows"], started_at=started)
    log.info("expense_file_validated", extra={"sim_date": sim_date, "run_id": run_id, **info})
    return info


# ---------------------------------------------------------- data quality
def evaluate_checks(summary: dict | None, reconciliation: list[dict]) -> list[dict]:
    """Rules applied after each batch run. "error" checks fail the task."""
    checks: list[dict] = []

    def add(name: str, ok: bool, detail: str, severity: str = "error") -> None:
        checks.append({"check": name, "ok": bool(ok), "severity": severity, "detail": detail})

    if summary is None:
        add("batch_summary_exists", False, "batch.daily_summary has no row for the day")
        return checks
    add("batch_summary_exists", True, "summary row present")
    add("trips_recorded", summary["trips"] > 0, f"{summary['trips']} trips")
    add("revenue_not_negative", summary["revenue_lkr"] >= 0, f"LKR {summary['revenue_lkr']}")
    events = summary["events_total"] or 0
    invalid_ratio = (summary["events_invalid"] or 0) / events if events else 1.0
    add("invalid_event_ratio_below_5pct", invalid_ratio <= 0.05, f"{invalid_ratio:.2%} invalid")
    rows = summary["expense_rows"] or 0
    quarantine_ratio = (summary["expense_rows_quarantined"] or 0) / rows if rows else 1.0
    add("expense_quarantine_below_20pct", quarantine_ratio <= 0.20, f"{quarantine_ratio:.1%} of partner rows quarantined")
    add("every_active_vehicle_has_costs", (summary["no_cost_data_vehicles"] or 0) == 0,
        f"{summary['no_cost_data_vehicles']} vehicles without partner costs", severity="warning")
    for row in reconciliation:
        pct = row.get("diff_pct")
        add(f"speed_vs_batch_{row['metric']}", pct is None or abs(float(pct)) <= 10.0,
            f"batch {row['batch_value']} vs speed {row['speed_value']} ({pct}%)", severity="warning")
    return checks


def data_quality_checks(sim_date: str, run_id: str) -> dict:
    started = datetime.now(timezone.utc)
    conn = connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM batch.daily_summary WHERE sim_date = %s", (sim_date,))
            summary = cur.fetchone()
            cur.execute("SELECT * FROM ops.layer_reconciliation WHERE sim_date = %s", (sim_date,))
            reconciliation = [dict(r) for r in cur.fetchall()]
        conn.commit()
    finally:
        conn.close()
    if summary:  # NUMERIC columns arrive as Decimal
        summary = {k: float(v) if isinstance(v, Decimal) else v for k, v in summary.items()}
    checks = evaluate_checks(summary, reconciliation)
    failed = [c for c in checks if not c["ok"] and c["severity"] == "error"]
    warnings = [c for c in checks if not c["ok"] and c["severity"] == "warning"]
    status = "failed" if failed else "success"
    record_stage(run_id, "data_quality", sim_date, status, {"checks": checks}, started_at=started)
    log.info("data_quality_checked", extra={"sim_date": sim_date, "run_id": run_id, "failed": len(failed),
                                            "warnings": len(warnings), "checks": len(checks)})
    if failed:
        raise ValueError(f"data quality checks failed: {[c['check'] for c in failed]}")
    return {"checks": len(checks), "warnings": [c["check"] for c in warnings]}


# ------------------------------------------------------------ maintenance
def purge_speed_layer(sim_date: str, run_id: str) -> dict:
    """Lambda rule: once the batch layer owns a day, the speed layer's copy of
    older days is no longer needed. Keep the processed day itself for the
    reconciliation view and drop everything before it."""
    keep_from = date.fromisoformat(sim_date) - timedelta(days=1)
    statements = {
        "speed.zone_hourly": ("DELETE FROM speed.zone_hourly WHERE window_start < %s", (keep_from,)),
        "speed.vehicle_daily": ("DELETE FROM speed.vehicle_daily WHERE sim_date < %s", (keep_from,)),
        "speed.idle_alerts": ("DELETE FROM speed.idle_alerts WHERE status = 'RESOLVED' AND resolved_at_sim < %s", (keep_from,)),
        "ops.speed_batches": ("DELETE FROM ops.speed_batches WHERE committed_at < now() - interval '1 hour'", None),
        "ops.dead_letters": ("DELETE FROM ops.dead_letters WHERE received_at < now() - interval '1 day'", None),
    }
    deleted = {}
    conn = connect()
    try:
        with transaction(conn) as cur:
            for table, (sql, params) in statements.items():
                cur.execute(sql, params)
                deleted[table] = cur.rowcount
    finally:
        conn.close()
    record_stage(run_id, "purge_speed_layer", sim_date, "success", {"deleted": deleted, "kept_from": keep_from.isoformat()},
                 rows_out=sum(deleted.values()))
    log.info("speed_layer_purged", extra={"sim_date": sim_date, "run_id": run_id, "deleted": deleted})
    return deleted


# ---------------------------------------------------------------- alerting
def notify_failure(context) -> None:
    """on_failure_callback: log and store an alert so failures show up in the
    same alert feed as the Prometheus alerts (API /api/v1/alerts/notifications)."""
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    exc = context.get("exception")
    labels = {
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(dag_run, "run_id", None),
        "try_number": getattr(ti, "try_number", None),
    }
    summary = f"Airflow task {labels['dag_id']}.{labels['task_id']} failed: {str(exc)[:300]}"
    log.error("airflow_task_failed", extra={**labels, "error": str(exc)[:1000]})
    try:
        conn = connect(retries=3)
        try:
            with transaction(conn) as cur:
                cur.execute(
                    "INSERT INTO ops.alert_notifications (source, alertname, severity, status, summary, labels, starts_at) "
                    "VALUES ('airflow', 'AirflowTaskFailed', 'critical', 'firing', %s, %s, now())",
                    (summary, Json(labels)),
                )
        finally:
            conn.close()
    except Exception as err:  # the callback must never raise
        log.error("failure_alert_not_stored", extra={"error": str(err)})
