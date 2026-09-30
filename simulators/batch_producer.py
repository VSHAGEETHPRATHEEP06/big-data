"""Daily-batch source: drops one partner expense file per simulated day.

Fuel partners and garages send one consolidated end-of-day extract. The file
for day D lands in ``/data/landing/expenses/`` after simulated midnight plus a
partner delay of 20-90 simulated minutes (10-45 real seconds). It is written
atomically (temp file + rename) and followed by a manifest with the row count
and SHA-256 checksum, so the batch layer can prove the file is complete.

Costs are realistic because they start from the true kilometres each vehicle
drove that day (ground truth written by the fleet world model at midnight):

    fuel litres  = km / (km per litre x efficiency)
    fuel cost    = litres x fuel price of the day
    maintenance  = routine wear by distance (+ a service bill on service days)

Vehicle profiles in the registry create the stories the report must find:
``degrading`` loses fuel efficiency and gains maintenance every day,
``high_maintenance`` has frequent service bills and ``fuel_anomaly`` reports
30-40% more distance and fuel than it really used.

A few faulty rows are added on purpose (duplicates, unknown vehicle, bad
numbers, missing id) so that the validation and quarantine steps are tested.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import signal
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from prometheus_client import Counter, Gauge, start_http_server

from fleet_common.db import connect
from fleet_common.logs import get_logger
from fleet_common.registry import Vehicle, load_registry
from fleet_common.settings import get_settings
from fleet_common.simclock import day_start, load_clock, wait_for_clock

SERVICE = "batch-producer"
log = get_logger(SERVICE, "ingestion")

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
FUEL_PARTNERS = ["FP-01", "FP-02", "FP-03"]
# Daily upkeep charged by the garage partner (insurance, tyres, lease share),
# paid on every working day, plus wear that grows with distance. All in LKR.
DAILY_UPKEEP = {"tuk": 900.0, "mini": 1800.0, "car": 2600.0}
ROUTINE_MAINT_PER_100KM = {"tuk": (150, 450), "mini": (300, 800), "car": (400, 1100)}
SERVICE_COST = {"tuk": (4000, 9000), "mini": (8000, 16000), "car": (10000, 25000)}
SERVICE_PROB = {"high_maintenance": 0.35, "degrading": 0.12}
DEFAULT_SERVICE_PROB = 0.05

FILES = Counter("batch_producer_files_total", "Expense files dropped in the landing zone")
ROWS = Counter("batch_producer_rows_total", "Rows written to expense files", ["kind"])
FAULTS = Counter("batch_producer_injected_faults_total", "Faulty rows added on purpose", ["kind"])
LAST_DROP = Gauge("batch_producer_last_drop_timestamp_seconds", "Unix time of the last file drop")
PENDING = Gauge("batch_producer_pending_days", "Finished simulated days whose file is not written yet")
SIM_TIME = Gauge("batch_producer_sim_time_seconds", "Simulated time seen by the batch source")


def partner_delay_minutes(sim_date: date, seed: int) -> float:
    return random.Random(f"{seed}-delay-{sim_date}").uniform(20, 90)


def is_late_file(sim_date: date, seed: int, rate: float) -> bool:
    return random.Random(f"{seed}-late-{sim_date}").random() < rate


def efficiency_factor(vehicle: Vehicle, day_index: int) -> float:
    if vehicle.profile == "degrading":
        return max(0.45, 1.0 - 0.15 * day_index)
    return 1.0


def build_rows(day: date, registry: list[Vehicle], truth: dict, settings, rng: random.Random) -> list[dict]:
    day_index = (day - settings.sim_start_date).days
    fuel_price = settings.fuel_price_lkr + rng.uniform(-3, 3)
    submitted = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = []
    for v in registry:
        km = float(truth.get(v.vehicle_id, {}).get("km", 0.0))
        service = rng.random() < SERVICE_PROB.get(v.profile, DEFAULT_SERVICE_PROB)
        if km <= 0 and not service:
            continue  # the vehicle did not work and had no garage visit
        litres = km / (v.km_per_litre * efficiency_factor(v, day_index) * rng.uniform(0.95, 1.05))
        reported_km = km * (1 + rng.gauss(0, 0.02))
        if v.profile == "fuel_anomaly":
            inflate = rng.uniform(1.30, 1.40)
            reported_km *= inflate
            litres *= inflate
        lo, hi = ROUTINE_MAINT_PER_100KM[v.vehicle_type]
        maintenance = rng.uniform(lo, hi) * max(km, 20.0) / 100.0
        if km > 0:
            maintenance += DAILY_UPKEEP[v.vehicle_type] * rng.uniform(0.9, 1.1)
        if v.profile == "high_maintenance":
            maintenance *= 2.0
        if v.profile == "degrading":
            maintenance *= 1 + 0.6 * day_index
        if service:
            maintenance += rng.uniform(*SERVICE_COST[v.vehicle_type])
        rows.append(
            {
                "record_id": f"EXP-{day:%Y%m%d}-{v.vehicle_id}",
                "expense_date": day.isoformat(),
                "vehicle_id": v.vehicle_id,
                "partner_id": rng.choice(FUEL_PARTNERS),
                "fuel_litres": f"{litres:.2f}",
                "fuel_cost": f"{litres * fuel_price:.2f}",
                "maintenance_cost": f"{maintenance:.2f}",
                "distance_covered": f"{max(reported_km, 0.0):.2f}",
                "service_flag": "Y" if service else "N",
                "submitted_at": submitted,
            }
        )
    return rows


def inject_faults(rows: list[dict], rng: random.Random, rate: float) -> list[str]:
    """Add a few bad rows that the batch layer has to quarantine or remove."""
    kinds = []
    if rows and rng.random() < rate:
        rows.append(dict(rng.choice(rows)))
        kinds.append("duplicate_row")
    if rows and rng.random() < rate * 0.7:
        bad = dict(rng.choice(rows))
        bad.update(record_id=bad["record_id"].replace("-V", "-X"), vehicle_id="V099")
        rows.append(bad)
        kinds.append("unknown_vehicle")
    if rows and rng.random() < rate * 0.6:
        bad = dict(rng.choice(rows))
        bad.update(record_id=bad["record_id"] + "-B", fuel_cost=rng.choice(["N/A", "-1250.00", ""]))
        rows.append(bad)
        kinds.append("bad_number")
    if rows and rng.random() < rate * 0.4:
        bad = dict(rng.choice(rows))
        bad.update(record_id=bad["record_id"] + "-M", vehicle_id="")
        rows.append(bad)
        kinds.append("missing_vehicle_id")
    rng.shuffle(rows)
    return kinds


def write_file(landing: Path, day: date, rows: list[dict]) -> dict:
    """Write the CSV atomically, then the manifest that marks it complete."""
    landing.mkdir(parents=True, exist_ok=True)
    name = f"expenses_{day.isoformat()}.csv"
    tmp = landing / f".{name}.tmp"
    with open(tmp, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=EXPENSE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    digest = hashlib.sha256(tmp.read_bytes()).hexdigest()
    os.replace(tmp, landing / name)
    manifest = {
        "file": name,
        "sim_date": day.isoformat(),
        "rows": len(rows),
        "sha256": digest,
        "schema_version": 1,
        "columns": EXPENSE_COLUMNS,
        "producer": "partner-expense-feed",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    mtmp = landing / f".{name}.manifest.tmp"
    mtmp.write_text(json.dumps(manifest, indent=2))
    os.replace(mtmp, landing / f"expenses_{day.isoformat()}.manifest.json")
    return manifest


def load_truth(settings, day: date) -> dict | None:
    path = settings.sim_state_dir / "ground_truth" / f"{day.isoformat()}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())["vehicles"]


def main() -> None:
    s = get_settings()
    start_http_server(s.metrics_port)
    LAST_DROP.set(time.time())  # so the "feed missing" alert measures from start-up
    registry = load_registry(s.registry_file)

    def new_conn():
        return connect(s, logger=log)

    clock = wait_for_clock(new_conn, logger=log)
    conn = new_conn()
    log.info("batch_source_started", extra={"landing_dir": str(s.landing_dir), "vehicles": len(registry)})

    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))

    last_refresh = time.time()
    while not stop["flag"]:
        if time.time() - last_refresh > 10:  # pick up clock re-bases
            try:
                clock = load_clock(conn) or clock
            except Exception as exc:
                log.warning("clock_refresh_failed", extra={"error": str(exc)})
                conn = new_conn()
            last_refresh = time.time()

        sim_now = clock.sim_now()
        SIM_TIME.set(sim_now.replace(tzinfo=timezone.utc).timestamp())
        pending = 0
        day = s.sim_start_date
        while day < sim_now.date():
            manifest = s.landing_dir / f"expenses_{day.isoformat()}.manifest.json"
            if not manifest.exists():
                delay = timedelta(minutes=partner_delay_minutes(day, s.sim_seed))
                if is_late_file(day, s.sim_seed, s.late_file_rate):
                    delay += timedelta(days=1, hours=6)  # partner misses the deadline
                due = day_start(day + timedelta(days=1)) + delay
                truth = load_truth(s, day)
                if sim_now < due or (truth is None and sim_now < due + timedelta(hours=3)):
                    pending += 1
                else:
                    if truth is None:
                        log.warning("ground_truth_missing", extra={"sim_date": day.isoformat()})
                        truth = {}
                    rng = random.Random(f"{s.sim_seed}-rows-{day}")
                    rows = build_rows(day, registry, truth, s, rng)
                    clean = len(rows)
                    kinds = inject_faults(rows, rng, s.expense_fault_rate)
                    info = write_file(s.landing_dir, day, rows)
                    FILES.inc()
                    ROWS.labels("valid").inc(clean)
                    ROWS.labels("faulty").inc(len(rows) - clean)
                    for kind in kinds:
                        FAULTS.labels(kind).inc()
                    LAST_DROP.set(time.time())
                    log.info(
                        "expense_file_dropped",
                        extra={
                            "sim_date": day.isoformat(),
                            "file": info["file"],
                            "rows": info["rows"],
                            "sha256": info["sha256"][:12],
                            "injected_faults": kinds,
                            "partner_delay_sim_min": round(delay.total_seconds() / 60),
                        },
                    )
            day += timedelta(days=1)
        PENDING.set(pending)
        time.sleep(2)
    log.info("shutting_down")


if __name__ == "__main__":
    main()
