"""Central configuration read from environment variables.

Every container gets the same variables from `.env` through docker compose, so
all components agree on the simulated clock, topic names, thresholds and
business rules. Defaults below match `.env.example`, which lets unit tests run
without any environment set up.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path


def _str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    # --- simulated clock -------------------------------------------------
    sim_speedup: float
    sim_start: str
    sim_timezone: str
    sim_seed: int

    # --- Kafka -----------------------------------------------------------
    kafka_bootstrap: str
    topic_telemetry: str
    topic_dlq: str
    telemetry_partitions: int
    topic_retention_hours: int

    # --- PostgreSQL ------------------------------------------------------
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str

    # --- paths -----------------------------------------------------------
    data_dir: Path
    config_dir: Path

    # --- simulation ------------------------------------------------------
    tick_seconds: float
    invalid_event_rate: float
    duplicate_event_rate: float
    outage_rate_per_sim_hour: float
    expense_fault_rate: float
    late_file_rate: float
    fuel_price_lkr: float

    # --- stream processing -----------------------------------------------
    watermark_minutes: int
    speed_trigger_seconds: int
    aggregate_trigger_seconds: int
    archive_trigger_seconds: int
    idle_alert_minutes: int
    offline_after_minutes: int
    max_offsets_per_trigger: int

    # --- business rules --------------------------------------------------
    driver_share: float
    at_risk_margin_pct: float
    distance_gap_alert_pct: float
    low_trips_factor: float
    high_fuel_cost_factor: float

    # --- observability ---------------------------------------------------
    metrics_port: int
    log_level: str

    # ----------------------------------------------------------------------
    @property
    def sim_start_dt(self) -> datetime:
        """Simulated start time (naive, Sri Lanka local time)."""
        return datetime.fromisoformat(self.sim_start)

    @property
    def sim_start_date(self) -> date:
        return self.sim_start_dt.date()

    @property
    def sim_day_real_seconds(self) -> float:
        """Real seconds that one simulated day lasts (720 s when speed-up is 120)."""
        return 86400.0 / self.sim_speedup

    @property
    def ping_interval_sim_minutes(self) -> float:
        """Simulated minutes between two GPS pings of the same vehicle."""
        return self.tick_seconds * self.sim_speedup / 60.0

    @property
    def zones_file(self) -> Path:
        return self.config_dir / "zones.json"

    @property
    def registry_file(self) -> Path:
        return self.config_dir / "fleet_registry.csv"

    # data lake layout ------------------------------------------------------
    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing" / "expenses"

    @property
    def raw_telemetry_dir(self) -> Path:
        return self.data_dir / "lake" / "raw" / "telemetry"

    @property
    def raw_expenses_dir(self) -> Path:
        return self.data_dir / "lake" / "raw" / "expenses"

    @property
    def curated_dir(self) -> Path:
        return self.data_dir / "lake" / "curated"

    @property
    def checkpoint_dir(self) -> Path:
        return self.data_dir / "checkpoints"

    @property
    def sim_state_dir(self) -> Path:
        return self.data_dir / "sim_state"

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / "reports"

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        sim_speedup=_float("SIM_SPEEDUP", 120.0),
        sim_start=_str("SIM_START", "2026-09-01T06:00:00"),
        sim_timezone=_str("SIM_TIMEZONE", "Asia/Colombo"),
        sim_seed=_int("SIM_SEED", 2026),
        kafka_bootstrap=_str("KAFKA_BOOTSTRAP", "kafka:9092"),
        topic_telemetry=_str("TOPIC_TELEMETRY", "fleet.telemetry.raw"),
        topic_dlq=_str("TOPIC_DLQ", "fleet.telemetry.dlq"),
        telemetry_partitions=_int("TELEMETRY_PARTITIONS", 6),
        topic_retention_hours=_int("TOPIC_RETENTION_HOURS", 168),
        pg_host=_str("POSTGRES_HOST", "postgres"),
        pg_port=_int("POSTGRES_PORT", 5432),
        pg_db=_str("POSTGRES_DB", "fleet"),
        pg_user=_str("POSTGRES_USER", "fleet"),
        pg_password=_str("POSTGRES_PASSWORD", "fleet"),
        data_dir=Path(_str("DATA_DIR", "/data")),
        config_dir=Path(_str("CONFIG_DIR", "/opt/fleet/config")),
        tick_seconds=_float("TICK_SECONDS", 1.0),
        invalid_event_rate=_float("INVALID_EVENT_RATE", 0.005),
        duplicate_event_rate=_float("DUPLICATE_EVENT_RATE", 0.01),
        outage_rate_per_sim_hour=_float("OUTAGE_RATE_PER_SIM_HOUR", 0.03),
        expense_fault_rate=_float("EXPENSE_FAULT_RATE", 0.6),
        late_file_rate=_float("LATE_FILE_RATE", 0.0),
        fuel_price_lkr=_float("FUEL_PRICE_LKR", 305.0),
        watermark_minutes=_int("WATERMARK_MINUTES", 10),
        speed_trigger_seconds=_int("SPEED_TRIGGER_SECONDS", 5),
        aggregate_trigger_seconds=_int("AGGREGATE_TRIGGER_SECONDS", 10),
        archive_trigger_seconds=_int("ARCHIVE_TRIGGER_SECONDS", 30),
        idle_alert_minutes=_int("IDLE_ALERT_MINUTES", 90),
        offline_after_minutes=_int("OFFLINE_AFTER_MINUTES", 30),
        max_offsets_per_trigger=_int("MAX_OFFSETS_PER_TRIGGER", 5000),
        driver_share=_float("DRIVER_SHARE", 0.30),
        at_risk_margin_pct=_float("AT_RISK_MARGIN_PCT", 15.0),
        distance_gap_alert_pct=_float("DISTANCE_GAP_ALERT_PCT", 15.0),
        low_trips_factor=_float("LOW_TRIPS_FACTOR", 0.6),
        high_fuel_cost_factor=_float("HIGH_FUEL_COST_FACTOR", 1.25),
        metrics_port=_int("METRICS_PORT", 8000),
        log_level=_str("LOG_LEVEL", "INFO"),
    )
