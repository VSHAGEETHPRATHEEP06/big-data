"""Shared test set-up.

Tests run inside the `tests` image (docker compose --profile test run --rm tests),
which has Java, PySpark and every Python dependency. The paths below also let
the pure-Python tests run from a local checkout.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for sub in ("common", "spark/jobs", "simulators", "api", "airflow/include"):
    path = str(ROOT / sub)
    if path not in sys.path:
        sys.path.insert(0, path)
os.environ.setdefault("CONFIG_DIR", str(ROOT / "config"))
os.environ.setdefault("TZ", "UTC")


@pytest.fixture(scope="session")
def settings():
    from fleet_common.settings import get_settings

    return get_settings()


@pytest.fixture(scope="session")
def zones(settings):
    from fleet_common.zones import load_zones

    return load_zones(settings.zones_file)


@pytest.fixture(scope="session")
def registry(settings):
    from fleet_common.registry import load_registry

    return load_registry(settings.registry_file)


@pytest.fixture(scope="session")
def spark():
    """Small local Spark session (same time-zone settings as the real jobs)."""
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("fleetpulse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
