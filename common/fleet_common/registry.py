"""Fleet registry (vehicles and drivers) loaded from config/fleet_registry.csv."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Vehicle:
    vehicle_id: str
    driver_id: str
    vehicle_type: str  # tuk | mini | car
    plate_no: str
    home_zone: str
    profile: str  # normal | degrading | high_maintenance | idle_prone | fuel_anomaly
    km_per_litre: float
    shift_start_hour: int
    shift_hours: int

    def on_shift(self, hour: int) -> bool:
        """True when ``hour`` (0-23) falls inside this driver's shift."""
        return (hour - self.shift_start_hour) % 24 < self.shift_hours


def load_registry(path: str | Path) -> list[Vehicle]:
    with open(path, newline="") as fh:
        return [
            Vehicle(
                vehicle_id=row["vehicle_id"],
                driver_id=row["driver_id"],
                vehicle_type=row["vehicle_type"],
                plate_no=row["plate_no"],
                home_zone=row["home_zone"],
                profile=row["profile"],
                km_per_litre=float(row["km_per_litre"]),
                shift_start_hour=int(row["shift_start_hour"]),
                shift_hours=int(row["shift_hours"]),
            )
            for row in csv.DictReader(fh)
        ]
