"""Zone reference data and geo helpers.

A GPS point belongs to the nearest zone centre that is within
``max_radius_km``, otherwise to the outer zone. For Spark we pre-compute a
small lookup grid (cell id -> zone) so that zone enrichment is a plain
broadcast join with reference data in both the speed and the batch layer.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


@dataclass(frozen=True)
class Zone:
    zone_id: str
    zone_name: str
    lat: float
    lon: float
    area_type: str


@dataclass(frozen=True)
class ZoneConfig:
    zones: tuple[Zone, ...]
    cell_size_deg: float
    max_radius_km: float
    bbox: dict
    outer_zone: str

    def by_name(self, name: str) -> Zone:
        for zone in self.zones:
            if zone.zone_name == name:
                return zone
        raise KeyError(name)

    def nearest_zone(self, lat: float, lon: float) -> str:
        best, best_d = None, float("inf")
        for zone in self.zones:
            d = haversine_km(lat, lon, zone.lat, zone.lon)
            if d < best_d:
                best, best_d = zone, d
        if best is None or best_d > self.max_radius_km:
            return self.outer_zone
        return best.zone_name

    def cell_index(self, lat: float, lon: float) -> tuple[int, int]:
        # Same arithmetic as the Spark expression in fleet_transforms.cell_id_col
        return math.floor(lat / self.cell_size_deg), math.floor(lon / self.cell_size_deg)

    def cell_id(self, lat: float, lon: float) -> str:
        i, j = self.cell_index(lat, lon)
        return f"{i}_{j}"

    def build_grid(self, margin_cells: int = 4) -> list[tuple[str, str]]:
        """(cell_id, zone_name) for every grid cell around the bounding box.

        Each cell takes the zone of its centre point. Cells whose centre is in
        the outer area are left out, so a failed lookup means "outer area".
        """
        size = self.cell_size_deg
        i0, j0 = self.cell_index(self.bbox["lat_min"], self.bbox["lon_min"])
        i1, j1 = self.cell_index(self.bbox["lat_max"], self.bbox["lon_max"])
        rows = []
        for i in range(i0 - margin_cells, i1 + margin_cells + 1):
            for j in range(j0 - margin_cells, j1 + margin_cells + 1):
                zone = self.nearest_zone((i + 0.5) * size, (j + 0.5) * size)
                if zone != self.outer_zone:
                    rows.append((f"{i}_{j}", zone))
        return rows

    def zone_via_grid(self, lat: float, lon: float, grid: dict[str, str] | None = None) -> str:
        grid = grid if grid is not None else dict(self.build_grid())
        return grid.get(self.cell_id(lat, lon), self.outer_zone)


def load_zones(path: str | Path) -> ZoneConfig:
    raw = json.loads(Path(path).read_text())
    zones = tuple(
        Zone(z["zone_id"], z["zone_name"], float(z["lat"]), float(z["lon"]), z["area_type"])
        for z in raw["zones"]
    )
    return ZoneConfig(
        zones=zones,
        cell_size_deg=float(raw["cell_size_deg"]),
        max_radius_km=float(raw["max_radius_km"]),
        bbox=raw["bbox"],
        outer_zone=raw.get("outer_zone", "Outer Area"),
    )
