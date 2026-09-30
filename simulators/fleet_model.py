"""Agent-based world model of a ride-hailing fleet in Colombo.

Every vehicle is a small state machine that moves in simulated time:

    offline --shift starts--> idle --ride request--> enroute --pickup--> on_trip --drop-off--> idle
    idle --driver takes a break--> idle (parked, speed 0)       idle --shift ends--> offline

The model is the "real world" of the simulation. The streaming simulator turns
its state into telemetry events, and at every simulated midnight the model
writes a ground-truth summary (true kilometres driven, trips, revenue) that the
daily expense simulator uses to produce realistic fuel and maintenance costs.
"""
from __future__ import annotations

import math
import random
import uuid
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, time, timedelta

from fleet_common.registry import Vehicle
from fleet_common.simclock import fmt_sim
from fleet_common.zones import ZoneConfig, haversine_km

ROUTE_FACTOR = 1.3  # road distance divided by straight-line distance

# Fares in Sri Lankan rupees (LKR)
FARES = {
    "tuk": {"base": 100.0, "per_km": 90.0, "per_min": 3.0, "minimum": 150.0},
    "mini": {"base": 200.0, "per_km": 120.0, "per_min": 4.0, "minimum": 250.0},
    "car": {"base": 300.0, "per_km": 150.0, "per_min": 5.0, "minimum": 400.0},
}
BASE_REQUEST_RATE = {"tuk": 3.3, "mini": 2.7, "car": 2.25}  # ride requests per simulated hour
BASE_SPEED_KMH = {"tuk": 24.0, "mini": 30.0, "car": 32.0}  # free-flow road speed

# Demand multiplier per hour of day (0..23) for each type of area
HOUR_PROFILE = {
    "cbd": [0.15, 0.10, 0.05, 0.05, 0.10, 0.30, 0.70, 1.30, 1.70, 1.40, 1.10, 1.10,
            1.30, 1.20, 1.10, 1.20, 1.50, 1.80, 1.60, 1.10, 0.80, 0.60, 0.40, 0.25],
    "commercial": [0.20, 0.10, 0.05, 0.05, 0.10, 0.30, 0.70, 1.20, 1.40, 1.20, 1.10, 1.10,
                   1.20, 1.10, 1.10, 1.20, 1.40, 1.60, 1.50, 1.30, 1.00, 0.70, 0.50, 0.30],
    "residential": [0.20, 0.10, 0.05, 0.05, 0.15, 0.50, 1.20, 1.60, 1.40, 0.90, 0.70, 0.70,
                    0.80, 0.80, 0.80, 0.90, 1.10, 1.40, 1.60, 1.50, 1.20, 0.90, 0.60, 0.30],
    "admin": [0.10, 0.05, 0.05, 0.05, 0.10, 0.20, 0.50, 1.10, 1.50, 1.30, 1.10, 1.00,
              1.10, 1.00, 1.00, 1.10, 1.30, 1.30, 0.90, 0.60, 0.40, 0.30, 0.20, 0.10],
}
# How attractive each area type is as a trip destination (people go to work in
# the morning and home in the evening)
ATTRACTION = {
    "morning": {"cbd": 1.6, "commercial": 1.3, "admin": 1.3, "residential": 0.6},
    "midday": {"cbd": 1.1, "commercial": 1.2, "admin": 1.0, "residential": 0.9},
    "evening": {"cbd": 0.7, "commercial": 1.1, "admin": 0.6, "residential": 1.6},
    "night": {"cbd": 0.6, "commercial": 0.9, "admin": 0.4, "residential": 1.5},
}
PROFILE_DEMAND = {"idle_prone": 0.55, "degrading": 0.9}
BREAK_RATE_PER_HOUR = {"idle_prone": 0.35}
BREAK_MINUTES = {"idle_prone": (70, 160)}
DEFAULT_BREAK_RATE = 0.07
DEFAULT_BREAK_MINUTES = (15, 45)
FAST_FORWARD_LIMIT = timedelta(hours=2)


def day_part(hour: int) -> str:
    if 6 <= hour < 11:
        return "morning"
    if 11 <= hour < 16:
        return "midday"
    if 16 <= hour < 22:
        return "evening"
    return "night"


def traffic_factor(hour: int) -> float:
    """Road speed multiplier: slow in Colombo rush hours, fast at night."""
    if hour in (7, 8, 17, 18):
        return 0.55
    if hour in (9, 16, 19):
        return 0.75
    if 10 <= hour <= 15:
        return 0.85
    if hour >= 22 or hour < 6:
        return 1.25
    return 1.0


def surge_multiplier(vehicle_type: str, hour: int) -> float:
    if vehicle_type != "tuk" and (7 <= hour < 10 or 17 <= hour < 20):
        return 1.2
    return 1.0


def trip_fare(vehicle_type: str, km: float, minutes: float, hour: int) -> float:
    f = FARES[vehicle_type]
    fare = (f["base"] + f["per_km"] * km + f["per_min"] * minutes) * surge_multiplier(vehicle_type, hour)
    return float(round(max(f["minimum"], fare)))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def offset_point(lat: float, lon: float, distance_km: float, bearing: float) -> tuple[float, float]:
    b = math.radians(bearing)
    dlat = distance_km / 111.32 * math.cos(b)
    dlon = distance_km / (111.32 * math.cos(math.radians(lat))) * math.sin(b)
    return lat + dlat, lon + dlon


def coast_lon(lat: float) -> float:
    """Rough longitude of the Colombo coastline (the sea is to the west)."""
    return 79.845 + (6.95 - lat) * 0.13


def on_land(lat: float, lon: float) -> tuple[float, float]:
    """Keep simulated points inside the city area and out of the sea."""
    lat = min(max(lat, 6.835), 6.965)
    lon = min(max(lon, coast_lon(lat) + 0.003), 79.935)
    return lat, lon


def downtime_window(vehicle: Vehicle, sim_date: date, day_index: int) -> tuple[datetime, datetime] | None:
    """A degrading vehicle spends more hours in the garage every day."""
    if vehicle.profile != "degrading" or day_index < 1:
        return None
    hours = min(2.5 * day_index, 9.0)
    start = datetime.combine(sim_date, time(0)) + timedelta(hours=vehicle.shift_start_hour + 3)
    return start, start + timedelta(hours=hours)


@dataclass
class VehicleState:
    vehicle_id: str
    driver_id: str
    vehicle_type: str
    profile: str
    status: str
    lat: float
    lon: float
    odometer_km: float
    speed: float = 0.0
    heading: float = 0.0
    target_lat: float | None = None
    target_lon: float | None = None
    trip_id: str | None = None
    trip_seq: int = 0
    pickup_lat: float | None = None
    pickup_lon: float | None = None
    trip_started_at: datetime | None = None
    trip_km: float = 0.0
    break_until: datetime | None = None
    # ground-truth counters for the current simulated day
    day_km: float = 0.0
    day_trips: int = 0
    day_revenue: float = 0.0
    day_online_min: float = 0.0
    day_idle_min: float = 0.0

    def to_dict(self) -> dict:
        out = asdict(self)
        for key in ("trip_started_at", "break_until"):
            if out[key] is not None:
                out[key] = out[key].isoformat()
        return out

    @classmethod
    def from_dict(cls, data: dict) -> "VehicleState":
        known = {f.name for f in fields(cls)}
        data = {k: v for k, v in data.items() if k in known}
        for key in ("trip_started_at", "break_until"):
            if data.get(key):
                data[key] = datetime.fromisoformat(data[key])
        return cls(**data)


class FleetWorld:
    """The simulated fleet. Call :meth:`advance_to` once per real-time tick."""

    def __init__(self, registry: list[Vehicle], zones: ZoneConfig, sim_start: datetime,
                 origin_date: date, seed: int | None = None) -> None:
        self.registry = {v.vehicle_id: v for v in registry}
        self.zones = zones
        self.origin_date = origin_date  # first simulated day (day index 0)
        self.rng = random.Random(seed)
        self.sim_time = sim_start
        self.states = {v.vehicle_id: self._initial_state(v) for v in registry}

    # ------------------------------------------------------------------ setup
    def _initial_state(self, v: Vehicle) -> VehicleState:
        home = self.zones.by_name(v.home_zone)
        lat, lon = on_land(*offset_point(home.lat, home.lon, self.rng.uniform(0.0, 0.8), self.rng.uniform(0, 360)))
        start_km = {"tuk": (20000, 90000), "mini": (30000, 120000), "car": (40000, 160000)}[v.vehicle_type]
        return VehicleState(
            vehicle_id=v.vehicle_id,
            driver_id=v.driver_id,
            vehicle_type=v.vehicle_type,
            profile=v.profile,
            status="offline",
            lat=lat,
            lon=lon,
            odometer_km=round(self.rng.uniform(*start_km), 1),
        )

    def to_dict(self) -> dict:
        return {
            "sim_time": self.sim_time.isoformat(),
            "origin_date": self.origin_date.isoformat(),
            "vehicles": {vid: st.to_dict() for vid, st in self.states.items()},
        }

    def restore(self, data: dict) -> None:
        self.sim_time = datetime.fromisoformat(data["sim_time"])
        for vid, raw in data.get("vehicles", {}).items():
            if vid in self.registry:
                self.states[vid] = VehicleState.from_dict(raw)

    # ------------------------------------------------------------ main loop
    def advance_to(self, sim_now: datetime) -> tuple[list[dict], list[dict]]:
        """Move the world forward to ``sim_now``.

        Returns ``(events, finished_days)``. Events are plain dicts in the
        telemetry schema. ``finished_days`` holds one ground-truth summary for
        every simulated midnight that was crossed.
        """
        events: list[dict] = []
        finished: list[dict] = []
        if sim_now <= self.sim_time:
            return events, finished
        if sim_now - self.sim_time > FAST_FORWARD_LIMIT:
            # The simulator was paused for a long time without a clock re-base.
            # Skip the gap instead of replaying hours of history in one burst.
            if sim_now.date() != self.sim_time.date():
                finished.append(self._close_day(self.sim_time.date()))
            for st in self.states.values():
                self._go_offline(st)
            self.sim_time = sim_now
            return events, finished

        while self.sim_time < sim_now:
            next_midnight = datetime.combine(self.sim_time.date() + timedelta(days=1), time(0))
            step_end = min(sim_now, next_midnight)
            for st in self.states.values():
                events.extend(self._advance_vehicle(st, self.sim_time, step_end))
            self.sim_time = step_end
            if step_end == next_midnight:
                finished.append(self._close_day(next_midnight.date() - timedelta(days=1)))

        events.extend(self._pings(sim_now))
        return events, finished

    def status_counts(self) -> dict[str, int]:
        counts = {"offline": 0, "idle": 0, "enroute": 0, "on_trip": 0}
        for st in self.states.values():
            counts[st.status] += 1
        return counts

    # ---------------------------------------------------------- vehicle logic
    def _available(self, v: Vehicle, t: datetime) -> bool:
        if not v.on_shift(t.hour):
            return False
        window = downtime_window(v, t.date(), (t.date() - self.origin_date).days)
        return not (window and window[0] <= t < window[1])

    def _advance_vehicle(self, st: VehicleState, t0: datetime, t1: datetime) -> list[dict]:
        v = self.registry[st.vehicle_id]
        events: list[dict] = []
        t = t0
        guard = 0
        while t < t1 and guard < 1000:  # guard only protects against an endless loop
            guard += 1
            if st.status == "offline":
                if self._available(v, t):
                    st.status = "idle"
                    self._choose_reposition(st, t)
                    continue
                t = t1
            elif st.status == "idle":
                t = self._advance_idle(st, v, t, t1)
            else:
                t, event = self._advance_moving(st, v, t, t1)
                if event:
                    events.append(event)
        return events

    def _advance_idle(self, st: VehicleState, v: Vehicle, t: datetime, t1: datetime) -> datetime:
        if not self._available(v, t) and st.break_until is None:
            self._go_offline(st)
            return t
        next_hour = t.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        seg_end = min(t1, next_hour)
        if st.break_until is not None:  # parked on a break
            until = min(seg_end, st.break_until)
            self._idle_move(st, t, until, parked=True)
            if until >= st.break_until:
                st.break_until = None
            return until

        zone = self.zones.by_name(self._zone_name(st))
        demand = HOUR_PROFILE[zone.area_type][t.hour] if zone else 0.3
        request_rate = BASE_REQUEST_RATE[v.vehicle_type] * demand * PROFILE_DEMAND.get(v.profile, 1.0)
        break_rate = BREAK_RATE_PER_HOUR.get(v.profile, DEFAULT_BREAK_RATE)
        total = request_rate + break_rate
        wait_h = self.rng.expovariate(total) if total > 0 else 1e9
        event_t = t + timedelta(hours=wait_h)
        if event_t >= seg_end:
            self._idle_move(st, t, seg_end)
            return seg_end
        self._idle_move(st, t, event_t)
        if self.rng.random() < request_rate / total:
            self._dispatch(st, event_t)
        else:
            lo, hi = BREAK_MINUTES.get(v.profile, DEFAULT_BREAK_MINUTES)
            st.break_until = event_t + timedelta(minutes=self.rng.uniform(lo, hi))
            st.target_lat = st.target_lon = None
        return event_t

    def _advance_moving(self, st: VehicleState, v: Vehicle, t: datetime, t1: datetime):
        """Drive towards the pickup (enroute) or the destination (on_trip)."""
        speed = BASE_SPEED_KMH[v.vehicle_type] * traffic_factor(t.hour) * self.rng.uniform(0.85, 1.15)
        st.speed = min(speed, 60.0)
        straight_km = haversine_km(st.lat, st.lon, st.target_lat, st.target_lon)
        needed_h = straight_km * ROUTE_FACTOR / st.speed
        available_h = (t1 - t).total_seconds() / 3600.0
        minutes = min(needed_h, available_h) * 60.0
        st.day_online_min += minutes
        if needed_h > available_h:
            self._move_towards(st, st.speed * available_h / ROUTE_FACTOR)
            return t1, None

        self._move_towards(st, straight_km)
        arrive = t + timedelta(hours=needed_h)
        if st.status == "enroute":  # passenger picked up
            st.status = "on_trip"
            st.pickup_lat, st.pickup_lon = st.lat, st.lon
            st.trip_started_at = arrive
            st.trip_km = 0.0
            st.target_lat, st.target_lon = self._choose_destination(st, arrive)
            return arrive, self._event(st, "trip_start", arrive, fare=0.0)

        # on_trip -> passenger dropped off
        duration_min = (arrive - st.trip_started_at).total_seconds() / 60.0
        fare = trip_fare(v.vehicle_type, st.trip_km, duration_min, arrive.hour)
        event = self._event(
            st,
            "trip_end",
            arrive,
            fare=fare,
            trip_distance_km=round(st.trip_km, 3),
            trip_duration_min=round(duration_min, 1),
            pickup_lat=round(st.pickup_lat, 6),
            pickup_lon=round(st.pickup_lon, 6),
        )
        st.day_trips += 1
        st.day_revenue += fare
        st.status = "idle"
        st.speed = 0.0
        st.trip_id = None
        st.pickup_lat = st.pickup_lon = None
        st.trip_started_at = None
        st.trip_km = 0.0
        self._choose_reposition(st, arrive)
        return arrive, event

    # -------------------------------------------------------------- helpers
    def _dispatch(self, st: VehicleState, t: datetime) -> None:
        st.trip_seq += 1
        st.trip_id = f"T{t:%Y%m%d}-{st.vehicle_id}-{st.trip_seq:03d}"
        st.status = "enroute"
        st.target_lat, st.target_lon = on_land(*offset_point(
            st.lat, st.lon, self.rng.uniform(0.3, 1.5), self.rng.uniform(0, 360)
        ))

    def _choose_destination(self, st: VehicleState, t: datetime) -> tuple[float, float]:
        """Most city trips are 2 to 9 km (straight line). Zones that match the
        wanted distance and are attractive at this time of day are preferred."""
        part = day_part(t.hour)
        wanted_km = min(10.0, max(2.0, self.rng.lognormvariate(math.log(4.5), 0.45)))
        weights = []
        for zone in self.zones.zones:
            d = haversine_km(st.lat, st.lon, zone.lat, zone.lon)
            weights.append(ATTRACTION[part][zone.area_type] * math.exp(-(((d - wanted_km) / 2.0) ** 2)) + 1e-6)
        zone = self.rng.choices(self.zones.zones, weights=weights, k=1)[0]
        for _ in range(5):
            lat, lon = on_land(*offset_point(zone.lat, zone.lon, self.rng.uniform(0.0, 1.2), self.rng.uniform(0, 360)))
            if haversine_km(st.lat, st.lon, lat, lon) >= 1.5:
                return lat, lon
        return on_land(*offset_point(st.lat, st.lon, wanted_km, self.rng.uniform(0, 360)))

    def _choose_reposition(self, st: VehicleState, t: datetime) -> None:
        """Idle drivers often cruise to a busier zone (dead kilometres)."""
        chance = 0.1 if st.profile == "idle_prone" else 0.5
        st.target_lat = st.target_lon = None
        if self.rng.random() >= chance:
            return
        best, best_demand = None, -1.0
        for zone in self.zones.zones:
            if haversine_km(st.lat, st.lon, zone.lat, zone.lon) <= 5.0:
                demand = HOUR_PROFILE[zone.area_type][t.hour]
                if demand > best_demand:
                    best, best_demand = zone, demand
        if best is not None:
            st.target_lat, st.target_lon = on_land(
                *offset_point(best.lat, best.lon, self.rng.uniform(0, 0.5), self.rng.uniform(0, 360))
            )

    def _idle_move(self, st: VehicleState, t0: datetime, t1: datetime, parked: bool = False) -> None:
        minutes = (t1 - t0).total_seconds() / 60.0
        st.day_online_min += minutes
        st.day_idle_min += minutes
        if parked or st.target_lat is None:
            st.speed = 0.0
            return
        st.speed = 12.0 * traffic_factor(t0.hour)
        remaining = haversine_km(st.lat, st.lon, st.target_lat, st.target_lon)
        step = st.speed * minutes / 60.0 / ROUTE_FACTOR
        self._move_towards(st, min(step, remaining))
        if step >= remaining:
            st.target_lat = st.target_lon = None
            st.speed = 0.0

    def _move_towards(self, st: VehicleState, straight_km: float) -> None:
        total = haversine_km(st.lat, st.lon, st.target_lat, st.target_lon)
        if total <= 1e-9 or straight_km <= 0:
            return
        frac = min(1.0, straight_km / total)
        st.heading = bearing_deg(st.lat, st.lon, st.target_lat, st.target_lon)
        st.lat += (st.target_lat - st.lat) * frac
        st.lon += (st.target_lon - st.lon) * frac
        road_km = straight_km * ROUTE_FACTOR
        st.odometer_km += road_km
        st.day_km += road_km
        if st.status == "on_trip":
            st.trip_km += road_km

    def _go_offline(self, st: VehicleState) -> None:
        st.status = "offline"
        st.speed = 0.0
        st.break_until = None
        st.target_lat = st.target_lon = None
        st.trip_id = None
        st.trip_started_at = None

    def _zone_name(self, st: VehicleState) -> str:
        name = self.zones.nearest_zone(st.lat, st.lon)
        if name == self.zones.outer_zone:
            # treat the outer area like the nearest real zone for demand purposes
            name = min(self.zones.zones, key=lambda z: haversine_km(st.lat, st.lon, z.lat, z.lon)).zone_name
        return name

    def _running_fare(self, st: VehicleState, t: datetime) -> float:
        if st.status != "on_trip" or st.trip_started_at is None:
            return 0.0
        minutes = max(0.0, (t - st.trip_started_at).total_seconds() / 60.0)
        f = FARES[st.vehicle_type]
        return float(round(f["base"] + f["per_km"] * st.trip_km + f["per_min"] * minutes))

    def _event(self, st: VehicleState, event_type: str, ts: datetime, fare: float, **extra) -> dict:
        event = {
            "schema_version": 1,
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_ts": fmt_sim(ts),
            "vehicle_id": st.vehicle_id,
            "driver_id": st.driver_id,
            "trip_id": st.trip_id,
            "status": st.status,
            "lat": round(st.lat, 6),
            "lon": round(st.lon, 6),
            "speed": round(st.speed, 1),
            "heading": round(st.heading),
            "odometer_km": round(st.odometer_km, 3),
            "fare": fare,
        }
        event.update(extra)
        return event

    def _pings(self, sim_now: datetime) -> list[dict]:
        return [
            self._event(st, "ping", sim_now, fare=self._running_fare(st, sim_now))
            for st in self.states.values()
            if st.status != "offline"
        ]

    def _close_day(self, sim_date: date) -> dict:
        summary = {
            "sim_date": sim_date.isoformat(),
            "vehicles": {
                vid: {
                    "km": round(st.day_km, 3),
                    "trips": st.day_trips,
                    "revenue": round(st.day_revenue, 2),
                    "online_min": round(st.day_online_min, 1),
                    "idle_min": round(st.day_idle_min, 1),
                }
                for vid, st in self.states.items()
            },
        }
        for st in self.states.values():
            st.day_km = 0.0
            st.day_trips = 0
            st.day_revenue = 0.0
            st.day_online_min = 0.0
            st.day_idle_min = 0.0
            st.trip_seq = 0
        return summary
