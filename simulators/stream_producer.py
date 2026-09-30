"""Streaming source: publishes fleet telemetry to Kafka in compressed real time.

Every tick (1 real second by default = 2 simulated minutes) the fleet world
model moves forward and each online vehicle sends one GPS ping. Trip start and
trip end events are sent at the moment they happen.

Messages are keyed by ``vehicle_id`` so all events of one vehicle go to the same
partition and keep their order. The producer uses ``acks=all`` with the
idempotent producer switched on, so broker retries never create duplicates.

To make the pipeline prove its cleaning logic, some problems are added on
purpose (rates are set in ``.env``):

* duplicates      - an event is sent twice, like a mobile app retry
* invalid events  - broken JSON, missing ids, (0,0) GPS, negative speed ...
* network outages - a vehicle loses signal for 20-60 simulated minutes, then
                    sends everything at once, so those events arrive late
"""
from __future__ import annotations

import json
import os
import random
import signal
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from fleet_common.db import connect
from fleet_common.logs import get_logger
from fleet_common.registry import load_registry
from fleet_common.settings import get_settings
from fleet_common.simclock import ensure_clock, fmt_sim, heartbeat
from fleet_common.zones import load_zones
from fleet_model import FleetWorld

SERVICE = "stream-producer"
log = get_logger(SERVICE, "ingestion")

SENT = Counter("producer_events_sent_total", "Telemetry events acknowledged by Kafka", ["event_type"])
PARTITION_SENT = Counter("producer_partition_events_total", "Acknowledged events per Kafka partition", ["partition"])
ATTEMPTS = Counter("producer_send_attempts_total", "Events handed to the Kafka client")
SEND_ERRORS = Counter("producer_send_errors_total", "Events that Kafka failed to deliver", ["reason"])
INJECTED = Counter("producer_injected_faults_total", "Data problems injected on purpose", ["kind"])
DELIVERY_LATENCY = Histogram(
    "producer_delivery_latency_seconds",
    "Time from produce() to broker acknowledgement",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
LAST_SUCCESS = Gauge("producer_last_success_timestamp_seconds", "Unix time of the last acknowledged event")
SIM_TIME = Gauge("sim_clock_time_seconds", "Simulated time as unix seconds (local wall clock)")
FLEET = Gauge("sim_fleet_vehicles", "Vehicles per state inside the simulator (ground truth)", ["status"])
BUFFERED = Gauge("producer_outage_buffered_events", "Events held back by simulated network outages")

CORRUPTIONS = (
    "truncated_json",
    "missing_vehicle_id",
    "gps_null_island",
    "negative_speed",
    "bad_status",
    "bad_timestamp",
)


class FaultInjector:
    """Adds realistic data problems so the pipeline has something to clean."""

    def __init__(self, settings, rng: random.Random) -> None:
        self.s = settings
        self.rng = rng
        self.outage_until: dict[str, datetime] = {}
        self.buffers: dict[str, list[dict]] = {}

    def maybe_start_outages(self, world: FleetWorld, sim_now: datetime, dt_hours: float) -> None:
        for vid, st in world.states.items():
            if st.status == "offline" or vid in self.outage_until:
                continue
            if self.rng.random() < self.s.outage_rate_per_sim_hour * dt_hours:
                minutes = self.rng.uniform(20, 60)
                self.outage_until[vid] = sim_now + timedelta(minutes=minutes)
                self.buffers[vid] = []
                INJECTED.labels("network_outage").inc()
                log.info(
                    "network_outage_started",
                    extra={"vehicle_id": vid, "sim_minutes": round(minutes, 1), "sim_time": fmt_sim(sim_now)},
                )

    def route(self, event: dict) -> list[dict]:
        """Events of a vehicle in an outage are held back instead of sent."""
        vid = event["vehicle_id"]
        if vid in self.outage_until:
            self.buffers[vid].append(event)
            return []
        return [event]

    def release_finished_outages(self, sim_now: datetime) -> list[dict]:
        released: list[dict] = []
        for vid, until in list(self.outage_until.items()):
            if sim_now < until:
                continue
            buffered = self.buffers.pop(vid, [])
            del self.outage_until[vid]
            limit = timedelta(minutes=self.s.watermark_minutes)
            late = sum(1 for e in buffered if sim_now - datetime.fromisoformat(e["event_ts"]) > limit)
            INJECTED.labels("late_event").inc(late)
            log.info(
                "network_outage_ended",
                extra={"vehicle_id": vid, "flushed_events": len(buffered), "late_events": late},
            )
            released.extend(buffered)
        return released

    def buffered_count(self) -> int:
        return sum(len(b) for b in self.buffers.values())

    def payload(self, event: dict) -> tuple[bytes, str | None]:
        """Serialise an event. A small share of pings is corrupted on purpose."""
        if event["event_type"] == "ping" and self.rng.random() < self.s.invalid_event_rate:
            kind = self.rng.choice(CORRUPTIONS)
            bad = dict(event)
            if kind == "truncated_json":
                text = json.dumps(bad)
                return text[: len(text) // 2].encode(), kind
            if kind == "missing_vehicle_id":
                bad.pop("vehicle_id", None)
            elif kind == "gps_null_island":
                bad["lat"], bad["lon"] = 0.0, 0.0
            elif kind == "negative_speed":
                bad["speed"] = -round(self.rng.uniform(5, 50), 1)
            elif kind == "bad_status":
                bad["status"] = "parked"
            elif kind == "bad_timestamp":
                bad["event_ts"] = "31/09/2026 25:61"
            return json.dumps(bad).encode(), kind
        return json.dumps(event, separators=(",", ":")).encode(), None

    def is_duplicate(self) -> bool:
        return self.rng.random() < self.s.duplicate_event_rate


class StateStore:
    """Simulator-internal files: world state and daily ground truth."""

    def __init__(self, base: Path) -> None:
        self.base = base
        (base / "ground_truth").mkdir(parents=True, exist_ok=True)

    def _atomic_write(self, path: Path, data: dict) -> None:
        tmp = path.with_name(f".{path.name}.tmp")
        tmp.write_text(json.dumps(data))
        os.replace(tmp, path)

    def load_world(self) -> dict | None:
        path = self.base / "fleet_state.json"
        return json.loads(path.read_text()) if path.exists() else None

    def save_world(self, data: dict) -> None:
        self._atomic_write(self.base / "fleet_state.json", data)

    def write_ground_truth(self, summary: dict) -> None:
        self._atomic_write(self.base / "ground_truth" / f"{summary['sim_date']}.json", summary)


def build_producer(settings) -> Producer:
    # logger=log routes librdkafka's own messages through our JSON formatter
    return Producer(
        {
            "logger": log,
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": "fleet-telemetry-simulator",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.num.messages": 1000,
            "message.send.max.retries": 10,
            "retry.backoff.ms": 200,
            "delivery.timeout.ms": 30000,
            "queue.buffering.max.messages": 100000,
        }
    )


def wait_for_topic(settings, timeout: float = 600.0) -> int:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            topic = admin.list_topics(timeout=5).topics.get(settings.topic_telemetry)
            if topic is not None and topic.error is None and topic.partitions:
                log.info("kafka_topic_ready", extra={"topic": settings.topic_telemetry, "partitions": len(topic.partitions)})
                return len(topic.partitions)
            log.info("waiting_for_topic", extra={"topic": settings.topic_telemetry})
        except KafkaException as exc:
            log.warning("kafka_not_ready", extra={"error": str(exc)})
        time.sleep(3)
    raise RuntimeError(f"topic {settings.topic_telemetry} not available")


def on_delivery(err, msg, event_type: str) -> None:
    if err is not None:
        SEND_ERRORS.labels(err.name()).inc()
        log.error("kafka_delivery_failed", extra={"error": str(err), "topic": msg.topic(), "event_type": event_type})
        return
    SENT.labels(event_type).inc()
    PARTITION_SENT.labels(str(msg.partition())).inc()
    LAST_SUCCESS.set(time.time())
    latency = msg.latency()
    if latency is not None:
        DELIVERY_LATENCY.observe(latency)


def produce_one(producer: Producer, topic: str, event: dict, value: bytes) -> None:
    event_type = event["event_type"]
    headers = [
        ("trace_id", event["event_id"].encode()),
        ("event_type", event_type.encode()),
        ("schema", b"telemetry.v1"),
        ("source", b"fleet-simulator"),
    ]
    while True:
        try:
            ATTEMPTS.inc()
            producer.produce(
                topic,
                key=event["vehicle_id"].encode(),
                value=value,
                headers=headers,
                callback=lambda err, msg, et=event_type: on_delivery(err, msg, et),
            )
            return
        except BufferError:
            producer.poll(0.5)  # local queue is full, wait for acknowledgements
        except KafkaException as exc:
            SEND_ERRORS.labels("produce_exception").inc()
            log.error("kafka_produce_failed", extra={"error": str(exc), "event_id": event["event_id"]})
            return


def send_events(producer: Producer, topic: str, events: list[dict], faults: FaultInjector) -> int:
    count = 0
    for event in events:
        value, corruption = faults.payload(event)
        if corruption:
            INJECTED.labels(f"invalid_{corruption}").inc()
        copies = 2 if faults.is_duplicate() else 1
        if copies == 2:
            INJECTED.labels("duplicate").inc()
        for _ in range(copies):
            produce_one(producer, topic, event, value)
            count += 1
    return count


def sim_epoch(ts: datetime) -> float:
    return ts.replace(tzinfo=timezone.utc).timestamp()


def main() -> None:
    s = get_settings()
    start_http_server(s.metrics_port)
    conn = connect(s, logger=log)
    # Re-base only after a long stop of the whole stack (5 real minutes). A short
    # restart of this container alone must not move the shared clock backwards,
    # because the other services keep reading it while we are away.
    clock = ensure_clock(
        conn,
        s.sim_start_dt,
        s.sim_speedup,
        resume_gap_seconds=300.0,
        logger=log,
    )
    log.info(
        "sim_clock_ready",
        extra={
            "sim_now": fmt_sim(clock.sim_now()),
            "speedup": clock.speedup,
            "real_seconds_per_sim_day": round(clock.real_seconds_per_sim_day),
        },
    )

    registry = load_registry(s.registry_file)
    zones = load_zones(s.zones_file)
    store = StateStore(s.sim_state_dir)
    world = FleetWorld(registry, zones, clock.sim_now(), s.sim_start_date, seed=s.sim_seed + int(time.time()))
    saved = store.load_world()
    if saved and datetime.fromisoformat(saved["sim_time"]) > clock.sim_now() + timedelta(hours=1):
        # The clock row was reset but the data volume was kept: the old state
        # belongs to a different timeline, so start the fleet fresh.
        log.warning("world_state_discarded", extra={"world_sim_time": saved["sim_time"]})
        saved = None
    if saved:
        world.restore(saved)
        log.info("world_state_restored", extra={"world_sim_time": saved["sim_time"]})
    faults = FaultInjector(s, random.Random(s.sim_seed * 7 + int(time.time())))

    wait_for_topic(s)
    producer = build_producer(s)

    stop = {"flag": False}

    def _stop(signum, _frame):
        log.info("stop_signal_received", extra={"signal": signum})
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    tick, sent_since_log = 0, 0
    last_sim = clock.sim_now()
    next_tick = time.monotonic()
    while not stop["flag"]:
        sim_now = clock.sim_now()
        events, finished_days = world.advance_to(sim_now)
        for summary in finished_days:
            store.write_ground_truth(summary)
            vehicles = summary["vehicles"].values()
            log.info(
                "sim_day_closed",
                extra={
                    "sim_date": summary["sim_date"],
                    "trips": sum(v["trips"] for v in vehicles),
                    "revenue_lkr": round(sum(v["revenue"] for v in vehicles)),
                    "km": round(sum(v["km"] for v in vehicles), 1),
                },
            )

        dt_hours = max(0.0, (sim_now - last_sim).total_seconds() / 3600.0)
        last_sim = sim_now
        faults.maybe_start_outages(world, sim_now, dt_hours)
        outgoing = faults.release_finished_outages(sim_now)
        for event in events:
            outgoing.extend(faults.route(event))
        sent_since_log += send_events(producer, s.topic_telemetry, outgoing, faults)
        producer.poll(0)

        SIM_TIME.set(sim_epoch(sim_now))
        for status, n in world.status_counts().items():
            FLEET.labels(status).set(n)
        BUFFERED.set(faults.buffered_count())

        # Saved every tick so a restart resumes from the last second, never
        # re-simulating minutes that were already sent (no reused trip ids).
        store.save_world(world.to_dict())
        tick += 1
        if tick % 10 == 0:
            try:
                heartbeat(conn)
            except Exception as exc:  # PostgreSQL restarted: reconnect and carry on
                log.warning("heartbeat_failed", extra={"error": str(exc)})
                conn = connect(s, logger=log)
            log.info(
                "telemetry_sent",
                extra={
                    "sim_time": fmt_sim(sim_now),
                    "events": sent_since_log,
                    "ticks": 10,
                    "fleet": world.status_counts(),
                    "buffered_by_outages": faults.buffered_count(),
                    "kafka_queue": len(producer),
                },
            )
            sent_since_log = 0

        next_tick += s.tick_seconds
        if next_tick - time.monotonic() <= 0:
            next_tick = time.monotonic()  # we fell behind, do not try to catch up in a burst
        # Wait for the next tick inside poll() so delivery callbacks run as soon as
        # Kafka acknowledges (this keeps the delivery-latency metric accurate).
        while (remaining := next_tick - time.monotonic()) > 0:
            producer.poll(remaining)

    log.info("shutting_down", extra={"undelivered": len(producer)})
    producer.flush(10)
    store.save_world(world.to_dict())
    try:
        heartbeat(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
