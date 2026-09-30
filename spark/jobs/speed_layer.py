"""Speed layer: Spark Structured Streaming job (Lambda architecture).

One Spark application runs four streaming queries on the Kafka topic
``fleet.telemetry.raw``. Each query has its own checkpoint, so each one
restarts exactly where it stopped.

    raw_archive    Kafka -> Parquet master dataset (append only, partitioned by
                   event_date). This is the immutable input of the batch layer.
    zone_hourly    validate -> watermark -> de-duplicate -> zone enrichment ->
                   1-hour tumbling windows per zone -> PostgreSQL upsert
    vehicle_daily  same cleaning -> 1-day windows per vehicle (revenue and
                   kilometres so far) -> PostgreSQL upsert
    vehicle_state  latest position/status per vehicle, idle-streak tracking,
                   idle alerts, dead letters (PostgreSQL + topic fleet.telemetry.dlq)

Sink writes are idempotent: every micro-batch first claims its
(query_name, batch_id) in ops.speed_batches inside the same PostgreSQL
transaction as the data. A batch replayed after a crash finds its id already
there and is skipped, which gives exactly-once results in PostgreSQL. Dead
letters are sent to Kafka only after that commit, so a replay never repeats them.

Late events: the watermark is WATERMARK_MINUTES (10 simulated minutes). Events
that arrive later than that are dropped by the stateful operators and counted
in speed_rows_dropped_by_watermark_total. The batch layer still sees them in
the master dataset and corrects the numbers for that day.
"""
from __future__ import annotations

import json
import signal
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from fleet_common.logs import get_logger
from fleet_common.settings import get_settings
from fleet_common.zones import load_zones
from fleet_transforms import (
    enrich_zones,
    kafka_to_raw,
    parse_telemetry,
    validate_events,
    vehicle_metric_aggs,
    with_event_date,
    zone_grid_df,
    zone_metric_aggs,
)
from speed_state import merge_vehicle_state, summarise_vehicles

SERVICE = "spark-speed-layer"
log = get_logger(SERVICE, "processing")
METRICS_PORT = 9108
# Small-footprint JVM for an 8 GB laptop: serial GC that gives memory back
# (heap free ratios), capped metaspace, code cache, direct memory and thread
# stacks, and only the C1 JIT compiler (C2 threads cost minutes of CPU).
JVM_OPTIONS = (
    "-Duser.timezone=UTC -XX:+UseSerialGC -XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=30 "
    "-XX:MaxMetaspaceSize=192m -XX:ReservedCodeCacheSize=64m -XX:MaxDirectMemorySize=128m -Xss512k "
    "-XX:TieredStopAtLevel=1 -Dlog4j.configurationFile=/opt/fleet/spark/conf/log4j2.properties"
)

# --- Prometheus metrics ------------------------------------------------------
Q_INPUT_ROWS = Counter("speed_query_input_rows_total", "Rows read from Kafka per streaming query", ["query"])
Q_INPUT_RATE = Gauge("speed_query_input_rows_per_second", "Input rate of the last micro-batch", ["query"])
Q_PROCESS_RATE = Gauge("speed_query_processed_rows_per_second", "Processing rate of the last micro-batch", ["query"])
Q_BATCH_SECONDS = Gauge("speed_query_batch_duration_seconds", "Trigger execution time of the last micro-batch", ["query"])
Q_BATCH_ID = Gauge("speed_query_last_batch_id", "Id of the last finished micro-batch", ["query"])
Q_LAST_PROGRESS = Gauge("speed_query_last_progress_timestamp_seconds", "Unix time of the last progress event", ["query"])
Q_ACTIVE = Gauge("speed_query_active", "1 while the streaming query is running", ["query"])
Q_STATE_ROWS = Gauge("speed_query_state_rows", "Rows kept in the state store", ["query"])
Q_DROPPED = Counter("speed_rows_dropped_by_watermark_total", "Late rows dropped by the watermark", ["query"])
Q_BEHIND = Gauge("speed_kafka_offsets_behind_latest", "Max Kafka offsets behind the latest offset (lag)", ["query"])
Q_WATERMARK = Gauge("speed_query_watermark_seconds", "Event-time watermark (simulated time as unix seconds)", ["query"])
SINK_ROWS = Counter("speed_sink_rows_written_total", "Rows upserted into PostgreSQL", ["table"])
SINK_SECONDS = Histogram("speed_sink_write_seconds", "Time to process and write one micro-batch", ["query"])
SINK_SKIPPED = Counter("speed_sink_replayed_batches_total", "Micro-batches skipped because they were already committed", ["query"])
SINK_LAST_COMMIT = Gauge("speed_last_sink_commit_timestamp_seconds", "Unix time of the last PostgreSQL commit", ["query"])
E2E_LATENCY = Gauge("speed_e2e_latency_seconds", "Kafka append time of the newest record to PostgreSQL commit", ["query"])
E2E_MAX_LATENCY = Gauge("speed_e2e_max_latency_seconds", "Kafka append time of the oldest record to PostgreSQL commit", ["query"])
VALID_EVENTS = Counter("speed_valid_events_total", "Valid events seen by the vehicle_state query")
DEAD_LETTERS = Counter("speed_dead_letter_events_total", "Invalid events found by validation", ["reason"])
IDLE_ALERTS = Counter("speed_idle_alerts_total", "Idle alerts opened or resolved", ["action"])
UP = Gauge("speed_layer_up", "1 while the speed layer driver is running")


def epoch(ts: datetime) -> float:
    """Spark returns naive datetimes in UTC (the container and session run in UTC)."""
    return ts.replace(tzinfo=timezone.utc).timestamp()


class MetricsListener(StreamingQueryListener):
    """Turns Spark progress events into Prometheus metrics and JSON logs."""

    def __init__(self) -> None:
        super().__init__()
        self.names: dict[str, str] = {}
        self.batches: dict[str, int] = {}

    def onQueryStarted(self, event) -> None:
        self.names[str(event.id)] = event.name
        Q_ACTIVE.labels(event.name).set(1)
        log.info("query_started", extra={"query": event.name, "run_id": str(event.runId)})

    def onQueryProgress(self, event) -> None:
        p = json.loads(event.progress.json)
        name = p.get("name") or "unknown"
        Q_INPUT_ROWS.labels(name).inc(p.get("numInputRows", 0))
        Q_INPUT_RATE.labels(name).set(p.get("inputRowsPerSecond") or 0.0)
        Q_PROCESS_RATE.labels(name).set(p.get("processedRowsPerSecond") or 0.0)
        Q_BATCH_SECONDS.labels(name).set((p.get("durationMs", {}).get("triggerExecution") or 0) / 1000.0)
        Q_BATCH_ID.labels(name).set(p.get("batchId", 0))
        Q_LAST_PROGRESS.labels(name).set(time.time())
        operators = p.get("stateOperators", [])
        Q_STATE_ROWS.labels(name).set(sum(op.get("numRowsTotal", 0) for op in operators))
        dropped = sum(op.get("numRowsDroppedByWatermark", 0) for op in operators)
        Q_DROPPED.labels(name).inc(dropped)
        behind = 0.0
        for source in p.get("sources", []):
            value = (source.get("metrics") or {}).get("maxOffsetsBehindLatest")
            if value not in (None, ""):
                behind = max(behind, float(value))
        Q_BEHIND.labels(name).set(behind)
        watermark = (p.get("eventTime") or {}).get("watermark")
        if watermark:
            Q_WATERMARK.labels(name).set(datetime.fromisoformat(watermark.replace("Z", "+00:00")).timestamp())
        count = self.batches.get(name, 0) + 1
        self.batches[name] = count
        if dropped or count % 12 == 0:  # about once a minute per query
            log.info(
                "query_progress",
                extra={
                    "query": name,
                    "batch_id": p.get("batchId"),
                    "input_rows": p.get("numInputRows"),
                    "input_rows_per_sec": round(p.get("inputRowsPerSecond") or 0.0, 1),
                    "trigger_ms": p.get("durationMs", {}).get("triggerExecution"),
                    "watermark": watermark,
                    "dropped_by_watermark": dropped,
                    "offsets_behind": behind,
                },
            )

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        name = self.names.get(str(event.id), "unknown")
        Q_ACTIVE.labels(name).set(0)
        if event.exception:
            log.error("query_terminated_with_error", extra={"query": name, "error": event.exception[:2000]})
        else:
            log.info("query_stopped", extra={"query": name})


class PostgresSink:
    """foreachBatch writers. Every write is one transaction with a batch claim."""

    def __init__(self, settings, zones) -> None:
        self.s = settings
        self.zones = zones
        self.grid = dict(zones.build_grid())
        self.pool = ThreadedConnectionPool(1, 4, settings.pg_dsn, application_name="speed-layer")
        self.offline_gap = timedelta(minutes=settings.offline_after_minutes)
        self.dlq = Producer({
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": "speed-layer-dlq",
            "acks": "all",
            "enable.idempotence": True,
            "logger": log,
        })

    def publish_dead_letters(self, dead: list, batch_id: int) -> None:
        """Forward invalid events to the dead-letter topic for inspection or replay."""
        for d in dead:
            value = {
                "errors": list(d["dq_errors"]),
                "raw_value": d["raw_value"],
                "source_partition": d["kafka_partition"],
                "source_offset": d["kafka_offset"],
                "speed_batch_id": batch_id,
            }
            self.dlq.produce(self.s.topic_dlq, key=(d["kafka_key"] or "").encode(), value=json.dumps(value).encode())
        remaining = self.dlq.flush(10)
        if remaining:
            log.error("dead_letters_not_delivered", extra={"batch_id": batch_id, "remaining": remaining})

    def zone_of(self, lat: float, lon: float) -> str:
        return self.zones.zone_via_grid(lat, lon, self.grid)

    @contextmanager
    def transaction(self):
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

    @staticmethod
    def claim(cur, query: str, batch_id: int, rows: int, max_event_ts=None) -> bool:
        cur.execute(
            "INSERT INTO ops.speed_batches (query_name, batch_id, rows_in, max_event_ts) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (query, batch_id, rows, max_event_ts),
        )
        return cur.rowcount == 1

    def _finish(self, query: str, started: float, newest_kafka_ts, oldest_kafka_ts=None) -> None:
        now = time.time()
        SINK_SECONDS.labels(query).observe(now - started)
        SINK_LAST_COMMIT.labels(query).set(now)
        if newest_kafka_ts is not None:
            E2E_LATENCY.labels(query).set(max(0.0, now - epoch(newest_kafka_ts)))
        if oldest_kafka_ts is not None:
            E2E_MAX_LATENCY.labels(query).set(max(0.0, now - epoch(oldest_kafka_ts)))

    # ------------------------------------------------------------ zone_hourly
    def write_zone_hourly(self, batch_df: DataFrame, batch_id: int) -> None:
        started = time.time()
        rows = batch_df.select(
            "zone",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "pings",
            "active_vehicles",
            "on_trip_pings",
            "enroute_pings",
            "idle_pings",
            "trips_completed",
            "earnings_lkr",
            "avg_speed_kmh",
            "max_kafka_ts",
        ).collect()
        if not rows:
            return
        with self.transaction() as cur:
            if not self.claim(cur, "zone_hourly", batch_id, len(rows)):
                SINK_SKIPPED.labels("zone_hourly").inc()
                return
            execute_values(
                cur,
                """
                INSERT INTO speed.zone_hourly (zone, window_start, window_end, pings, active_vehicles,
                    on_trip_pings, enroute_pings, idle_pings, trips_completed, earnings_lkr,
                    avg_speed_kmh, last_batch_id)
                VALUES %s
                ON CONFLICT (zone, window_start) DO UPDATE SET
                    window_end = EXCLUDED.window_end, pings = EXCLUDED.pings,
                    active_vehicles = EXCLUDED.active_vehicles, on_trip_pings = EXCLUDED.on_trip_pings,
                    enroute_pings = EXCLUDED.enroute_pings, idle_pings = EXCLUDED.idle_pings,
                    trips_completed = EXCLUDED.trips_completed, earnings_lkr = EXCLUDED.earnings_lkr,
                    avg_speed_kmh = EXCLUDED.avg_speed_kmh, last_batch_id = EXCLUDED.last_batch_id,
                    updated_at = now()
                """,
                [(*r[:11], batch_id) for r in rows],
            )
        SINK_ROWS.labels("speed.zone_hourly").inc(len(rows))
        self._finish("zone_hourly", started, max(r["max_kafka_ts"] for r in rows))

    # ---------------------------------------------------------- vehicle_daily
    def write_vehicle_daily(self, batch_df: DataFrame, batch_id: int) -> None:
        started = time.time()
        rows = batch_df.select(
            "vehicle_id",
            F.to_date(F.col("window.start")).alias("sim_date"),
            "driver_id",
            "pings",
            "on_trip_pings",
            "enroute_pings",
            "idle_pings",
            "trips_completed",
            "revenue_lkr",
            "odo_min",
            "odo_max",
            "last_event_ts",
            "max_kafka_ts",
        ).collect()
        if not rows:
            return
        values = [
            (*r[:11], round(max(0.0, (r["odo_max"] or 0.0) - (r["odo_min"] or 0.0)), 3), r["last_event_ts"], batch_id)
            for r in rows
        ]
        with self.transaction() as cur:
            if not self.claim(cur, "vehicle_daily", batch_id, len(rows)):
                SINK_SKIPPED.labels("vehicle_daily").inc()
                return
            execute_values(
                cur,
                """
                INSERT INTO speed.vehicle_daily (vehicle_id, sim_date, driver_id, pings, on_trip_pings,
                    enroute_pings, idle_pings, trips_completed, revenue_lkr, odo_min, odo_max,
                    distance_km, last_event_ts, last_batch_id)
                VALUES %s
                ON CONFLICT (vehicle_id, sim_date) DO UPDATE SET
                    driver_id = EXCLUDED.driver_id, pings = EXCLUDED.pings,
                    on_trip_pings = EXCLUDED.on_trip_pings, enroute_pings = EXCLUDED.enroute_pings,
                    idle_pings = EXCLUDED.idle_pings, trips_completed = EXCLUDED.trips_completed,
                    revenue_lkr = EXCLUDED.revenue_lkr, odo_min = EXCLUDED.odo_min,
                    odo_max = EXCLUDED.odo_max, distance_km = EXCLUDED.distance_km,
                    last_event_ts = EXCLUDED.last_event_ts, last_batch_id = EXCLUDED.last_batch_id,
                    updated_at = now()
                """,
                values,
            )
        SINK_ROWS.labels("speed.vehicle_daily").inc(len(rows))
        self._finish("vehicle_daily", started, max(r["max_kafka_ts"] for r in rows))

    # ---------------------------------------------------------- vehicle_state
    def process_vehicle_events(self, batch_df: DataFrame, batch_id: int) -> None:
        """A micro-batch here is small (maxOffsetsPerTrigger caps it), so one
        collect is much cheaper than several Spark jobs per batch."""
        started = time.time()
        rows = batch_df.select(
            "is_valid",
            "dq_errors",
            F.when(~F.col("is_valid"), F.col("raw_value")).alias("raw_value"),
            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            "kafka_ts",
            "event_id",
            "vehicle_id",
            "driver_id",
            "trip_id",
            "status",
            "lat",
            "lon",
            "speed",
            "odometer_km",
            "event_time",
        ).collect()
        if not rows:
            return
        valid = [r.asDict() for r in rows if r["is_valid"]]
        dead = [r for r in rows if not r["is_valid"]]
        stream_time = max((e["event_time"] for e in valid), default=None)
        summaries = summarise_vehicles(valid, self.zone_of)

        opened = resolved = 0
        with self.transaction() as cur:
            if not self.claim(cur, "vehicle_state", batch_id, len(rows), stream_time):
                SINK_SKIPPED.labels("vehicle_state").inc()
                return
            if dead:
                execute_values(
                    cur,
                    "INSERT INTO ops.dead_letters (speed_batch_id, kafka_partition, kafka_offset, "
                    "kafka_key, errors, raw_value) VALUES %s",
                    [(batch_id, d["kafka_partition"], d["kafka_offset"], d["kafka_key"], list(d["dq_errors"]),
                      (d["raw_value"] or "")[:2000]) for d in dead],
                )
            if summaries:
                self._upsert_state(cur, summaries, batch_id)
                opened, resolved = self._update_idle_alerts(cur, stream_time)
        if dead:
            self.publish_dead_letters(dead, batch_id)

        VALID_EVENTS.inc(len(valid))
        for d in dead:
            for reason in d["dq_errors"]:
                DEAD_LETTERS.labels(reason).inc()
        IDLE_ALERTS.labels("opened").inc(opened)
        IDLE_ALERTS.labels("resolved").inc(resolved)
        SINK_ROWS.labels("speed.vehicle_state").inc(len(summaries))
        newest = max(r["kafka_ts"] for r in rows)
        self._finish("vehicle_state", started, newest, min(r["kafka_ts"] for r in rows))
        log.info(
            "vehicle_state_batch",
            extra={
                "batch_id": batch_id,
                "rows": len(rows),
                "valid": len(valid),
                "dead_letters": len(dead),
                "vehicles_updated": len(summaries),
                "idle_alerts_opened": opened,
                "idle_alerts_resolved": resolved,
                "stream_time": str(stream_time),
                "e2e_latency_s": round(time.time() - epoch(newest), 2),
                "duration_ms": int((time.time() - started) * 1000),
            },
        )

    def _upsert_state(self, cur, summaries: list[dict], batch_id: int) -> None:
        cur.execute(
            "SELECT vehicle_id, status, idle_since, last_event_ts FROM speed.vehicle_state "
            "WHERE vehicle_id = ANY(%s) FOR UPDATE",
            ([s["vehicle_id"] for s in summaries],),
        )
        prev = {r[0]: {"status": r[1], "idle_since": r[2], "last_event_ts": r[3]} for r in cur.fetchall()}
        values = []
        for summary in summaries:
            row = merge_vehicle_state(prev.get(summary["vehicle_id"]), summary, self.offline_gap)
            if row is not None:
                values.append(
                    (
                        row["vehicle_id"], row["driver_id"], row["status"], row["trip_id"], row["zone"],
                        row["lat"], row["lon"], row["speed_kmh"], row["odometer_km"],
                        row["last_event_ts"], row["idle_since"], batch_id,
                    )
                )
        if values:
            execute_values(
                cur,
                """
                INSERT INTO speed.vehicle_state (vehicle_id, driver_id, status, trip_id, zone, lat, lon,
                    speed_kmh, odometer_km, last_event_ts, idle_since, last_batch_id)
                VALUES %s
                ON CONFLICT (vehicle_id) DO UPDATE SET
                    driver_id = EXCLUDED.driver_id, status = EXCLUDED.status, trip_id = EXCLUDED.trip_id,
                    zone = EXCLUDED.zone, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
                    speed_kmh = EXCLUDED.speed_kmh, odometer_km = EXCLUDED.odometer_km,
                    last_event_ts = EXCLUDED.last_event_ts, idle_since = EXCLUDED.idle_since,
                    last_batch_id = EXCLUDED.last_batch_id, updated_at = now()
                """,
                values,
            )

    def _update_idle_alerts(self, cur, stream_time) -> tuple[int, int]:
        """Open one alert per idle streak longer than the threshold, keep open
        alerts up to date and resolve them when the vehicle works again or goes offline."""
        params = {
            "threshold": self.s.idle_alert_minutes,
            "stream_time": stream_time,
            "offline": self.s.offline_after_minutes,
        }
        cur.execute(
            """
            INSERT INTO speed.idle_alerts (vehicle_id, driver_id, zone, idle_since, detected_at_sim,
                idle_minutes_at_detection, current_idle_minutes, last_seen_sim, status)
            SELECT s.vehicle_id, s.driver_id, s.zone, s.idle_since, s.last_event_ts,
                   round((extract(epoch FROM s.last_event_ts - s.idle_since) / 60)::numeric, 1),
                   round((extract(epoch FROM s.last_event_ts - s.idle_since) / 60)::numeric, 1),
                   s.last_event_ts, 'OPEN'
            FROM speed.vehicle_state s
            WHERE s.status = 'idle' AND s.idle_since IS NOT NULL
              AND s.last_event_ts - s.idle_since >= make_interval(mins => %(threshold)s)
              AND NOT EXISTS (SELECT 1 FROM speed.idle_alerts a
                              WHERE a.vehicle_id = s.vehicle_id
                                AND (a.status = 'OPEN' OR a.idle_since = s.idle_since))
            """,
            params,
        )
        opened = cur.rowcount
        cur.execute(
            """
            UPDATE speed.idle_alerts a
               SET current_idle_minutes = round((extract(epoch FROM s.last_event_ts - a.idle_since) / 60)::numeric, 1),
                   last_seen_sim = s.last_event_ts, zone = s.zone
              FROM speed.vehicle_state s
             WHERE a.vehicle_id = s.vehicle_id AND a.status = 'OPEN'
               AND s.status = 'idle' AND s.idle_since = a.idle_since
            """
        )
        cur.execute(
            """
            UPDATE speed.idle_alerts a
               SET status = 'RESOLVED', resolved_at = now(),
                   resolved_at_sim = LEAST(s.last_event_ts, %(stream_time)s),
                   resolution = CASE WHEN s.last_event_ts < %(stream_time)s - make_interval(mins => %(offline)s)
                                     THEN 'went_offline' ELSE 'back_to_work' END
              FROM speed.vehicle_state s
             WHERE a.vehicle_id = s.vehicle_id AND a.status = 'OPEN'
               AND (s.status <> 'idle' OR s.idle_since IS DISTINCT FROM a.idle_since
                    OR s.last_event_ts < %(stream_time)s - make_interval(mins => %(offline)s))
            """,
            params,
        )
        return opened, cur.rowcount


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "512m")  # Spark needs at least ~450 MB of usable heap
        .config("spark.driver.extraJavaOptions", JVM_OPTIONS)
        .config("spark.sql.session.timeZone", "UTC")
        # a micro-batch holds a few hundred rows: one shuffle partition (and one
        # state-store partition per stateful operator) keeps per-batch overhead low
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.streaming.minBatchesToRetain", "20")
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "30s")
        .config("spark.sql.debug.maxToStringFields", "100")
        .config("spark.ui.port", "4040")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.retainedJobs", "50")
        .config("spark.ui.retainedStages", "50")
        .config("spark.ui.retainedTasks", "2000")
        .config("spark.sql.ui.retainedExecutions", "20")
        .config("spark.sql.streaming.ui.retainedQueries", "10")
        .config("spark.sql.streaming.ui.retainedProgressUpdates", "60")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, s) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", s.topic_telemetry)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", s.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )


def main() -> int:
    s = get_settings()
    start_http_server(METRICS_PORT)
    UP.set(1)
    spark = build_spark(SERVICE)
    spark.sparkContext.setLogLevel("WARN")
    zones = load_zones(s.zones_file)
    grid = zone_grid_df(spark, zones).cache()
    sink = PostgresSink(s, zones)
    spark.streams.addListener(MetricsListener())

    trigger = f"{s.speed_trigger_seconds} seconds"  # live vehicle state and alerts
    agg_trigger = f"{s.aggregate_trigger_seconds} seconds"  # windowed aggregates
    watermark = f"{s.watermark_minutes} minutes"
    ckpt = s.checkpoint_dir

    def parsed_stream() -> DataFrame:
        return validate_events(parse_telemetry(kafka_to_raw(read_kafka(spark, s))))

    def clean_stream() -> DataFrame:
        events = parsed_stream().filter("is_valid")
        return events.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(["event_id"])

    # 1) master dataset (input of the batch layer)
    archive = (
        with_event_date(kafka_to_raw(read_kafka(spark, s)))
        .writeStream.queryName("raw_archive")
        .format("parquet")
        .option("path", str(s.raw_telemetry_dir))
        .option("checkpointLocation", str(ckpt / "raw_archive"))
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime=f"{s.archive_trigger_seconds} seconds")
        .start()
    )

    # 2) zone x hour metrics
    zone_hourly = (
        enrich_zones(clean_stream(), grid, zones)
        .groupBy(F.window("event_time", "1 hour").alias("window"), F.col("metric_zone").alias("zone"))
        .agg(
            *zone_metric_aggs(),
            F.approx_count_distinct(F.when(F.col("event_type") == "ping", F.col("vehicle_id"))).alias("active_vehicles"),
            F.max("kafka_ts").alias("max_kafka_ts"),
        )
        .writeStream.queryName("zone_hourly")
        .outputMode("update")
        .foreachBatch(sink.write_zone_hourly)
        .option("checkpointLocation", str(ckpt / "zone_hourly"))
        .trigger(processingTime=agg_trigger)
        .start()
    )

    # 3) vehicle x day running totals
    vehicle_daily = (
        clean_stream()
        .groupBy(F.window("event_time", "1 day").alias("window"), F.col("vehicle_id"))
        .agg(*vehicle_metric_aggs(), F.max("kafka_ts").alias("max_kafka_ts"))
        .writeStream.queryName("vehicle_daily")
        .outputMode("update")
        .foreachBatch(sink.write_vehicle_daily)
        .option("checkpointLocation", str(ckpt / "vehicle_daily"))
        .trigger(processingTime=agg_trigger)
        .start()
    )

    # 4) live vehicle state, idle alerts, dead letters (PostgreSQL + DLQ topic)
    vehicle_state = (
        parsed_stream()
        .writeStream.queryName("vehicle_state")
        .foreachBatch(sink.process_vehicle_events)
        .option("checkpointLocation", str(ckpt / "vehicle_state"))
        .trigger(processingTime=trigger)
        .start()
    )

    queries = [archive, zone_hourly, vehicle_daily, vehicle_state]
    log.info(
        "speed_layer_started",
        extra={
            "queries": [q.name for q in queries],
            "trigger": trigger,
            "aggregate_trigger": agg_trigger,
            "watermark": watermark,
            "topic": s.topic_telemetry,
        },
    )

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    exit_code = 0
    try:
        while not stop.is_set():
            if spark.streams.awaitAnyTermination(timeout=5):
                exit_code = 1  # a query ended on its own: let Docker restart us
                break
    except Exception as exc:  # a query failed: log it, Docker restarts the container
        log.error("streaming_query_failed", extra={"error": str(exc)[:4000]})
        exit_code = 1
    finally:
        UP.set(0)
        for q in spark.streams.active:
            q.stop()
        log.info("speed_layer_stopped", extra={"exit_code": exit_code})
        spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
