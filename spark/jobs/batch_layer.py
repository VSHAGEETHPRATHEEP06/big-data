"""Batch layer: recompute the exact views of one simulated day (Lambda architecture).

Started by Airflow (spark-submit, local mode) once per simulated day, or by
hand to recompute an old day:

    spark-submit batch_layer.py --sim-date 2026-09-01 --run-id manual__fix

Inputs come only from the immutable master dataset:
    /data/lake/raw/telemetry/event_date=D/          (written by the speed layer's archive query)
    /data/lake/raw/expenses/expense_date=D/*.csv    (partner file archived by Airflow)

Why the batch layer is more correct than the speed layer:
    * it sees late events that arrived after the streaming watermark
    * it removes duplicates over the whole day, not only inside a watermark
    * it counts distinct vehicles exactly (streaming uses an approximation)
    * it can order each vehicle's pings in time (window functions) to measure
      minutes on trip / idle and the longest idle streak
    * it joins the day's telemetry with the partner cost file (the two sources)

Outputs: curated Parquet (the batch layer's own history, used for trends) and
one PostgreSQL transaction that replaces every batch view of day D, so the
serving layer never shows a half-published day.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import date, datetime, timedelta, timezone

from psycopg2.extras import Json, execute_values
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from fleet_common.db import connect, transaction
from fleet_common.logs import get_logger
from fleet_common.registry import load_registry
from fleet_common.settings import get_settings
from fleet_common.simclock import load_clock
from fleet_common.zones import load_zones
from fleet_transforms import (
    becoming_unprofitable_col,
    enrich_zones,
    parse_telemetry,
    status_col,
    trend_col,
    validate_events,
    values_df,
    vehicle_metric_aggs,
    zone_grid_df,
    zone_metric_aggs,
)

log = get_logger("spark-batch-layer", "processing")

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
NUMERIC_EXPENSE_COLUMNS = ["fuel_litres", "fuel_cost", "maintenance_cost", "distance_covered"]
EXPENSE_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in EXPENSE_COLUMNS])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("fleet-batch-layer")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        # one simulated day is small (about 25k events): adaptive re-planning costs
        # more time than it saves, so it is switched off
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _flag(condition, reason: str):
    return F.when(condition, F.lit(reason))


# ---------------------------------------------------------------- telemetry
def telemetry_views(spark, s, zones, registry_df, day: date, clock) -> dict:
    """Clean, de-duplicate and aggregate the day's telemetry."""
    raw_path = s.raw_telemetry_dir
    if not raw_path.exists():
        raise RuntimeError(f"master dataset {raw_path} does not exist yet")
    # Reading the sink root makes Spark use _spark_metadata, so only files
    # committed by the streaming query are read (no half-written files).
    raw = spark.read.parquet(str(raw_path)).filter(F.col("event_date") == day.isoformat())
    events = validate_events(parse_telemetry(raw)).cache()
    total = events.count()
    if total == 0:
        raise RuntimeError(f"no telemetry in the master dataset for {day}")
    valid = events.filter("is_valid")
    valid_count = valid.count()
    dedup = valid.dropDuplicates(["event_id"]).cache()
    dedup_count = dedup.count()

    # Estimated lateness: simulated arrival time (Kafka append time mapped to
    # the simulated clock) minus event time. Late events are the ones the speed
    # layer may have dropped after its watermark.
    sim_anchor = int(clock.sim_start.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
    real_anchor = int(clock.real_start * 1_000_000)
    arrival_sim = F.lit(sim_anchor) + (F.unix_micros("kafka_ts") - F.lit(real_anchor)) * F.lit(clock.speedup)
    delay_min = (arrival_sim - F.unix_micros("event_time")) / 60_000_000.0
    dedup = dedup.withColumn("is_late", delay_min > F.lit(float(s.watermark_minutes)))
    late_count = dedup.filter("is_late").count()

    grid = zone_grid_df(spark, zones)
    enriched = enrich_zones(dedup, grid, zones).cache()

    zone_hourly = (
        enriched.withColumn("hour", F.hour("event_time"))
        .groupBy(F.col("metric_zone").alias("zone"), "hour")
        .agg(
            *zone_metric_aggs(),
            F.countDistinct(F.when(F.col("event_type") == "ping", F.col("vehicle_id"))).alias("active_vehicles"),
        )
        .withColumn(
            "utilization_pct",
            F.when(F.col("pings") > 0, F.round(F.col("on_trip_pings") * 100.0 / F.col("pings"), 2)),
        )
        .cache()  # used twice: curated Parquet and the PostgreSQL publish
    )

    # Time spent in each status: a ping "covers" the time until the next ping,
    # up to 3 ping intervals (a longer gap means the vehicle was offline).
    ping_min = s.ping_interval_sim_minutes
    cap = 3 * ping_min
    w = Window.partitionBy("vehicle_id").orderBy("event_time", "kafka_partition", "kafka_offset")
    pings = dedup.filter(F.col("event_type") == "ping").select(
        "vehicle_id", "event_time", "status", "kafka_partition", "kafka_offset"
    )
    gap_next = (F.unix_micros(F.col("next_ts")) - F.unix_micros(F.col("event_time"))) / 60_000_000.0
    gap_prev = (F.unix_micros(F.col("event_time")) - F.unix_micros(F.col("prev_ts"))) / 60_000_000.0
    pings = (
        pings.withColumn("next_ts", F.lead("event_time").over(w))
        .withColumn("prev_ts", F.lag("event_time").over(w))
        .withColumn("prev_status", F.lag("status").over(w))
        .withColumn(
            "covered_min",
            F.when(F.col("next_ts").isNull() | (gap_next > cap), F.lit(ping_min)).otherwise(gap_next),
        )
        .withColumn(
            "new_run",
            F.when(
                F.col("prev_ts").isNull() | (F.col("prev_status") != F.col("status")) | (gap_prev > cap), 1
            ).otherwise(0),
        )
        .withColumn("run_id", F.sum("new_run").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    )

    def minutes_in(status: str):
        return F.round(F.sum(F.when(F.col("status") == status, F.col("covered_min")).otherwise(0.0)), 1)

    time_in_status = pings.groupBy("vehicle_id").agg(
        F.round(F.sum("covered_min"), 1).alias("online_min"),
        minutes_in("on_trip").alias("on_trip_min"),
        minutes_in("enroute").alias("enroute_min"),
        minutes_in("idle").alias("idle_min"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
    )
    idle_runs = (
        pings.filter(F.col("status") == "idle")
        .groupBy("vehicle_id", "run_id")
        .agg(F.sum("covered_min").alias("run_min"))
    )
    idle_stats = idle_runs.groupBy("vehicle_id").agg(
        F.round(F.max("run_min"), 1).alias("max_idle_streak_min"),
        F.count(F.when(F.col("run_min") >= s.idle_alert_minutes, 1)).alias("long_idle_episodes"),
    )
    late_by_vehicle = dedup.filter("is_late").groupBy("vehicle_id").agg(F.count(F.lit(1)).alias("late_events"))

    vehicle_daily = (
        dedup.groupBy("vehicle_id")
        .agg(*vehicle_metric_aggs())
        .join(time_in_status, "vehicle_id", "left")
        .join(idle_stats, "vehicle_id", "left")
        .join(late_by_vehicle, "vehicle_id", "left")
        .join(registry_df.select("vehicle_id", "vehicle_type"), "vehicle_id", "left")
        .withColumn("gps_distance_km", F.round(F.col("odo_max") - F.col("odo_min"), 3))
        .withColumn(
            "utilization_pct",
            F.when(F.col("online_min") > 0, F.round(F.col("on_trip_min") * 100.0 / F.col("online_min"), 2)),
        )
        .withColumnRenamed("trips_completed", "trips")
        .fillna({"late_events": 0, "long_idle_episodes": 0, "max_idle_streak_min": 0.0})
    ).cache()

    trips = (
        enriched.filter(F.col("event_type") == "trip_end")
        .select(
            "trip_id",
            "vehicle_id",
            "pickup_zone",
            F.col("zone").alias("dropoff_zone"),
            F.timestamp_micros(
                F.unix_micros("event_time") - (F.col("trip_duration_min") * 60_000_000.0).cast("long")
            ).alias("start_ts"),
            F.col("event_time").alias("end_ts"),
            F.round("trip_duration_min", 1).alias("duration_min"),
            F.round("trip_distance_km", 3).alias("distance_km"),
            F.col("fare").alias("fare_lkr"),
        )
        .dropDuplicates(["trip_id"])
        .cache()
    )

    return {
        "zone_hourly": zone_hourly,
        "vehicle_daily": vehicle_daily,
        "trips": trips,
        "stats": {
            "events_total": total,
            "events_invalid": total - valid_count,
            "duplicates_removed": valid_count - dedup_count,
            "late_events": late_count,
        },
    }


# ----------------------------------------------------------------- expenses
def expense_views(spark, path: str, day: date, known_vehicles: list[str]) -> dict:
    """Validate the partner file row by row. Bad rows go to quarantine."""
    raw = spark.read.option("header", True).schema(EXPENSE_SCHEMA).csv(path)
    raw = raw.withColumn("_mid", F.monotonically_increasing_id())
    raw = raw.withColumn("row_no", F.row_number().over(Window.orderBy("_mid"))).drop("_mid")
    typed = raw
    for col in NUMERIC_EXPENSE_COLUMNS:
        typed = typed.withColumn(f"{col}_d", F.expr(f"try_cast(trim({col}) AS DOUBLE)"))
    first_seen = F.row_number().over(Window.partitionBy("record_id").orderBy("row_no"))
    typed = typed.withColumn("_dup_rank", first_seen)

    c = F.col
    checks = [
        _flag(c("record_id").isNull() | (F.trim(c("record_id")) == ""), "missing_record_id"),
        _flag(c("_dup_rank") > 1, "duplicate_record"),
        _flag(c("vehicle_id").isNull() | (F.trim(c("vehicle_id")) == ""), "missing_vehicle_id"),
        _flag(c("vehicle_id").isNotNull() & (F.trim(c("vehicle_id")) != "") & ~c("vehicle_id").isin(known_vehicles), "unknown_vehicle"),
        _flag(c("expense_date").isNull() | (c("expense_date") != F.lit(day.isoformat())), "wrong_date"),
        _flag(c("service_flag").isNull() | ~c("service_flag").isin("Y", "N"), "bad_service_flag"),
    ]
    for col in NUMERIC_EXPENSE_COLUMNS:
        checks.append(_flag(c(f"{col}_d").isNull(), f"bad_number:{col}"))
        checks.append(_flag(c(f"{col}_d") < 0, f"negative:{col}"))
    typed = typed.withColumn("reasons", F.filter(F.array(*checks), lambda x: x.isNotNull())).cache()

    quarantine = typed.filter(F.size("reasons") > 0).select(
        "row_no",
        "record_id",
        "vehicle_id",
        "reasons",
        F.to_json(F.struct(*[F.col(x) for x in EXPENSE_COLUMNS])).alias("raw_record"),
    )
    valid = typed.filter(F.size("reasons") == 0)
    per_vehicle = valid.groupBy("vehicle_id").agg(
        F.round(F.sum("fuel_cost_d"), 2).alias("fuel_cost_lkr"),
        F.round(F.sum("maintenance_cost_d"), 2).alias("maintenance_cost_lkr"),
        F.round(F.sum("fuel_litres_d"), 2).alias("fuel_litres"),
        F.round(F.sum("distance_covered_d"), 3).alias("reported_distance_km"),
        F.max(F.col("service_flag") == "Y").alias("service_day"),
        F.count(F.lit(1)).alias("expense_records"),
    )
    total = typed.count()
    bad = typed.filter(F.size("reasons") > 0).count()
    return {"per_vehicle": per_vehicle, "quarantine": quarantine, "stats": {"rows": total, "valid": total - bad, "quarantined": bad}}


# ------------------------------------------------------------ profitability
def load_history(spark, s, day: date) -> DataFrame | None:
    path = s.curated_dir / "vehicle_profitability"
    if not path.exists():
        return None
    wanted = [(day - timedelta(days=i)).isoformat() for i in (1, 2)]
    return spark.read.parquet(str(path)).filter(F.col("sim_date").isin(wanted))


def profitability_view(spark, s, day: date, registry_df, vehicle_daily, expenses) -> DataFrame:
    share = s.driver_share
    has_cost = F.col("expense_records").isNotNull()
    revenue = F.coalesce(F.col("revenue_lkr"), F.lit(0.0))
    fuel = F.coalesce(F.col("fuel_cost_lkr"), F.lit(0.0))
    maint = F.coalesce(F.col("maintenance_cost_lkr"), F.lit(0.0))
    gps = F.col("gps_distance_km")

    prof = (
        registry_df.join(vehicle_daily.drop("vehicle_type", "driver_id"), "vehicle_id", "left")
        .join(expenses, "vehicle_id", "left")
        .filter(F.col("pings").isNotNull() | F.col("expense_records").isNotNull())
        .withColumn("trips", F.coalesce(F.col("trips"), F.lit(0)))
        .withColumn("revenue_lkr", F.round(revenue, 2))
        .withColumn("driver_payout_lkr", F.round(revenue * share, 2))
        .withColumn("total_cost_lkr", F.when(has_cost, F.round(fuel + maint, 2)))
        .withColumn("net_profit_lkr", F.round(revenue - revenue * share - fuel - maint, 2))
        .withColumn("margin_pct", F.when(revenue > 0, F.round((revenue - revenue * share - fuel - maint) / revenue * 100.0, 2)))
        .withColumn(
            "distance_gap_pct",
            F.when((gps > 0) & F.col("reported_distance_km").isNotNull(),
                   F.round((F.col("reported_distance_km") - gps) / gps * 100.0, 2)),
        )
        .withColumn("fuel_cost_per_km", F.when((gps > 0) & has_cost, F.round(fuel / gps, 3)))
        .withColumn("cost_per_km", F.when((gps > 0) & has_cost, F.round((fuel + maint) / gps, 3)))
        .withColumn("revenue_per_km", F.when(gps > 0, F.round(revenue / gps, 3)))
        .withColumn("service_day", F.coalesce(F.col("service_day"), F.lit(False)))
    )

    # Benchmarks per vehicle type (a tuk-tuk is compared with tuk-tuks)
    bench = prof.groupBy("vehicle_type").agg(
        F.expr("percentile_approx(trips, 0.5)").alias("type_median_trips"),
        F.avg("fuel_cost_per_km").alias("type_avg_fuel_per_km"),
    )
    prof = prof.join(bench, "vehicle_type", "left")

    # Trend needs the two previous days from the batch layer's own history
    history = load_history(spark, s, day)
    if history is not None:
        p1 = history.filter(F.col("sim_date") == (day - timedelta(days=1)).isoformat()).select(
            "vehicle_id", F.col("net_profit_lkr").alias("prev_net_profit_lkr"), F.col("status").alias("prev_status")
        )
        p2 = history.filter(F.col("sim_date") == (day - timedelta(days=2)).isoformat()).select(
            "vehicle_id", F.col("net_profit_lkr").alias("prev2_net_profit_lkr")
        )
        prof = prof.join(p1, "vehicle_id", "left").join(p2, "vehicle_id", "left")
    else:
        prof = (
            prof.withColumn("prev_net_profit_lkr", F.lit(None).cast("double"))
            .withColumn("prev_status", F.lit(None).cast("string"))
            .withColumn("prev2_net_profit_lkr", F.lit(None).cast("double"))
        )

    known = [F.col("net_profit_lkr"), F.col("prev_net_profit_lkr"), F.col("prev2_net_profit_lkr")]
    total = sum(F.coalesce(k, F.lit(0.0)) for k in known)
    count = sum(F.when(k.isNotNull(), 1).otherwise(0) for k in known)
    prof = prof.withColumn("net_profit_3d_avg_lkr", F.round(total / count, 2))

    net = F.col("net_profit_lkr")
    prof = (
        prof.withColumn("status", status_col(net, F.col("revenue_lkr"), has_cost, s.at_risk_margin_pct))
        .withColumn("trend", trend_col(F.col("prev2_net_profit_lkr"), F.col("prev_net_profit_lkr"), net))
        .withColumn(
            "becoming_unprofitable",
            becoming_unprofitable_col(F.col("status"), F.col("trend"), F.col("prev_status"), F.col("service_day")),
        )
    )

    reasons = F.filter(
        F.array(
            _flag(~has_cost, "no expense record from partners"),
            F.when(
                F.col("service_day") & (F.col("maintenance_cost_lkr") >= 5000),
                F.concat(F.lit("service day: maintenance LKR "), F.format_number("maintenance_cost_lkr", 0)),
            ),
            F.when(
                F.col("trips") < F.col("type_median_trips") * s.low_trips_factor,
                F.concat(F.lit("low trips: "), F.col("trips").cast("string"), F.lit(" vs type median "),
                         F.col("type_median_trips").cast("string")),
            ),
            F.when(
                F.col("fuel_cost_per_km") > F.col("type_avg_fuel_per_km") * s.high_fuel_cost_factor,
                F.concat(F.lit("high fuel cost: LKR "), F.format_number("fuel_cost_per_km", 1),
                         F.lit("/km vs type average "), F.format_number("type_avg_fuel_per_km", 1)),
            ),
            F.when(
                F.abs(F.col("distance_gap_pct")) > s.distance_gap_alert_pct,
                F.concat(F.lit("reported km differ from GPS by "), F.format_number("distance_gap_pct", 1), F.lit("%")),
            ),
            F.when(
                F.col("idle_min") > F.col("online_min") * 0.4,
                F.concat(F.lit("idle "), F.format_number(F.col("idle_min") * 100.0 / F.col("online_min"), 0),
                         F.lit("% of online time")),
            ),
        ),
        lambda x: x.isNotNull(),
    )
    return prof.withColumn("reasons", reasons)


def cost_rates_view(spark, s, day: date, profitability: DataFrame) -> DataFrame:
    """Per-vehicle cost rates over the last three processed days (for live projection)."""
    cols = ["vehicle_id", "fuel_cost_lkr", "maintenance_cost_lkr", "gps_distance_km", "status"]
    today = profitability.select(*cols)
    history = load_history(spark, s, day)
    frames = today if history is None else today.unionByName(history.select(*cols))
    km = F.sum("gps_distance_km")
    return (
        frames.filter(F.col("status") != "NO_COST_DATA")
        .groupBy("vehicle_id")
        .agg(
            # Spark 4 runs in ANSI mode, where x / 0 raises an error, so guard it
            F.when(km > 0, F.round(F.sum("fuel_cost_lkr") / km, 3)).alias("fuel_cost_per_km"),
            F.round(F.avg("maintenance_cost_lkr"), 2).alias("maintenance_per_day"),
            F.count(F.lit(1)).alias("days_used"),
        )
        .filter(F.col("fuel_cost_per_km").isNotNull())
    )


# ------------------------------------------------------------------ publish
def speed_totals(cur, day: date) -> dict:
    cur.execute(
        "SELECT coalesce(sum(trips_completed), 0), coalesce(sum(earnings_lkr), 0), coalesce(sum(pings), 0) "
        "FROM speed.zone_hourly WHERE window_start >= %s AND window_start < %s",
        (day, day + timedelta(days=1)),
    )
    trips, earnings, pings = cur.fetchone()
    return {"trips_completed": float(trips), "earnings_lkr": float(earnings), "pings": float(pings)}


def rows(df: DataFrame, columns: list[str]) -> list[tuple]:
    return [tuple(r) for r in df.select(*columns).collect()]


def publish(conn, s, day: date, run_id: str, views: dict, expense: dict, stats: dict) -> dict:
    zh_cols = ["zone", "hour", "pings", "active_vehicles", "on_trip_pings", "enroute_pings", "idle_pings",
               "trips_completed", "earnings_lkr", "avg_speed_kmh", "utilization_pct"]
    vd_cols = ["vehicle_id", "driver_id", "vehicle_type", "trips", "revenue_lkr", "online_min", "on_trip_min",
               "enroute_min", "idle_min", "utilization_pct", "gps_distance_km", "max_idle_streak_min",
               "long_idle_episodes", "late_events", "first_seen", "last_seen"]
    tr_cols = ["trip_id", "vehicle_id", "pickup_zone", "dropoff_zone", "start_ts", "end_ts", "duration_min",
               "distance_km", "fare_lkr"]
    pr_cols = ["vehicle_id", "driver_id", "vehicle_type", "plate_no", "trips", "revenue_lkr", "driver_payout_lkr",
               "fuel_cost_lkr", "maintenance_cost_lkr", "total_cost_lkr", "net_profit_lkr", "margin_pct",
               "gps_distance_km", "reported_distance_km", "distance_gap_pct", "fuel_cost_per_km", "cost_per_km",
               "revenue_per_km", "utilization_pct", "idle_min", "service_day", "prev_net_profit_lkr",
               "net_profit_3d_avg_lkr", "trend", "status", "prev_status", "becoming_unprofitable", "reasons"]
    qr_cols = ["row_no", "record_id", "vehicle_id", "reasons", "raw_record"]
    cr_cols = ["vehicle_id", "fuel_cost_per_km", "maintenance_per_day", "days_used"]

    zone_rows = rows(views["zone_hourly"], zh_cols)
    vehicle_rows = rows(views["vehicle_daily"], vd_cols)
    trip_rows = rows(views["trips"], tr_cols)
    prof_rows = rows(views["profitability"], pr_cols)
    quarantine_rows = rows(expense["quarantine"], qr_cols)
    rate_rows = rows(views["cost_rates"], cr_cols)

    prof = [dict(zip(pr_cols, r)) for r in prof_rows]
    vehicles = [dict(zip(vd_cols, r)) for r in vehicle_rows]

    def total(key, items=prof):
        return round(sum((x.get(key) or 0.0) for x in items), 2)

    online = total("online_min", vehicles)
    summary = {
        "trips": int(total("trips")),
        "revenue_lkr": total("revenue_lkr"),
        "driver_payout_lkr": total("driver_payout_lkr"),
        "fuel_cost_lkr": total("fuel_cost_lkr"),
        "maintenance_cost_lkr": total("maintenance_cost_lkr"),
        "net_profit_lkr": total("net_profit_lkr"),
        "active_vehicles": sum(1 for v in vehicles if (v["online_min"] or 0) > 0),
        "fleet_utilization_pct": round(total("on_trip_min", vehicles) * 100.0 / online, 2) if online else None,
        "unprofitable_vehicles": sum(1 for p in prof if p["status"] == "UNPROFITABLE"),
        "at_risk_vehicles": sum(1 for p in prof if p["status"] == "AT_RISK"),
        "becoming_unprofitable_vehicles": sum(1 for p in prof if p["becoming_unprofitable"]),
        "no_cost_data_vehicles": sum(1 for p in prof if p["status"] == "NO_COST_DATA"),
        "distance_mismatch_vehicles": sum(
            1 for p in prof if p["distance_gap_pct"] is not None and abs(p["distance_gap_pct"]) > s.distance_gap_alert_pct
        ),
        "long_idle_episodes": int(total("long_idle_episodes", vehicles)),
        **stats,
        "expense_rows": expense["stats"]["rows"],
        "expense_rows_valid": expense["stats"]["valid"],
        "expense_rows_quarantined": expense["stats"]["quarantined"],
    }
    batch_totals = {
        "trips_completed": float(sum(r[7] for r in zone_rows)),
        "earnings_lkr": float(sum(r[8] for r in zone_rows)),
        "pings": float(sum(r[2] for r in zone_rows)),
    }

    with transaction(conn) as cur:
        for table in ("batch.zone_hourly", "batch.vehicle_daily", "batch.trips",
                      "batch.vehicle_profitability", "batch.expense_quarantine"):
            cur.execute(f"DELETE FROM {table} WHERE sim_date = %s", (day,))
        execute_values(
            cur,
            f"INSERT INTO batch.zone_hourly (sim_date, {', '.join(zh_cols)}, batch_run_id) VALUES %s",
            [(day, *r, run_id) for r in zone_rows],
        )
        execute_values(
            cur,
            f"INSERT INTO batch.vehicle_daily (sim_date, {', '.join(vd_cols)}, batch_run_id) VALUES %s",
            [(day, *r, run_id) for r in vehicle_rows],
        )
        if trip_rows:
            execute_values(
                cur,
                f"INSERT INTO batch.trips (sim_date, {', '.join(tr_cols)}, batch_run_id) VALUES %s",
                [(day, *r, run_id) for r in trip_rows],
            )
        execute_values(
            cur,
            f"INSERT INTO batch.vehicle_profitability (sim_date, {', '.join(pr_cols)}, batch_run_id) VALUES %s",
            [(day, *r, run_id) for r in prof_rows],
        )
        if quarantine_rows:
            execute_values(
                cur,
                f"INSERT INTO batch.expense_quarantine (sim_date, {', '.join(qr_cols)}, batch_run_id) VALUES %s",
                [(day, r[0], r[1], r[2], list(r[3]), Json(json.loads(r[4])), run_id) for r in quarantine_rows],
            )
        if rate_rows:
            execute_values(
                cur,
                "INSERT INTO batch.vehicle_cost_rates (vehicle_id, fuel_cost_per_km, maintenance_per_day, days_used, "
                "as_of_date, driver_share, at_risk_margin_pct, batch_run_id) VALUES %s "
                "ON CONFLICT (vehicle_id) DO UPDATE SET fuel_cost_per_km = EXCLUDED.fuel_cost_per_km, "
                "maintenance_per_day = EXCLUDED.maintenance_per_day, days_used = EXCLUDED.days_used, "
                "as_of_date = EXCLUDED.as_of_date, driver_share = EXCLUDED.driver_share, "
                "at_risk_margin_pct = EXCLUDED.at_risk_margin_pct, batch_run_id = EXCLUDED.batch_run_id, "
                "updated_at = now() WHERE batch.vehicle_cost_rates.as_of_date <= EXCLUDED.as_of_date",
                [(*r, day, s.driver_share, s.at_risk_margin_pct, run_id) for r in rate_rows],
            )
        cols = list(summary.keys())
        cur.execute(
            f"INSERT INTO batch.daily_summary (sim_date, {', '.join(cols)}, batch_run_id) "
            f"VALUES (%s, {', '.join(['%s'] * len(cols))}, %s) "
            f"ON CONFLICT (sim_date) DO UPDATE SET {', '.join(f'{c} = EXCLUDED.{c}' for c in cols)}, "
            f"batch_run_id = EXCLUDED.batch_run_id, computed_at = now()",
            (day, *summary.values(), run_id),
        )
        speed = speed_totals(cur, day)
        recon = []
        for metric, batch_value in batch_totals.items():
            speed_value = speed[metric]
            diff = batch_value - speed_value
            pct = round(diff * 100.0 / batch_value, 3) if batch_value else None
            recon.append((day, metric, speed_value, batch_value, round(diff, 2), pct))
        # The comparison is a snapshot taken while the speed layer still holds the
        # day. When an old day is recomputed after its speed rows were purged, the
        # stored comparison is kept instead of being replaced by a comparison with zero.
        if speed["pings"] > 0:
            cur.execute("DELETE FROM ops.layer_reconciliation WHERE sim_date = %s", (day,))
            execute_values(
                cur,
                "INSERT INTO ops.layer_reconciliation (sim_date, metric, speed_value, batch_value, diff, diff_pct) VALUES %s",
                recon,
            )
        cur.execute(
            "UPDATE ops.batch_files SET rows_valid = %s, rows_quarantined = %s, processed_run_id = %s WHERE sim_date = %s",
            (expense["stats"]["valid"], expense["stats"]["quarantined"], run_id, day),
        )
    return {"summary": summary, "reconciliation": {m: {"speed": sv, "batch": bv, "diff_pct": p} for _, m, sv, bv, _, p in recon},
            "rows": {"zone_hourly": len(zone_rows), "vehicle_daily": len(vehicle_rows), "trips": len(trip_rows),
                     "vehicle_profitability": len(prof_rows), "expense_quarantine": len(quarantine_rows)}}


def record_run(conn, run_id: str, day: date, status: str, started: datetime, details: dict,
               rows_in: int | None = None, rows_out: int | None = None) -> None:
    with transaction(conn) as cur:
        cur.execute(
            "INSERT INTO ops.pipeline_runs (run_id, stage, sim_date, status, started_at, finished_at, rows_in, rows_out, details) "
            "VALUES (%s, 'batch_layer', %s, %s, %s, now(), %s, %s, %s) "
            "ON CONFLICT (run_id, stage) DO UPDATE SET status = EXCLUDED.status, finished_at = now(), "
            "rows_in = EXCLUDED.rows_in, rows_out = EXCLUDED.rows_out, details = EXCLUDED.details, sim_date = EXCLUDED.sim_date",
            (run_id, day, status, started, rows_in, rows_out, Json(details)),
        )


def write_curated(views: dict, s, day: date) -> None:
    for name in ("zone_hourly", "vehicle_daily", "trips", "profitability"):
        target = "vehicle_profitability" if name == "profitability" else name
        (
            views[name].withColumn("sim_date", F.lit(day.isoformat()))
            .write.mode("overwrite")
            .partitionBy("sim_date")
            .parquet(str(s.curated_dir / target))
        )


class StepTimer:
    """Logs how long each step of the batch run takes (JSON log line per step)."""

    def __init__(self, sim_date: date, run_id: str) -> None:
        self.sim_date, self.run_id = sim_date.isoformat(), run_id
        self.last = time.time()
        self.steps: dict[str, float] = {}

    def step(self, name: str, **fields) -> None:
        now = time.time()
        self.steps[name] = round(now - self.last, 1)
        self.last = now
        log.info("batch_step_done", extra={"step": name, "seconds": self.steps[name], "sim_date": self.sim_date,
                                           "run_id": self.run_id, **fields})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sim-date", required=True, help="simulated day to (re)compute, YYYY-MM-DD")
    parser.add_argument("--run-id", required=True, help="Airflow run id (or any unique id)")
    parser.add_argument("--expense-file", help="archived partner file (default: lake raw zone)")
    args = parser.parse_args(argv)

    s = get_settings()
    day = date.fromisoformat(args.sim_date)
    started_at = datetime.now(timezone.utc)
    t0 = time.time()
    conn = connect(s, logger=log)
    expense_path = args.expense_file or str(
        s.raw_expenses_dir / f"expense_date={day.isoformat()}" / f"expenses_{day.isoformat()}.csv"
    )
    log.info("batch_run_started", extra={"sim_date": day.isoformat(), "run_id": args.run_id, "expense_file": expense_path})
    spark = None
    timer = StepTimer(day, args.run_id)
    try:
        clock = load_clock(conn)
        if clock is None:
            raise RuntimeError("simulated clock not initialised")
        spark = build_spark()
        spark.sparkContext.setLogLevel("WARN")
        zones = load_zones(s.zones_file)
        registry = load_registry(s.registry_file)
        registry_df = values_df(
            spark,
            [(v.vehicle_id, v.driver_id, v.vehicle_type, v.plate_no) for v in registry],
            ["vehicle_id", "driver_id", "vehicle_type", "plate_no"],
        )
        timer.step("spark_start")

        telemetry = telemetry_views(spark, s, zones, registry_df, day, clock)
        timer.step("clean_telemetry", **telemetry["stats"])
        expense = expense_views(spark, expense_path, day, [v.vehicle_id for v in registry])
        timer.step("validate_expenses", **expense["stats"])
        # localCheckpoint cuts the lineage to the curated history we just read,
        # otherwise Spark refuses to overwrite a path it is also reading from.
        profitability = profitability_view(
            spark, s, day, registry_df, telemetry["vehicle_daily"], expense["per_vehicle"]
        ).localCheckpoint(eager=True)
        timer.step("join_and_profitability")
        views = {
            "zone_hourly": telemetry["zone_hourly"],
            "vehicle_daily": telemetry["vehicle_daily"],
            "trips": telemetry["trips"],
            "profitability": profitability,
        }
        views["cost_rates"] = cost_rates_view(spark, s, day, profitability)
        write_curated(views, s, day)
        timer.step("write_curated_parquet")
        result = publish(conn, s, day, args.run_id, views, expense, telemetry["stats"])
        timer.step("publish_postgres", **result["rows"])
        duration = round(time.time() - t0, 1)
        details = {**result, "duration_s": duration, "steps_s": timer.steps, "expense_file": expense_path}
        record_run(
            conn, args.run_id, day, "success", started_at, details,
            rows_in=telemetry["stats"]["events_total"] + expense["stats"]["rows"],
            rows_out=sum(result["rows"].values()),
        )
        log.info("batch_run_finished", extra={"sim_date": day.isoformat(), "run_id": args.run_id, "duration_s": duration,
                                              "summary": result["summary"], "reconciliation": result["reconciliation"]})
        # last stdout line = XCom value for the Airflow BashOperator
        print(json.dumps({"sim_date": day.isoformat(), "run_id": args.run_id, "duration_s": duration,
                          "summary": result["summary"], "reconciliation": result["reconciliation"]}, default=str))
        return 0
    except Exception as exc:
        log.error("batch_run_failed", extra={"sim_date": day.isoformat(), "run_id": args.run_id,
                                             "error": str(exc)[:2000], "trace": traceback.format_exc()[-4000:]})
        try:
            record_run(conn, args.run_id, day, "failed", started_at, {"error": str(exc)[:2000]})
        except Exception:
            pass
        return 1
    finally:
        if spark is not None:
            spark.stop()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
