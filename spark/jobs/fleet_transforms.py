"""Transformations shared by the speed layer (streaming) and the batch layer.

The biggest weakness of a Lambda architecture is that the same business logic
is written twice, once for streaming and once for batch, and the two copies
drift apart. We avoid that by keeping parsing, validation, zone enrichment,
the zone-hour metric definitions and the profitability rules in this one
module. Spark uses the same DataFrame API for streams and for static data, so
both layers call exactly these functions.

Time zone note: simulated timestamps are naive Sri Lanka local time. Spark
runs with ``spark.sql.session.timeZone=UTC`` so the values are never shifted,
and hourly/daily windows line up with local hours and days.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from fleet_common import rules
from fleet_common.zones import ZoneConfig

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_ts", T.StringType()),
        T.StructField("vehicle_id", T.StringType()),
        T.StructField("driver_id", T.StringType()),
        T.StructField("trip_id", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("speed", T.DoubleType()),
        T.StructField("heading", T.DoubleType()),
        T.StructField("odometer_km", T.DoubleType()),
        T.StructField("fare", T.DoubleType()),
        T.StructField("trip_distance_km", T.DoubleType()),
        T.StructField("trip_duration_min", T.DoubleType()),
        T.StructField("pickup_lat", T.DoubleType()),
        T.StructField("pickup_lon", T.DoubleType()),
    ]
)

VALID_STATUS = ["idle", "enroute", "on_trip"]
VALID_EVENT_TYPES = ["ping", "trip_start", "trip_end"]
# Sri Lanka bounding box: anything outside is a GPS error
LAT_RANGE = (5.8, 10.0)
LON_RANGE = (79.5, 82.0)
MAX_SPEED_KMH = 150.0

RAW_COLUMNS = ["kafka_key", "raw_value", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_ts"]


# ---------------------------------------------------------------------------
# 1. Raw records (same columns in Kafka and in the Parquet master dataset)
# ---------------------------------------------------------------------------
def kafka_to_raw(kafka_df: DataFrame) -> DataFrame:
    """Kafka source rows -> raw record with lineage columns (topic/partition/offset)."""
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )


def with_event_date(raw_df: DataFrame) -> DataFrame:
    """Partition column for the master dataset: simulated event date or 'unknown'."""
    ts = F.try_to_timestamp(F.get_json_object(F.col("raw_value"), "$.event_ts"))
    return raw_df.withColumn("event_date", F.coalesce(F.date_format(ts, "yyyy-MM-dd"), F.lit("unknown")))


# ---------------------------------------------------------------------------
# 2. Parse + validate (cleaning)
# ---------------------------------------------------------------------------
def parse_telemetry(raw_df: DataFrame) -> DataFrame:
    """Parse the JSON payload. Broken JSON gives NULL fields, caught by validation."""
    parsed = raw_df.withColumn("e", F.from_json(F.col("raw_value"), EVENT_SCHEMA))
    return parsed.select(*RAW_COLUMNS, "e.*").withColumn("event_time", F.try_to_timestamp(F.col("event_ts")))


def _flag(condition: Column, reason: str) -> Column:
    return F.when(condition, F.lit(reason))


def validate_events(df: DataFrame) -> DataFrame:
    """Adds ``dq_errors`` (array of rule names) and ``is_valid``."""
    c = F.col
    checks = [
        _flag(c("event_id").isNull(), "unparseable_or_missing_event_id"),
        _flag(c("vehicle_id").isNull() | (F.trim(c("vehicle_id")) == ""), "missing_vehicle_id"),
        _flag(c("event_time").isNull(), "bad_event_ts"),
        _flag(c("event_type").isNull() | ~c("event_type").isin(VALID_EVENT_TYPES), "bad_event_type"),
        _flag(c("status").isNull() | ~c("status").isin(VALID_STATUS), "bad_status"),
        _flag(
            c("lat").isNull()
            | c("lon").isNull()
            | ~c("lat").between(*LAT_RANGE)
            | ~c("lon").between(*LON_RANGE),
            "gps_out_of_range",
        ),
        _flag(c("speed").isNull() | (c("speed") < 0) | (c("speed") > MAX_SPEED_KMH), "bad_speed"),
        _flag(c("fare").isNull() | (c("fare") < 0), "bad_fare"),
        _flag(
            (c("event_type") == "trip_end") & (c("trip_id").isNull() | (c("fare") <= 0)),
            "incomplete_trip_end",
        ),
    ]
    errors = F.filter(F.array(*checks), lambda x: x.isNotNull())
    return df.withColumn("dq_errors", errors).withColumn("is_valid", F.size("dq_errors") == 0)


# ---------------------------------------------------------------------------
# 3. Enrichment with reference data (zone lookup grid)
# ---------------------------------------------------------------------------
def values_df(spark: SparkSession, rows: list[tuple], columns: list[str]) -> DataFrame:
    """Small reference table built with SQL VALUES (string columns).

    spark.createDataFrame(list) would start Python worker processes (about
    60 MB each) that stay alive, so a SQL literal table is used instead.
    """
    def quote(value) -> str:
        return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"

    body = ", ".join("(" + ", ".join(quote(v) for v in row) + ")" for row in rows)
    return spark.sql(f"SELECT * FROM VALUES {body} AS t({', '.join(columns)})")


def zone_grid_df(spark: SparkSession, zones: ZoneConfig) -> DataFrame:
    return values_df(spark, zones.build_grid(), ["cell_id", "zone"])


def cell_id_col(lat: Column, lon: Column, cell_size: float) -> Column:
    # Same arithmetic as ZoneConfig.cell_index in Python (IEEE double division + floor)
    return F.concat_ws(
        "_",
        F.floor(lat / F.lit(cell_size)).cast("string"),
        F.floor(lon / F.lit(cell_size)).cast("string"),
    )


def add_zone(df: DataFrame, grid: DataFrame, zones: ZoneConfig, lat: str, lon: str, out: str) -> DataFrame:
    """Stream-static (or static-static) broadcast join: GPS point -> zone name."""
    key = f"_cell_{out}"
    lookup = grid.select(F.col("cell_id").alias(key), F.col("zone").alias(out))
    return (
        df.withColumn(key, cell_id_col(F.col(lat), F.col(lon), zones.cell_size_deg))
        .join(F.broadcast(lookup), on=key, how="left")
        .drop(key)
        .withColumn(out, F.coalesce(F.col(out), F.lit(zones.outer_zone)))
    )


def enrich_zones(df: DataFrame, grid: DataFrame, zones: ZoneConfig) -> DataFrame:
    """Adds ``zone`` (where the vehicle is), ``pickup_zone`` (trip_end only) and
    ``metric_zone``: pings count in the zone where the vehicle is, while a
    completed trip counts (with its fare) in the zone where it started."""
    df = add_zone(df, grid, zones, "lat", "lon", "zone")
    df = add_zone(df, grid, zones, "pickup_lat", "pickup_lon", "pickup_zone")
    return df.withColumn(
        "metric_zone",
        F.when(F.col("event_type") == "trip_end", F.col("pickup_zone")).otherwise(F.col("zone")),
    )


# ---------------------------------------------------------------------------
# 4. Metric definitions used by BOTH layers
# ---------------------------------------------------------------------------
def _is_ping() -> Column:
    return F.col("event_type") == "ping"


def _is_trip_end() -> Column:
    return F.col("event_type") == "trip_end"


def zone_metric_aggs() -> list[Column]:
    """Aggregations for the zone x hour table (active_vehicles is added per layer:
    approximate in streaming, exact in batch)."""
    ping, trip_end = _is_ping(), _is_trip_end()
    return [
        F.count(F.when(ping, 1)).alias("pings"),
        F.count(F.when(ping & (F.col("status") == "on_trip"), 1)).alias("on_trip_pings"),
        F.count(F.when(ping & (F.col("status") == "enroute"), 1)).alias("enroute_pings"),
        F.count(F.when(ping & (F.col("status") == "idle"), 1)).alias("idle_pings"),
        F.count(F.when(trip_end, 1)).alias("trips_completed"),
        F.round(F.coalesce(F.sum(F.when(trip_end, F.col("fare"))), F.lit(0.0)), 2).alias("earnings_lkr"),
        F.round(F.avg(F.when(ping & (F.col("status") != "idle"), F.col("speed"))), 2).alias("avg_speed_kmh"),
    ]


def vehicle_metric_aggs() -> list[Column]:
    """Per-vehicle day aggregations shared by the speed and batch layers."""
    ping, trip_end = _is_ping(), _is_trip_end()
    return [
        F.max("driver_id").alias("driver_id"),
        F.count(F.when(ping, 1)).alias("pings"),
        F.count(F.when(ping & (F.col("status") == "on_trip"), 1)).alias("on_trip_pings"),
        F.count(F.when(ping & (F.col("status") == "enroute"), 1)).alias("enroute_pings"),
        F.count(F.when(ping & (F.col("status") == "idle"), 1)).alias("idle_pings"),
        F.count(F.when(trip_end, 1)).alias("trips_completed"),
        F.round(F.coalesce(F.sum(F.when(trip_end, F.col("fare"))), F.lit(0.0)), 2).alias("revenue_lkr"),
        F.min("odometer_km").alias("odo_min"),
        F.max("odometer_km").alias("odo_max"),
        F.max("event_time").alias("last_event_ts"),
    ]


# ---------------------------------------------------------------------------
# 5. Profitability rules (Spark twin of fleet_common.rules)
# ---------------------------------------------------------------------------
def status_col(net: Column, revenue: Column, has_cost: Column, at_risk_margin_pct: float) -> Column:
    margin = F.when(revenue > 0, net / revenue * 100.0)
    return (
        F.when(~has_cost, F.lit(rules.NO_COST_DATA))
        .when(net < 0, F.lit(rules.UNPROFITABLE))
        .when(margin.isNull() | (margin < at_risk_margin_pct), F.lit(rules.AT_RISK))
        .otherwise(F.lit(rules.PROFITABLE))
    )


def trend_col(prev2: Column, prev1: Column, today: Column) -> Column:
    return (
        F.when(prev2.isNull() | prev1.isNull() | today.isNull(), F.lit(rules.NEW))
        .when((prev2 > prev1) & (prev1 > today), F.lit(rules.DECLINING))
        .when((prev2 < prev1) & (prev1 < today), F.lit(rules.IMPROVING))
        .otherwise(F.lit(rules.STABLE))
    )


def becoming_unprofitable_col(status: Column, trend: Column, prev_status: Column, service_day: Column) -> Column:
    losing = status.isin(rules.UNPROFITABLE, rules.AT_RISK) & ~service_day
    flipped = (status == rules.UNPROFITABLE) & (prev_status == rules.PROFITABLE)
    return losing & ((trend == rules.DECLINING) | F.coalesce(flipped, F.lit(False)))
