-- FleetPulse serving database (PostgreSQL 17)
--
--   ref      reference data (vehicles, zones)
--   ops      pipeline operations: simulated clock, micro-batch ledger, dead
--            letters, run audit trail, alert notifications, reconciliation
--   speed    real-time views written by the speed layer (Spark Streaming)
--   batch    batch views recomputed once per simulated day (Airflow + Spark)
--   serving  views that merge speed and batch views (Lambda serving layer)
--
-- Simulated times are "timestamp" (naive Sri Lanka local time).
-- Real times are "timestamptz".

CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS speed;
CREATE SCHEMA IF NOT EXISTS batch;
CREATE SCHEMA IF NOT EXISTS serving;

-- =========================================================== reference data
CREATE TABLE ref.vehicles (
    vehicle_id       text PRIMARY KEY,
    driver_id        text NOT NULL,
    vehicle_type     text NOT NULL CHECK (vehicle_type IN ('tuk', 'mini', 'car')),
    plate_no         text NOT NULL,
    home_zone        text NOT NULL,
    profile          text NOT NULL,
    km_per_litre     numeric(5, 2) NOT NULL,
    shift_start_hour smallint NOT NULL,
    shift_hours      smallint NOT NULL
);

CREATE TABLE ref.zones (
    zone_id   text PRIMARY KEY,
    zone_name text UNIQUE NOT NULL,
    lat       double precision NOT NULL,
    lon       double precision NOT NULL,
    area_type text NOT NULL
);

-- =============================================================== operations
CREATE TABLE ops.sim_clock (
    id             smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    real_start     timestamptz NOT NULL,
    sim_start      timestamp NOT NULL,
    speedup        double precision NOT NULL CHECK (speedup > 0),
    last_heartbeat timestamptz,
    rebase_count   integer NOT NULL DEFAULT 0,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- Current simulated time, usable from any SQL client (Grafana, API, psql)
CREATE FUNCTION ops.sim_now() RETURNS timestamp LANGUAGE sql STABLE AS $$
    SELECT sim_start + make_interval(secs => extract(epoch FROM (now() - real_start))::double precision * speedup)
    FROM ops.sim_clock WHERE id = 1
$$;

-- One row per committed speed-layer micro-batch (idempotent sink ledger)
CREATE TABLE ops.speed_batches (
    query_name   text NOT NULL,
    batch_id     bigint NOT NULL,
    rows_in      integer,
    max_event_ts timestamp,
    committed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);
CREATE INDEX speed_batches_committed_idx ON ops.speed_batches (committed_at);

CREATE TABLE ops.dead_letters (
    id              bigserial PRIMARY KEY,
    received_at     timestamptz NOT NULL DEFAULT now(),
    speed_batch_id  bigint,
    kafka_partition integer,
    kafka_offset    bigint,
    kafka_key       text,
    errors          text[],
    raw_value       text
);
CREATE INDEX dead_letters_received_idx ON ops.dead_letters (received_at);

-- Audit trail of every orchestrated step (lineage: run -> inputs -> outputs)
CREATE TABLE ops.pipeline_runs (
    run_id      text NOT NULL,
    stage       text NOT NULL,
    sim_date    date,
    status      text NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    rows_in     bigint,
    rows_out    bigint,
    details     jsonb,
    PRIMARY KEY (run_id, stage)
);
CREATE INDEX pipeline_runs_stage_idx ON ops.pipeline_runs (stage, status, finished_at);

CREATE TABLE ops.batch_files (
    sim_date         date PRIMARY KEY,
    file_name        text NOT NULL,
    sha256           text NOT NULL,
    rows_declared    integer,
    rows_valid       integer,
    rows_quarantined integer,
    archived_path    text,
    received_at      timestamptz NOT NULL DEFAULT now(),
    processed_run_id text
);

CREATE TABLE ops.alert_notifications (
    id          bigserial PRIMARY KEY,
    received_at timestamptz NOT NULL DEFAULT now(),
    source      text NOT NULL,
    alertname   text,
    severity    text,
    status      text,
    summary     text,
    labels      jsonb,
    starts_at   timestamptz,
    ends_at     timestamptz,
    fingerprint text
);
CREATE INDEX alert_notifications_received_idx ON ops.alert_notifications (received_at);

-- Speed layer vs batch layer totals for each processed day
CREATE TABLE ops.layer_reconciliation (
    sim_date    date NOT NULL,
    metric      text NOT NULL,
    speed_value numeric,
    batch_value numeric,
    diff        numeric,
    diff_pct    numeric,
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, metric)
);

-- ============================================================== speed layer
CREATE TABLE speed.zone_hourly (
    zone            text NOT NULL,
    window_start    timestamp NOT NULL,
    window_end      timestamp NOT NULL,
    pings           integer NOT NULL,
    active_vehicles integer NOT NULL,
    on_trip_pings   integer NOT NULL,
    enroute_pings   integer NOT NULL,
    idle_pings      integer NOT NULL,
    trips_completed integer NOT NULL,
    earnings_lkr    numeric(14, 2) NOT NULL,
    avg_speed_kmh   numeric(8, 2),
    last_batch_id   bigint,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (zone, window_start)
);
CREATE INDEX speed_zone_hourly_window_idx ON speed.zone_hourly (window_start);

CREATE TABLE speed.vehicle_daily (
    vehicle_id      text NOT NULL,
    sim_date        date NOT NULL,
    driver_id       text,
    pings           integer NOT NULL,
    on_trip_pings   integer NOT NULL,
    enroute_pings   integer NOT NULL,
    idle_pings      integer NOT NULL,
    trips_completed integer NOT NULL,
    revenue_lkr     numeric(14, 2) NOT NULL,
    odo_min         double precision,
    odo_max         double precision,
    distance_km     numeric(10, 3),
    last_event_ts   timestamp,
    last_batch_id   bigint,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (vehicle_id, sim_date)
);

CREATE TABLE speed.vehicle_state (
    vehicle_id    text PRIMARY KEY,
    driver_id     text,
    status        text NOT NULL,
    trip_id       text,
    zone          text,
    lat           double precision,
    lon           double precision,
    speed_kmh     double precision,
    odometer_km   double precision,
    last_event_ts timestamp NOT NULL,
    idle_since    timestamp,
    last_batch_id bigint,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE speed.idle_alerts (
    alert_id                  bigserial PRIMARY KEY,
    vehicle_id                text NOT NULL,
    driver_id                 text,
    zone                      text,
    idle_since                timestamp NOT NULL,
    detected_at_sim           timestamp NOT NULL,
    idle_minutes_at_detection numeric(8, 1),
    current_idle_minutes      numeric(8, 1),
    last_seen_sim             timestamp,
    status                    text NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
    resolution                text,
    resolved_at_sim           timestamp,
    created_at                timestamptz NOT NULL DEFAULT now(),
    resolved_at               timestamptz,
    UNIQUE (vehicle_id, idle_since)
);
-- at most one open alert per vehicle
CREATE UNIQUE INDEX idle_alerts_one_open_idx ON speed.idle_alerts (vehicle_id) WHERE status = 'OPEN';

-- ============================================================== batch layer
CREATE TABLE batch.zone_hourly (
    sim_date        date NOT NULL,
    zone            text NOT NULL,
    hour            smallint NOT NULL,
    pings           integer NOT NULL,
    active_vehicles integer NOT NULL,
    on_trip_pings   integer NOT NULL,
    enroute_pings   integer NOT NULL,
    idle_pings      integer NOT NULL,
    trips_completed integer NOT NULL,
    earnings_lkr    numeric(14, 2) NOT NULL,
    avg_speed_kmh   numeric(8, 2),
    utilization_pct numeric(6, 2),
    batch_run_id    text NOT NULL,
    computed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, zone, hour)
);

CREATE TABLE batch.vehicle_daily (
    sim_date            date NOT NULL,
    vehicle_id          text NOT NULL,
    driver_id           text,
    vehicle_type        text,
    trips               integer NOT NULL,
    revenue_lkr         numeric(14, 2) NOT NULL,
    online_min          numeric(8, 1),
    on_trip_min         numeric(8, 1),
    enroute_min         numeric(8, 1),
    idle_min            numeric(8, 1),
    utilization_pct     numeric(6, 2),
    gps_distance_km     numeric(10, 3),
    max_idle_streak_min numeric(8, 1),
    long_idle_episodes  integer,
    late_events         integer,
    first_seen          timestamp,
    last_seen           timestamp,
    batch_run_id        text NOT NULL,
    PRIMARY KEY (sim_date, vehicle_id)
);

CREATE TABLE batch.trips (
    sim_date     date NOT NULL,
    trip_id      text NOT NULL,
    vehicle_id   text NOT NULL,
    pickup_zone  text,
    dropoff_zone text,
    start_ts     timestamp,
    end_ts       timestamp,
    duration_min numeric(8, 1),
    distance_km  numeric(10, 3),
    fare_lkr     numeric(12, 2),
    batch_run_id text NOT NULL,
    PRIMARY KEY (sim_date, trip_id)
);

CREATE TABLE batch.vehicle_profitability (
    sim_date              date NOT NULL,
    vehicle_id            text NOT NULL,
    driver_id             text,
    vehicle_type          text,
    plate_no              text,
    trips                 integer,
    revenue_lkr           numeric(14, 2),
    driver_payout_lkr     numeric(14, 2),
    fuel_cost_lkr         numeric(14, 2),
    maintenance_cost_lkr  numeric(14, 2),
    total_cost_lkr        numeric(14, 2),
    net_profit_lkr        numeric(14, 2),
    margin_pct            numeric(8, 2),
    gps_distance_km       numeric(10, 3),
    reported_distance_km  numeric(10, 3),
    distance_gap_pct      numeric(8, 2),
    fuel_cost_per_km      numeric(10, 3),
    cost_per_km           numeric(10, 3),
    revenue_per_km        numeric(10, 3),
    utilization_pct       numeric(6, 2),
    idle_min              numeric(8, 1),
    service_day           boolean,
    prev_net_profit_lkr   numeric(14, 2),
    net_profit_3d_avg_lkr numeric(14, 2),
    trend                 text,
    status                text NOT NULL,
    prev_status           text,
    becoming_unprofitable boolean NOT NULL DEFAULT false,
    reasons               text[],
    batch_run_id          text NOT NULL,
    PRIMARY KEY (sim_date, vehicle_id)
);

-- Cost rates published by the batch layer and applied to live speed-layer data
CREATE TABLE batch.vehicle_cost_rates (
    vehicle_id          text PRIMARY KEY,
    as_of_date          date NOT NULL,
    fuel_cost_per_km    numeric(10, 3),
    maintenance_per_day numeric(12, 2),
    driver_share        numeric(4, 3) NOT NULL,
    at_risk_margin_pct  numeric(6, 2) NOT NULL,
    days_used           integer,
    batch_run_id        text NOT NULL,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE batch.expense_quarantine (
    sim_date     date NOT NULL,
    row_no       integer NOT NULL,
    record_id    text,
    vehicle_id   text,
    reasons      text[] NOT NULL,
    raw_record   jsonb,
    batch_run_id text NOT NULL,
    PRIMARY KEY (sim_date, row_no)
);

CREATE TABLE batch.daily_summary (
    sim_date                       date PRIMARY KEY,
    trips                          integer,
    revenue_lkr                    numeric(14, 2),
    driver_payout_lkr              numeric(14, 2),
    fuel_cost_lkr                  numeric(14, 2),
    maintenance_cost_lkr           numeric(14, 2),
    net_profit_lkr                 numeric(14, 2),
    active_vehicles                integer,
    fleet_utilization_pct          numeric(6, 2),
    unprofitable_vehicles          integer,
    at_risk_vehicles               integer,
    becoming_unprofitable_vehicles integer,
    no_cost_data_vehicles          integer,
    distance_mismatch_vehicles     integer,
    long_idle_episodes             integer,
    events_total                   bigint,
    events_invalid                 bigint,
    duplicates_removed             bigint,
    late_events                    bigint,
    expense_rows                   integer,
    expense_rows_valid             integer,
    expense_rows_quarantined       integer,
    batch_run_id                   text NOT NULL,
    computed_at                    timestamptz NOT NULL DEFAULT now()
);

-- ========================================================= serving layer
CREATE TABLE serving.report_index (
    sim_date     date PRIMARY KEY,
    html_path    text NOT NULL,
    csv_path     text NOT NULL,
    run_id       text,
    generated_at timestamptz NOT NULL DEFAULT now()
);

CREATE VIEW serving.v_sim_clock AS
SELECT ops.sim_now()               AS sim_now,
       ops.sim_now()::date         AS sim_date,
       speedup,
       round((86400 / speedup)::numeric, 1) AS real_seconds_per_sim_day,
       real_start,
       sim_start,
       rebase_count,
       last_heartbeat
FROM ops.sim_clock
WHERE id = 1;

-- Lambda merge: exact batch views for finished days, speed views otherwise
CREATE VIEW serving.v_zone_hourly AS
SELECT b.sim_date, b.zone, b.hour,
       (b.sim_date + make_interval(hours => b.hour))::timestamp AS window_start,
       b.pings, b.active_vehicles, b.on_trip_pings, b.enroute_pings, b.idle_pings,
       b.trips_completed, b.earnings_lkr, b.avg_speed_kmh, b.utilization_pct,
       'batch'::text AS source
FROM batch.zone_hourly b
UNION ALL
SELECT s.window_start::date, s.zone, extract(hour FROM s.window_start)::smallint, s.window_start,
       s.pings, s.active_vehicles, s.on_trip_pings, s.enroute_pings, s.idle_pings,
       s.trips_completed, s.earnings_lkr, s.avg_speed_kmh,
       round(100.0 * s.on_trip_pings / nullif(s.pings, 0), 2),
       'speed'::text
FROM speed.zone_hourly s
WHERE NOT EXISTS (SELECT 1 FROM batch.daily_summary d WHERE d.sim_date = s.window_start::date);

-- A vehicle is "offline" when it has not reported for 30 simulated minutes
-- (15 real seconds). Normal pipeline delay is 5 to 16 simulated minutes.
CREATE VIEW serving.v_vehicle_live AS
SELECT v.vehicle_id, v.driver_id, r.vehicle_type, r.plate_no, v.zone, v.lat, v.lon,
       v.speed_kmh, v.trip_id, v.status AS last_status,
       CASE WHEN v.last_event_ts < ops.sim_now() - make_interval(mins => 30) THEN 'offline'
            ELSE v.status END AS live_status,
       v.last_event_ts, v.idle_since,
       CASE WHEN v.status = 'idle' AND v.idle_since IS NOT NULL
            THEN round((extract(epoch FROM (v.last_event_ts - v.idle_since)) / 60)::numeric, 1)
       END AS idle_minutes,
       v.updated_at
FROM speed.vehicle_state v
JOIN ref.vehicles r USING (vehicle_id);

CREATE VIEW serving.v_fleet_live AS
SELECT ops.sim_now() AS sim_now,
       count(*) FILTER (WHERE live_status <> 'offline') AS active_vehicles,
       count(*) FILTER (WHERE live_status = 'on_trip')  AS on_trip,
       count(*) FILTER (WHERE live_status = 'enroute')  AS enroute,
       count(*) FILTER (WHERE live_status = 'idle')     AS idle,
       count(*) FILTER (WHERE live_status = 'offline')  AS offline,
       round(100.0 * count(*) FILTER (WHERE live_status = 'idle')
             / nullif(count(*) FILTER (WHERE live_status <> 'offline'), 0), 1) AS idle_ratio_pct,
       round(100.0 * count(*) FILTER (WHERE live_status = 'on_trip')
             / nullif(count(*) FILTER (WHERE live_status <> 'offline'), 0), 1) AS utilization_pct
FROM serving.v_vehicle_live;

-- Lambda merge for money: today's live revenue and kilometres (speed layer)
-- combined with each vehicle's cost rates from recent batch runs
CREATE VIEW serving.v_vehicle_profit_live AS
WITH clock AS (SELECT ops.sim_now() AS t),
base AS (
    SELECT d.vehicle_id, r.vehicle_type, r.plate_no, d.sim_date,
           d.trips_completed AS trips, d.revenue_lkr, d.distance_km,
           c.fuel_cost_per_km, c.maintenance_per_day, c.driver_share, c.at_risk_margin_pct,
           c.as_of_date AS cost_rates_from,
           LEAST(1.0, GREATEST(0.0, extract(epoch FROM (clock.t - d.sim_date::timestamp)) / 86400.0)) AS day_fraction
    FROM speed.vehicle_daily d
    CROSS JOIN clock
    JOIN ref.vehicles r USING (vehicle_id)
    LEFT JOIN batch.vehicle_cost_rates c USING (vehicle_id)
    WHERE d.sim_date = clock.t::date
),
calc AS (
    SELECT base.*,
           round(distance_km * fuel_cost_per_km + maintenance_per_day * day_fraction, 2) AS est_cost_lkr,
           round(revenue_lkr * (1 - driver_share)
                 - (distance_km * fuel_cost_per_km + maintenance_per_day * day_fraction), 2) AS projected_net_lkr
    FROM base
)
SELECT calc.*,
       round(100 * projected_net_lkr / nullif(revenue_lkr, 0), 1) AS projected_margin_pct,
       CASE WHEN fuel_cost_per_km IS NULL THEN 'NO_COST_DATA'
            WHEN projected_net_lkr < 0 THEN 'UNPROFITABLE'
            WHEN revenue_lkr <= 0 OR 100 * projected_net_lkr / revenue_lkr < at_risk_margin_pct THEN 'AT_RISK'
            ELSE 'PROFITABLE' END AS live_status
FROM calc;

CREATE VIEW serving.v_pipeline_freshness AS
-- before the first write, freshness counts from the moment the simulated clock started
SELECT coalesce((SELECT extract(epoch FROM now() - max(updated_at)) FROM speed.vehicle_state),
                (SELECT extract(epoch FROM now() - created_at) FROM ops.sim_clock))       AS speed_state_age_s,
       (SELECT extract(epoch FROM now() - max(updated_at)) FROM speed.zone_hourly)      AS speed_zone_age_s,
       (SELECT extract(epoch FROM now() - max(finished_at)) FROM ops.pipeline_runs
         WHERE stage = 'batch_layer' AND status = 'success')                           AS batch_success_age_s,
       (SELECT max(sim_date) FROM batch.daily_summary)                                  AS last_batch_sim_date,
       (SELECT count(*) FROM ops.dead_letters WHERE received_at > now() - interval '5 minutes') AS dead_letters_5m,
       (SELECT count(*) FROM speed.idle_alerts WHERE status = 'OPEN')                   AS open_idle_alerts;
