# FleetPulse: ride-hailing fleet analytics on a Lambda architecture

EC8203 Applied Big Data Engineering, mini project (use case 1: Ride-Hailing Fleet Operations).
Faculty of Engineering, University of Ruhuna.

| Member | Registration no. |
|---|---|
| Arivanan V. | EG/2021/4414 |
| Shageethpratheep V. | EG/2021/4809 |
| Shayanthavi T. | EG/2021/4811 |

FleetPulse simulates a ride-hailing fleet of 30 vehicles (tuk-tuks, mini cars and cars) in 12 Colombo
zones and answers the business question of the use case:

> *What is fleet utilization and earnings by area and time of day **right now**, and which vehicles are
> **becoming unprofitable** once yesterday's fuel and maintenance costs are factored in?*

* **Right now** comes from the **speed layer**: Kafka to Spark Structured Streaming to PostgreSQL, a few seconds behind real time.
* **Unprofitable vehicles** come from the **batch layer**: every simulated day Airflow runs a Spark batch job that
  recomputes the day exactly from the raw master dataset and joins it with the partner cost file.
* The **serving layer** (FastAPI, Grafana, a daily HTML/CSV report) merges both, for example live fares with
  cost rates from recent batch runs to project which vehicles lose money *today*.

---

## 1. Architecture

```
                 INGESTION                     PROCESSING                           STORAGE            SERVING
 ┌──────────────────────┐  JSON, key=vehicle_id  ┌────────────────────────────────┐
 │ stream_producer.py   │──► Kafka topic ────────►│ SPEED LAYER  Spark Structured  │── upserts ──►┌────────────┐   ┌─────────────┐
 │ 30 vehicles, 1 ping  │  fleet.telemetry.raw   │ Streaming (4 queries)          │              │ PostgreSQL │──►│ FastAPI     │
 │ per real second      │  (6 partitions)        │  zone x hour windows, vehicle  │              │ speed.*    │   │ /api/v1/... │
 └──────────────────────┘        │               │  day windows, live state, idle │              │ batch.*    │   ├─────────────┤
                                 │               │  alerts, dead-letter queue     │              │ serving.*  │──►│ Grafana     │
                                 │               └───────────┬────────────────────┘              │ ops.*      │   │ 3 dashboards│
                                 │ raw archive (append only)   │ invalid events                   └─────▲──────┘   ├─────────────┤
                                 ▼                             ▼                                        │          │ Daily HTML/ │
                  ┌──────────────────────────┐    Kafka topic fleet.telemetry.dlq                      │          │ CSV report  │
                  │ MASTER DATASET (Parquet) │                                                        │          └─────────────┘
 ┌────────────┐   │ lake/raw/telemetry       │   ┌────────────────────────────────────┐               │
 │ batch_     │──►│ lake/raw/expenses        │──►│ BATCH LAYER  Airflow DAG -> Spark   │── one txn ────┘
 │ producer.py│   └──────────────────────────┘   │ batch job per simulated day        │
 │ 1 CSV/day  │ landing/expenses (+manifest)     │ exact metrics, join with costs,    │
 └────────────┘                                  │ profitability, trends, report      │
                                                 └────────────────────────────────────┘
 OBSERVABILITY: JSON logs from every stage · Prometheus metrics (producers, Spark listener, API, Airflow via
 StatsD) · 14 alert rules -> Alertmanager -> API webhook -> ops.alert_notifications · run audit ops.pipeline_runs
```

| Layer | What runs | Code |
|---|---|---|
| Streaming source | Agent-based fleet simulator, Kafka producer (`acks=all`, idempotent, lz4) | `simulators/stream_producer.py`, `simulators/fleet_model.py` |
| Daily-batch source | Partner expense CSV + manifest (row count, SHA-256) once per simulated day | `simulators/batch_producer.py` |
| Ingestion | Apache Kafka 4.3 (KRaft, official GraalVM native image to save memory), topics `fleet.telemetry.raw` (6 partitions) and `fleet.telemetry.dlq` | `simulators/create_topics.py` |
| Speed layer | Spark 4.1 Structured Streaming: validation, watermark (10 sim min), de-duplication, zone enrichment, 1-hour and 1-day windows, idle alerts, exactly-once PostgreSQL sink | `spark/jobs/speed_layer.py` |
| Master dataset | Immutable Parquet (telemetry archived by Spark, partner files archived by Airflow) | `/data/lake/raw` |
| Batch layer | Airflow 3.3 DAG `fleet_daily_batch` runs `spark-submit batch_layer.py` | `airflow/dags/`, `spark/jobs/batch_layer.py` |
| Shared logic | Parsing, validation, zone lookup, metrics and business rules used by **both** layers | `spark/jobs/fleet_transforms.py`, `common/fleet_common/` |
| Serving | PostgreSQL 17 (schemas `speed`, `batch`, `serving`, `ops`, `ref`), FastAPI, Grafana 12, HTML/CSV report | `db/init/`, `api/`, `observability/grafana/` |
| Observability | Prometheus 3, Alertmanager, StatsD exporter, JSON logs | `observability/` |

## 2. Simulated clock

Time is compressed so a full business day fits in a demo. The default speed-up is **120**:

| Real time | Simulated time |
|---|---|
| 1 second | 2 minutes |
| 30 seconds | 1 hour |
| **12 minutes** | **1 day** |

The simulation starts on **Tuesday 1 September 2026 at 06:00** (Sri Lanka time). Day 1 is therefore 18 hours
long and ends 9 real minutes after start-up. Every later day takes 12 real minutes. All simulated timestamps
are Sri Lanka local time written without an offset. The clock anchor lives in PostgreSQL (`ops.sim_clock`),
so every container agrees on "now" (`SELECT ops.sim_now();`). If the stack is stopped for more than five
minutes, simulated time pauses and continues from where it stopped after the restart. During a shorter stop
simulated time keeps running. Change `SIM_SPEEDUP` in `.env` to make days shorter (for example 288 gives
5-minute days).

## 3. Quick start

**Needs:** Docker Desktop with at least 4 GB of memory for Docker, about 12 GB of free disk and internet for
the first build. On an 8 GB laptop please close heavy apps (browsers with many tabs) while the stack runs.

```bash
cp .env.example .env            # demo settings and local passwords
docker compose up -d --build    # first build takes 10 to 15 minutes
docker compose ps               # wait until services are "healthy" (about 2 minutes)
```

The `Makefile` has the same steps as short commands (`make up`, `make ps`, `make test`, `make down`,
`make reset`, see `make help`). A minute-by-minute plan for the demo video is in [DEMO.md](DEMO.md), and
sample outputs of a real run (daily reports in HTML and CSV) are in [docs/sample_output](docs/sample_output).

| Service | URL | Login |
|---|---|---|
| Grafana dashboards | http://localhost:3000 | anonymous viewer (admin / `GRAFANA_ADMIN_PASSWORD`) |
| Airflow | http://localhost:8080 | `admin` / `admin` (from `.env`) |
| Serving API docs | http://localhost:8000/docs | none |
| Daily reports | http://localhost:8000/api/v1/reports | none |
| Prometheus (alerts) | http://localhost:9090/alerts | none |
| Alertmanager | http://localhost:9093 | none |
| Spark UI (Structured Streaming tab) | http://localhost:4040 | none |
| PostgreSQL | `localhost:5433`, db `fleet` | `fleet` / `fleet_pw` |
| Kafka (from the host) | `localhost:29092` | none |

Stop with `docker compose stop` (keeps data) or `docker compose down -v` (deletes all data).

## 4. What happens after start-up

| Real minutes after start | What you can see |
|---|---|
| 0 to 1 | Topics created, simulated clock starts at 06:00, vehicles come online |
| 1 onwards | Grafana "Fleet operations (live)": map, utilization, fares by zone, idle alerts |
| about 9 | Simulated midnight. The partner expense file for day 1 lands 10 to 45 seconds later |
| about 10 to 12 | Airflow `fleet_daily_batch` validates the file, runs the Spark batch job, checks quality and writes the report |
| every 12 after that | One more simulated day and one more daily report |

The "becoming unprofitable" story needs a few days of history: vehicle **V007** is a degrading mini car that loses
fuel efficiency and spends more time in the garage every day, so it turns from profitable to unprofitable by
day 3 or 4. Leave the stack running for about 45 minutes to see it. Other built-in stories: **V013** (high
maintenance), **V026** (reports 30 to 40% more kilometres than GPS shows, possible fuel fraud), **V021** and
**V027** (drivers who stay idle for long periods).

## 5. Demo script (5 to 10 minutes)

```bash
# 1. Ingestion: topics, partitions and live events
docker run --rm --network fleetpulse_default apache/kafka:4.3.1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic fleet.telemetry.raw
docker run --rm --network fleetpulse_default apache/kafka:4.3.1 \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic fleet.telemetry.raw \
  --max-messages 3 --property print.key=true --property print.partition=true
docker run --rm --network fleetpulse_default apache/kafka:4.3.1 \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic fleet.telemetry.dlq \
  --from-beginning --max-messages 2

# 2. Structured logs of every stage
docker compose logs --no-log-prefix stream-producer | tail -3
docker compose logs --no-log-prefix spark-streaming | grep vehicle_state_batch | tail -2

# 3. Serving API
curl -s localhost:8000/api/v1/fleet/realtime | python3 -m json.tool | head -30
curl -s localhost:8000/api/v1/profitability/live?status=UNPROFITABLE | python3 -m json.tool
curl -s localhost:8000/api/v1/pipeline/status | python3 -m json.tool | head -40

# 4. Batch layer: re-compute any finished day by hand (Lambda "recompute from raw data")
docker compose exec airflow airflow dags trigger fleet_daily_batch -c '{"sim_date": "2026-09-01"}'

# 5. Failure and alert: stop the streaming source, watch alerts fire, then start it again
docker compose stop stream-producer      # about 75 s: ServiceDown and NoTelemetryIngested, about 2 min: SpeedLayerStale
curl -s localhost:9090/api/v1/alerts | python3 -m json.tool | grep alertname
curl -s localhost:8000/api/v1/alerts | python3 -m json.tool | head -40   # what reached the webhook (NoTelemetryIngested is inhibited)
docker compose start stream-producer
```

Open the daily report of a finished day at `http://localhost:8000/reports/2026-09-01.html`.

## 6. Observability

* **Structured logs.** Every Python component writes JSON lines with the same keys
  (`ts`, `level`, `service`, `stage`, `event` plus context such as `batch_id`, `run_id`, `sim_date`).
  Spark JVM logs use Spark's JSON layout. Example: `docker compose logs --no-log-prefix spark-streaming | jq 'select(.level=="ERROR")'`.
* **Metrics.** Producers (events, errors, delivery latency, injected faults), Spark (a `StreamingQueryListener`
  exports input rate, batch duration, state rows, rows dropped by the watermark, Kafka lag and end-to-end latency),
  Airflow (StatsD exporter) and the API (request rate and latency, plus business and freshness gauges read from
  PostgreSQL).
* **Alerts** (`observability/prometheus/alert_rules.yml`): service down, no telemetry ingested, speed layer
  stale, streaming query down, consumer lag, slow micro-batches, end-to-end latency, producer error rate,
  invalid event rate, batch layer behind, expense feed missing, Airflow scheduler down, vehicle idle too long,
  vehicles becoming unprofitable. Alertmanager sends them to the API webhook, which logs and stores them.
* **Tracing and lineage.** Every event has an `event_id` (also a Kafka header `trace_id`). The master dataset keeps
  Kafka topic, partition, offset and append time for every event. Speed-layer batches are recorded in
  `ops.speed_batches`, every orchestrated step in `ops.pipeline_runs`, and every partner file with its SHA-256 in
  `ops.batch_files`. Each batch-view row carries the Airflow `batch_run_id`.

## 7. Tests

```bash
docker compose --profile test run --rm tests     # 39 tests: simulators, Spark transforms, batch layer, API, DAG logic
```

## 8. Configuration

All settings are in `.env` (see `.env.example`). The most useful ones:

| Variable | Default | Meaning |
|---|---|---|
| `SIM_SPEEDUP` | 120 | simulated seconds per real second |
| `INVALID_EVENT_RATE` / `DUPLICATE_EVENT_RATE` | 0.005 / 0.01 | share of broken and repeated events sent on purpose |
| `OUTAGE_RATE_PER_SIM_HOUR` | 0.03 | chance per vehicle and hour of a 20 to 60 minute network outage (late events) |
| `LATE_FILE_RATE` | 0.0 | set to 1.0 to make every partner file late (tests the "feed missing" alert) |
| `WATERMARK_MINUTES` | 10 | lateness the speed layer accepts (simulated minutes) |
| `IDLE_ALERT_MINUTES` | 90 | idle streak that raises an alert (simulated minutes) |
| `DRIVER_SHARE` / `AT_RISK_MARGIN_PCT` | 0.30 / 15 | profit rules |

## 9. Project structure

```
common/fleet_common/   settings, JSON logging, simulated clock, zones, registry, business rules
config/                zones.json (12 Colombo zones), fleet_registry.csv (30 vehicles)
simulators/            stream_producer.py, batch_producer.py, fleet_model.py, create_topics.py
spark/jobs/            speed_layer.py, batch_layer.py, fleet_transforms.py (shared), speed_state.py
airflow/               dags/fleet_daily_batch.py, include/ (task logic, report builder, template)
api/                   FastAPI serving layer
db/init/               PostgreSQL schema, views, reference data, grants
observability/         Prometheus rules, Alertmanager, StatsD mapping, Grafana dashboards (+ generator)
tests/                 pytest suite
report/                LaTeX project report (main.tex, compiled to main.pdf), tools/make_numbers.py
docs/sample_output/    daily reports (HTML and CSV) from our 15-day test run
```

## 10. Troubleshooting

* **Services restart or are very slow.** Docker needs about 3 GB of free memory. Give Docker Desktop more memory
  (Settings, Resources) or close other apps. `docker stats` shows memory per container.
* **Docker hangs on macOS when a container mounts a folder under `~/Downloads`.** FleetPulse does not bind-mount
  host folders (all configuration is built into the images), so this does not affect it. If Docker is stuck,
  use `docker desktop restart`.
* **Start from zero:** `docker compose down -v && docker compose up -d --build`.

## 11. Individual contributions

| Member | Main work |
|---|---|
| Arivanan V. (EG/2021/4414) | Data simulators (fleet model, streaming and daily-batch sources) and Kafka ingestion |
| Shageethpratheep V. (EG/2021/4809) | Spark speed layer and batch layer, Lambda architecture decision |
| Shayanthavi T. (EG/2021/4811) | Airflow orchestration, PostgreSQL and API serving layer, Grafana and observability |
| All three | Report, testing and the demo |
