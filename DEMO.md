# Demo script (5 to 10 minutes)

A plan for the demo video or a live demo. Start the stack about **40 minutes before** recording
(`make up` or `docker compose up -d --build`), so that three or four simulated days have finished and the
daily reports show the "becoming unprofitable" story. Open these tabs before you start:
Grafana (http://localhost:3000), Airflow (http://localhost:8080, admin/admin), the API docs
(http://localhost:8000/docs), Prometheus alerts (http://localhost:9090/alerts) and a terminal in the
project folder.

| Time | Show | Say |
|---|---|---|
| 0:00 | README architecture diagram | The use case (ride-hailing fleet in Colombo), the business question, and why we chose Lambda: a live stream plus a daily cost file, exact money numbers, cheap recompute. |
| 1:00 | Terminal: `docker compose ps` | 11 running services (the 2 set-up jobs have finished), the ones with a health check show healthy. Simulated clock: 1 real second = 2 simulated minutes, 1 day = 12 minutes. |
| 1:30 | Terminal: Kafka commands from README section 5 | Topic with 6 partitions, key = vehicle id. Live JSON events. The dead-letter topic with broken events and the reason. |
| 2:30 | Grafana "Fleet operations (live)" | Speed layer: vehicles on the map, utilisation, fares by zone and hour, idle alerts, profit projection for today (Lambda merge). |
| 3:30 | Spark UI http://localhost:4040/StreamingQuery/ | Four streaming queries, watermark, micro-batches every 5 to 10 seconds. |
| 4:00 | Airflow DAG `fleet_daily_batch` (grid and graph) | Batch layer: sensor waits for the partner file, checksum check, Spark batch job, quality checks, report, purge. |
| 5:00 | Daily report http://localhost:8000/api/v1/reports | Vehicles that need attention and why: V007 degrading, V013 high maintenance, V026 reported km do not match GPS. Speed vs batch reconciliation. |
| 6:00 | Grafana "Daily profitability" | Pick a day, net profit by vehicle, problem vehicles by day. |
| 6:30 | Terminal: `docker compose exec airflow airflow dags trigger fleet_daily_batch -c '{"sim_date": "2026-09-01"}'` | Recompute any day from the immutable master dataset. |
| 7:00 | Terminal: `docker compose stop stream-producer`, then Prometheus alerts and Grafana "Pipeline health" | Observability: metrics of every stage. After about 75 s ServiceDown (stream-producer) and NoTelemetryIngested fire, and after about 2 minutes SpeedLayerStale. Alertmanager sends ServiceDown and SpeedLayerStale to the API webhook and holds back NoTelemetryIngested, because an inhibition rule knows the producer itself is down. |
| 9:00 | Terminal: `docker compose start stream-producer`, JSON logs with `jq` | Alerts resolve. Structured logs of every stage, lineage tables `ops.pipeline_runs` and `ops.batch_files`. |
| 9:40 | Terminal: `make test` (or show the last result) | 39 automated tests. Wrap up with limitations and production changes. |

Tip: run `make test` before recording and show the result instead of waiting for it on camera.
