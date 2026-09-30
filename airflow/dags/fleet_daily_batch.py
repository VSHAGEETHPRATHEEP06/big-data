"""
### Fleet daily batch layer (Lambda architecture)

One run per **simulated day** (12 real minutes with the default speed-up of 120).

1. **resolve_target_day**: pick the oldest simulated day without batch results
   (or the day given in the `sim_date` parameter when re-running by hand).
2. **wait_for_expense_file**: sensor. Waits for the partner cost file and its
   manifest. Times out after 1.5 simulated days, which fails the task and
   raises an alert (health check for the daily source).
3. **validate_expense_file**: checksum, header and row count against the
   manifest, then copy into the immutable raw zone of the data lake.
4. **wait_for_raw_telemetry**: waits until the day plus a 2-hour grace period
   for late events is over and the master dataset has the day's files.
5. **spark_batch_recompute**: `spark-submit batch_layer.py`. Recomputes the
   exact day from raw data, joins telemetry with partner costs and publishes
   all batch views in one PostgreSQL transaction.
6. **data_quality_checks**: rules on the published results.
7. **build_daily_report**: HTML and CSV report for fleet managers.
8. **purge_speed_layer**: drops speed-layer rows the batch layer now owns.

Every task writes to `ops.pipeline_runs` (audit trail) and any failure is
stored in `ops.alert_notifications`.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import fleet_batch_tasks as tasks
import fleet_report
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from fleet_common.settings import get_settings

SETTINGS = get_settings()
SIM_DAY = timedelta(seconds=SETTINGS.sim_day_real_seconds)

# Small JVM for a short job on a laptop: C1 JIT only, serial GC, capped overheads.
SPARK_SUBMIT = (
    "spark-submit --master 'local[2]' --driver-memory 512m "
    "--conf spark.ui.enabled=false --conf spark.ui.showConsoleProgress=false "
    "--conf 'spark.driver.extraJavaOptions=-Duser.timezone=UTC -XX:+UseSerialGC -XX:TieredStopAtLevel=1 "
    "-XX:MaxMetaspaceSize=192m -XX:ReservedCodeCacheSize=64m -Xss512k "
    "-Dlog4j.configurationFile=/opt/fleet/spark/conf/log4j2.properties' "
    "/opt/fleet/spark/jobs/batch_layer.py "
    "--sim-date {{ ti.xcom_pull(task_ids='resolve_target_day') }} "
    "--run-id '{{ run_id }}'"
)

with DAG(
    dag_id="fleet_daily_batch",
    description="Lambda batch layer: partner costs + master dataset -> daily profitability report",
    schedule=SIM_DAY,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "fleet-data-team",
        "retries": 1,
        "retry_delay": timedelta(seconds=20),
        "on_failure_callback": tasks.notify_failure,
    },
    params={
        "sim_date": Param(
            None,
            type=["null", "string"],
            description="Re-compute this simulated day (YYYY-MM-DD). Leave empty for the next finished day.",
        )
    },
    tags=["lambda", "batch-layer", "fleet"],
    doc_md=__doc__,
) as dag:

    @task
    def resolve_target_day() -> str:
        day = tasks.resolve_target_day(get_current_context()["params"].get("sim_date"))
        if day is None:
            raise AirflowSkipException("every finished simulated day is already processed")
        return day

    target_day = resolve_target_day()

    wait_for_expense_file = PythonSensor(
        task_id="wait_for_expense_file",
        python_callable=tasks.expense_file_ready,
        op_kwargs={"sim_date": target_day},
        poke_interval=10,
        timeout=int(SIM_DAY.total_seconds() * 1.5),
        mode="poke",
        retries=0,
    )

    @task
    def validate_expense_file(sim_date: str) -> dict:
        return tasks.validate_and_archive(sim_date, get_current_context()["run_id"])

    wait_for_raw_telemetry = PythonSensor(
        task_id="wait_for_raw_telemetry",
        python_callable=tasks.raw_data_ready,
        op_kwargs={"sim_date": target_day},
        poke_interval=10,
        timeout=int(SIM_DAY.total_seconds()),
        mode="poke",
        retries=0,
    )

    spark_batch_recompute = BashOperator(
        task_id="spark_batch_recompute",
        bash_command=SPARK_SUBMIT,
        execution_timeout=timedelta(minutes=10),
    )

    @task
    def data_quality_checks(sim_date: str) -> dict:
        return tasks.data_quality_checks(sim_date, get_current_context()["run_id"])

    @task
    def build_daily_report(sim_date: str) -> dict:
        return fleet_report.build_report(sim_date, get_current_context()["run_id"])

    @task
    def purge_speed_layer(sim_date: str) -> dict:
        return tasks.purge_speed_layer(sim_date, get_current_context()["run_id"])

    validated = validate_expense_file(target_day)
    checked = data_quality_checks(target_day)
    report = build_daily_report(target_day)
    purged = purge_speed_layer(target_day)

    target_day >> wait_for_expense_file >> validated >> wait_for_raw_telemetry
    wait_for_raw_telemetry >> spark_batch_recompute >> checked >> report >> purged
