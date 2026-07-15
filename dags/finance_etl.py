from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

local_tz = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="finance_etl",
    description="Migrated UC4 workflow for FINANCE_ETL from FINANCE_APP",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["finance", "uc4_migration", "finance_app", "etl"],
) as dag:

    wait_for_load_dim = ExternalTaskSensor(
        task_id="wait_for_load_dim",
        external_dag_id="load_dim",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=300,
        timeout=7200,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    run_finance_etl = BashOperator(
        task_id="run_finance_etl",
        bash_command="cd /opt/airflow/dags/scripts/finance && sh run_etl.sh",
        append_env=True,
        execution_timeout=timedelta(hours=4),
        retries=3,
        retry_delay=timedelta(minutes=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(hours=1),
    )

    wait_for_load_dim >> run_finance_etl