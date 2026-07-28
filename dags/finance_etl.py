from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

UTC = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=120),
}

with DAG(
    dag_id="finance_etl",
    description="Migrated UC4 FINANCE_ETL workflow",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["uc4_migration", "finance", "etl"],
) as dag:

    wait_for_load_dim = ExternalTaskSensor(
        task_id="wait_for_load_dim",
        external_dag_id="load_dim",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=300,
        timeout=14400,
    )

    finance_etl = BashOperator(
        task_id="finance_etl",
        bash_command="cd /opt/airflow/scripts/finance && sh run_etl.sh",
        env={
            "APP_NAME": "FINANCE_APP",
            "TZ": "UTC",
        },
        append_env=True,
    )

    wait_for_load_dim >> finance_etl
