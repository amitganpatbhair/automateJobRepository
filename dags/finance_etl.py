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
}

with DAG(
    dag_id="finance_etl",
    description="Migrated UC4 ETL workflow for FINANCE_APP",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    dagrun_timeout=timedelta(minutes=120),
    tags=["uc4_migration", "finance", "etl"],
) as dag:

    wait_for_load_dim = ExternalTaskSensor(
        task_id="wait_for_load_dim",
        external_dag_id="load_dim",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=300,
        timeout=3600,
        execution_delta=None,
        check_existence=True,
    )

    finance_etl_task = BashOperator(
        task_id="finance_etl_task",
        bash_command="cd /opt/airflow/dags/scripts/finance && sh run_etl.sh",
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=60),
        env={
            "ENVIRONMENT": "prod",
        },
        append_env=True,
        do_xcom_push=False,
        pool="finance_etl_pool",
        priority_weight=5,
        queue="default",
    )

    wait_for_load_dim >> finance_etl_task