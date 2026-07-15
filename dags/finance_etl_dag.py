from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

LOCAL_TZ = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="finance_etl_dag",
    description="Migrated UC4 workflow for FINANCE_ETL execution and dependency orchestration",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    dagrun_timeout=timedelta(hours=2),
    tags=["finance", "etl", "uc4_migration"],
) as dag:

    load_dim_dependency = ExternalTaskSensor(
        task_id="load_dim_dependency",
        external_dag_id="load_dim_dag",
        external_task_id="load_dim",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=300,
        timeout=7200,
    )

    finance_etl = BashOperator(
        task_id="finance_etl",
        bash_command="cd /opt/airflow/jobs/finance && sh run_etl.sh",
        env={
            "TZ": "UTC",
            "APP_NAME": "FINANCE_APP",
        },
        execution_timeout=timedelta(hours=1),
        append_env=True,
        do_xcom_push=False,
    )

    load_dim_dependency >> finance_etl

dag_id = dag.dag_id