from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule


LOCAL_TZ = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}


with DAG(
    dag_id="finance_etl",
    description="Migrated UC4 workflow for FINANCE_ETL execution",
    start_date=datetime(2024, 1, 1, tzinfo=LOCAL_TZ),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    dagrun_timeout=timedelta(minutes=120),
    default_args=default_args,
    tags=["finance", "etl", "uc4-migration"],
    render_template_as_native_obj=True,
) as dag:

    load_dim = ExternalTaskSensor(
        task_id="wait_for_load_dim",
        external_dag_id="load_dim",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
    )

    finance_etl_task = BashOperator(
        task_id="finance_etl_task",
        bash_command="sh run_etl.sh",
        cwd="/opt/airflow/scripts/finance",
        append_env=True,
        env={
            "FINANCE_ENV": "prod",
            "TZ": "UTC",
        },
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    load_dim >> finance_etl_task