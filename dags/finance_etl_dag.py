from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

with DAG(
    dag_id="finance_etl_dag",
    description="Airflow migration of UC4 FINANCE_ETL shell-based ETL workflow.",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "etl", "uc4_migration", "shell_job"],
    orientation="LR",
) as dag:

    wait_for_load_dim = ExternalTaskSensor(
        task_id="wait_for_load_dim",
        external_dag_id="load_dim_dag",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
    )

    run_finance_etl = BashOperator(
        task_id="run_finance_etl",
        bash_command="cd /opt/airflow/scripts/finance && sh run_etl.sh",
        append_env=True,
        env={
            "ENVIRONMENT": "{{ var.value.finance_environment }}",
            "FINANCE_SCRIPT_BASE_PATH": "{{ var.value.finance_script_base_path }}",
        },
        do_xcom_push=False,
    )

    wait_for_load_dim >> run_finance_etl