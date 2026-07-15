from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

local_tz = pendulum.timezone("UTC")

with DAG(
    dag_id="finance_app_finance_etl",
    description="Migrated UC4 workflow for FINANCE_ETL",
    start_date=pendulum.datetime(2025, 1, 1, 0, 0, tz=local_tz),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["uc4_migration", "finance", "etl"],
    render_template_as_native_obj=True,
) as dag:

    load_dim = BashOperator(
        task_id="load_dim",
        bash_command='echo "LOAD_DIM dependency satisfied"',
        execution_timeout=timedelta(minutes=30),
        append_env=True,
        do_xcom_push=False,
    )

    finance_etl = BashOperator(
        task_id="finance_etl",
        bash_command="sh run_etl.sh",
        execution_timeout=timedelta(minutes=120),
        append_env=True,
        do_xcom_push=False,
    )

    load_dim >> finance_etl
