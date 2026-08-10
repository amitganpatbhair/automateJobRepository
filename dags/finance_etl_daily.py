from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

UTC = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finance_etl_daily",
    description="Migrated UC4 FINANCE_ETL workflow for Finance application batch ETL processing.",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "etl", "uc4-migration", "daily"],
) as dag:

    load_dim_task = BashOperator(
        task_id="load_dim_task",
        bash_command="echo 'LOAD_DIM placeholder task completed successfully'",
        append_env=True,
        do_xcom_push=False,
    )

    finance_etl_task = BashOperator(
        task_id="finance_etl_task",
        bash_command="sh run_etl.sh",
        append_env=True,
        do_xcom_push=False,
    )

    load_dim_task >> finance_etl_task