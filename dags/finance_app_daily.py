from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finance_app_daily",
    description="Migrated UC4 FINANCE_ETL workflow for Finance application ETL processing",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "etl", "uc4-migration"],
) as dag:

    load_dim = BashOperator(
        task_id="load_dim",
        bash_command="echo 'LOAD_DIM dependency completed successfully'",
        execution_timeout=timedelta(minutes=60),
    )

    finance_etl = BashOperator(
        task_id="finance_etl",
        bash_command="cd /opt/airflow/scripts/finance && sh run_etl.sh",
        env={
            "APP_NAME": "FINANCE_APP",
            "TZ": "UTC",
        },
        execution_timeout=timedelta(minutes=60),
    )

    load_dim >> finance_etl

generated_dag_id = "finance_app_daily"
