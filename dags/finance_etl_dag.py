from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="finance_etl_dag",
    description="Production-grade Airflow DAG migrated from UC4 FINANCE_ETL",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["finance", "etl", "uc4_migration"],
) as dag:

    load_dim = BashOperator(
        task_id="load_dim",
        bash_command="sh load_dim.sh",
        execution_timeout=timedelta(hours=2),
    )

    finance_etl = BashOperator(
        task_id="finance_etl",
        bash_command="sh run_etl.sh",
        execution_timeout=timedelta(hours=2),
    )

    load_dim >> finance_etl
