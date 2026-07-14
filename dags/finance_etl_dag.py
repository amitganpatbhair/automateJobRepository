from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="finance_etl_dag",
    description="Migrated UC4 workflow for FINANCE_ETL",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["finance", "uc4-migration", "etl"],
) as dag:

    load_dim = BashOperator(
        task_id="load_dim",
        bash_command="echo 'Placeholder task for LOAD_DIM dependency'",
    )

    finance_etl = BashOperator(
        task_id="finance_etl",
        bash_command="sh run_etl.sh",
        env={
            "TZ": "UTC",
        },
        execution_timeout=timedelta(minutes=120),
    )

    load_dim >> finance_etl
