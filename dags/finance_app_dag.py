from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finance_app_dag_update",
    description="UC4 migrated DAG for FINANCE_APP",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["finance", "uc4-migration"],
    max_active_runs=1,
) as dag:

    load_dim = BashOperator(
        task_id="load_dim",
        bash_command="echo LOAD_DIM placeholder",
    )

    finance_etl = BashOperator(
        task_id="finance_etl",
        bash_command="sh /home/airflow/gcs/Shellscripts/run_etl.sh",
        queue="default",
    )

    load_dim >> finance_etl
