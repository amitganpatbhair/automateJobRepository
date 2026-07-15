from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=120),
}

with DAG(
    dag_id="finance_app_dag",
    description="Migrated UC4 workflow for FINANCE_ETL execution",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    tags=["finance", "uc4_migration", "etl"],
    default_view="graph",
) as dag:

    load_dim = BashOperator(
        task_id="load_dim",
        bash_command="echo 'LOAD_DIM completed successfully'",
        append_env=True,
        do_xcom_push=False,
    )

    finance_etl = BashOperator(
        task_id="finance_etl",
        bash_command="sh /opt/airflow/scripts/finance/run_etl.sh",
        cwd="/opt/airflow/scripts/finance",
        append_env=True,
        do_xcom_push=False,
    )

    load_dim >> finance_etl