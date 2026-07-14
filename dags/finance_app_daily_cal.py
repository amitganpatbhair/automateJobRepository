from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'finance',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

with DAG(
    dag_id='finance_app_daily_cal',
    description='Airflow migration of UC4 job FINANCE_ETL for FINANCE_APP',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['finance', 'etl', 'uc4-migration'],
) as dag:

    load_dim = EmptyOperator(
        task_id='load_dim'
    )

    finance_etl = BashOperator(
        task_id='finance_etl',
        bash_command='sh run_etl.sh',
        env={
            'APP_NAME': 'FINANCE_APP',
            'JOB_NAME': 'FINANCE_ETL',
            'EXECUTION_TIMEZONE': 'UTC',
        },
        execution_timeout=timedelta(hours=1),
        append_env=True,
    )

    load_dim >> finance_etl