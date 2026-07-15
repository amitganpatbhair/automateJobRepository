from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState


def validate_configuration():
    required_values = {
        'dag_id': 'finance_app_daily_cal',
        'external_dag_id': 'load_dim_dag',
        'external_task_id': 'load_dim',
        'bash_command': 'sh run_etl.sh',
        'cwd': '/opt/airflow/dags/scripts/finance',
    }

    for key, value in required_values.items():
        if value is None or not str(value).strip():
            raise ValueError(f'Missing required configuration: {key}')


validate_configuration()

DEFAULT_ARGS = {
    'owner': 'finance',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

with DAG(
    dag_id='finance_app_daily_cal',
    description='Migrated UC4 workflow for FINANCE_ETL under FINANCE_APP',
    default_args=DEFAULT_ARGS,
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['finance', 'uc4-migration', 'etl', 'shell'],
    default_view='graph',
    orientation='LR',
) as dag:

    wait_for_load_dim = ExternalTaskSensor(
        task_id='wait_for_load_dim',
        external_dag_id='load_dim_dag',
        external_task_id='load_dim',
        allowed_states=[DagRunState.SUCCESS],
        failed_states=['failed'],
        mode='reschedule',
        poke_interval=300,
        timeout=7200,
        check_existence=True,
    )

    finance_etl = BashOperator(
        task_id='finance_etl',
        bash_command='sh run_etl.sh',
        append_env=True,
        cwd='/opt/airflow/dags/scripts/finance',
        execution_timeout=timedelta(minutes=120),
    )

    wait_for_load_dim >> finance_etl