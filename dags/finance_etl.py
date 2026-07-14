from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finance_etl",
    description="Migrated UC4 FINANCE_ETL workflow from FINANCE_APP",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args=default_args,
    render_template_as_native_obj=True,
    is_paused_upon_creation=True,
    tags=["uc4-migration", "finance", "etl", "finance_app"],
    doc_md="""
    # FINANCE_ETL DAG

    Migrated from UC4 to Apache Airflow.
    Executes finance ETL workflow after successful completion
    of upstream LOAD_DIM DAG.
    """,
) as dag:

    wait_for_load_dim = ExternalTaskSensor(
        task_id="wait_for_load_dim",
        external_dag_id="load_dim",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",
        poke_interval=300,
        timeout=14400,
        check_existence=True,
    )

    run_finance_etl = BashOperator(
        task_id="run_finance_etl",
        bash_command="sh run_etl.sh",
        cwd="/opt/airflow/scripts/finance",
        append_env=True,
        do_xcom_push=False,
        env={
            "APP_NAME": "FINANCE_APP",
            "JOB_NAME": "FINANCE_ETL",
            "EXECUTION_ENV": "{{ var.value.environment }}",
        },
    )

    mark_success = EmptyOperator(
        task_id="mark_success",
    )

    wait_for_load_dim >> run_finance_etl >> mark_success

dag_id = "finance_etl"