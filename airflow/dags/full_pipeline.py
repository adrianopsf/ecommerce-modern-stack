"""DAG: Full ELT pipeline — ingest raw data then run dbt transformations."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DBT_PROJECT_DIR = "/opt/airflow/dbt/olist_dw"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def load_raw_data(**context):
    import os

    from loguru import logger

    from plugins.operators.olist_operator import OlistRawLoader

    data_path = os.getenv("DATA_RAW_PATH", "/opt/airflow/data/raw")
    loader = OlistRawLoader(data_path=data_path)
    tables_loaded = loader.load_all()
    logger.info(f"Loaded {len(tables_loaded)} tables into raw schema")
    return tables_loaded


with DAG(
    dag_id="full_pipeline",
    description="Full ELT pipeline: ingest Olist CSVs + dbt transformations",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["olist", "pipeline", "full"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=load_raw_data,
    )

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"dbt run --select staging --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"dbt run --select intermediate --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=f"dbt run --select mart --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_all",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    ingest >> dbt_staging >> dbt_intermediate >> dbt_mart >> dbt_test
