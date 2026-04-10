"""DAG: Ingest raw Olist CSVs into PostgreSQL raw schema."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def load_raw_data(**context):
    """Load all raw CSVs from DATA_RAW_PATH into PostgreSQL raw schema."""
    import os

    from loguru import logger

    from plugins.operators.olist_operator import OlistRawLoader

    data_path = os.getenv("DATA_RAW_PATH", "/opt/airflow/data/raw")
    loader = OlistRawLoader(data_path=data_path)
    tables_loaded = loader.load_all()
    logger.info(f"Loaded {len(tables_loaded)} tables: {tables_loaded}")
    return tables_loaded


with DAG(
    dag_id="ingest_raw_data",
    description="Ingests Olist raw CSVs into PostgreSQL raw schema",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["olist", "ingest", "raw"],
) as dag:
    ingest_task = PythonOperator(
        task_id="load_raw_csvs_to_postgres",
        python_callable=load_raw_data,
    )
