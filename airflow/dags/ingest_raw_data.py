"""DAG: olist_ingest_raw — ingests Olist raw CSVs into PostgreSQL raw schema."""

from datetime import datetime, timedelta
import logging
from pathlib import Path

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import DAG
from airflow.operators.python import get_current_context

from plugins.operators.olist_operator import OlistPostgresOperator

log = logging.getLogger(__name__)

DATA_RAW_PATH = "/opt/airflow/data/raw"

# Maps task_id -> (csv_filename, raw_table_name)
# Table names mirror the CSV filenames (without .csv) to match dbt source definitions.
OLIST_FILES: dict[str, tuple[str, str]] = {
    "load_orders": ("olist_orders_dataset.csv", "olist_orders_dataset"),
    "load_customers": ("olist_customers_dataset.csv", "olist_customers_dataset"),
    "load_order_items": ("olist_order_items_dataset.csv", "olist_order_items_dataset"),
    "load_products": ("olist_products_dataset.csv", "olist_products_dataset"),
    "load_sellers": ("olist_sellers_dataset.csv", "olist_sellers_dataset"),
    "load_order_payments": ("olist_order_payments_dataset.csv", "olist_order_payments_dataset"),
    "load_order_reviews": ("olist_order_reviews_dataset.csv", "olist_order_reviews_dataset"),
    "load_geolocation": ("olist_geolocation_dataset.csv", "olist_geolocation_dataset"),
    "load_category_translation": (
        "product_category_name_translation.csv",
        "product_category_name_translation",
    ),
}

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="olist_ingest_raw",
    description="Ingests Olist raw CSVs into PostgreSQL raw schema",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "ingestion", "raw"],
    default_args=default_args,
) as dag:

    @task(task_id="check_files_exist")
    def check_files_exist():
        """Verify all 9 Olist CSVs exist before starting load tasks."""
        raw_path = Path(DATA_RAW_PATH)
        missing = [
            filename
            for filename, _ in OLIST_FILES.values()
            if not (raw_path / filename).exists()
        ]
        if missing:
            raise AirflowSkipException(
                f"Missing CSV files in {DATA_RAW_PATH}: {missing}. "
                "Download the Olist dataset and place CSVs in data/raw/."
            )
        log.info("All %d CSV files found in %s", len(OLIST_FILES), DATA_RAW_PATH)

    @task(task_id="validate_row_counts")
    def validate_row_counts():
        """Pull XCom row counts from all load tasks and assert none are empty."""
        ctx = get_current_context()
        ti = ctx["ti"]

        results: dict[str, int] = {}
        for task_id, (_, table_name) in OLIST_FILES.items():
            row_count = ti.xcom_pull(task_ids=task_id) or 0
            results[table_name] = row_count
            log.info("Ingestion complete: %s: %d rows", table_name, row_count)

        empty_tables = [t for t, r in results.items() if r == 0]
        if empty_tables:
            raise ValueError(f"Tables loaded with 0 rows: {empty_tables}")

        return results

    check = check_files_exist()

    load_tasks = [
        OlistPostgresOperator(
            task_id=task_id,
            data_path=f"{DATA_RAW_PATH}/{filename}",
            table_name=table_name,
        )
        for task_id, (filename, table_name) in OLIST_FILES.items()
    ]

    validate = validate_row_counts()

    check >> load_tasks >> validate
