"""DAG: olist_full_pipeline — weekly orchestration of ingest + dbt via TriggerDagRunOperator."""

from datetime import datetime, timedelta
import logging

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "sla": timedelta(hours=2),
}

with DAG(
    dag_id="olist_full_pipeline",
    description="Weekly full ELT pipeline: triggers ingest then dbt transformations",
    schedule_interval="0 6 * * 1",  # Every Monday at 06:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "full-pipeline"],
    default_args=default_args,
) as dag:

    trigger_ingest = TriggerDagRunOperator(
        task_id="trigger_ingest",
        trigger_dag_id="olist_ingest_raw",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="olist_dbt_transformations",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
    )

    @task(task_id="notify_success")
    def notify_success(**context):
        execution_date = context.get("execution_date") or context.get("logical_date")
        log.info("Full pipeline completed successfully at %s", execution_date)

    trigger_ingest >> trigger_dbt >> notify_success()
