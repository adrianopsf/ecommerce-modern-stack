"""DAG: olist_dbt_transformations — runs dbt layers staging → intermediate → mart."""

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/olist_dw"
DBT_PROFILES_DIR = "/opt/airflow/dbt/olist_dw"
DBT_BASE = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="olist_dbt_transformations",
    description="Runs dbt models: staging → intermediate → mart, with tests and docs",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "dbt", "transformation"],
    default_args=default_args,
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"dbt deps {DBT_BASE}",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"dbt run --select staging {DBT_BASE}",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"dbt test --select staging {DBT_BASE}",
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"dbt run --select intermediate {DBT_BASE}",
    )

    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=f"dbt run --select mart {DBT_BASE}",
    )

    dbt_test_mart = BashOperator(
        task_id="dbt_test_mart",
        bash_command=f"dbt test --select mart {DBT_BASE}",
    )

    dbt_generate_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=f"dbt docs generate {DBT_BASE}",
    )

    (
        dbt_deps
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_intermediate
        >> dbt_run_mart
        >> dbt_test_mart
        >> dbt_generate_docs
    )
