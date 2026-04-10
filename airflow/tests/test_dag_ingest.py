"""Tests for the olist_ingest_raw DAG."""

from pathlib import Path

import pytest

from airflow.models import DagBag

DAGS_DIR = str(Path(__file__).parents[1] / "dags")
DAG_ID = "olist_ingest_raw"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.get_dag(DAG_ID)


def test_dag_no_import_errors(dagbag):
    """DagBag must load without any import errors."""
    assert dagbag.import_errors == {}, (
        f"Import errors found: {dagbag.import_errors}"
    )


def test_dag_loaded(dag):
    """DAG object must exist in the DagBag."""
    assert dag is not None, f"DAG '{DAG_ID}' not found in DagBag"


def test_dag_has_correct_tags(dag):
    assert set(dag.tags) == {"olist", "ingestion", "raw"}


def test_dag_task_count(dag):
    """1 check + 9 load tasks + 1 validate = 11 tasks."""
    assert len(dag.tasks) == 11


def test_check_files_task_exists(dag):
    task_ids = {t.task_id for t in dag.tasks}
    assert "check_files_exist" in task_ids


def test_all_load_tasks_exist(dag):
    expected = {
        "load_orders",
        "load_customers",
        "load_order_items",
        "load_products",
        "load_sellers",
        "load_order_payments",
        "load_order_reviews",
        "load_geolocation",
        "load_category_translation",
    }
    task_ids = {t.task_id for t in dag.tasks}
    assert expected.issubset(task_ids)


def test_validate_task_exists(dag):
    task_ids = {t.task_id for t in dag.tasks}
    assert "validate_row_counts" in task_ids


def test_dag_no_catchup(dag):
    assert dag.catchup is False


def test_dag_schedule(dag):
    assert dag.schedule_interval == "@daily"


def test_check_task_is_upstream_of_load_tasks(dag):
    """check_files_exist must be a direct upstream of every load task."""
    load_task_ids = {
        "load_orders", "load_customers", "load_order_items", "load_products",
        "load_sellers", "load_order_payments", "load_order_reviews",
        "load_geolocation", "load_category_translation",
    }
    check_task = dag.get_task("check_files_exist")
    downstream_ids = {t.task_id for t in check_task.downstream_list}
    assert load_task_ids.issubset(downstream_ids)


def test_validate_task_is_downstream_of_all_load_tasks(dag):
    """validate_row_counts must depend on all 9 load tasks."""
    validate_task = dag.get_task("validate_row_counts")
    upstream_ids = {t.task_id for t in validate_task.upstream_list}
    load_task_ids = {
        "load_orders", "load_customers", "load_order_items", "load_products",
        "load_sellers", "load_order_payments", "load_order_reviews",
        "load_geolocation", "load_category_translation",
    }
    assert load_task_ids.issubset(upstream_ids)
