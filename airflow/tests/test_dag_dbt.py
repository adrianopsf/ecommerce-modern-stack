"""Tests for the olist_dbt_transformations DAG."""

from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.operators.bash import BashOperator

DAGS_DIR = str(Path(__file__).parents[1] / "dags")
DAG_ID = "olist_dbt_transformations"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.get_dag(DAG_ID)


def test_dag_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"


def test_dag_loaded(dag):
    assert dag is not None, f"DAG '{DAG_ID}' not found in DagBag"


def test_dag_has_seven_tasks(dag):
    assert len(dag.tasks) == 7


def test_all_tasks_are_bash_operators(dag):
    """Every task in the dbt DAG must be a BashOperator."""
    for task in dag.tasks:
        assert isinstance(task, BashOperator), (
            f"Task '{task.task_id}' is {type(task).__name__}, expected BashOperator"
        )


def test_dag_task_order(dag):
    """dbt_run_staging must precede dbt_test_staging in the topological sort."""
    sorted_ids = [t.task_id for t in dag.topological_sort()]
    assert sorted_ids.index("dbt_run_staging") < sorted_ids.index("dbt_test_staging")
    assert sorted_ids.index("dbt_test_staging") < sorted_ids.index("dbt_run_intermediate")
    assert sorted_ids.index("dbt_run_intermediate") < sorted_ids.index("dbt_run_mart")
    assert sorted_ids.index("dbt_run_mart") < sorted_ids.index("dbt_test_mart")
    assert sorted_ids.index("dbt_test_mart") < sorted_ids.index("dbt_generate_docs")


def test_dag_no_catchup(dag):
    assert dag.catchup is False


def test_dag_has_correct_tags(dag):
    assert set(dag.tags) == {"olist", "dbt", "transformation"}


def test_dbt_deps_is_first_task(dag):
    """dbt_deps has no upstream tasks."""
    deps_task = dag.get_task("dbt_deps")
    assert deps_task.upstream_list == []


def test_dbt_generate_docs_is_last_task(dag):
    """dbt_generate_docs has no downstream tasks."""
    docs_task = dag.get_task("dbt_generate_docs")
    assert docs_task.downstream_list == []
