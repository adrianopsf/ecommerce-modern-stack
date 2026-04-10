"""Tests for run_dbt_transformations DAG."""


def test_dag_import():
    """DAG module imports without errors."""
    from airflow.dags import run_dbt_transformations  # noqa: F401


def test_dag_id():
    from airflow.dags.run_dbt_transformations import dag

    assert dag.dag_id == "run_dbt_transformations"


def test_dag_task_count():
    from airflow.dags.run_dbt_transformations import dag

    assert len(dag.tasks) == 4


def test_dag_task_order():
    from airflow.dags.run_dbt_transformations import dag

    task_ids = [t.task_id for t in dag.topological_sort()]
    assert task_ids.index("dbt_run_staging") < task_ids.index("dbt_run_intermediate")
    assert task_ids.index("dbt_run_intermediate") < task_ids.index("dbt_run_mart")
    assert task_ids.index("dbt_run_mart") < task_ids.index("dbt_test")


def test_dag_no_catchup():
    from airflow.dags.run_dbt_transformations import dag

    assert dag.catchup is False
