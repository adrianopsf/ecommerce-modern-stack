"""Tests for ingest_raw_data DAG."""

import pytest


def test_dag_import():
    """DAG module imports without errors."""
    from airflow.dags import ingest_raw_data  # noqa: F401


def test_dag_id():
    from airflow.dags.ingest_raw_data import dag

    assert dag.dag_id == "ingest_raw_data"


def test_dag_has_expected_task():
    from airflow.dags.ingest_raw_data import dag

    task_ids = {t.task_id for t in dag.tasks}
    assert "load_raw_csvs_to_postgres" in task_ids


def test_dag_schedule():
    from airflow.dags.ingest_raw_data import dag

    assert dag.schedule_interval == "@daily"


def test_dag_no_catchup():
    from airflow.dags.ingest_raw_data import dag

    assert dag.catchup is False
