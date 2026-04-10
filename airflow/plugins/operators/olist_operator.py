"""Custom Airflow operator and standalone loader for Olist CSV files."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text

try:
    from airflow.models import BaseOperator

    _AIRFLOW_AVAILABLE = True
except ImportError:
    BaseOperator = object  # type: ignore[assignment,misc]
    _AIRFLOW_AVAILABLE = False


class OlistPostgresOperator(BaseOperator):  # type: ignore[misc]
    """
    Custom operator to load a single Olist CSV file into PostgreSQL raw schema.

    Uses Airflow's PostgresHook so the connection is managed via the Airflow
    Connections UI (conn_id: olist_postgres).
    """

    template_fields = ("data_path", "table_name")

    def __init__(
        self,
        data_path: str,
        table_name: str,
        postgres_conn_id: str = "olist_postgres",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.data_path = data_path
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):  # noqa: ARG002
        self.log.info("Loading %s -> raw.%s", self.data_path, self.table_name)

        csv_path = Path(self.data_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        df = pd.read_csv(csv_path, dtype=str)
        row_count = len(df)
        self.log.info("Read %d rows from %s", row_count, csv_path.name)

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        engine = hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))

        df.to_sql(
            name=self.table_name,
            schema="raw",
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=5000,
        )

        self.log.info("Loaded %d rows into raw.%s", row_count, self.table_name)
        return row_count


class OlistRawLoader:
    """Loads Olist CSV files into PostgreSQL raw schema (standalone, no Airflow deps)."""

    # Table names mirror CSV filenames (without .csv) to match dbt source definitions.
    TABLES = {
        "olist_orders_dataset.csv": "olist_orders_dataset",
        "olist_customers_dataset.csv": "olist_customers_dataset",
        "olist_order_items_dataset.csv": "olist_order_items_dataset",
        "olist_products_dataset.csv": "olist_products_dataset",
        "olist_order_payments_dataset.csv": "olist_order_payments_dataset",
        "olist_sellers_dataset.csv": "olist_sellers_dataset",
        "olist_order_reviews_dataset.csv": "olist_order_reviews_dataset",
        "olist_geolocation_dataset.csv": "olist_geolocation_dataset",
        "product_category_name_translation.csv": "product_category_name_translation",
    }

    def __init__(self, data_path: str | None = None):
        from sqlalchemy import create_engine

        self.data_path = Path(data_path or os.getenv("DATA_RAW_PATH", "./data/raw"))
        self.engine = self._build_engine()

    def _build_engine(self):
        from sqlalchemy import create_engine

        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "olist_dw")
        user = os.getenv("POSTGRES_USER", "olist_user")
        password = os.getenv("POSTGRES_PASSWORD", "olist_pass")
        return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    def _ensure_schema(self):
        with self.engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))

    def load_table(self, csv_file: str, table_name: str) -> bool:
        from loguru import logger

        csv_path = self.data_path / csv_file
        if not csv_path.exists():
            logger.warning(f"File not found, skipping: {csv_path}")
            return False

        df = pd.read_csv(csv_path, dtype=str)
        df.to_sql(
            name=table_name,
            schema="raw",
            con=self.engine,
            if_exists="replace",
            index=False,
            chunksize=10_000,
        )
        logger.info(f"Loaded {len(df):,} rows into raw.{table_name}")
        return True

    def load_all(self) -> list[str]:
        self._ensure_schema()
        loaded = []
        for csv_file, table_name in self.TABLES.items():
            if self.load_table(csv_file, table_name):
                loaded.append(table_name)
        return loaded
