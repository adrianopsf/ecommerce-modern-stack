"""Custom operator to load Olist raw CSVs into PostgreSQL raw schema."""

import os
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text


class OlistRawLoader:
    """Loads Olist CSV files into a PostgreSQL raw schema."""

    TABLES = {
        "olist_orders_dataset.csv": "orders",
        "olist_customers_dataset.csv": "customers",
        "olist_order_items_dataset.csv": "order_items",
        "olist_products_dataset.csv": "products",
        "olist_order_payments_dataset.csv": "order_payments",
        "olist_sellers_dataset.csv": "sellers",
        "olist_order_reviews_dataset.csv": "order_reviews",
        "olist_geolocation_dataset.csv": "geolocation",
        "product_category_name_translation.csv": "product_category_translation",
    }

    def __init__(self, data_path: str | None = None):
        self.data_path = Path(data_path or os.getenv("DATA_RAW_PATH", "./data/raw"))
        self.engine = self._build_engine()

    def _build_engine(self):
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "olist_dw")
        user = os.getenv("POSTGRES_USER", "olist_user")
        password = os.getenv("POSTGRES_PASSWORD", "olist_pass")
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        return create_engine(url)

    def _ensure_schema(self):
        with self.engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))

    def load_table(self, csv_file: str, table_name: str) -> bool:
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
