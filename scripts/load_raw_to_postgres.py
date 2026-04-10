"""Standalone script to load Olist raw CSVs into PostgreSQL raw schema.

Usage:
    python scripts/load_raw_to_postgres.py

Reads DATA_RAW_PATH and Postgres credentials from environment variables
(or .env file via python-dotenv).
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).parents[1] / ".env")

# Table names mirror CSV filenames (without .csv) to match dbt source definitions.
OLIST_FILES = {
    "olist_orders_dataset.csv": "olist_orders_dataset",
    "olist_customers_dataset.csv": "olist_customers_dataset",
    "olist_order_items_dataset.csv": "olist_order_items_dataset",
    "olist_products_dataset.csv": "olist_products_dataset",
    "olist_sellers_dataset.csv": "olist_sellers_dataset",
    "olist_order_payments_dataset.csv": "olist_order_payments_dataset",
    "olist_order_reviews_dataset.csv": "olist_order_reviews_dataset",
    "olist_geolocation_dataset.csv": "olist_geolocation_dataset",
    "product_category_name_translation.csv": "product_category_name_translation",
}


def _build_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "olist_dw")
    user = os.getenv("POSTGRES_USER", "olist_user")
    password = os.getenv("POSTGRES_PASSWORD", "olist_pass")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def load_all(data_path: Path | None = None) -> dict[str, int]:
    """Load all Olist CSVs into PostgreSQL raw schema. Returns {table: row_count}."""
    data_path = data_path or Path(os.getenv("DATA_RAW_PATH", "./data/raw"))
    engine = _build_engine()

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))

    results: dict[str, int] = {}
    for filename, table_name in OLIST_FILES.items():
        csv_path = data_path / filename
        if not csv_path.exists():
            logger.warning(f"File not found, skipping: {csv_path}")
            continue
        df = pd.read_csv(csv_path, dtype=str)
        rows = len(df)
        df.to_sql(
            name=table_name,
            schema="raw",
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=5000,
        )
        logger.info(f"Loading {filename}: {rows} rows -> raw.{table_name}")
        results[table_name] = rows

    return results


def main() -> None:
    logger.info("Starting raw data load into PostgreSQL...")
    results = load_all()
    if results:
        logger.info(
            f"Load complete: {len(results)} tables, "
            f"{sum(results.values()):,} total rows"
        )
        for table, rows in results.items():
            logger.info(f"  raw.{table}: {rows:,} rows")
    else:
        logger.warning("No files loaded. Check that DATA_RAW_PATH contains the Olist CSVs.")


if __name__ == "__main__":
    main()
