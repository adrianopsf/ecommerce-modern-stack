"""Standalone script to load Olist raw CSVs into PostgreSQL raw schema.

Usage:
    python scripts/load_raw_to_postgres.py

Reads DATA_RAW_PATH and Postgres credentials from environment variables
(or .env file via python-dotenv).
"""

import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

# Load .env from project root
load_dotenv(Path(__file__).parents[1] / ".env")

# Make sure the plugins package is importable when run from project root
sys.path.insert(0, str(Path(__file__).parents[1] / "airflow"))

from plugins.operators.olist_operator import OlistRawLoader  # noqa: E402


def main() -> None:
    logger.info("Starting raw data load into PostgreSQL...")
    loader = OlistRawLoader()
    tables = loader.load_all()

    if tables:
        logger.info(f"Successfully loaded {len(tables)} table(s): {tables}")
    else:
        logger.warning("No tables were loaded. Check that DATA_RAW_PATH contains the Olist CSVs.")


if __name__ == "__main__":
    main()
