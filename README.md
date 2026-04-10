# ecommerce-modern-stack

[![CI](https://github.com/adrianopsf/ecommerce-modern-stack/actions/workflows/ci.yml/badge.svg)](https://github.com/adrianopsf/ecommerce-modern-stack/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)](https://postgresql.org)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)
[![dbt docs](https://img.shields.io/badge/dbt%20docs-live-FF694B)](https://adrianopsf.github.io/ecommerce-modern-stack)

Modern ELT pipeline orchestrated with **Apache Airflow 2.8**, layered transformations with **dbt Core 1.7**, and **PostgreSQL 15** as the data warehouse — using the [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset.

## Overview

The Olist dataset captures the full lifecycle of orders on Brazil's largest B2B marketplace: customers, sellers, products, payments, and reviews across 100k+ orders. Transforming this raw data into reliable analytical tables requires an orchestrated pipeline that handles schema evolution, enforces data contracts, and separates ingestion concerns from transformation logic.

This project implements a production-grade ELT stack entirely in Docker: Airflow coordinates daily ingestion of 9 CSV files into a `raw` PostgreSQL schema, dbt transforms them through three layers (staging → intermediate → mart) with 40+ automated tests, and GitHub Actions validates every push with lint, DAG tests, and SQL compilation checks. The result is a fully documented, testable data warehouse ready for BI tooling.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Apache Airflow 2.8                       │
│  ┌───────────────┐  ┌──────────────────┐  ┌───────────────┐ │
│  │ olist_ingest  │→ │ olist_dbt_trans  │→ │ olist_full_   │ │
│  │ _raw (daily)  │  │ formations(daily)│  │ pipeline(wkly)│ │
│  └───────────────┘  └──────────────────┘  └───────────────┘ │
└──────────────────────────────────────────────────────────────┘
          │                     │
          ▼                     ▼
  ┌──────────────┐    ┌─────────────────────────────────────┐
  │  raw schema  │    │           dbt  olist_dw             │
  │  (9 tables)  │    │  staging/ → intermediate/ → mart/   │
  └──────────────┘    └─────────────────────────────────────┘
          │                     │
          └─────────────────────┘
                      │
                      ▼
            ┌──────────────────┐
            │   PostgreSQL 15  │
            │   olist_dw       │
            └──────────────────┘
```

## dbt Lineage

```
[raw.olist_orders_dataset]            → stg_orders          ─┐
[raw.olist_customers_dataset]         → stg_customers        ├→ int_orders_enriched  → mart_orders
[raw.olist_order_items_dataset]       → stg_order_items      ┤                       → mart_monthly_sales
[raw.olist_order_payments_dataset]    → stg_order_payments  ─┘
                                                              ↓
[raw.olist_customers_dataset]         → stg_customers    ─→ int_customers_orders → mart_customers
[raw.olist_orders_dataset]            → stg_orders       ─┘

[raw.olist_products_dataset]          → stg_products     ─→ mart_products
[raw.product_category_name_trans...]  ─┘
```

## Data Model

```
              ┌──────────────────┐
              │  mart_customers  │
              │     (dim)        │
              └────────┬─────────┘
                       │ customer_id
         ┌─────────────▼──────────────┐      ┌──────────────────┐
         │        mart_orders         │─────▶│  mart_products   │
         │          (fact)            │      │     (dim)        │
         └─────────────┬──────────────┘      └──────────────────┘
                       │ order_month
         ┌─────────────▼──────────────┐
         │    mart_monthly_sales      │
         │       (aggregate)          │
         └────────────────────────────┘
```

## Airflow DAGs

| DAG | Schedule | Description | SLA |
|-----|----------|-------------|-----|
| `olist_ingest_raw` | `@daily` | Loads 9 Olist CSVs into PostgreSQL `raw` schema via `OlistPostgresOperator` | — |
| `olist_dbt_transformations` | `@daily` | Runs dbt layers: deps → staging → test → intermediate → mart → test → docs | — |
| `olist_full_pipeline` | Mon 06:00 | Triggers ingest + dbt sequentially via `TriggerDagRunOperator` | 2 h |

## dbt Models

### Staging (views — cleaning & typing)

| Model | Source table | Key transformations |
|-------|-------------|---------------------|
| `stg_orders` | `olist_orders_dataset` | Timestamp casts, null filter |
| `stg_customers` | `olist_customers_dataset` | `LOWER(TRIM(city))`, `UPPER(state)` |
| `stg_order_items` | `olist_order_items_dataset` | Numeric casts, `total_item_value = price + freight` |
| `stg_products` | `olist_products_dataset` + translation | `LEFT JOIN` translation, `COALESCE` to `'unknown'` |
| `stg_order_payments` | `olist_order_payments_dataset` | Integer/numeric casts |
| `stg_sellers` | `olist_sellers_dataset` | City/state normalisation |

### Intermediate (ephemeral — joins & enrichment)

| Model | Description |
|-------|-------------|
| `int_orders_enriched` | Orders + payment totals + item counts + `delivery_days` + `is_late_delivery` + `order_month` |
| `int_customers_orders` | Customers + lifetime aggregates + `customer_segment` (high / medium / low value) |

### Mart (tables — analytical exposure)

| Model | Description | Row grain |
|-------|-------------|-----------|
| `mart_orders` | Delivered orders with full metrics | 1 row per delivered order |
| `mart_customers` | Customer profile + LTV | 1 row per unique customer |
| `mart_products` | Product catalog + sales performance | 1 row per product |
| `mart_monthly_sales` | Monthly KPIs incl. `late_delivery_pct` | 1 row per month |

## Getting Started

**Prerequisites:** Docker & Docker Compose, Python 3.11+, [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) CSVs.

```bash
# 1. Clone the repository
git clone https://github.com/adrianopsf/ecommerce-modern-stack.git
cd ecommerce-modern-stack

# 2. Configure environment variables
cp .env.example .env
# Edit .env and set AIRFLOW__CORE__FERNET_KEY and AIRFLOW__WEBSERVER__SECRET_KEY
# Generate a Fernet key: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 3. Place Olist CSVs in data/raw/
#    Files expected: olist_orders_dataset.csv, olist_customers_dataset.csv, ...
#    (download from https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

# 4. Initialise Airflow (creates DB, admin user)
make init

# 5. Start all services (webserver, scheduler, worker, postgres, redis)
make up

# 6. Open Airflow UI
#    http://localhost:8080  →  admin / admin

# 7. Load raw CSVs into PostgreSQL
make load-raw

# 8. Activate the olist_full_pipeline DAG in the Airflow UI
#    (or trigger it manually via the UI or CLI)

# 9. Validate dbt transformations
make dbt-test
```

> **Airflow Connection required:** Create a Postgres connection named `olist_postgres` in the Airflow UI
> (Admin → Connections) pointing to `postgres:5432 / olist_dw / olist_user / olist_pass`
> before running DAGs that use `OlistPostgresOperator`.

## Testing

```bash
# Run DAG unit tests (no Docker required)
make test
# or directly:
pytest airflow/tests/ -v

# Run dbt tests against a running PostgreSQL instance
make dbt-test
# or:
dbt test --project-dir dbt/olist_dw --profiles-dir dbt/olist_dw

# Lint Python code
make lint
```

The test suite covers:
- **DAG structure** — task count, task types, dependency order, tags, schedule
- **dbt schema tests** — `not_null`, `unique`, `accepted_values` on all key columns
- **Custom dbt tests** — `assert_positive_order_value`, `assert_delivery_days_range`

## Makefile Reference

| Target | Description |
|--------|-------------|
| `make init` | Initialise Airflow DB and create admin user |
| `make up` | Start all Docker services in the background |
| `make down` | Stop all containers |
| `make down-volumes` | Stop containers and delete all volumes (destructive) |
| `make logs` | Stream scheduler logs |
| `make load-raw` | Load Olist CSVs from `data/raw/` into `raw` schema |
| `make dbt-run` | Run all dbt models |
| `make dbt-test` | Run all dbt tests |
| `make dbt-docs` | Generate and serve dbt docs on port 8081 |
| `make test` | Run pytest suite for Airflow DAGs |
| `make lint` | Run ruff linter on `airflow/` and `scripts/` |

## License

[MIT](LICENSE) © 2024 adrianopsf
