.PHONY: init up down down-volumes logs dbt-run dbt-test dbt-docs load-raw test lint

init:
	docker compose up airflow-init

up:
	docker compose up -d --scale airflow-init=0

down:
	docker compose down

down-volumes:
	docker compose down --volumes --remove-orphans

logs:
	docker compose logs -f airflow-scheduler

dbt-run:
	docker compose exec airflow-scheduler dbt run --project-dir /opt/airflow/dbt/olist_dw

dbt-test:
	docker compose exec airflow-scheduler dbt test --project-dir /opt/airflow/dbt/olist_dw

dbt-docs:
	docker compose exec airflow-scheduler bash -c "dbt docs generate --project-dir /opt/airflow/dbt/olist_dw && dbt docs serve --project-dir /opt/airflow/dbt/olist_dw --port 8081"

load-raw:
	python scripts/load_raw_to_postgres.py

test:
	pytest airflow/tests/ -v

lint:
	ruff check airflow/ dbt/
