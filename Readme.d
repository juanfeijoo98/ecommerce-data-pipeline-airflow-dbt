# End-to-End Data Engineering – E-commerce Analytics (Airflow + dbt + Docker)

Complete pipeline **bronze → silver → gold** with:
- **Airflow** (DAGs: `batch_sales`, `weather_ingest_v2`, `dbt_run`)
- **Docker Compose** (Postgres + Airflow webserver/scheduler)
- **dbt (Postgres)**: dims/facts + tests
- **Python (pandas/SQLAlchemy)** for ingestion and loading

## How to lift
1) Requirements: Docker Desktop.
2) `docker compose up -d`
3) Airflow UI → http://localhost:8080 (user `admin` / pass `admin`)
4) Run `batch_sales` and/or `weather_ingest_v2` (they trigger `dbt_run`).

## Structure
