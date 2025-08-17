# 🛒 E-commerce Data Pipeline (Airflow + dbt + Docker + Postgres)

This project demonstrates an **end-to-end data engineering pipeline** for an e-commerce sales dataset.  
It follows the **Medallion Architecture (Bronze → Silver → Gold)** and integrates **Airflow, dbt, Docker, and Postgres**.

---

## 🚀 Features
- **Airflow** orchestrates the entire workflow:
  - `batch_sales` → moves raw CSVs to *bronze*, validates, converts to parquet (*silver*), loads into Postgres, then triggers dbt.
  - `weather_ingest_v2` → ingests external weather data.
  - `dbt_run` → executes dbt transformations and tests.
- **dbt** models implement:
  - Staging models for raw sources
  - Dimension and fact tables
  - Data quality tests (`not_null`, `unique`, `relationships`)
- **Docker Compose** runs all services in isolated containers (Airflow, Postgres, dbt).
- **Medallion Architecture** applied:
  - **Bronze**: raw ingested data
  - **Silver**: cleaned & standardized parquet files
  - **Gold**: analytics-ready tables (facts, dims, marts)

---

## 🏗️ Architecture
```mermaid
flowchart LR
    A[CSV Sales Data] -->|Ingest| B(Bronze Layer)
    B -->|Validation & Parquet| C(Silver Layer)
    C -->|Load to Postgres| D(dbt Staging Models)
    D --> E[Dims & Facts]
    E --> F[Gold Layer - Analytics]
