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


.
├── airflow/
│   ├── dags/
│   │   ├── batch_sales.py
│   │   ├── weather_ingest_v2.py
│   │   └── dbt_run.py
│   └── include/dbt_profiles/profiles.yml
├── de_dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── dims/
│   │   ├── facts/
│   │   └── gold/
│   └── schema.yml
├── docker/
│   └── docker-compose.yml
├── data_seed/   # raw CSV files
├── requirements.txt
└── README.md

⚙️ How to Run
git clone https://github.com/juanfeijoo98/ecommerce-data-pipeline-airflow-dbt.git
cd ecommerce-data-pipeline-airflow-dbt

Start services with Docker

docker compose up -d
Access Airflow

Web UI → http://localhost:8080

User: admin | Password: admin

Run DAGs

Trigger batch_sales (ingests and loads sales data)

Trigger weather_ingest_v2 (ingests external data)

dbt models are executed automatically via dbt_run

📊 Example Outputs

Airflow DAG Graph View
(screenshot here)

dbt test results
(screenshot here)

Final schema (Gold Layer): fact_orders, dim_customers, dim_products, enriched with weather data.

🛠️ Tech Stack

Airflow – orchestration

dbt (Postgres) – transformations & tests

Docker Compose – containerized environment

Postgres – data warehouse

Python (pandas, SQLAlchemy) – data ingestion

🎯 Use Cases

Retail sales analytics

ETL/ELT best practices

Medallion architecture example

Portfolio project for Data Engineering interviews

📌 Author

👤 Juan Pablo Feijoo
Aspiring Data Engineer | Data Scientist | Passionate about building scalable data pipelines

