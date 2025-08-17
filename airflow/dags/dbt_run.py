# DAG para ejecutar dbt (run + test)

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "de", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="dbt_run",
    start_date=datetime(2025, 8, 1),
    schedule=None,        # se dispara manualmente o desde otros DAGs
    catchup=False,
    default_args=default_args,
    tags=["dbt"],
):
    run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            # fail fast + log; cd al proyecto de dbt dentro del contenedor
            "set -euo pipefail && cd /opt/project/de_dbt && "
            "dbt deps && dbt run"
        ),
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt_profiles"},
    )

    test = BashOperator(
        task_id="dbt_test",
        bash_command="set -euo pipefail && cd /opt/project/de_dbt && dbt test",
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt_profiles"},
    )

    run >> test
