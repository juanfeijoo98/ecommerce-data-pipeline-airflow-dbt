from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
import pandas as pd


# ─── Rutas de proyecto ────────────────────────────
PROJ = Path("/opt/project")
SRC = PROJ / "data_seed"
BRONZE = PROJ / "bronze" / "sales"
SILVER = PROJ / "silver" / "sales"


# ─── Funciones auxiliares ─────────────────────────
def _read_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path)
    raise ValueError(f"Formato no soportado: {path.name}")


def _latest_seed():
    files = sorted([*SRC.glob("ventas_*.csv"), *SRC.glob("ventas_*.xlsx")])
    assert files, f"No hay archivos ventas_*.csv|xlsx en {SRC}"
    return files[-1]


def move_csv_to_bronze():
    BRONZE.mkdir(parents=True, exist_ok=True)
    latest = _latest_seed()
    dst = BRONZE / latest.name
    dst.write_bytes(latest.read_bytes())
    print(f"Archivo movido a bronze → {dst}")


def validate_sales_csv():
    files = sorted([*BRONZE.glob("ventas_*.csv"), *BRONZE.glob("ventas_*.xlsx")])
    assert files, f"No hay archivos en {BRONZE}"
    f = files[-1]
    print("Leyendo:", f)
    df = _read_any(f)
    cols_lower = {c.lower() for c in df.columns}
    required = {"order_id", "product", "quantity", "price", "total"}
    missing = required - cols_lower
    assert not missing, f"Faltan columnas: {missing}"
    print(f"CSV/XLSX OK ({len(df)} filas)")


def to_silver_parquet():
    files = sorted([*BRONZE.glob("ventas_*.csv"), *BRONZE.glob("ventas_*.xlsx")])
    assert files, "No hay archivos en bronze/sales"
    dfs = []
    for f in files:
        df = _read_any(f)
        df.columns = [c.lower() for c in df.columns]
        df = df.astype({"order_id": "int64", "product": "string", "quantity": "int64"})
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["total"] = pd.to_numeric(df["total"], errors="coerce")
        dfs.append(df)
    out = pd.concat(dfs, ignore_index=True).drop_duplicates(
        subset=["order_id", "product"]
    )
    SILVER.mkdir(parents=True, exist_ok=True)
    path = SILVER / "sales_clean.parquet"
    out.to_parquet(path, index=False)
    print(f"Silver ventas actualizado → {path} (filas: {len(out)})")


# ─── Definición del DAG ───────────────────────────
with DAG(
    dag_id="batch_sales",
    start_date=datetime(2025, 8, 17),
    schedule_interval=None,  # Manual o puedes poner "0 6 * * *"
    catchup=False,
    tags=["batch", "sales"],
) as dag:

    t1 = PythonOperator(
        task_id="move_csv_to_bronze",
        python_callable=move_csv_to_bronze,
    )

    t2 = PythonOperator(
        task_id="validate_sales_csv",
        python_callable=validate_sales_csv,
    )

    t3 = PythonOperator(
        task_id="to_silver_parquet",
        python_callable=to_silver_parquet,
    )

    # Dependencias: primero mover → validar → transformar
    _ = t1 >> t2 >> t3
