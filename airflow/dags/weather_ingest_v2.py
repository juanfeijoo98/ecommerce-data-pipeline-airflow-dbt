# ==========================
# DAG: Ingesta de Clima
# ==========================
from datetime import (
    datetime,
    timedelta,
    timezone,
)  # Importa clases para manejar fechas y horas
from airflow import DAG  # Importa la clase DAG de Airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import (
    TriggerDagRunOperator,
)  # Permite disparar otro DAG desde este
from pathlib import Path  # Para manejar rutas de archivos de forma sencilla
import json  # Para trabajar con datos en formato JSON

# Define rutas base para los datos en diferentes capas (bronze, silver, gold)
PROJ = Path("/opt/project")
BRONZE = PROJ / "bronze" / "weather"
SILVER = PROJ / "silver" / "weather"
GOLD = PROJ / "gold" / "weather"

# Coordenadas de Buenos Aires
LAT, LON = -34.61, -58.38
# URL de la API de clima con los parámetros necesarios
API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    f"?latitude={LAT}&longitude={LON}&hourly=temperature_2m,relative_humidity_2m&timezone=UTC"
)


def ensure_dirs():
    # Crea los directorios bronze, silver y gold si no existen
    for p in [BRONZE, SILVER, GOLD]:
        p.mkdir(parents=True, exist_ok=True)


def fetch_weather():
    import requests  # Importa requests para hacer peticiones HTTP

    ensure_dirs()  # Asegura que los directorios existen
    r = requests.get(API_URL, timeout=30)  # Hace la petición a la API de clima
    r.raise_for_status()  # Lanza error si la respuesta no es 200 OK
    payload = r.json()  # Convierte la respuesta a JSON
    ts = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )  # Timestamp actual en formato ISO
    out = BRONZE / f"weather_{ts}.json"  # Define el nombre del archivo de salida
    out.write_text(json.dumps(payload))  # Guarda el JSON en el archivo
    print(f"Guardado bronze: {out}")  # Imprime confirmación


def validate_weather_json():
    # Valida que el último archivo JSON descargado tenga la estructura esperada
    latest = sorted(BRONZE.glob("weather_*.json"))[-1]  # Busca el archivo más reciente
    payload = json.loads(latest.read_text())  # Lee y carga el JSON
    assert (
        "hourly" in payload
    ), "No viene 'hourly' en la respuesta"  # Verifica que exista 'hourly'
    for key in ["time", "temperature_2m", "relative_humidity_2m"]:
        assert (
            key in payload["hourly"]
        ), f"Falta '{key}' en hourly"  # Verifica claves necesarias
    n = len(payload["hourly"]["time"])  # Cuenta registros
    assert (
        n
        == len(payload["hourly"]["temperature_2m"])
        == len(payload["hourly"]["relative_humidity_2m"])
    )  # Consistencia
    print(f"JSON OK ({n} registros) → {latest.name}")  # Imprime confirmación


def to_silver_parquet():
    import pandas as pd  # Importa pandas para manipulación de datos

    ensure_dirs()  # Asegura que los directorios existen
    latest = sorted(BRONZE.glob("weather_*.json"))[
        -1
    ]  # Busca el archivo JSON más reciente
    payload = json.loads(latest.read_text())  # Lee y carga el JSON
    h = payload["hourly"]  # Extrae la sección 'hourly'
    # Crea un DataFrame con los datos relevantes
    df = pd.DataFrame(
        {
            "ts_utc": pd.to_datetime(h["time"]),
            "temp_c": h["temperature_2m"],
            "rh_pct": h["relative_humidity_2m"],
            "lat": payload.get("latitude"),
            "lon": payload.get("longitude"),
        }
    )
    out = SILVER / "weather_hourly.parquet"  # Archivo de salida en formato parquet
    if out.exists():
        old = pd.read_parquet(out)  # Si ya existe, lo lee
        # Concatena datos nuevos y viejos, elimina duplicados y ordena
        df = (
            pd.concat([old, df])
            .drop_duplicates(subset=["ts_utc"])
            .sort_values("ts_utc")
        )
    df.to_parquet(out, index=False)  # Guarda el DataFrame en parquet
    print(f"Silver actualizado → {out} (filas: {len(df)})")  # Imprime confirmación


def load_to_postgres():
    """
    Lee el parquet de SILVER y lo carga en Postgres en public.weather_hourly
    sin romper vistas: TRUNCATE + APPEND.
    """
    from pathlib import Path
    import pandas as pd
    from sqlalchemy import create_engine, text  # <- import correcto en SQLAlchemy 2.x

    # 1) Verifico el archivo
    src = Path("/opt/project/silver/weather/weather_hourly.parquet")
    assert src.exists(), f"[load_to_postgres] Parquet no existe: {src}"

    # 2) Leo parquet
    df = pd.read_parquet(src)
    print(f"[load_to_postgres] filas a cargar: {len(df)}")

    # 3) Conexión a Postgres del compose
    url = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(url, pool_pre_ping=True)

    # 4) Aseguro tabla y hago TRUNCATE (si no existe, la creará to_sql al hacer append)
    with engine.begin() as conn:
        try:
            conn.execute(text("TRUNCATE TABLE public.weather_hourly"))
            print("[load_to_postgres] TRUNCATE public.weather_hourly")
        except Exception:
            # si la tabla aún no existe, el TRUNCATE falla: lo ignoramos
            print("[load_to_postgres] Tabla no existía, se creará con to_sql")

    # 5) Inserto (append) sin dropear la tabla (no rompe vistas)
    df.to_sql(
        "weather_hourly",
        engine,
        schema="public",
        if_exists="append",  # <- clave
        index=False,
        method="multi",
        chunksize=1000,
    )

    # 6) Verificación rápida
    with engine.begin() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM public.weather_hourly;")
        ).scalar()

    print(f"[load_to_postgres] cargado OK → public.weather_hourly (filas: {count})")


def to_gold_aggregates():
    import pandas as pd  # Importa pandas

    src = SILVER / "weather_hourly.parquet"  # Ruta del archivo parquet
    df = pd.read_parquet(src)  # Lee los datos
    df["date"] = df["ts_utc"].dt.date  # Extrae la fecha de la columna de timestamp
    # Agrupa por fecha y calcula promedio, mínimo y máximo de temperatura
    daily = df.groupby("date", as_index=False).agg(
        avg_temp_c=("temp_c", "mean"),
        min_temp_c=("temp_c", "min"),
        max_temp_c=("temp_c", "max"),
    )
    out = GOLD / "weather_daily.csv"  # Archivo de salida CSV
    GOLD.mkdir(parents=True, exist_ok=True)  # Asegura que el directorio existe
    daily.to_csv(out, index=False)  # Guarda el CSV
    print(f"Gold actualizado → {out} (filas: {len(daily)})")  # Imprime confirmación


# Argumentos por defecto para las tareas del DAG
default_args = {"owner": "de", "retries": 1, "retry_delay": timedelta(minutes=2)}

# Definición del DAG de Airflow
with DAG(
    dag_id="weather_ingest_v2",  # Nombre del DAG
    start_date=datetime(2025, 8, 1),  # Fecha de inicio
    schedule="0 */3 * * *",  # Corre cada 3 horas
    catchup=False,  # No ejecuta tareas pasadas
    default_args=default_args,  # Argumentos por defecto
    tags=["api", "weather", "bronze", "silver", "gold", "postgres"],  # Etiquetas
):
    # Define cada tarea del DAG usando PythonOperator
    t_fetch = PythonOperator(task_id="fetch_weather", python_callable=fetch_weather)
    t_check = PythonOperator(
        task_id="validate_weather_json", python_callable=validate_weather_json
    )
    t_silver = PythonOperator(
        task_id="to_silver_parquet", python_callable=to_silver_parquet
    )
    t_pg = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t_gold = PythonOperator(
        task_id="to_gold_aggregates", python_callable=to_gold_aggregates
    )

    # Tarea para disparar otro DAG (dbt_run) al finalizar el flujo
    kickoff_dbt = TriggerDagRunOperator(
        task_id="kickoff_dbt",
        trigger_dag_id="dbt_run",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    # Define el orden de ejecución de las tareas
    _ = t_fetch >> t_check >> t_silver >> t_pg >> t_gold >> kickoff_dbt
