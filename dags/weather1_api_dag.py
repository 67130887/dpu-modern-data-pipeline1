import json
import requests
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

# --------- CONFIG ---------
API_KEY = Variable.get("airvisual_api_key")
API_URL = "https://api.airvisual.com/v2/city"
CITY = "Bangkok"
STATE = "Bangkok"
COUNTRY = "Thailand"
DAG_FOLDER = "/opt/airflow/dags"
DATA_FILE = f"{DAG_FOLDER}/aqi_data.json"

# --------- TASK 1: Extract ---------
def extract_aqi():
    params = {
        "city": CITY,
        "state": STATE,
        "country": COUNTRY,
        "key": API_KEY
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    data = response.json()
    print(data)
    with open(DATA_FILE, "w") as f:
        json.dump(data, f)

# --------- TASK 2: Validate ---------
def validate_aqi_data():
    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    pollution = data.get("data", {}).get("current", {}).get("pollution", {})
    assert pollution, "Pollution data missing"
    assert pollution.get("aqius") >= 0, "AQI must be >= 0"

# --------- TASK 3: Transform ---------
def transform_aqi_data():
    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    pollution = data["data"]["current"]["pollution"]
    weather = data["data"]["current"]["weather"]

    transformed = {
        "timestamp": pollution["ts"],
        "aqi": pollution["aqius"],
        "temperature": weather["tp"],
        "humidity": weather["hu"],
        "pm2_5": pollution.get("pm25", None)
    }

    with open(DATA_FILE, "w") as f:
        json.dump(transformed, f)
    print("Transformed Data:", transformed)

# --------- TASK 4: Create Table ---------
def create_aqi_table():
    pg_hook = PostgresHook(postgres_conn_id="weather1_postgres_conn", schema="postgres")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bangkok_aqi (
            timestamp TIMESTAMP PRIMARY KEY,
            aqi INTEGER NOT NULL,
            temperature FLOAT,
            humidity FLOAT,
            pm2_5 FLOAT
        )
    """)
    connection.commit()

# --------- TASK 5: Load ---------
def load_to_postgres():
    with open(DATA_FILE, "r") as f:
        data = json.load(f)

    pg_hook = PostgresHook(postgres_conn_id="weather1_postgres_conn", schema="postgres")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        INSERT INTO bangkok_aqi (timestamp, aqi, temperature, humidity, pm2_5)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (timestamp) DO NOTHING
    """
    cursor.execute(sql, (
        data["timestamp"],
        data["aqi"],
        data["temperature"],
        data["humidity"],
        data["pm2_5"]
    ))
    connection.commit()

# --------- DAG ---------
default_args = {
    "email": ["youremail@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "weather1_aqi_dag",
    default_args=default_args,
    #schedule="0 7 * * *",  # รันทุก 3 ชม
    schedule="0 */3 * * *",  # รันทุก 3 ชม
    start_date=timezone.datetime(2025, 2, 1),
    catchup=False,
    tags=["capstone", "aqi"],
) as dag:

    start = EmptyOperator(task_id="start")

    t1 = PythonOperator(task_id="extract_aqi", python_callable=extract_aqi)
    t2 = PythonOperator(task_id="validate_aqi_data", python_callable=validate_aqi_data)
    t3 = PythonOperator(task_id="transform_aqi_data", python_callable=transform_aqi_data)
    t4 = PythonOperator(task_id="create_aqi_table", python_callable=create_aqi_table)
    t5 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)

    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> t3 >> t4 >> t5 >> end
