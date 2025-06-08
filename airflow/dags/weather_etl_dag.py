from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
import pandas as pd
from sqlalchemy import create_engine
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# List of cities to track
CITIES = ['London', 'New York', 'Tokyo']
API_KEY = os.getenv('OPENWEATHER_API_KEY')
POSTGRES_CONN = 'postgresql://airflow:airflow@postgres/weather_data'

def fetch_weather_data(city):
    """Fetch weather data from OpenWeatherMap API."""
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        return {
            'city': city,
            'timestamp': datetime.now(),
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'wind_speed': data['wind']['speed'],
            'weather_description': data['weather'][0]['description']
        }
    else:
        raise Exception(f"Failed to fetch weather data for {city}")

def process_weather_data(**context):
    """Process and store weather data for all cities."""
    engine = create_engine(POSTGRES_CONN)
    
    # Fetch data for each city
    all_data = []
    for city in CITIES:
        try:
            weather_data = fetch_weather_data(city)
            all_data.append(weather_data)
        except Exception as e:
            print(f"Error fetching data for {city}: {str(e)}")
    
    # Convert to DataFrame and store in PostgreSQL
    if all_data:
        df = pd.DataFrame(all_data)
        df.to_sql('weather_data', engine, if_exists='append', index=False)

# Create the DAG
dag = DAG(
    'weather_etl',
    default_args=default_args,
    description='ETL pipeline for weather data',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# Create tables if they don't exist
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        city VARCHAR(100),
        timestamp TIMESTAMP,
        temperature FLOAT,
        humidity FLOAT,
        pressure FLOAT,
        wind_speed FLOAT,
        weather_description VARCHAR(100)
    );
    """,
    dag=dag
)

# Task to fetch and process weather data
process_data = PythonOperator(
    task_id='process_weather_data',
    python_callable=process_weather_data,
    dag=dag
)

# Set task dependencies
create_tables >> process_data 