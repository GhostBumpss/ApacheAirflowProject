from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
# from airflow.utils.dates import 
import pendulum
import requests, json


# Location coordinates for Kolkata, India
LATITUDE = '22.5726'
LONGITUDE = '88.3639'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now().subtract(days=1),
    'retries': 1,
}

# Creating DAG
with DAG(dag_id='weather_etl_pipeline', default_args=default_args, schedule='@daily', catchup=False) as dags:
    @task()
    def extract_weather_data():
        http_hook = HttpHook(method='GET', http_conn_id=API_CONN_ID)
        # endpoint = f'https://api.open-meteo.com/v1/forecast?latitude=22.5726&longitude=88.3639&current_weather=true'
        endpoint = f'v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        try:
            response = http_hook.run(endpoint)
            print("I am printing the response", response.json())
            return response.json()
        except Exception as e:
            raise ValueError(f"Failed to fetch weather data due to {e} - Status Code - {response.status_code}")
    
    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data["current_weather"]
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
        }
        print("I am transforming the data", transformed_data)
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS AirflowProject1.weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        INSERT INTO AirflowProject1.weather_data(latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES(%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    # ETL Pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)