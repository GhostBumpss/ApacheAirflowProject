# ...existing code...
import pyodbc
from airflow.hooks.base import BaseHook

@task()
def load_weather_data(transformed_data):
    # Fetch connection from Airflow
    conn = BaseHook.get_connection('mssql_default')
    # Build connection string
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={conn.host},{conn.port};"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};"
        f"PWD={conn.password};"
        "TrustServerCertificate=yes;"
    )
    with pyodbc.connect(conn_str) as sql_conn:
        cursor = sql_conn.cursor()
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='weather_data' AND xtype='U')
        CREATE TABLE weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            createdat DATETIME DEFAULT GETDATE()
        );
        """)
        cursor.execute("""
        INSERT INTO weather_data(latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))
        sql_conn.commit()
        cursor.close()
# ...existing code...