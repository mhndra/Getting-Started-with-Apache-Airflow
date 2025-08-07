import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
) as dag:
    # Create a task using the TaskFlow API
    @task(depends_on_past=True)
    def create_database_file():
        # Set the path to the SQLite database file
        db_path = "/usr/local/airflow/include/market_data.db"
        # Create the SQLite database file if it does not exist
        if not os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            # Execute a SQL command to create the market_data table
            conn.execute(""" 
                CREATE TABLE IF NOT EXISTS market_data(
                    status VARCHAR(255),
                    date DATETIME,
                    symbol VARCHAR(10),
                    open NUMERIC(10, 2),
                    high NUMERIC(10, 2),
                    low NUMERIC(10, 2),
                    close NUMERIC(10, 2),
                    volume INT,
                    afterHours NUMERIC(10, 2),
                    preMarket NUMERIC(10, 2)
                );""")
            conn.commit()
            conn.close()

    @task()
    def hit_polygon_api(**context):
        # Instantiate a list of tickers that will be pulled and looped over
        stock_ticker = "AMZN"
        # Set variables
        polygon_api_key = "_nSOgY2sw_p9LEGXVZVleuvYdxohITF3"
        ds = context.get("ds")
        # Create the URL
        url = f"https://api.polygon.io/v1/open-close/{stock_ticker}/{ds}?adjusted=true&apiKey={polygon_api_key}"
        response = requests.get(url)
        # Return the raw data
        return response.json()

    @task()
    def flatten_market_data(polygon_response, **context):
        # Create a list of headers and a list to store the normalized data in
        columns = {
            "status": "closed",
            "from": context.get("ds"),
            "symbol": "AMZN",
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None,
            "afterHours": None,
            "preMarket": None,
        }
        # Create a list to append the data to
        flattened_record = []
        for header_name, default_value in columns.items():
            # Append the data
            flattened_record.append(polygon_response.get(header_name, default_value))
        # Convert to a pandas DataFrame
        flattened_dataframe = pd.DataFrame(
            [flattened_record], columns=columns.keys()
        ).rename(columns={"from": "date"})
        # Return the DataFrame as a list of dictionaries
        return flattened_dataframe.to_dict(orient="records")

    @task()
    def load_market_data(data_records):
        # Pull the connection
        market_database_hook = SqliteHook(sqlite_conn_id="market_database_conn")
        market_database_conn = market_database_hook.get_sqlalchemy_engine()
        # Load the table to SQLite, append if it exists
        flattened_dataframe = pd.DataFrame(data_records)
        if data_records[0]["status"] == "OK":
            flattened_dataframe.to_sql(
                name="market_data",
                con=market_database_conn,
                if_exists="append",
                index=False,
            )
        else:
            pass  # Handle the case where the status is not OK
        print(market_database_hook.get_records("SELECT * FROM market_data;"))

    # Set dependencies
    raw_market_data = hit_polygon_api()
    transformed_market_data = flatten_market_data(raw_market_data)
    create_database_file() >> load_market_data(transformed_market_data)
