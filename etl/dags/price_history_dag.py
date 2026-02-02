# Import libraries
from datetime import datetime, timedelta
import os
import pandas as pd

# Imports from airflow
from airflow.sdk import dag, task

# Import functions
from etl.src.get_yfinance_price_data import get_daily_price_data, transform_price_data
from etl.src.utils import extract_query, load_query

# Setup connection string, default args
db_conn_str = os.getenv('DATA_DB_CONN') # Remember to set this in airflow.config

# Define tasks
@task(retry_delay=timedelta(hours=3))
def extract(**kwargs):
    # Get ticker list
    import_query = """
    SELECT ticker
    FROM tickers
    WHERE `active` = 'Y'
    """
    ticker_list = extract_query(sql_query=import_query, db_conn_str=db_conn_str)

    run_date = kwargs.get("logical_date")
    run_date_id = kwargs.get("ts_nodash")
    # Get data from yfinance
    df = get_daily_price_data(ticker_list=ticker_list, start_date=run_date-timedelta(days=1), end_date=run_date)

    file_name = f"/opt/airflow/data/extracted_data_{run_date_id}.csv"
    # Write to csv
    df.to_csv(file_name, index=False)

@task(retry_delay=timedelta(minutes=5))
def transform(**kwargs):
    run_date_id = kwargs.get("ts_nodash")
    import_file_name = f"/opt/airflow/data/extracted_data_{run_date_id}.csv"

    try:
        # Load raw data
        raw_df = pd.read_csv(import_file_name)

        # Apply transformations
        df = transform_price_data(df=raw_df)

        export_file_name = f"/opt/airflow/data/transformed_data_{run_date_id}.csv"
        # Write to csv
        df.to_csv(export_file_name, index=False)

    finally:
        # Delete raw data csv
        if os.path.exists(import_file_name):
            os.remove(import_file_name)

@task(retry_delay=timedelta(minutes=5))
def load(**kwargs):
    run_date_id = kwargs.get("ts_nodash")
    import_file_name = f"/opt/airflow/data/transformed_data_{run_date_id}.csv"

    try:
        # Load raw data
        transformed_df = pd.read_csv(import_file_name)

        # Load to database
        load_query(table_name="price_history", df=transformed_df, db_conn_str=db_conn_str)
    finally:
        # Delete transformed data csv
        if os.path.exists(import_file_name):
            os.remove(import_file_name)

@dag(
    dag_id="price_history_dag",
    start_date=datetime(2026, 1, 7),
    schedule='0 21 * * 1-5',
    catchup=True,
    default_args={'retries': 3},
)
def price_history_etl_dag():
    extract() >> transform() >> load()

dag_instance = price_history_etl_dag()