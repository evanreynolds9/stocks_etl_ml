# Import libraries
from datetime import datetime, timedelta

# Imports from airflow
from airflow.sdk import dag, task
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python import PythonOperator

# Import functions
from etl.src.get_yfinance_price_data import get_daily_price_data, transform_price_data
from etl.src.utils import extract_query, load_query

# Setup connection string, default args
conn = MySqlHook.get_connection(conn_id="data_db") # Remember to set this in airflow.config
db_conn_str = f"{conn.conn_type}://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

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
    # Get data from yfinance
    df = get_daily_price_data(ticker_list=ticker_list, start_date=run_date, end_date=run_date+timedelta(days=1))

    # Load to temp table
    load_query(table_name="raw_price_history", df=df, append=False, db_conn_str=db_conn_str)

@task(retry_delay=timedelta(minutes=5))
def transform():
    # Load raw data
    raw_df = extract_query(table_name="raw_price_history", db_conn_str=db_conn_str)

    # Apply transformations
    df = transform_price_data(df=raw_df)

    # Load transformed data
    load_query(table_name="transformed_price_history", df=df, append=False, db_conn_str=db_conn_str)

@task(retry_delay=timedelta(minutes=5))
def load():
    # Load transformed data
    df = extract_query(table_name="transformed_price_history", db_conn_str=db_conn_str)

    # Load to database
    load_query(table_name="price_history", df=df, db_conn_str=db_conn_str)

@dag(
    dag_id="price_history_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 0 * * 1-5',
    default_args={'retries': 3},
)
def price_history_etl_dag():
    extract() >> transform() >> load()

dag_instance = price_history_etl_dag()