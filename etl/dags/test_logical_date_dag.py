# Import libraries
from datetime import datetime, timedelta
import os

# Imports from airflow
from airflow.sdk import dag, task

# Import functions
from etl.logger_setup import logger

# Setup connection string, default args
db_conn_str = os.getenv('DATA_DB_CONN') # Remember to set this in airflow.config

# Define tasks
@task
def logical_date(**kwargs):
    run_id = kwargs.get('run_id')
    logger.info(f'For run: {run_id}')

    logical_date = kwargs.get('logical_date')
    logger.info(f"The logical date is: {logical_date}")
    logger.info(f"So the day before would be: {logical_date-timedelta(days=1)}")

@task
def ts_string(**kwargs):
    run_id = kwargs.get('run_id')
    logger.info(f'For run: {run_id}')

    run_date_id = kwargs.get("ts_nodash")
    logger.info(f"The ts_nodash value is: {run_date_id}")


@dag(
    dag_id="test_logical_date_dag",
    start_date=datetime(2026, 1, 7),
    schedule='0 21 * * 1-5',
    catchup=True,
    default_args={'retries': 3},
)
def test_logical_date_dag():
    logical_date() >> ts_string()

dag_instance = test_logical_date_dag()