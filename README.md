# stocks_etl_ml: A functional Airflow setup for yfinance price data
This project provides a functional template to orchestrate ETL processes, which fetch stock ticker price data from yahoo
finance, with Airflow. It features robust logging with the `loguru` library and unit tests with `pytest`. The Airflow
setup is minimal, using the LocalExecutor to execute tasks. The project is set up to send the data to a MySQL database 
on the host machine but can be customized to support other database systems.

# Getting Started
## A) Setting up your environment
1. [Download and install MySQL](https://www.mysql.com/downloads/)
    - Note that you do not have to store data in MySQL on the host machine, but you will have to adjust the programs
        accordingly if you choose to use another setup.
2. Setup a MySQL database, and create the necessary tables by running `SQL\init\Create_DB_Tables.sql` on your MySQL Server
3. Create a `.env` file with the following variables:
    - `DB_HOST`: the database host defined in step 2
    - `DB_PORT`: the database port defined in step 2
    - `DB_USERNAME`: the database username defined in step 2
    - `DB_PASSWORD`: the database password defined in step 2
    - `DB_NAME`: stock_data, or a different name if `SQL\init\Create_DB_Tables.sql` was altered
    - `LOG_FILE_PATH`: The file location and name where you would like your log file to be stored. An absolute path is not recommended,
        as it may pose issues when used from the docker container.
4. Install the local requirements, which are different that the requirements for the docker containers, by running 
    `pip install -r requirements-local.txt`
5. Populate the `tickers` table with a list of tickers you want to pull data on. The script `etl\src\get_tsx_tickers.py`
    will pull the most recent list of tickers from the TSX website and load them into the table for you, however to use
    it you must [install chromedriver](https://developer.chrome.com/docs/chromedriver/downloads) and set the absolute
    path to it on your machine in the `.env` file created in 3 as `CHROME_DRIVER_PATH`. However, you do not need to use
    this list of tickers, so feel free to populate the table with whatever tickers you are interested in using any method.
6. Load historic daily data for these tickers by running `etl\src\get_yfinance_price_data.py` and supplying the date range
    you are interested in.

## B) Setting up your docker container
1. [Download and install Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. Run ` openssl rand -hex 32 ` to generate a JWT key, and save it to the `.env` file created in A)3. under `AIRFLOW_JWT_SECRET`.
3. Add `AIRFLOW_UID=50000` to the same file, or customize the value if needed.
4. Adjust the start date of `etl\dags\price_history_dag.py` accordingly, as it will backfill by default.
5. Initialize the Airflow database by running `docker compose up airflow-init`, and wait for the 0 exit code.
6. Start Airflow with `docker compose up`
7. Visit `localhost:8080` to view the Airflow UI
8. Ensure the MySQL database setup in 1.c) is running.
9. Unpause the `price_history_dag`.
10. Let Airflow run and pull price data for you on a daily basis! Note that null rows in the ETL process will be stored in
    a separate table on the database. This way, the data for these tickers/dates can be pulled at a later date if it
    was not available.

## C) Going Further
- Run the existing tests by navigating to the `test` folder and running `pytest`, and feel free to add others!
- Adjust the DAG and source code to a more granular frequency if intraday/hourly data is needed.
- Play around with the `docker-compose.yaml` file to suit your needs. It currently uses the `LocalExecutor` to run tasks,
    and uses EST to configure its timezone.
- Create other DAGs to schedule other stock ETL or ML related tasks! I will continue to build other functionality into
    this library!