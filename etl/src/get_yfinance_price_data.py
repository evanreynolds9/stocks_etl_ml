# Pull yfinance price data for listings in the tsx constituent db table
# Import packages
import yfinance as yf
import os
import pandas as pd
from dotenv import load_dotenv

# Import utilities
from etl.src.utils import extract_query, load_query

# Setup global logger
from etl.logger_setup import logger

# Define function to pull price data
@logger.catch(reraise=True)
def get_daily_price_data(ticker_list: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    # Create list for batch exports
    dfs = []

    i = 0
    size = 50
    while i < len(ticker_list):
        _ticker_list = ticker_list['ticker'].iloc[i:i + size].to_list()
        _tickers = yf.Tickers(_ticker_list)
        df = _tickers.history(start = start_date, end = end_date)
        df = df.stack(level=1, future_stack=True).reset_index()
        df.drop(columns=['Adj Close'], inplace=True, errors='ignore')
        dfs.append(df)
        i += size

    logger.info("Price data retrieved successfully.")

    # Combine dfs
    combined_df = pd.concat(dfs, ignore_index=True)

    return combined_df

# Define function to transform data
@logger.catch(reraise=True)
def transform_price_data(df: pd.DataFrame) -> pd.DataFrame:
    # Rename columns
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Reorder columns
    df = df[["date", "ticker", "open", "high", "low", "close", "dividends", "stock_splits", "volume"]]

    # Remove nulls and log
    null_rows = len(df[df.isnull().any(axis=1)])

    if null_rows > 0:
        logger.warning(f"{null_rows} rows will be dropped due to null values.")
        df.dropna(inplace=True)

    return df

# Define main function
def main():
    load_dotenv()

    # Create db connection string
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_conn_str = f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Fetch start and end dates from users
    start_str = input("Enter start date (YYYY-MM-DD): ")
    end_str = input("Enter end date (YYYY-MM-DD): ")

    logger.info(f'Starting ETL process for ticker prices between {start_str} and {end_str}.')

    # Import current constituent list - get both ticker and id
    import_query = """
    SELECT ticker
    FROM tickers
    WHERE `active` = 'Y'
    """

    ticker_list = extract_query(sql_query=import_query, db_conn_str=db_conn_str)

    # Pull price data from yfinance
    df_raw = get_daily_price_data(ticker_list=ticker_list, start_date=start_str, end_date=end_str)

    # Transform data
    df_cleaned = transform_price_data(df=df_raw)

    # Load data to database table
    load_query(table_name="price_history", df=df_cleaned, append=True, db_conn_str=db_conn_str)

    logger.info('Finished ETL process for ticker prices.')

if __name__ == "__main__":
    main()