### ETL Utilities ###
# Packages:
import pandas as pd
import os
from sqlalchemy import create_engine


# extract_query: fetch the results of a SQL query at a given file path and return the results in a pandas dataframe
# Inputs:
# db_conn_str: str - pymysql connection string
# sql_query_path: str - path to the SQL query to be executed and returned
# Outputs:
# df: pd.DataFrame - data frame containing the results of the query
def extract_query(db_conn_str: str, sql_query_path: str) -> pd.DataFrame:
    try:
        with open(sql_query_path, "r") as file:
            sql_query = file.read()

        # Create sqlalchemy engine
        engine = create_engine(db_conn_str)
        with engine.connect() as conn:
            print("Connection Successful!")
            df = pd.read_sql(sql_query, conn)
            print(f"SQL script at {os.path.basename(sql_query_path)} executed successfully!")

    except Exception as e:
        print(f"Error during connection or execution of query: {e}")
        df = None

    return df
