### ETL Utilities ###
# Packages:
import pandas as pd
import os
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import DataError
from typing import Optional, Union, Dict, Sequence, Any


# extract_query: fetch the results of a SQL query at a given file path and return the results in a pandas dataframe
# Inputs:
# db_conn_str: str - pymysql connection string
# sql_query_path: str - path to the SQL query to be executed and returned
# params: optional dict or sequence - parameters to be passed into the sql query
# Outputs:
# df: pd.DataFrame - data frame containing the results of the query
def extract_query(db_conn_str: str,
                  sql_query: str,
                  params: Optional[Union[Dict[str, Any], Sequence[Any]]] = None
) -> pd.DataFrame:
    try:
        # Create sqlalchemy engine
        engine = create_engine(db_conn_str)

        # Run query
        with engine.connect() as conn:
            print("Connection Successful!")
            df = pd.read_sql(sql_query, conn, params = params)
            print(f"SQL script executed successfully!")

    except Exception as e:
        print(f"Error during connection or execution of query: {e}")
        df = None

    return df

# load_query: load a dataframe into an existing database table
# Inputs:
# db_conn_str: str - pymysql connection string
# table_name: str - name of table that data is being pushed to in database
# df: pd.DataFrame - dataframe of data being pushed to the relevant table
# Outputs:
# None
def load_query(db_conn_str: str, table_name: str, df: pd.DataFrame) -> None:
    # Create mysql engine
    engine = create_engine(db_conn_str)

    # Get metadata from db table
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    # Ensure column names in dataframe and db table align
    column_types = {column.name: column.type for column in table.columns if not column.primary_key}


    if list(column_types.keys()) == list(df.columns):
        print("Table columns align: continuing to data upload.")

        # Append data to table
        try:
            with engine.begin() as conn:
                df.to_sql(table_name, conn, if_exists="replace", index=False, dtype=column_types, method = 'multi')
                print(f"{len(df)} rows uploaded successfully to {table_name}.")

        except DataError as e:
            print(f"A data error occurred, likely due to a mismatch of data types: {e}")

    else:
        print("Table column names do not align - ensure alignment before proceeding.")
        print(f"SQL Table columns: {column_types.keys()}")
        print(f"Dataframe columns: {df.columns}")