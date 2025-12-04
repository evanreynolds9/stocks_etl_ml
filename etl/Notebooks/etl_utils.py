### ETL Utilities ###
# Packages:
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Optional, Union, Dict, Sequence, Any


# extract_query: fetch the results of a SQL query at a given file path and return the results in a pandas dataframe
# Last updated: 2025-11-23

# Inputs:
# table_name: str - name of table in database. If passed, SELECT * from table will be returned
    # Cannot be passed with sql_query
# sql_query: str - sql query to be executed and returned. Cannot be passed with table_name
# db_conn_str: str - pymysql connection string. Should not be passed with engine, as it would be redundant
# engine: sqlalchemy.Engine - engine for db connection. Should not be passed with db_conn_str, as it would be redundant
# params: optional dict or sequence - parameters to be passed into the sql query
# Outputs:
# df: pd.DataFrame - data frame containing the results of the query

def extract_query(table_name: Optional[str] = None,
                sql_query: Optional[str] = None,
                db_conn_str: Optional[str] = None,
                engine: Optional[Engine] = None,
                params: Optional[Union[Dict[str, Any], Sequence[Any]]] = None) -> Optional[pd.DataFrame]:
    # Confirm that only one of sql_query or table_name were passed
    if (table_name is None) == (sql_query is None):
        raise ValueError("Only one, and exactly one of table_name or sql_query must be supplied."
                         "Otherwise execution is ambiguous.")

    # Create engine if not passed
    if engine is None:
        if db_conn_str is None:
            raise ValueError("One of engine or db_conn_str must be supplied.")
        try:
            engine = create_engine(db_conn_str)
        except Exception as e:
            print(f"Error creating engine: {e}")

    try:
        # Run query
        with engine.connect() as conn:
            print("Connection Successful!")
            if table_name is not None:
                df = pd.read_sql_table(table_name, con=conn)
                print(f"{table_name} loaded successfully!")
            else:
                df = pd.read_sql(sql = sql_query, con = conn, params = params)
                print(f"SQL script executed successfully!")

    except Exception as e:
        print(f"Error during connection or execution of query: {e}")

    if df is not None:
        return df

# load_query: load a dataframe into an existing database table, or create table if table does not yet exist
# Last updated: 2025-11-23

# Inputs:
# table_name: str - name of table that data is being pushed to in database
# df: pd.DataFrame - dataframe of data being pushed to the relevant table
# append: bool - whether df should be appended to the existing table or replace it
# db_conn_str: str - pymysql connection string. Should not be passed with engine, as it would be redundant
# engine: sqlalchemy.Engine - engine for db connection. Should not be passed with db_conn_str, as it would be redundant
# Outputs:
# None

def load_query(table_name: str,
               df: pd.DataFrame,
               append: bool = True,
               db_conn_str: Optional[str] = None,
               engine: Optional[Engine] = None) -> None:
    # Create engine if not passed
    if engine is None:
        if db_conn_str is None:
            raise ValueError("One of engine or db_conn_str must be supplied.")
        try:
            engine = create_engine(db_conn_str)
        except Exception as e:
            print(f"Error creating engine: {e}")

    # Append/replace data to/in table
    try:
        with engine.begin() as conn:
            if append:
                df.to_sql(table_name, conn, if_exists="append", index=False, method = 'multi')
            else:
                df.to_sql(table_name, conn, if_exists="replace", index=False, method='multi')
            print(f"{len(df)} rows uploaded successfully to {table_name}.")

    except Exception as e:
        print(f"An Error occurred: {e}")