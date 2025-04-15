import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL
from datetime import datetime, timedelta

def get_database_connection(server: str, database: str, username: str) -> sa.engine.base.Engine:
    """Create a database connection using ActiveDirectoryInteractive authentication"""
    driver = 'ODBC Driver 17 for SQL Server'
    connection_url = URL.create(
        "mssql+pyodbc",
        username=username,
        password=None,  # Leave as None for Active Directory Interactive
        host=server,
        database=database,
        query={"driver": driver, "Authentication": "ActiveDirectoryInteractive"}
    )
    return sa.create_engine(connection_url)

def get_data(use_parquet: bool = True, connection_params: dict = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Get data either from parquet files or database
    
    Args:
        use_parquet (bool): If True, read from parquet files, otherwise query database
        connection_params (dict): Database connection parameters if use_parquet is False
            Required keys: server, database, username
            
    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: (maxout_df, actuations_df, signals_df, has_data_df)
    """
    if use_parquet:
        try:
            maxout_df = pd.read_parquet('MaxOut.parquet')
            actuations_df = pd.read_parquet('Actuations.parquet')
            signals_df = pd.read_parquet('signals.parquet')
            has_data_df = pd.read_parquet('has_data.parquet')
            return maxout_df, actuations_df, signals_df, has_data_df
        except Exception as e:
            print(f"Error reading parquet files: {e}")
            return None, None, None, None
    else:
        if not connection_params:
            raise ValueError("Database connection parameters required when use_parquet is False")
        
        engine = get_database_connection(
            connection_params['server'],
            connection_params['database'],
            connection_params['username']
        )
        
        # Get current date and three weeks ago
        today = datetime.today().date()
        three_weeks_ago = today - timedelta(weeks=3)
        
        # Query terminations data
        terminations_query = f"""
        SELECT * FROM terminations
        WHERE TimeStamp >= '{three_weeks_ago}'
        """
        maxout_df = pd.read_sql(terminations_query, engine)
        
        # Query detector health data
        detector_query = f"""
        SELECT * FROM detector_health
        WHERE TimeStamp >= '{three_weeks_ago}'
        """
        actuations_df = pd.read_sql(detector_query, engine)
        
        # Query signals data
        signals_query = "SELECT * FROM signals"
        signals_df = pd.read_sql(signals_query, engine)
        
        # Query has_data for missing data analysis
        has_data_query = f"""
        SELECT * FROM has_data
        WHERE TimeStamp >= '{three_weeks_ago}'
        """
        has_data_df = pd.read_sql(has_data_query, engine)
        
        return maxout_df, actuations_df, signals_df, has_data_df