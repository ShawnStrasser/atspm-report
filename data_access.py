import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL
from datetime import datetime, timedelta, date # Added date
from utils import log_message # Import the utility function

def get_database_connection(server: str, database: str, username: str, verbosity: int) -> sa.engine.base.Engine:
    """Create a database connection using ActiveDirectoryInteractive authentication"""
    log_message(f"Attempting to connect to database {database} on server {server}...", 1, verbosity)
    driver = 'ODBC Driver 17 for SQL Server'
    connection_url = URL.create(
        "mssql+pyodbc",
        username=username,
        password=None,  # Leave as None for Active Directory Interactive
        host=server,
        database=database,
        query={"driver": driver, "Authentication": "ActiveDirectoryInteractive"}
    )
    engine = sa.create_engine(connection_url)
    
    # Test connection
    try:
        with engine.connect() as conn:
            # Get list of tables to help with debugging
            tables_query = """
            SELECT SCHEMA_NAME(schema_id) as schema_name, name 
            FROM sys.tables 
            ORDER BY schema_name, name;
            """
            log_message(f"Executing SQL: {tables_query}", 2, verbosity) # Debug level
            tables = pd.read_sql(tables_query, conn)
            log_message("\nAvailable tables:", 2, verbosity) # Debug level
            log_message(tables.to_string(), 2, verbosity) # Debug level
            log_message("\nConnection successful!", 1, verbosity)
    except Exception as e:
        log_message(f"Error testing connection: {str(e)}", 1, verbosity)
        raise
    
    return engine

def get_data(use_parquet: bool = True, connection_params: dict = None, signals_query: str = None, verbosity: int = 1, days_back: int = 21) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Get data either from parquet files or database
    
    Args:
        use_parquet (bool): If True, read from parquet files, otherwise query database
        connection_params (dict): Database connection parameters if use_parquet is False
            Required keys: server, database, username
        signals_query (str): Custom SQL query to get signals data, must return columns:
            - DeviceId
            - Name
            - Region
        verbosity (int): Verbosity level (0=silent, 1=info, 2=debug).
        days_back (int): Number of days back to query raw data.
            
    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: (maxout_df, actuations_df, signals_df, has_data_df, ped_df)
    """
    if use_parquet:
        try:
            maxout_df = pd.read_parquet('raw_data/terminations.parquet') # Updated path and filename
            actuations_df = pd.read_parquet('raw_data/detector_health.parquet') # Updated path and filename
            signals_df = pd.read_parquet('raw_data/signals.parquet') # Updated path
            has_data_df = pd.read_parquet('raw_data/has_data.parquet') # Updated path
            ped_df = pd.read_parquet('raw_data/full_ped.parquet')
            return maxout_df, actuations_df, signals_df, has_data_df, ped_df
        except Exception as e:
            log_message(f"Error reading parquet files: {e}", 1, verbosity)
            return None, None, None, None, None
    else:
        if not connection_params:
            raise ValueError("Database connection parameters required when use_parquet is False")
        
        if not signals_query:
            raise ValueError("Custom signals query required when use_parquet is False. Query must return DeviceId, Name, and Region columns.")
        
        try:
            engine = get_database_connection(
                connection_params['server'],
                connection_params['database'],
                connection_params['username'],
                verbosity=verbosity # Pass verbosity
            )
        except Exception as e:
            log_message(f"Failed to connect to database: {str(e)}", 1, verbosity)
            return None, None, None, None, None
            
        # Get today's date (midnight) and the start date
        today_midnight = date.today() # Use date for midnight comparison
        start_date = today_midnight - timedelta(days=days_back)
        log_message(f"Querying data from {start_date} up to (but not including) {today_midnight}", 1, verbosity)
        
        try:
            # Query terminations data
            log_message("\nQuerying terminations data...", 1, verbosity)
            terminations_query = f"""
            SELECT * FROM dbo.terminations
            WHERE TimeStamp >= '{start_date}' AND TimeStamp < '{today_midnight}'
            """
            log_message(f"Executing SQL: {terminations_query}", 2, verbosity) # Debug level
            maxout_df = pd.read_sql(terminations_query, engine)
            log_message(f"Retrieved {len(maxout_df)} termination records", 1, verbosity)
            
            # Query detector health data
            log_message("\nQuerying detector health data...", 1, verbosity)
            detector_query = f"""
            SELECT * FROM dbo.detector_health
            WHERE TimeStamp >= '{start_date}' AND TimeStamp < '{today_midnight}'
            """
            log_message(f"Executing SQL: {detector_query}", 2, verbosity) # Debug level
            actuations_df = pd.read_sql(detector_query, engine)
            log_message(f"Retrieved {len(actuations_df)} detector health records", 1, verbosity)
            
            # Query signals data using custom query
            log_message("\nQuerying signals data...", 1, verbosity)
            try:
                log_message(f"Executing SQL: {signals_query}", 2, verbosity) # Debug level
                signals_df = pd.read_sql(signals_query, engine)
                log_message(f"Retrieved {len(signals_df)} signal records", 1, verbosity)
                
                # Validate required columns
                required_cols = {'DeviceId', 'Name', 'Region'}
                missing_cols = required_cols - set(signals_df.columns)
                if missing_cols:
                    raise ValueError(f"Custom signals query missing required columns: {missing_cols}")
                    
            except Exception as e:
                log_message(f"Error executing custom signals query: {str(e)}", 1, verbosity)
                log_message("Make sure your query returns DeviceId, Name, and Region columns", 1, verbosity)
                return None, None, None, None, None
            
            # Query has_data for missing data analysis
            log_message("\nQuerying has_data...", 1, verbosity)
            has_data_query = f"""
            SELECT * FROM dbo.has_data
            WHERE TimeStamp >= '{start_date}' AND TimeStamp < '{today_midnight}'
            """
            log_message(f"Executing SQL: {has_data_query}", 2, verbosity) # Debug level
            has_data_df = pd.read_sql(has_data_query, engine)
            log_message(f"Retrieved {len(has_data_df)} has_data records", 1, verbosity)
            
            return maxout_df, actuations_df, signals_df, has_data_df, None
            
        except Exception as e:
            log_message(f"\nError executing queries: {str(e)}", 1, verbosity)
            log_message(f"Query that failed: {e.__context__.args[1] if hasattr(e, '__context__') else 'Unknown query'}", 2, verbosity) # Debug level
            return None, None, None, None, None