import duckdb
import ibis
from datetime import date, timedelta
from statistical_analysis import cusum, alert

def process_maxout_data(df):
    """Process the max out data to calculate daily aggregates"""
    sql = """
    SELECT
        TimeStamp::date as Date,
        DeviceId,
        Phase,
        SUM(CASE WHEN PerformanceMeasure IN ('MaxOut', 'ForceOff') THEN Total ELSE 0 END) / SUM(Total) as "Percent MaxOut",
        SUM(Total) as Services
    FROM df
    GROUP BY ALL
    """
    return duckdb.sql(sql).df()

def process_actuations_data(actuations):
    """Process the actuations data to calculate daily aggregates"""
    sql = """
    SELECT
        TimeStamp::date as Date,
        DeviceId,
        Detector,
        SUM(Total)::int as Total,
        SUM(anomaly::float) / COUNT(*) as PercentAnomalous
    FROM actuations
    GROUP BY ALL
    """
    return duckdb.sql(sql).df()

def process_missing_data(has_data_df):
    """Process the missing data to calculate daily percent missing data"""
    # Convert to Ibis table
    ibis.options.interactive = True
    has_data_table = ibis.memtable(has_data_df)
    
    # Extract the date from the TimeStamp
    has_data_table = has_data_table.mutate(Date=has_data_table['TimeStamp'].date())
    
    # Get min/max dates
    min_max_dates = has_data_table.aggregate(
        MinDate=has_data_table.Date.min(),
        MaxDate=has_data_table.Date.max()
    ).execute()
    
    min_date_val = min_max_dates['MinDate'].iloc[0]
    max_date_val = min_max_dates['MaxDate'].iloc[0]
    
    # Generate complete date range
    date_list = [min_date_val + timedelta(days=i) for i in range((max_date_val - min_date_val).days + 1)]
    all_dates_table = ibis.memtable({"Date": date_list})
    
    # Get distinct devices
    distinct_devices = has_data_table[['DeviceId']].distinct()
    
    # Create scaffold with all DeviceId-Date combinations
    scaffold = distinct_devices.cross_join(all_dates_table)
    
    # Aggregate original data
    daily_counts = has_data_table.group_by(['DeviceId', 'Date']).aggregate(
        RecordCount=has_data_table.count()
    )
    
    # Join scaffold with counts and calculate missing data percentage
    data_availability = scaffold.left_join(
        daily_counts,
        ['DeviceId', 'Date']
    ).mutate(
        # Fill missing counts with 0
        RecordCount=ibis.coalesce(ibis._.RecordCount, 0)
    ).mutate(
        # Calculate missing data percentage (96 is expected records per day)
        MissingData=(1 - ibis._.RecordCount / 96.0)
    )
    
    # Select final columns
    result = data_availability.select('DeviceId', 'Date', 'MissingData')
    
    return result.execute()