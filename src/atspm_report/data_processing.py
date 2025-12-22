import duckdb
import ibis
from datetime import date, timedelta
from .statistical_analysis import cusum, alert

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
    ORDER BY Date, DeviceId, Phase
    """

    # Get Max Date minus 7 days for filtering hourly data
    start_date = duckdb.sql("SELECT max(TimeStamp::date) - interval '6 days' as startdate from df").fetchone()[0]
    sql2 = f"""
    SELECT
        time_bucket(interval '60 minutes', TimeStamp) as TimeStamp,
        DeviceId,
        Phase,
        SUM(CASE WHEN PerformanceMeasure IN ('MaxOut', 'ForceOff') THEN Total ELSE 0 END) / SUM(Total) as "Percent MaxOut",
        SUM(Total) as Services
    FROM df
    WHERE TimeStamp >= '{start_date}'
    GROUP BY ALL
    ORDER BY TimeStamp, DeviceId, Phase
    """
    return duckdb.sql(sql).df(), duckdb.sql(sql2).df()

def process_actuations_data(df):
    """Process the actuations data to calculate daily aggregates"""
    sql = """
    SELECT
        TimeStamp::date as Date,
        DeviceId,
        Detector,
        SUM(Total)::int as Total,
        SUM(anomaly::float) / COUNT(*) as PercentAnomalous
    FROM df
    GROUP BY ALL
    ORDER BY Date, DeviceId, Detector
    """

    # Get Max Date minus 7 days for filtering hourly data
    start_date = duckdb.sql("SELECT max(TimeStamp::date) - interval '6 days' as startdate from df").fetchone()[0]
    # Filter for hourly data
    sql2 = f"""
    SELECT
        time_bucket(interval '60 minutes', TimeStamp) as TimeStamp,
        DeviceId,
        Detector,
        SUM(Total)::int as Total,
        SUM(prediction::int) as Forecast
    FROM df
    WHERE TimeStamp >= '{start_date}'
    GROUP BY ALL
    ORDER BY TimeStamp, DeviceId, Detector
    """
    return duckdb.sql(sql).df(), duckdb.sql(sql2).df()

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

def process_ped(df_ped, df_maxout, df_intersections):
    """Process the max out data to calculate daily aggregates"""
    sql = """
    WITH t1 AS (
    SELECT
        TimeStamp::date as Date,
        DeviceId,
        Phase,
        SUM(PedServices) as PedServices,
        SUM(PedActuation) as PedActuation
    FROM df_ped
    GROUP BY ALL
    ),
    t2 AS (
    SELECT
        Date,
        DeviceId,
        Phase,
        Region,
        PedServices,
        PedActuation,
        Services,
        CASE WHEN PedServices = 0 or (PedServices = 0 and Services = 0) THEN NULL ELSE PedActuation / PedServices END AS Ped_APS,
        CASE WHEN Services = 0 or (Services = 0 and PedServices = 0) THEN NULL ELSE PedServices / Services END AS Ped_Percent
    FROM t1
    NATURAL JOIN df_maxout
    NATURAL JOIN df_intersections
    ),
    _medians AS (
    SELECT DeviceId, Phase,
    MEDIAN(Ped_Percent) as _median_percent,
    MEDIAN(Ped_APS) as _median_aps,
    MEDIAN(PedActuation) as _median_actuation,
    from t2
    GROUP BY ALL
    ),

    t3 AS (
    select * EXCLUDE(_median_percent, _median_aps, _median_actuation),
    CASE WHEN Services < 30 OR Ped_Percent + _median_percent = 0 THEN NULL ELSE ((2 * (Ped_Percent - _median_percent)^2) / (Ped_Percent + _median_percent)) * SIGN(Ped_Percent - _median_percent) END as Ped_Percent_GEH_,
    CASE WHEN Services < 30 OR Ped_APS + _median_aps = 0 THEN NULL ELSE ((2 * (Ped_APS - _median_aps)^2) / (Ped_APS + _median_aps)) * SIGN(Ped_APS - _median_aps) END as Ped_APS_GEH_,
    CASE WHEN Services < 30 OR PedActuation + _median_actuation = 0 THEN NULL ELSE ((2 * (PedActuation - _median_actuation)^2) / (PedActuation + _median_actuation)) * SIGN(PedActuation - _median_actuation) END as PedActuation_GEH_,
    from t2
    NATURAL JOIN _medians
    ),

    _group_stats AS (
    SELECT Date, Region,
    AVG(Ped_Percent_GEH_) as Ped_Percent_GEH_Avg,
    STDDEV(Ped_Percent_GEH_) as Ped_Percent_GEH_Std,
    AVG(Ped_APS_GEH_) as Ped_APS_GEH_Avg,
    STDDEV(Ped_APS_GEH_) as Ped_APS_GEH_Std,
    AVG(PedActuation_GEH_) as PedActuation_GEH_Avg,
    STDDEV(PedActuation_GEH_) as PedActuation_GEH_Std
    from t3
    GROUP BY ALL
    ),

    t4 AS (
    select * EXCLUDE(Ped_Percent_GEH_Avg, Ped_Percent_GEH_Std, Ped_APS_GEH_Avg, Ped_APS_GEH_Std, PedActuation_GEH_Avg, PedActuation_GEH_Std),
    CASE WHEN Ped_Percent_GEH_ IS NULL THEN NULL ELSE (Ped_Percent_GEH_ - Ped_Percent_GEH_Avg) / Ped_Percent_GEH_Std END as Ped_Percent_ZScore,
    CASE WHEN Ped_APS_GEH_ IS NULL THEN NULL ELSE (Ped_APS_GEH_ - Ped_APS_GEH_Avg) / Ped_APS_GEH_Std END as Ped_APS_ZScore,
    CASE WHEN PedActuation_GEH_ IS NULL THEN NULL ELSE (PedActuation_GEH_ - PedActuation_GEH_Avg) / PedActuation_GEH_Std END as PedActuation_ZScore,
    from t3
    NATURAL JOIN _group_stats
    ),

    t5 AS (
    select *,
    CASE WHEN Ped_Percent_ZScore < 0 THEN ABS(Ped_Percent_ZScore * Ped_APS_ZScore) ELSE Ped_percent_ZScore * Ped_APS_ZScore END as Ped_Combined_ZScore
    from t4
    ),

    t6 AS (
    SELECT 
        *,
        CASE 
            WHEN COUNT(*) FILTER (WHERE "PedActuation_ZScore" > 4) 
                OVER (
                    PARTITION BY "DeviceId", "Phase" 
                    ORDER BY "Date" 
                    RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW
                ) = 2
            THEN 1
            ELSE 0
        END AS Ped_Actuations_Alert
    FROM t5
    ORDER BY "DeviceId", "Phase", "Date"
    )

    select Date, DeviceId, Phase
    from t6
    WHERE Ped_Combined_ZScore <=-11-- OR Ped_Actuations_Alert = 1--eratic alerts is broken for now need to fix!
    ORDER BY ALL
    """

    start_date = duckdb.sql("SELECT max(TimeStamp::date) - interval '6 days' as startdate from df_ped").fetchone()[0]
    sql2 = f"""
    WITH t1 AS (
        SELECT
            time_bucket(interval '60 minutes', TimeStamp) as TimeStamp,
            DeviceId,
            Phase,
            SUM(PedServices) as PedServices,
            SUM(PedActuation) as PedActuation
        FROM df_ped
        WHERE TimeStamp >= '{start_date}'
        GROUP BY ALL
    ),
    devices_phases AS (
        SELECT DISTINCT DeviceId, Phase FROM t1
    ),
    time_boundaries AS (
        SELECT 
            MIN(TimeStamp) AS min_time,
            MAX(TimeStamp) AS max_time
        FROM t1
    ),
    time_series AS (
        SELECT
            generate_series AS TimeStamp
        FROM generate_series(
            (SELECT min_time FROM time_boundaries),
            (SELECT max_time FROM time_boundaries),
            INTERVAL '1 HOUR'
        )
    ),
    scaffold AS (
        SELECT 
            ts.TimeStamp,
            dp.DeviceId,
            dp.Phase
        FROM time_series ts
        CROSS JOIN devices_phases dp
    ),
    filled_data AS (
        SELECT
            s.TimeStamp,
            s.DeviceId,
            s.Phase,
            COALESCE(t1.PedServices, 0) AS PedServices,
            COALESCE(t1.PedActuation, 0) AS PedActuation
        FROM scaffold s
        LEFT JOIN t1 ON 
            s.TimeStamp = t1.TimeStamp
            AND s.DeviceId = t1.DeviceId
            AND s.Phase = t1.Phase
    )
    SELECT * FROM filled_data
    ORDER BY TimeStamp, DeviceId, Phase
    """
    
    return duckdb.sql(sql).df(), duckdb.sql(sql2).df()