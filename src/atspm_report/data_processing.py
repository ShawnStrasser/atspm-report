import ibis
import pandas as pd
from datetime import date, timedelta
from .statistical_analysis import cusum, alert

def process_maxout_data(df):
    """Process the max out data to calculate daily aggregates"""
    ibis.options.interactive = True
    
    # Convert to Ibis table
    t = ibis.memtable(df)
    
    # Daily aggregates
    t_daily = t.mutate(Date=t['TimeStamp'].cast('date'))
    t_daily = t_daily.mutate(
        MaxOutTotal=(
            t_daily['PerformanceMeasure'].isin(['MaxOut', 'ForceOff'])
            .ifelse(t_daily['Total'], 0)
        )
    )
    
    daily_result = t_daily.group_by(['Date', 'DeviceId', 'Phase']).aggregate(
        **{
            'Percent MaxOut': t_daily.MaxOutTotal.sum() / t_daily.Total.sum(),
            'Services': t_daily.Total.sum()
        }
    ).order_by(['Date', 'DeviceId', 'Phase'])
    
    # Get max date minus 6 days for hourly data
    max_date = t['TimeStamp'].cast('date').max().execute()
    start_date = max_date - timedelta(days=6)
    
    # Hourly aggregates
    t_hourly = t.filter(t['TimeStamp'] >= start_date)
    t_hourly = t_hourly.mutate(
        TimeStamp=t_hourly['TimeStamp'].truncate('hour')
    )
    t_hourly = t_hourly.mutate(
        MaxOutTotal=(
            t_hourly['PerformanceMeasure'].isin(['MaxOut', 'ForceOff'])
            .ifelse(t_hourly['Total'], 0)
        )
    )
    
    hourly_result = t_hourly.group_by(['TimeStamp', 'DeviceId', 'Phase']).aggregate(
        **{
            'Percent MaxOut': t_hourly.MaxOutTotal.sum() / t_hourly.Total.sum(),
            'Services': t_hourly.Total.sum()
        }
    ).order_by(['TimeStamp', 'DeviceId', 'Phase'])
    
    return daily_result.execute(), hourly_result.execute()

def process_actuations_data(df):
    """Process the actuations data to calculate daily aggregates"""
    ibis.options.interactive = True
    
    # Convert to Ibis table
    t = ibis.memtable(df)
    
    # Daily aggregates
    t_daily = t.mutate(Date=t['TimeStamp'].cast('date'))
    daily_result = t_daily.group_by(['Date', 'DeviceId', 'Detector']).aggregate(
        Total=t_daily.Total.sum().cast('int'),
        PercentAnomalous=t_daily.anomaly.cast('float').sum() / t_daily.count()
    ).order_by(['Date', 'DeviceId', 'Detector'])
    
    # Get max date minus 6 days for hourly data
    max_date = t['TimeStamp'].cast('date').max().execute()
    start_date = max_date - timedelta(days=6)
    
    # Hourly aggregates
    t_hourly = t.filter(t['TimeStamp'] >= start_date)
    t_hourly = t_hourly.mutate(
        TimeStamp=t_hourly['TimeStamp'].truncate('hour')
    )
    hourly_result = t_hourly.group_by(['TimeStamp', 'DeviceId', 'Detector']).aggregate(
        Total=t_hourly.Total.sum().cast('int'),
        Forecast=t_hourly.prediction.cast('int').sum()
    ).order_by(['TimeStamp', 'DeviceId', 'Detector'])
    
    return daily_result.execute(), hourly_result.execute()

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
    ibis.options.interactive = True
    
    # Convert to Ibis tables
    ped = ibis.memtable(df_ped)
    maxout = ibis.memtable(df_maxout)
    intersections = ibis.memtable(df_intersections)
    
    # --- Daily Aggregation Logic ---
    
    # t1: Aggregate by Date, DeviceId, Phase
    t1 = ped.mutate(Date=ped['TimeStamp'].cast('date'))
    t1 = t1.group_by(['Date', 'DeviceId', 'Phase']).aggregate(
        PedServices=t1.PedServices.sum(),
        PedActuation=t1.PedActuation.sum()
    )
    
    # t2: Join with maxout and intersections
    # Assuming maxout has Date, DeviceId, Phase and intersections has DeviceId
    t2 = t1.join(maxout, ['Date', 'DeviceId', 'Phase'], how='inner')
    t2 = t2.join(intersections, ['DeviceId'], how='inner')
    
    # Calculate Ped_APS and Ped_Percent
    t2 = t2.mutate(
        Ped_APS=((t2.PedServices == 0) | ((t2.PedServices == 0) & (t2.Services == 0)))
            .ifelse(ibis.null(), t2.PedActuation / t2.PedServices),
        Ped_Percent=((t2.Services == 0) | ((t2.Services == 0) & (t2.PedServices == 0)))
            .ifelse(ibis.null(), t2.PedServices / t2.Services)
    )
    
    # _medians: Group by DeviceId, Phase
    medians = t2.group_by(['DeviceId', 'Phase']).aggregate(
        _median_percent=t2.Ped_Percent.median(),
        _median_aps=t2.Ped_APS.median(),
        _median_actuation=t2.PedActuation.median()
    )
    
    # t3: Join t2 and medians
    t3 = t2.join(medians, ['DeviceId', 'Phase'])
    
    # Calculate GEH
    def geh_calc(val, median, services):
        return (
            ((services < 30) | ((val + median) == 0))
            .ifelse(ibis.null(), ((2 * (val - median).pow(2)) / (val + median)) * (val - median).sign())
        )

    t3 = t3.mutate(
        Ped_Percent_GEH_=geh_calc(t3.Ped_Percent, t3._median_percent, t3.Services),
        Ped_APS_GEH_=geh_calc(t3.Ped_APS, t3._median_aps, t3.Services),
        PedActuation_GEH_=geh_calc(t3.PedActuation, t3._median_actuation, t3.Services)
    )
    
    # _group_stats: Group by Date, Region
    group_stats = t3.group_by(['Date', 'Region']).aggregate(
        Ped_Percent_GEH_Avg=t3.Ped_Percent_GEH_.mean(),
        Ped_Percent_GEH_Std=t3.Ped_Percent_GEH_.std(),
        Ped_APS_GEH_Avg=t3.Ped_APS_GEH_.mean(),
        Ped_APS_GEH_Std=t3.Ped_APS_GEH_.std(),
        PedActuation_GEH_Avg=t3.PedActuation_GEH_.mean(),
        PedActuation_GEH_Std=t3.PedActuation_GEH_.std()
    )
    
    # t4: Join t3 and group_stats
    t4 = t3.join(group_stats, ['Date', 'Region'])
    
    # Calculate Z-Scores
    def zscore_calc(val, avg, std):
        return (
            val.isnull()
            .ifelse(ibis.null(), (val - avg) / std)
        )

    t4 = t4.mutate(
        Ped_Percent_ZScore=zscore_calc(t4.Ped_Percent_GEH_, t4.Ped_Percent_GEH_Avg, t4.Ped_Percent_GEH_Std),
        Ped_APS_ZScore=zscore_calc(t4.Ped_APS_GEH_, t4.Ped_APS_GEH_Avg, t4.Ped_APS_GEH_Std),
        PedActuation_ZScore=zscore_calc(t4.PedActuation_GEH_, t4.PedActuation_GEH_Avg, t4.PedActuation_GEH_Std)
    )
    
    # t5: Combined Z-Score
    t5 = t4.mutate(
        Ped_Combined_ZScore=(t4.Ped_Percent_ZScore < 0)
            .ifelse((t4.Ped_Percent_ZScore * t4.Ped_APS_ZScore).abs(), t4.Ped_Percent_ZScore * t4.Ped_APS_ZScore)
    )
    
    # t6: Window function for alert
    w = ibis.window(
        group_by=['DeviceId', 'Phase'],
        order_by='Date',
        preceding=ibis.interval(days=1),
        following=0
    )
    
    t5 = t5.mutate(
        High_ZScore=(t5.PedActuation_ZScore > 4).cast('int')
    )
    
    t6 = t5.mutate(
        Ped_Actuations_Alert=(t5.High_ZScore.sum().over(w) == 2).cast('int')
    )
    
    # Final selection
    result1 = t6.filter(t6.Ped_Combined_ZScore <= -11).select('Date', 'DeviceId', 'Phase').order_by(['Date', 'DeviceId', 'Phase'])
    
    
    # --- Hourly Aggregation Logic ---
    
    # Get max date and start date
    max_date = ped['TimeStamp'].cast('date').max().execute()
    start_date = max_date - timedelta(days=6)
    
    # t1: Hourly aggregation
    t1_hourly = ped.filter(ped['TimeStamp'] >= start_date)
    t1_hourly = t1_hourly.mutate(TimeStamp=t1_hourly['TimeStamp'].truncate('hour'))
    t1_hourly = t1_hourly.group_by(['TimeStamp', 'DeviceId', 'Phase']).aggregate(
        PedServices=t1_hourly.PedServices.sum(),
        PedActuation=t1_hourly.PedActuation.sum()
    )
    
    # Scaffold
    # Distinct devices/phases
    devices_phases = t1_hourly.select('DeviceId', 'Phase').distinct()
    
    # Time series
    min_time = t1_hourly['TimeStamp'].min().execute()
    max_time = t1_hourly['TimeStamp'].max().execute()
    
    # Generate hourly timestamps
    date_range = pd.date_range(start=min_time, end=max_time, freq='h')
    time_series = ibis.memtable({'TimeStamp': date_range})
    
    scaffold = time_series.cross_join(devices_phases)
    
    # Join and fillna
    filled_data = scaffold.left_join(
        t1_hourly,
        ['TimeStamp', 'DeviceId', 'Phase']
    ).mutate(
        PedServices=ibis.coalesce(t1_hourly.PedServices, 0),
        PedActuation=ibis.coalesce(t1_hourly.PedActuation, 0)
    )
    
    result2 = filled_data.order_by(['TimeStamp', 'DeviceId', 'Phase'])
    
    return result1.execute(), result2.execute()