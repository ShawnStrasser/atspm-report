import duckdb
import ibis


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


def cusum(df, k_value=0.5, forgetfulness=2):
    """Calculate CUSUM statistics for anomaly detection"""
    ibis.options.interactive = True
    
    # Create ibis table from dataframe
    table = ibis.memtable(df)

    # Set whether to use Phase or Detector
    if 'Phase' in table.columns:
        column = 'Percent MaxOut'
        cusum_column_name = 'CUSUM_Percent MaxOut'
        group_column = 'Phase'
    else:
        column = 'PercentAnomalous'
        cusum_column_name = 'CUSUM_PercentAnomalous'
        group_column = 'Detector'

    # Calculate group-based metrics
    group_metrics = table.group_by(['DeviceId', group_column]).aggregate(
        _MinDate=table['Date'].min(),
        _MaxDate=table['Date'].max(),
        _Average=table[column].mean(),
        _StdDev=table[column].std()
    )

    # Join the metrics back to the original table
    joined = table.join(group_metrics, ['DeviceId', group_column])

    window = ibis.window(
        group_by=['DeviceId', group_column],
        order_by=['Date'],
        preceding=ibis.interval(days=6),
        following=0
    )

    # Add date weight column
    result = joined.mutate(
        DateWeight=(joined['Date'].delta(joined['_MinDate'], unit='days') + 1)**forgetfulness,
    )
    # Add Date Weight Sum
    result = result.mutate(
        DateWeightSum=result['DateWeight'].sum().over(window),
    )
    result = result.mutate(
        DaySum=ibis.greatest(0, result[column] - result['_Average'] - k_value * result['_StdDev']) *
                result['DateWeight']
    )

    # Use the dynamic column name here
    result = result.mutate(**{
        cusum_column_name: result['DaySum'].sum().over(window) / result['DateWeightSum'] * 7
    })

    # Drop everything but the CUSUM column
    result = result.drop(
        ['DateWeight', 'DateWeightSum', 'DaySum']
    )

    return result


def alert(table):
    """Generate alerts based on CUSUM analysis results"""
    if 'Percent MaxOut' in table.columns:
        cusum_column_name = 'CUSUM_Percent MaxOut'
        column = 'Percent MaxOut'
        group_column = 'Phase'
    else:
        cusum_column_name = 'CUSUM_PercentAnomalous'
        column = 'PercentAnomalous'
        group_column = 'Detector'

    # Add z-score column
    result = table.mutate(
        z_score=(table[column] - table['_Average']) / table['_StdDev']
    )

    if 'Percent MaxOut' in table.columns:
        # Include Services in alert conditions for Percent MaxOut
        result = result.mutate(
            Alert=(
                (result[cusum_column_name] > 0.25) &
                (result['Services'] > 30) &
                (result['z_score'] > 4) &
                (result[column] > 0.2)
            ).cast('int32')  # Convert boolean to 0/1
        )
    else:
        # Alert condition for PercentAnomalous
        result = result.mutate(
            Alert=(
                (result[cusum_column_name] > 0.25) &
                (result['z_score'] > 4) &
                (result[column] > 0.15)
            ).cast('int32')  # Convert boolean to 0/1
        )

    # Calculate days from max date for each row
    result = result.mutate(
        days_from_max=result['Date'].delta(result['_MaxDate'], unit='days').abs(),
    )

    # Find DeviceId/Phase pairs that have alerts within the last week
    alert_pairs = (
        result.filter(
            (result['Alert'] == 1) &
            (result['days_from_max'] <= 6)  # Within last week (0 to 6 days)
        )
        .select('DeviceId', group_column)
        .distinct()
    )

    # Join back to get all records for these DeviceId/Phase pairs
    final_result = result.semi_join(
        alert_pairs,
        ['DeviceId', group_column]
    )

    return final_result