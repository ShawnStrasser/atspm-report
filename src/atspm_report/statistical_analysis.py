import ibis

def cusum(df, k_value=0.5, forgetfulness=2):
    """Calculate CUSUM statistics for anomaly detection"""
    ibis.options.interactive = True
    
    # Create ibis table from dataframe
    table = ibis.memtable(df)

    # Set whether to use Phase, Detector, or just DeviceId (for missing data)
    if 'Phase' in table.columns:
        column = 'Percent MaxOut'
        cusum_column_name = 'CUSUM_Percent MaxOut'
        group_column = 'Phase'
    elif 'Detector' in table.columns:
        column = 'PercentAnomalous'
        cusum_column_name = 'CUSUM_PercentAnomalous'
        group_column = 'Detector'
    elif 'MissingData' in table.columns:
        column = 'MissingData'
        cusum_column_name = 'CUSUM_MissingData'
        group_column = None
    else:
        raise ValueError("Unknown data format for CUSUM analysis")

    # Calculate group-based metrics
    if group_column:
        group_metrics = table.group_by(['DeviceId', group_column]).aggregate(
            _MinDate=table['Date'].min(),
            _MaxDate=table['Date'].max(),
            _Average=table[column].mean(),
            _StdDev=table[column].std()
        )
        # Join the metrics back to the original table
        joined = table.join(group_metrics, ['DeviceId', group_column])
        # Set up window for calculations
        window = ibis.window(
            group_by=['DeviceId', group_column],
            order_by=['Date'],
            preceding=ibis.interval(days=6),
            following=0
        )
    else:
        # For missing data (no group_column)
        group_metrics = table.group_by(['DeviceId']).aggregate(
            _MinDate=table['Date'].min(),
            _MaxDate=table['Date'].max(),
            _Average=table[column].mean(),
            _StdDev=table[column].std()
        )
        # Join the metrics back to the original table
        joined = table.join(group_metrics, ['DeviceId'])
        # Set up window for calculations
        window = ibis.window(
            group_by=['DeviceId'],
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

# Phase termination alerting thresholds. A day is flagged only when all four are
# exceeded, so raising any one of them makes the check less sensitive.
MAXOUT_ALERT_DEFAULTS = {
    'maxout_cusum_threshold': 0.25,      # cumulative sum of the shift, over a 7-day window
    'maxout_zscore_threshold': 4.0,      # standard deviations above the phase's 21-day mean
    'maxout_percent_threshold': 0.2,     # share of terminations that maxed out that day
    'maxout_min_services': 30,           # ignore phases that barely ran
}

# Days back from the newest data an alert may have occurred and still be
# reported. 0 means it must have happened on the most recent day: an issue that
# came and went earlier in the week is not a live problem.
DEFAULT_ALERT_RECENCY_DAYS = 0


def alert(table, maxout_thresholds: dict = None,
          recency_days: int = DEFAULT_ALERT_RECENCY_DAYS):
    """Generate alerts based on CUSUM analysis results.

    maxout_thresholds overrides MAXOUT_ALERT_DEFAULTS for phase terminations; the
    detector and missing-data conditions are unaffected. recency_days is how far
    back from the newest data an alert may have occurred and still be reported.
    """
    limits = {**MAXOUT_ALERT_DEFAULTS, **(maxout_thresholds or {})}

    if 'Percent MaxOut' in table.columns:
        cusum_column_name = 'CUSUM_Percent MaxOut'
        column = 'Percent MaxOut'
        group_column = 'Phase'
    elif 'PercentAnomalous' in table.columns:
        cusum_column_name = 'CUSUM_PercentAnomalous'
        column = 'PercentAnomalous'
        group_column = 'Detector'
    elif 'MissingData' in table.columns:
        cusum_column_name = 'CUSUM_MissingData'
        column = 'MissingData'
        group_column = None
    else:
        raise ValueError("Unknown data format for alert generation")

    # Add z-score column
    result = table.mutate(
        z_score=(table[column] - table['_Average']) / table['_StdDev']
    )

    if 'Percent MaxOut' in table.columns:
        # Include Services in alert conditions for Percent MaxOut
        result = result.mutate(
            Alert=(
                (result[cusum_column_name] > limits['maxout_cusum_threshold']) &
                (result['Services'] > limits['maxout_min_services']) &
                (result['z_score'] > limits['maxout_zscore_threshold']) &
                (result[column] > limits['maxout_percent_threshold'])
            ).cast('int32')  # Convert boolean to 0/1
        )
    elif 'PercentAnomalous' in table.columns:
        # Alert condition for PercentAnomalous
        result = result.mutate(
            Alert=(
                (result[cusum_column_name] > 0.20) &
                (result['z_score'] > 3.5) &
                (result[column] > 0.10)
            ).cast('int32')  # Convert boolean to 0/1
        )
    else:
        # Alert condition for MissingData
        result = result.mutate(
            Alert=(
                (result[cusum_column_name] > 0.1) &
                (result['z_score'] > 3) &
                (result[column] > 0.05) 
            ).cast('int32')  # Convert boolean to 0/1
        )

    # Calculate days from max date for each row
    result = result.mutate(
        days_from_max=result['Date'].delta(result['_MaxDate'], unit='days').abs(),
    )

    # Find DeviceId/Group pairs that alerted recently enough to report
    if group_column:
        alert_pairs = (
            result.filter(
                (result['Alert'] == 1) &
                (result['days_from_max'] <= recency_days)
            )
            .select('DeviceId', group_column)
            .distinct()
        )

        # Join back to get all records for these DeviceId/Group pairs
        final_result = result.semi_join(
            alert_pairs,
            ['DeviceId', group_column]
        )
    else:
        # For missing data (no group_column)
        alert_devices = (
            result.filter(
                (result['Alert'] == 1) &
                (result['days_from_max'] <= recency_days)
            )
            .select('DeviceId')
            .distinct()
        )

        # Join back to get all records for these DeviceId
        final_result = result.semi_join(
            alert_devices,
            ['DeviceId']
        )

    return final_result