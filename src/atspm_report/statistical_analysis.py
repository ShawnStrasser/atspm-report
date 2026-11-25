import pandas as pd

from .duckdb_utils import TableLike, columns, quote, run_sql


# The phase trend checks (measure_checks) run the max-out method on these daily values per DeviceId and Phase. The
# value is the share of cycles that failed, and the platoon ratio negated so that a fall is a rise, as the method
# looks for a rise above the phase's own history.
SPLIT_FAIL_COLUMN = 'Percent SplitFail'
PLATOON_FALL_COLUMN = 'Platoon Ratio Negated'
TREND_COLUMNS = (SPLIT_FAIL_COLUMN, PLATOON_FALL_COLUMN)


def _cusum_columns(columns):
    """(value column, CUSUM column name, group columns) for maxout, missing data or a phase trend."""
    for column in TREND_COLUMNS:
        if column in columns:
            return column, f'CUSUM_{column}', ['DeviceId', 'Phase']
    if 'Phase' in columns:
        return 'Percent MaxOut', 'CUSUM_Percent MaxOut', ['DeviceId', 'Phase']
    if 'MissingData' in columns:
        return 'MissingData', 'CUSUM_MissingData', ['DeviceId']
    raise ValueError("Unknown data format for CUSUM analysis")


def cusum(df: TableLike, k_value=0.5, forgetfulness=2) -> pd.DataFrame:
    """Calculate CUSUM statistics for anomaly detection.

    Adds _MinDate, _MaxDate, _Average and _StdDev (per DeviceId, or per
    DeviceId/Phase for max outs) and the CUSUM column: a date-weighted sum of the
    positive shift above average + k standard deviations over the trailing 7 days.
    """
    column, cusum_column_name, groups = _cusum_columns(columns(df))
    col = quote(column)
    keys = ', '.join(groups)
    on = ' AND '.join(f't.{g} = g.{g}' for g in groups)
    window = (f"PARTITION BY {keys} ORDER BY Date "
              "RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW")
    return run_sql(f"""
        WITH joined AS (
            SELECT t.*, g._MinDate, g._MaxDate, g._Average, g._StdDev
            FROM t
            JOIN (
                SELECT {keys},
                    MIN(Date) AS _MinDate,
                    MAX(Date) AS _MaxDate,
                    AVG({col}) AS _Average,
                    STDDEV_SAMP({col}) AS _StdDev
                FROM t
                GROUP BY {keys}
            ) AS g ON {on}
        ),
        weighted AS (
            SELECT *, POWER(DATE_DIFF('day', _MinDate, Date) + 1, $forgetfulness) AS DateWeight
            FROM joined
        ),
        summed AS (
            SELECT *,
                SUM(DateWeight) OVER ({window}) AS DateWeightSum,
                GREATEST(0, ({col} - _Average) - (_StdDev * $k)) * DateWeight AS DaySum
            FROM weighted
        )
        SELECT * EXCLUDE (DateWeight, DateWeightSum, DaySum),
            (SUM(DaySum) OVER ({window}) / DateWeightSum) * 7 AS {quote(cusum_column_name)}
        FROM summed
        ORDER BY {keys}, Date
    """, params={'k': k_value, 'forgetfulness': forgetfulness}, single_thread=True, t=df)


# Phase termination alerting thresholds. A day is flagged only when all four are
# exceeded, so raising any one of them makes the check less sensitive.
MAXOUT_ALERT_DEFAULTS = {
    'maxout_cusum_threshold': 0.25,      # cumulative sum of the shift, over a 7-day window
    'maxout_zscore_threshold': 4.0,      # standard deviations above the phase's 21-day mean
    'maxout_percent_threshold': 0.2,     # share of terminations that maxed out that day
    'maxout_min_services': 30,           # ignore phases that barely ran
}

# Max-out drain: a flagged day also needs its max outs to still be high when the day ends. Hour by hour
# through that day, each hour adds how far its max-out share sits above the same hour's average on the earlier days, less
# the drain, and the total never drops below zero. A spike that went back to normal drains away; a phase still maxing out
# keeps building. The total is in max-out share x hours: 1.0 is about one hour fully maxed out above normal.
MAXOUT_DRAIN_DEFAULTS = {
    'maxout_drain_enabled': True,
    'maxout_drain_per_hour': 0.1,
    'maxout_drain_threshold': 1.0,
}


def maxout_end_of_day_totals(days: pd.DataFrame, terminations: TableLike, drain_per_hour: float,
                             hours: int = 24) -> pd.Series:
    """The running max-out total at the end of each (DeviceId, Phase, Date) in days, as described at MAXOUT_DRAIN_DEFAULTS.

    terminations: the same rows the daily check used. Hours of the day with no terminations count as normal. hours: the
    day's hours run through (a today-so-far run stops at the hours there are). Returns a Series indexed like days.
    """
    if days.empty:
        return pd.Series(dtype='float64', index=days.index)
    wanted = days[['DeviceId', 'Phase']].drop_duplicates()
    hourly = run_sql("""
        SELECT t.DeviceId, t.Phase, DATE_TRUNC('hour', t.TimeStamp) AS Hour,
            SUM(CASE WHEN t.PerformanceMeasure IN ('MaxOut', 'ForceOff') THEN t.Total ELSE 0 END) / SUM(t.Total) AS Share
        FROM t SEMI JOIN w ON t.DeviceId = w.DeviceId AND t.Phase = w.Phase
        GROUP BY 1, 2, 3
        HAVING SUM(t.Total) > 0
    """, single_thread=True, t=terminations, w=wanted)
    hourly['Hour'] = pd.to_datetime(hourly['Hour'])
    hourly['Phase'] = hourly['Phase'].astype('int64')
    groups = {key: frame for key, frame in hourly.groupby(['DeviceId', 'Phase'])}
    totals = {}
    for index, row in days.iterrows():
        mine = groups.get((row['DeviceId'], int(row['Phase'])))
        day = pd.Timestamp(row['Date']).normalize()
        total = 0.0
        if mine is not None:
            before = mine[mine['Hour'] < day]
            usual = before.groupby(before['Hour'].dt.hour)['Share'].mean()
            today = mine[(mine['Hour'] >= day) & (mine['Hour'] < day + pd.Timedelta(days=1))]
            shares = dict(zip(today['Hour'].dt.hour, today['Share']))
            for hour in range(hours):
                normal = usual.get(hour, 0.0)
                total = max(0.0, total + shares.get(hour, normal) - normal - drain_per_hour)
        totals[index] = total
    return pd.Series(totals, dtype='float64')


def drain_maxout_alerts(alerts: pd.DataFrame, terminations: TableLike, drain_per_hour: float, threshold: float,
                        recency_days: int = None, hours: int = 24) -> pd.DataFrame:
    """alert()'s max-out result without the flagged days whose running total had drained to threshold or less by the day's
    end (hours: the hours of it there are), and without the phases left with no recent flagged day. Adds MaxOutEndTotal on
    the flagged days."""
    if alerts.empty:
        return alerts
    recency_days = DEFAULT_ALERT_RECENCY_DAYS if recency_days is None else recency_days
    alerts = alerts.copy()
    flagged = alerts[(alerts['Alert'] == 1) & (alerts['days_from_max'] <= recency_days)]
    totals = maxout_end_of_day_totals(flagged, terminations, drain_per_hour, hours)
    alerts['MaxOutEndTotal'] = totals.reindex(alerts.index)
    drained = totals.index[totals <= threshold]
    alerts.loc[drained, 'Alert'] = 0
    recent = alerts[(alerts['Alert'] == 1) & (alerts['days_from_max'] <= recency_days)][['DeviceId', 'Phase']].drop_duplicates()
    return alerts.merge(recent, on=['DeviceId', 'Phase'])


# Days back from the newest data an alert may have occurred and still be
# reported. 0 means it must have happened on the most recent day: an issue that
# came and went earlier in the week is not a live problem.
DEFAULT_ALERT_RECENCY_DAYS = 0


def alert(table: TableLike, maxout_thresholds: dict = None,
          recency_days: int = DEFAULT_ALERT_RECENCY_DAYS) -> pd.DataFrame:
    """Generate alerts based on CUSUM analysis results (the output of cusum()).

    maxout_thresholds overrides MAXOUT_ALERT_DEFAULTS for phase terminations; the
    missing-data conditions are unaffected. recency_days is how far
    back from the newest data an alert may have occurred and still be reported.
    Returns every row of each DeviceId (or DeviceId/Phase) that alerted recently
    enough, with z_score, Alert (0/1) and days_from_max added.
    """
    limits = {**MAXOUT_ALERT_DEFAULTS, **(maxout_thresholds or {})}
    table_columns = columns(table)

    trend = next((column for column in TREND_COLUMNS if column in table_columns), None)
    if trend is not None:
        # A phase trend: the same four tests as max outs, with the trend's own limits (measure_checks): the rise over
        # the phase's average in place of a fixed share, and its own count of cycles or vehicles (Volume).
        column, cusum_column_name, groups = trend, f'CUSUM_{trend}', ['DeviceId', 'Phase']
        condition = ("({cusum} > $cusum) AND (Volume >= $min_volume) "
                     "AND ((({col} - _Average) / _StdDev) > $zscore) AND ({col} - _Average >= $rise)")
        params = {key: limits[key] for key in ('cusum', 'min_volume', 'zscore', 'rise')}
    elif 'Percent MaxOut' in table_columns:
        column, cusum_column_name, groups = 'Percent MaxOut', 'CUSUM_Percent MaxOut', ['DeviceId', 'Phase']
        condition = ("({cusum} > $cusum) AND (Services > $min_services) "
                     "AND ((({col} - _Average) / _StdDev) > $zscore) AND ({col} > $percent)")
        params = {
            'cusum': limits['maxout_cusum_threshold'],
            'min_services': limits['maxout_min_services'],
            'zscore': limits['maxout_zscore_threshold'],
            'percent': limits['maxout_percent_threshold'],
        }
    elif 'MissingData' in table_columns:
        column, cusum_column_name, groups = 'MissingData', 'CUSUM_MissingData', ['DeviceId']
        condition = ("({cusum} > 0.1) AND ((({col} - _Average) / _StdDev) > 3) "
                     "AND ({col} > 0.05)")
        params = {}
    else:
        raise ValueError("Unknown data format for alert generation")

    col, cusum_col = quote(column), quote(cusum_column_name)
    condition = condition.format(cusum=cusum_col, col=col)
    keys = ', '.join(groups)
    on = ' AND '.join(f'r.{g} = a.{g}' for g in groups)
    params['recency_days'] = recency_days

    result = run_sql(f"""
        WITH r AS (
            SELECT *,
                ({col} - _Average) / _StdDev AS z_score,
                CAST({condition} AS INTEGER) AS Alert,
                ABS(DATE_DIFF('day', _MaxDate, Date)) AS days_from_max
            FROM t
        )
        SELECT r.*
        FROM r
        SEMI JOIN (
            SELECT DISTINCT {keys} FROM r WHERE Alert = 1 AND days_from_max <= $recency_days
        ) AS a ON {on}
        ORDER BY {', '.join('r.' + g for g in groups)}, r.Date
    """, params=params, single_thread=True, t=table)
    for date_column in ('Date', '_MinDate', '_MaxDate'):
        result[date_column] = result[date_column].astype('datetime64[us]')
    # Alert is 0/1; a NULL comparison (no spread to score against) counts as no alert.
    if result['Alert'].isna().any():
        result['Alert'] = result['Alert'].astype('float64')
    else:
        result['Alert'] = result['Alert'].astype('int32')
    return result
