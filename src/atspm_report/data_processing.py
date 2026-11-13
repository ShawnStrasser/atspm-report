import pandas as pd

from .duckdb_utils import TableLike, run_sql

# Inputs may be pandas DataFrames or DuckDB relations; results are pandas DataFrames.


def process_maxout_data(df: TableLike):
    """Process the max out data to calculate daily and hourly aggregates.

    Returns (daily, hourly) DataFrames.
    """
    daily = run_sql("""
        SELECT
            Date, DeviceId, Phase,
            SUM(MaxOutTotal) / SUM(Total) AS "Percent MaxOut",
            CAST(SUM(Total) AS BIGINT) AS Services
        FROM (
            SELECT
                CAST(TimeStamp AS DATE) AS Date, DeviceId, Phase, Total,
                CASE WHEN PerformanceMeasure IN ('MaxOut', 'ForceOff') THEN Total ELSE 0 END AS MaxOutTotal
            FROM t
        )
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """, t=df)
    daily['Date'] = daily['Date'].astype('datetime64[s]')

    # Hourly aggregates over the last 7 days (newest date minus 6 days onward)
    hourly = run_sql("""
        SELECT
            TimeStamp, DeviceId, Phase,
            SUM(MaxOutTotal) / SUM(Total) AS "Percent MaxOut",
            CAST(SUM(Total) AS BIGINT) AS Services
        FROM (
            SELECT
                DATE_TRUNC('hour', TimeStamp) AS TimeStamp, DeviceId, Phase, Total,
                CASE WHEN PerformanceMeasure IN ('MaxOut', 'ForceOff') THEN Total ELSE 0 END AS MaxOutTotal
            FROM t
            WHERE TimeStamp >= (SELECT MAX(CAST(TimeStamp AS DATE)) FROM t) - INTERVAL 6 DAY
        )
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """, t=df)
    return daily, hourly


# The feed has stopped when at least this share of the signals, and this many, last reported within FEED_STOP_SPREAD of each
# other, and that is more than FEED_STOP_SPREAD before the data should end.
FEED_STOP_SHARE = 0.6
FEED_STOP_MIN_SIGNALS = 5
FEED_STOP_SPREAD = pd.Timedelta(minutes=30)
BIN = pd.Timedelta(minutes=15)
# A last day holding less than this is too little to judge a signal's share of it missing.
FEED_STOP_MIN_DAY = pd.Timedelta(hours=2)
# A fault this close to the stop ran into the end of the data, and its end says nothing about it.
FEED_STOP_OPEN = pd.Timedelta(minutes=30)


def detect_feed_stop(has_data: TableLike, expected_end=None):
    """When the data stopped for the whole network, from the data alone, or None when it did not.

    A signal that goes quiet stops on its own; when the source itself stops being updated most signals' last records end together,
    short of when the data should end. expected_end: when that is (the end of the last full day a report covers); by default the
    end of the day most signals last reported on. Returns the end of those signals' last 15-minute record.
    """
    last = run_sql("""
        SELECT DeviceId, MAX(TimeStamp) AS LastBin FROM t
        WHERE TimeStamp >= (SELECT MAX(TimeStamp) FROM t) - INTERVAL 2 DAY
        GROUP BY 1
    """, t=has_data)
    if len(last) < FEED_STOP_MIN_SIGNALS:
        return None
    stamps = pd.to_datetime(last['LastBin'])
    middle = stamps.median().floor(BIN)
    stop = middle + BIN
    end = pd.Timestamp(expected_end) if expected_end is not None else middle.normalize() + pd.Timedelta(days=1)
    if end - stop <= FEED_STOP_SPREAD or ((stamps - middle).abs() <= FEED_STOP_SPREAD).mean() < FEED_STOP_SHARE:
        return None
    return stop


def trim_findings_to_stop(findings: pd.DataFrame, stop) -> pd.DataFrame:
    """Detector health findings without what lies past the data's end: nothing can be judged there."""
    if stop is None or findings is None or findings.empty:
        return findings
    starts = pd.to_datetime(findings['Start'], errors='coerce')
    kept = findings[starts.isna() | (starts < stop)].copy()
    ends = pd.to_datetime(kept['End'], errors='coerce')
    kept['End'] = ends.where(ends.isna() | (ends <= stop), stop)
    return kept


def mark_open_periods(alerts: pd.DataFrame, stop) -> pd.DataFrame:
    """The alerts with each period that ran into the end of the data marked 'open', so nobody reads its end as the fault's."""
    if stop is None or alerts is None or alerts.empty:
        return alerts

    def marked(periods):
        if not isinstance(periods, (list, tuple)):
            return periods
        return [{**period, 'open': bool(pd.to_datetime(period.get('end'), errors='coerce') >= stop - FEED_STOP_OPEN)} for period in periods]

    return alerts.assign(Periods=alerts['Periods'].map(marked))


def process_missing_data(has_data_df: TableLike, stop=None):
    """Process the missing data to calculate daily percent missing data.

    Every device gets a row for every date between the first and last date in
    the data; 96 fifteen-minute records is a full day. The first Sunday of November
    is 92: its repeated 1:00-2:00 am is left out of the data, so that hour is not missing.

    stop: when the whole feed stopped (detect_feed_stop). Nothing after it is missing: that day expects only the records up to
    it, and a device short of those has really lost them.
    """
    keep, after_stop = '', '0'
    if stop is not None:
        midnight = pd.Timestamp(stop).normalize()
        stop = pd.Timestamp(stop) if pd.Timestamp(stop) - midnight >= FEED_STOP_MIN_DAY else midnight
        keep = f"WHERE TimeStamp < TIMESTAMP '{stop}'"
        if stop > midnight:
            bins = round((midnight + pd.Timedelta(days=1) - stop) / BIN)
            after_stop = f"CASE WHEN dates.Date = DATE '{midnight.date()}' THEN {bins} ELSE 0 END"
    return run_sql(f"""
        WITH d AS (
            SELECT DeviceId, CAST(TimeStamp AS DATE) AS Date FROM t {keep}
        ),
        dates AS (
            SELECT CAST(generate_series AS DATE) AS Date
            FROM generate_series(
                CAST((SELECT MIN(Date) FROM d) AS TIMESTAMP),
                CAST((SELECT MAX(Date) FROM d) AS TIMESTAMP),
                INTERVAL 1 DAY)
        ),
        counts AS (
            SELECT DeviceId, Date, COUNT(*) AS RecordCount FROM d GROUP BY 1, 2
        )
        SELECT
            dev.DeviceId,
            dates.Date,
            GREATEST(0.0, 1 - COALESCE(counts.RecordCount, 0)
                / ((CASE WHEN month(dates.Date) = 11 AND day(dates.Date) <= 7 AND dayofweek(dates.Date) = 0
                        THEN 92.0 ELSE 96.0 END) - {after_stop})) AS MissingData
        FROM (SELECT DISTINCT DeviceId FROM d) AS dev
        CROSS JOIN dates
        LEFT JOIN counts
            ON dev.DeviceId = counts.DeviceId AND dates.Date = counts.Date
        ORDER BY dev.DeviceId, dates.Date
    """, t=has_data_df)


# Days of hourly missing data the report shows, up to the most recent day of data.
MISSING_HOURLY_DAYS = 7
# The first Sunday of November's repeated 1:00-2:00 am is left out of the data: that hour is not missing.
_DST_HOUR = "month({h}) = 11 AND day({h}) <= 7 AND dayofweek({h}) = 0 AND hour({h}) = 1"


def _hourly_grid(has_data_df: TableLike, stop, days):
    """(start, end, the filter of records before `stop`, the SQL of each hour's expected 15-minute records) of the hours of
    the last `days` days of data, from its first day at the earliest and up to `stop`, or None without data."""
    keep = f"WHERE TimeStamp < TIMESTAMP '{pd.Timestamp(stop)}'" if stop is not None else ''
    span = run_sql(f"SELECT MIN(CAST(TimeStamp AS DATE)) AS First, MAX(CAST(TimeStamp AS DATE)) AS Last FROM t {keep}", t=has_data_df)
    first, last = span['First'].iloc[0], span['Last'].iloc[0]
    if pd.isna(last):
        return None
    end = pd.Timestamp(last) + pd.Timedelta(days=1)
    if stop is not None:
        end = min(end, pd.Timestamp(stop).ceil('h'))
    start = max(pd.Timestamp(last) - pd.Timedelta(days=days - 1), pd.Timestamp(first))
    # The hour the feed stopped in expects only its records before the stop.
    expected = '4.0'
    if stop is not None and pd.Timestamp(stop) < end:
        stop = pd.Timestamp(stop)
        expected = f"CASE WHEN hours.Hour = TIMESTAMP '{stop.floor('h')}' THEN {round((stop - stop.floor('h')) / BIN)}.0 ELSE 4.0 END"
    return start, end, keep, expected


def missing_data_by_hour(has_data_df: TableLike, stop=None, days: int = MISSING_HOURLY_DAYS) -> pd.DataFrame:
    """Each device's share of missing 15-minute records per hour (DeviceId, TimeStamp = the hour, MissingData) over the last
    `days` days of data, to place an outage in time. Every device in the data gets every hour; the first Sunday of November's
    1:00 am hour is left out, and so is everything after `stop` (detect_feed_stop)."""
    grid = _hourly_grid(has_data_df, stop, days)
    if grid is None:
        return pd.DataFrame(columns=['DeviceId', 'TimeStamp', 'MissingData'])
    start, end, keep, expected = grid
    hourly = run_sql(f"""
        WITH d AS (SELECT DISTINCT DeviceId, TimeStamp FROM t {keep}),
        hours AS (
            SELECT generate_series AS Hour
            FROM generate_series(TIMESTAMP '{start}', TIMESTAMP '{end}' - INTERVAL 1 HOUR, INTERVAL 1 HOUR)
        ),
        counts AS (
            SELECT DeviceId, DATE_TRUNC('hour', TimeStamp) AS Hour, COUNT(*) AS Records
            FROM d WHERE TimeStamp >= TIMESTAMP '{start}' GROUP BY 1, 2
        )
        SELECT dev.DeviceId, hours.Hour AS TimeStamp,
               GREATEST(0.0, 1 - COALESCE(counts.Records, 0) / ({expected})) AS MissingData
        FROM (SELECT DISTINCT DeviceId FROM d) AS dev
        CROSS JOIN hours
        LEFT JOIN counts ON dev.DeviceId = counts.DeviceId AND hours.Hour = counts.Hour
        WHERE NOT ({_DST_HOUR.format(h='hours.Hour')})
        ORDER BY 1, 2
    """, t=has_data_df)
    hourly['TimeStamp'] = pd.to_datetime(hourly['TimeStamp'])
    return hourly


def region_missing_by_hour(has_data_df: TableLike, signals: TableLike, stop=None) -> pd.DataFrame:
    """Each region's average share of missing 15-minute records per hour (Region, TimeStamp, MissingData) over every day of
    data, the hourly view of the system outages. A region's devices are its devices in the data, as for the daily check."""
    grid = _hourly_grid(has_data_df, stop, days=10_000)
    if grid is None:
        return pd.DataFrame(columns=['Region', 'TimeStamp', 'MissingData'])
    start, end, keep, expected = grid
    hourly = run_sql(f"""
        WITH d AS (SELECT DISTINCT DeviceId, TimeStamp FROM t {keep}),
        devices AS (
            SELECT s.Region, COUNT(DISTINCT d.DeviceId) AS Devices
            FROM (SELECT DISTINCT DeviceId FROM d) AS d JOIN sig AS s ON d.DeviceId = s.DeviceId GROUP BY 1
        ),
        hours AS (
            SELECT generate_series AS Hour
            FROM generate_series(TIMESTAMP '{start}', TIMESTAMP '{end}' - INTERVAL 1 HOUR, INTERVAL 1 HOUR)
        ),
        counts AS (
            SELECT s.Region, DATE_TRUNC('hour', d.TimeStamp) AS Hour, COUNT(*) AS Records
            FROM d JOIN sig AS s ON d.DeviceId = s.DeviceId GROUP BY 1, 2
        )
        SELECT devices.Region, hours.Hour AS TimeStamp,
               GREATEST(0.0, 1 - COALESCE(counts.Records, 0) / (devices.Devices * ({expected}))) AS MissingData
        FROM devices
        CROSS JOIN hours
        LEFT JOIN counts ON devices.Region = counts.Region AND hours.Hour = counts.Hour
        WHERE NOT ({_DST_HOUR.format(h='hours.Hour')})
        ORDER BY 1, 2
    """, single_thread=True, t=has_data_df, sig=signals)
    hourly['TimeStamp'] = pd.to_datetime(hourly['TimeStamp'])
    return hourly
