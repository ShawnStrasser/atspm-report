"""Controller alarm processing.

Alarms come from raw EventId 174, which the ATSPM timeline query decodes into
one interval per set bit, and from EventId 173, whose enum names which kind of
flash a controller is in. A single 174 event can produce several alarm
intervals at the same instant.

The report shows a (signal, alarm type) pair only when that pair alarmed again
on the most recent day of data, but the count it displays covers the trailing
six weeks. That makes the section self-clearing: once a problem is fixed the
row stops appearing, and while it remains unfixed the row returns every day
carrying its accumulated history.

Timeline only ever covers a single day, so the six-week totals are accumulated
across runs in a small history file rather than recomputed from raw events.
"""

import pandas as pd

from .duckdb_utils import query_table
from .utils import log_message


# EventClass values the timeline query decodes from the EventId 174 bitmap.
BITMAP_ALARM_EVENT_CLASSES = [
    'Cycle Fault',
    'Coord Fault',
    'Coord Fail',
    'Cycle Fail',
    'MMU Flash',
    'Local Flash',
]

# Every class that means the controller is in flash. 'MMU Flash' and 'Local
# Flash' are bits in the EventId 174 bitmap; the 'Flash - *' classes come from
# the EventId 173 enum, which names the cause rather than just the fact.
#
# 'Flash - Not Flash' is deliberately excluded. It is the enum's healthy value,
# reported by nearly every signal every day (979 of them on 2026-08-30), so it
# is a normal running state, not an alarm, and not something to filter around.
NOT_FLASH_EVENT_CLASS = 'Flash - Not Flash'
FLASH_EVENT_CLASSES = [
    'MMU Flash',
    'Local Flash',
    'Flash - Other',
    'Flash - Automatic',
    'Flash - Local Manual',
    'Flash - Fault Monitor',
    'Flash - MMU',
    'Flash - Startup',
    'Flash - Preempt',
]

# Flash classes that still drive the clearance filter but are not worth
# reporting as alarms:
#
#   Flash - Automatic  Scheduled overnight flash, done deliberately, so it is
#                      normal operation rather than a fault.
#   Local Flash        The EventId 174 bit is redundant with the EventId 173
#                      enum, which names the cause. Verified on 2026-08-30: all
#                      28 Local Flash events coincided with a specific flash
#                      type (28 Flash - Local Manual), none stood alone.
#
# Flash - Startup is kept: a power-up is normal, but knowing a signal restarted
# is useful.
UNREPORTED_FLASH_EVENT_CLASSES = [
    'Flash - Automatic',
    'Local Flash',
]

# Alarms shown in the report: the 174 bitmap classes plus every flash type,
# minus the ones above.
ALARM_EVENT_CLASSES = [
    event_class
    for event_class in BITMAP_ALARM_EVENT_CLASSES + FLASH_EVENT_CLASSES
    if event_class not in UNREPORTED_FLASH_EVENT_CLASSES
]
ALARM_EVENT_CLASSES = list(dict.fromkeys(ALARM_EVENT_CLASSES))

ALARM_HISTORY_COLUMNS = ['DeviceId', 'AlarmType', 'Date', 'Count', 'LatestAlarm']
ALARM_ALERT_COLUMNS = ['DeviceId', 'AlarmType', 'Date', 'DayCount', 'TotalCount', 'LatestAlarm']

# Six weeks, inclusive of the report day.
ALARM_HISTORY_DAYS = 42


def _empty_history() -> pd.DataFrame:
    return pd.DataFrame(columns=ALARM_HISTORY_COLUMNS)


def summarize_daily_alarms(timeline: pd.DataFrame) -> pd.DataFrame:
    """Collapse timeline alarm intervals into one row per device, type and day.

    Returns columns DeviceId, AlarmType, Date, Count, LatestAlarm. Count is the
    number of alarm occurrences that started that day; LatestAlarm is the last
    such start time. An alarm reported at once and again when it ended has two
    rows with the same start: it counts once.
    """
    if timeline is None or timeline.empty or 'EventClass' not in timeline.columns:
        return _empty_history()

    alarms = timeline[timeline['EventClass'].isin(ALARM_EVENT_CLASSES)].copy()
    if alarms.empty:
        return _empty_history()

    alarms['StartTime'] = pd.to_datetime(alarms['StartTime'], errors='coerce')
    alarms = alarms.dropna(subset=['StartTime'])
    if alarms.empty:
        return _empty_history()

    alarms['DeviceId'] = alarms['DeviceId'].astype(str)
    alarms['AlarmType'] = alarms['EventClass'].astype(str)
    alarms['Date'] = alarms['StartTime'].dt.normalize()
    alarms = alarms.drop_duplicates(['DeviceId', 'AlarmType', 'StartTime'])

    daily = (
        alarms.groupby(['DeviceId', 'AlarmType', 'Date'], as_index=False)
        .agg(Count=('StartTime', 'size'), LatestAlarm=('StartTime', 'max'))
    )
    return daily[ALARM_HISTORY_COLUMNS]


def update_alarm_history(
    daily_alarms: pd.DataFrame,
    past_history: pd.DataFrame,
    retention_days: int = ALARM_HISTORY_DAYS,
    verbosity: int = 1,
) -> pd.DataFrame:
    """Merge today's counts into the stored history and drop anything expired.

    Rows for a date already present are replaced rather than added, so re-running
    the report for the same day does not inflate the totals.
    """
    history = past_history.copy() if past_history is not None and not past_history.empty else _empty_history()
    if not history.empty:
        history = history.reindex(columns=ALARM_HISTORY_COLUMNS)
        history['DeviceId'] = history['DeviceId'].astype(str)
        history['Date'] = pd.to_datetime(history['Date'], errors='coerce')
        history['LatestAlarm'] = pd.to_datetime(history['LatestAlarm'], errors='coerce')
        history = history.dropna(subset=['Date'])

    new_rows = daily_alarms if daily_alarms is not None else _empty_history()
    if not new_rows.empty and not history.empty:
        # Replace, don't append, for dates this run recomputed.
        refreshed_dates = set(new_rows['Date'].unique())
        history = history[~history['Date'].isin(refreshed_dates)]

    combined = pd.concat(
        [df for df in (history, new_rows) if not df.empty],
        ignore_index=True,
    ) if not (history.empty and new_rows.empty) else _empty_history()

    if combined.empty:
        return _empty_history()

    if retention_days > 0:
        cutoff = combined['Date'].max() - pd.Timedelta(days=retention_days - 1)
        before = len(combined)
        combined = combined[combined['Date'] >= cutoff]
        dropped = before - len(combined)
        if dropped > 0:
            log_message(
                f"Dropped {dropped} alarm history rows older than {retention_days} days.",
                2,
                verbosity,
            )

    return combined.reindex(columns=ALARM_HISTORY_COLUMNS).reset_index(drop=True)


def build_alarm_alerts(
    history: pd.DataFrame,
    report_date=None,
    window_days: int = ALARM_HISTORY_DAYS,
) -> pd.DataFrame:
    """Select the pairs that alarmed again on the report day, with six-week totals.

    A row appears only when the pair has at least one occurrence on report_date.
    TotalCount and LatestAlarm summarize the trailing `window_days` window.
    """
    if history is None or history.empty:
        return pd.DataFrame(columns=ALARM_ALERT_COLUMNS)

    hist = history.reindex(columns=ALARM_HISTORY_COLUMNS).copy()
    hist['DeviceId'] = hist['DeviceId'].astype(str)
    hist['Date'] = pd.to_datetime(hist['Date'], errors='coerce')
    hist['LatestAlarm'] = pd.to_datetime(hist['LatestAlarm'], errors='coerce')
    hist = hist.dropna(subset=['Date'])
    if hist.empty:
        return pd.DataFrame(columns=ALARM_ALERT_COLUMNS)

    report_date = hist['Date'].max() if report_date is None else pd.to_datetime(report_date).normalize()

    window_start = report_date - pd.Timedelta(days=window_days - 1)
    window = hist[(hist['Date'] >= window_start) & (hist['Date'] <= report_date)]
    if window.empty:
        return pd.DataFrame(columns=ALARM_ALERT_COLUMNS)

    totals = (
        window.groupby(['DeviceId', 'AlarmType'], as_index=False)
        .agg(TotalCount=('Count', 'sum'), LatestAlarm=('LatestAlarm', 'max'))
    )

    today = window[window['Date'] == report_date]
    if today.empty:
        return pd.DataFrame(columns=ALARM_ALERT_COLUMNS)

    today = (
        today.groupby(['DeviceId', 'AlarmType'], as_index=False)
        .agg(DayCount=('Count', 'sum'))
    )

    result = today.merge(totals, on=['DeviceId', 'AlarmType'], how='left')
    result['Date'] = report_date
    result['TotalCount'] = result['TotalCount'].fillna(result['DayCount']).astype(int)
    result['DayCount'] = result['DayCount'].astype(int)

    # One count per calendar day for the trend sparkline. A day without alarms
    # is a zero, not a gap. The series starts on the first day the history
    # covers, so days before any data was collected are not shown as zeros.
    days = pd.date_range(max(window_start, window['Date'].min()), report_date, freq='D')
    daily = window.groupby(['DeviceId', 'AlarmType', 'Date'])['Count'].sum()
    result['DailyCounts'] = [
        daily.loc[(device, alarm)].reindex(days, fill_value=0).astype(int).tolist()
        for device, alarm in zip(result['DeviceId'], result['AlarmType'])
    ]

    return result[ALARM_ALERT_COLUMNS + ['DailyCounts']].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Flash verification (flash_verification_enabled): what the raw 173 / 174 events say about each flash alarm
# ---------------------------------------------------------------------------
#
# Read from the raw events (controller_events: DeviceId, TimeStamp, EventId, Parameter; 173, 174 and the green starts 1),
# every signal in one query:
#  * Controllers re-log their flash state at midnight (23:59:59.x / 00:00:00.x): a 173 re-log that repeats the previous
#    state is not a change and is dropped. 174 is a bitmap: only a bit that turns on is an onset (MMU flash bit 4, local
#    flash bit 5), and onsets less than FLASH_BURST_SECONDS apart are one burst.
#  * Kept cycling: a flash the signal did not stop for, a green beginning within FLASH_GREEN_SECONDS after at least
#    half of its onsets (173: each onset; 174: each burst, from its start to FLASH_GREEN_SECONDS after its end). Only
#    MMU, fault monitor and local manual flash (173 = 6, 5, 4; 174 bits 4, 5): the controller's flash input flipping,
#    not a signal in flash. A review item.
#  * Ongoing: the flash is still on at the end of the data (the last 173 is that flash state; the 174 bit is still set
#    in the last 174). Only an MMU, fault monitor or startup flash still on is urgent; one that is over is a review item.
#  * 'MMU Flash' (174) and 'Flash - MMU' (173) are the same flash: the 174 row is left out where the 173 one is there.
# Automatic flash (a time-of-day schedule) is never an alarm here (UNREPORTED_FLASH_EVENT_CLASSES).

FLASH_EVENT_IDS = (1, 173, 174)
FLASH_GREEN_SECONDS = 180
FLASH_BURST_SECONDS = 60
# Flash alarms that are urgent while still on (and the signal not cycling through them).
URGENT_FLASH_CLASSES = ('MMU Flash', 'Flash - MMU', 'Flash - Fault Monitor', 'Flash - Startup')
# The same flash told by both events: the 174 class is left out where the 173 one is there.
SAME_FLASH = {'MMU Flash': 'Flash - MMU', 'Local Flash': 'Flash - Local Manual'}
# The flash a kept-cycling row is about, in words.
FLASH_INPUT_WORDS = {'MMU Flash': 'MMU', 'Flash - MMU': 'MMU', 'Flash - Fault Monitor': 'fault monitor',
                     'Local Flash': 'local', 'Flash - Local Manual': 'local'}
FLASH_VERDICT_COLUMNS = ['DeviceId', 'AlarmType', 'Onsets', 'KeptCycling', 'Ongoing', 'OngoingNow']

FLASH_VERDICT_SQL = """
WITH ev AS MATERIALIZED (
  SELECT CAST(DeviceId AS VARCHAR) AS d, ts, EventId AS code, Parameter AS p
  FROM (SELECT DeviceId, CAST(TimeStamp AS TIMESTAMP) AS ts, EventId, Parameter FROM raw WHERE EventId IN (1, 173, 174))
  WHERE ts >= {start} AND ts < {now_end}),
day AS (SELECT * FROM ev WHERE ts < {end}),
greens AS (SELECT d, ts FROM ev WHERE code = 1),
last AS (  -- the flash state at the end of the report day, and at the end of the data (as_of)
  SELECT d, arg_max(p, ts) FILTER (WHERE code = 173 AND ts < {end}) AS s173_day, arg_max(p, ts) FILTER (WHERE code = 173) AS s173_now,
         arg_max(p, ts) FILTER (WHERE code = 174 AND ts < {end}) AS v174_day, arg_max(p, ts) FILTER (WHERE code = 174) AS v174_now
  FROM ev WHERE code IN (173, 174) GROUP BY d),
f173 AS (  -- flash state changes; a midnight re-log of the same state is dropped
  SELECT d, ts, p FROM (SELECT d, ts, p, LAG(p) OVER (PARTITION BY d ORDER BY ts) AS prev FROM day WHERE code = 173)
  WHERE prev IS NULL OR p <> prev OR NOT (strftime(ts, '%H:%M:%S') IN ('23:59:59', '00:00:00'))),
fla AS (  -- flash onsets (a state other than 2, Not Flash) and whether a green began within {green} s
  SELECT f.d, f.p, g.ts IS NOT NULL AND epoch(g.ts) - epoch(f.ts) <= {green} AS cycled
  FROM (SELECT * FROM f173 WHERE p <> 2) f ASOF LEFT JOIN greens g ON g.d = f.d AND g.ts >= f.ts),
flash AS (
  SELECT f.d, 'Flash - ' || CASE f.p WHEN 1 THEN 'Other' WHEN 3 THEN 'Automatic' WHEN 4 THEN 'Local Manual'
                                     WHEN 5 THEN 'Fault Monitor' WHEN 6 THEN 'MMU' WHEN 7 THEN 'Startup' WHEN 8 THEN 'Preempt'
                                     ELSE 'state ' || f.p END AS event,
         COUNT(*) AS n, f.p IN (4, 5, 6) AND AVG(f.cycled::INT) >= 0.5 AS kept_cycling,
         COALESCE(f.p = ANY_VALUE(l.s173_day), false) AS ongoing, COALESCE(f.p = ANY_VALUE(l.s173_now), false) AS ongoing_now
  FROM fla f LEFT JOIN last l USING (d) GROUP BY f.d, f.p),
a174 AS (SELECT d, ts, p AS v, LAG(p) OVER (PARTITION BY d ORDER BY ts) AS prev FROM day WHERE code = 174),
on174 AS (  -- the MMU / local flash bit turning on; onsets less than {burst} s apart are one burst
  SELECT d, bit, ts, SUM(CASE WHEN gap IS NULL OR gap > {burst} THEN 1 ELSE 0 END) OVER (PARTITION BY d, bit ORDER BY ts) AS burst
  FROM (SELECT *, epoch(ts) - epoch(LAG(ts) OVER (PARTITION BY d, bit ORDER BY ts)) AS gap
        FROM (SELECT a.d, a.ts, b.bit FROM a174 a, (VALUES (4), (5)) b(bit)
              WHERE (a.v >> b.bit) & 1 = 1 AND (COALESCE(a.prev, 0) >> b.bit) & 1 = 0))),
bursts AS (  -- a green began during the burst or within {green} s after it: the signal kept cycling
  SELECT b.*, (g.ts IS NOT NULL AND g.ts <= b.t1 + INTERVAL {green} SECOND)::INT AS cycled
  FROM (SELECT d, bit, burst, COUNT(*) AS n, MIN(ts) AS t0, MAX(ts) AS t1 FROM on174 GROUP BY ALL) b
  ASOF LEFT JOIN greens g ON g.d = b.d AND g.ts >= b.t0),
alarm AS (
  SELECT b.d, CASE b.bit WHEN 4 THEN 'MMU Flash' ELSE 'Local Flash' END AS event, SUM(b.n)::BIGINT AS n,
         AVG(b.cycled) >= 0.5 AS kept_cycling,
         COALESCE((ANY_VALUE(l.v174_day) >> b.bit) & 1 = 1, false) AS ongoing,
         COALESCE((ANY_VALUE(l.v174_now) >> b.bit) & 1 = 1, false) AS ongoing_now
  FROM bursts b LEFT JOIN last l USING (d) GROUP BY b.d, b.bit)
SELECT d AS DeviceId, event AS AlarmType, n AS Onsets, kept_cycling AS KeptCycling, ongoing AS Ongoing, ongoing_now AS OngoingNow
FROM (SELECT * FROM flash UNION ALL BY NAME SELECT * FROM alarm)
ORDER BY 1, 2
"""


def sql_time(value) -> str:
    """A timestamp as a SQL literal."""
    return f"TIMESTAMP '{pd.Timestamp(value).isoformat(sep=' ')}'"


def flash_verdicts(controller_events, report_day, as_of=None) -> pd.DataFrame:
    """Each signal's flashes of the report day as the raw events tell them (FLASH_VERDICT_COLUMNS), every signal in one
    query: Onsets (173 onsets, or 174 bit onsets), KeptCycling, Ongoing (still on at the end of the report day) and
    OngoingNow (still on at as_of, from the rows after the report day up to it; at the end of the report day without
    it). controller_events: a DataFrame or DuckDB relation (DeviceId, TimeStamp, EventId, Parameter) with at least 1, 173
    and 174 over the report day (and up to as_of)."""
    if controller_events is None:
        return pd.DataFrame(columns=FLASH_VERDICT_COLUMNS)
    start = pd.Timestamp(report_day).normalize()
    end = start + pd.Timedelta(days=1)
    now_end = max(end, pd.Timestamp(as_of)) if as_of is not None else end
    sql = FLASH_VERDICT_SQL.format(start=sql_time(start), end=sql_time(end), now_end=sql_time(now_end),
                                   green=FLASH_GREEN_SECONDS, burst=FLASH_BURST_SECONDS)
    return query_table(controller_events, sql).reindex(columns=FLASH_VERDICT_COLUMNS)


def verify_flash_alarms(alarms: pd.DataFrame, verdicts: pd.DataFrame) -> pd.DataFrame:
    """The alarm rows with what the raw events tell of each flash: Onsets, KeptCycling, Ongoing and OngoingNow (None where
    the events tell nothing, as for an alarm that is no flash). A 174 flash is left out where the 173 row tells the same
    flash (SAME_FLASH)."""
    if alarms is None or alarms.empty:
        return alarms
    devices = alarms['DeviceId'].astype(str)
    told = set(zip(devices, alarms['AlarmType']))
    same = [alarm in SAME_FLASH and (device, SAME_FLASH[alarm]) in told for device, alarm in zip(devices, alarms['AlarmType'])]
    out = alarms[[not s for s in same]].copy()
    columns = ['Onsets', 'KeptCycling', 'Ongoing', 'OngoingNow']
    if verdicts is None or verdicts.empty:
        verdicts = pd.DataFrame(columns=FLASH_VERDICT_COLUMNS)
    found = {(str(row.DeviceId), row.AlarmType): row for row in verdicts.itertuples(index=False)}
    keys = list(zip(out['DeviceId'].astype(str), out['AlarmType']))
    for column in columns:
        values = [getattr(found[key], column) if key in found else None for key in keys]
        out[column] = pd.Series([None if v is None or pd.isna(v) else (int(v) if column == 'Onsets' else bool(v)) for v in values],
                                index=out.index, dtype=object)
    return out.reset_index(drop=True)
