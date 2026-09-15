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
    such start time.
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

    return result[ALARM_ALERT_COLUMNS].reset_index(drop=True)
