"""Preempt frequency monitoring.

Preempt calls come from raw EventIds 102/104 (call input on/off), which the
ATSPM timeline query pairs into one ``Preempt`` interval per call with
``EventValue`` holding the preempt number. This module counts those calls per
signal, preempt number and day, and flags signal/preempt pairs whose recent
call frequency has shifted away from their own baseline.

Daily counts are sparse Poisson-like integers (most pairs fire about once a
day), so the percentage-based CUSUM in ``statistical_analysis`` does not fit:
a sample standard deviation near zero makes every blip a multi-sigma event.
Instead, each pair gets a robust baseline (the median daily count over the
older history) and a Poisson spread (``sqrt`` of the baseline, floored at 1),
and a two-sided CUSUM accumulates excesses over the most recent days of data.

Timeline only ever covers a single day, so daily counts are accumulated across
runs in a history file, exactly like controller alarms. The history also
records which devices reported data each day (rows with ``Preempt == 0``) so
that a day with no calls counts as zero rather than as a gap, and so a preempt
number that first appears at a signal is compared against a baseline of zeros.
"""

import numpy as np
import pandas as pd

from .utils import log_message


PREEMPT_HISTORY_COLUMNS = [
    'DeviceId', 'Preempt', 'Date', 'Count', 'ValidCount', 'TotalDuration', 'MaxDuration',
]
PREEMPT_ALERT_COLUMNS = [
    'DeviceId', 'Preempt', 'Date', 'Direction', 'BaselinePerDay', 'RecentPerDay',
    'BaselineDays', 'RecentDays', 'CusumScore', 'DailyCounts',
]

# Preempt numbers are 1-based, so 0 is free to mark "device reported data this
# day" without any calls being implied.
PRESENCE_PREEMPT = 0

# A day of timeline data has to span at least this many hours before it counts
# as a reported day. Partial outages otherwise read as a drop in preempt calls,
# which the missing data section already covers.
PREEMPT_MIN_COVERAGE_HOURS = 12

# Six weeks, inclusive of the report day.
PREEMPT_HISTORY_DAYS = 42

# Most recent days of data that the CUSUM accumulates over. Everything older
# forms the baseline, which must have at least PREEMPT_MIN_BASELINE_DAYS.
PREEMPT_RECENT_DAYS = 7
PREEMPT_MIN_BASELINE_DAYS = 10

# Textbook CUSUM settings, in units of the pair's spread (sigma). k is the
# slack subtracted from each day's deviation before it accumulates; h is the
# accumulated total that raises an alert. Decreases use a lower h because a
# pair can lose at most its baseline per day, so the low side accumulates
# slowly; 3 sigma catches a once-a-day preempt that goes silent for a week.
PREEMPT_CUSUM_K = 0.5
PREEMPT_CUSUM_H_INCREASE = 5.0
PREEMPT_CUSUM_H_DECREASE = 3.0

# Drops are only meaningful for preempts that normally fire at least daily; a
# quiet week from a preempt that fires every few days is not evidence of much.
PREEMPT_DECREASE_MIN_BASELINE_PER_DAY = 1.0

DIRECTION_INCREASE = 'Increase'
DIRECTION_DECREASE = 'Decrease'


def _empty_history() -> pd.DataFrame:
    return pd.DataFrame(columns=PREEMPT_HISTORY_COLUMNS)


def _empty_alerts() -> pd.DataFrame:
    return pd.DataFrame(columns=PREEMPT_ALERT_COLUMNS)


def _clean_history(history: pd.DataFrame) -> pd.DataFrame:
    if history is None or history.empty:
        return _empty_history()
    hist = history.reindex(columns=PREEMPT_HISTORY_COLUMNS).copy()
    hist['DeviceId'] = hist['DeviceId'].astype(str)
    hist['Preempt'] = pd.to_numeric(hist['Preempt'], errors='coerce')
    hist['Date'] = pd.to_datetime(hist['Date'], errors='coerce')
    hist = hist.dropna(subset=['Preempt', 'Date'])
    hist['Preempt'] = hist['Preempt'].astype(int)
    for col in ('Count', 'ValidCount'):
        hist[col] = pd.to_numeric(hist[col], errors='coerce').fillna(0).astype(int)
    for col in ('TotalDuration', 'MaxDuration'):
        hist[col] = pd.to_numeric(hist[col], errors='coerce')
    return hist


def summarize_daily_preempts(
    timeline: pd.DataFrame,
    min_coverage_hours: float = PREEMPT_MIN_COVERAGE_HOURS,
) -> pd.DataFrame:
    """Collapse timeline preempt intervals into one row per device, preempt and day.

    Returns the PREEMPT_HISTORY_COLUMNS. Count is the number of calls that
    started that day; ValidCount, TotalDuration and MaxDuration only cover
    calls whose 102/104 pair closed cleanly, since an unmatched call's
    duration is just the gap to the next call. A ``Preempt == 0`` row is added
    for every device whose timeline data spans at least ``min_coverage_hours``
    that day, recording that the device reported data.
    """
    if timeline is None or timeline.empty or 'EventClass' not in timeline.columns:
        return _empty_history()

    starts = pd.to_datetime(timeline['StartTime'], errors='coerce')
    device_ids = timeline['DeviceId'].astype(str)

    coverage = (
        pd.DataFrame({'DeviceId': device_ids, 'StartTime': starts})
        .dropna(subset=['StartTime'])
        .assign(Date=lambda df: df['StartTime'].dt.normalize())
        .groupby(['DeviceId', 'Date'])['StartTime']
        .agg(['min', 'max'])
    )
    coverage_hours = (coverage['max'] - coverage['min']).dt.total_seconds() / 3600
    presence = coverage[coverage_hours >= min_coverage_hours].reset_index()[['DeviceId', 'Date']]
    presence['Preempt'] = PRESENCE_PREEMPT
    presence['Count'] = 0
    presence['ValidCount'] = 0
    presence['TotalDuration'] = 0.0
    presence['MaxDuration'] = np.nan

    is_preempt = timeline['EventClass'].astype(str) == 'Preempt'
    calls = pd.DataFrame({
        'DeviceId': device_ids[is_preempt],
        'StartTime': starts[is_preempt],
        'Preempt': pd.to_numeric(timeline.loc[is_preempt, 'EventValue'], errors='coerce'),
        'Duration': pd.to_numeric(timeline.loc[is_preempt, 'Duration'], errors='coerce'),
        'IsValid': timeline.loc[is_preempt, 'IsValid'].fillna(False).astype(bool),
    }).dropna(subset=['StartTime', 'Preempt'])
    calls = calls[calls['Preempt'] > PRESENCE_PREEMPT]

    if calls.empty:
        daily = _empty_history()
    else:
        calls['Preempt'] = calls['Preempt'].astype(int)
        calls['Date'] = calls['StartTime'].dt.normalize()
        calls['ValidDuration'] = calls['Duration'].where(calls['IsValid'])
        daily = (
            calls.groupby(['DeviceId', 'Preempt', 'Date'], as_index=False)
            .agg(
                Count=('StartTime', 'size'),
                ValidCount=('ValidDuration', 'count'),
                TotalDuration=('ValidDuration', 'sum'),
                MaxDuration=('ValidDuration', 'max'),
            )
        )

    combined = pd.concat(
        [df for df in (presence, daily) if not df.empty],
        ignore_index=True,
    ) if not (presence.empty and daily.empty) else _empty_history()
    return combined.reindex(columns=PREEMPT_HISTORY_COLUMNS).reset_index(drop=True)


def update_preempt_history(
    daily_preempts: pd.DataFrame,
    past_history: pd.DataFrame,
    retention_days: int = PREEMPT_HISTORY_DAYS,
    verbosity: int = 1,
) -> pd.DataFrame:
    """Merge today's counts into the stored history and drop anything expired.

    Rows for a date already present are replaced rather than added, so
    re-running the report for the same day does not inflate the counts.
    """
    history = _clean_history(past_history)
    new_rows = _clean_history(daily_preempts)

    if not new_rows.empty and not history.empty:
        # Replace, don't append, for dates this run recomputed.
        refreshed_dates = set(new_rows['Date'].unique())
        history = history[~history['Date'].isin(refreshed_dates)]

    if history.empty and new_rows.empty:
        return _empty_history()
    combined = pd.concat(
        [df for df in (history, new_rows) if not df.empty],
        ignore_index=True,
    )

    if retention_days > 0:
        cutoff = combined['Date'].max() - pd.Timedelta(days=retention_days - 1)
        before = len(combined)
        combined = combined[combined['Date'] >= cutoff]
        dropped = before - len(combined)
        if dropped > 0:
            log_message(
                f"Dropped {dropped} preempt history rows older than {retention_days} days.",
                2,
                verbosity,
            )

    return combined.reindex(columns=PREEMPT_HISTORY_COLUMNS).reset_index(drop=True)


def build_preempt_alerts(
    history: pd.DataFrame,
    report_date=None,
    recent_days: int = PREEMPT_RECENT_DAYS,
    min_baseline_days: int = PREEMPT_MIN_BASELINE_DAYS,
    k: float = PREEMPT_CUSUM_K,
    h_increase: float = PREEMPT_CUSUM_H_INCREASE,
    h_decrease: float = PREEMPT_CUSUM_H_DECREASE,
    decrease_min_baseline: float = PREEMPT_DECREASE_MIN_BASELINE_PER_DAY,
) -> pd.DataFrame:
    """Flag signal/preempt pairs whose recent call frequency shifted from baseline.

    Only devices that reported data on ``report_date`` (default: the latest
    date in the history) are evaluated, so a pair is judged on fresh data.
    For each pair, days the device reported but the preempt did not fire count
    as zero. The most recent ``recent_days`` reported days form the CUSUM
    window; all earlier reported days form the baseline, which needs at least
    ``min_baseline_days`` days.

    With mu = median baseline daily count and sigma = sqrt(max(mu, 1)):

        increase score = sum(max(0, count - mu - k*sigma)) / sigma  > h_increase
        decrease score = sum(max(0, mu - count - k*sigma)) / sigma  > h_decrease

    A decrease is only considered when mu >= ``decrease_min_baseline``.
    Returns one row per alerting pair with the PREEMPT_ALERT_COLUMNS;
    DailyCounts holds the pair's full daily series for plotting.
    """
    hist = _clean_history(history)
    if hist.empty:
        return _empty_alerts()

    report_date = hist['Date'].max() if report_date is None else pd.to_datetime(report_date).normalize()

    presence = hist[['DeviceId', 'Date']].drop_duplicates()
    devices_on_report_date = set(presence.loc[presence['Date'] == report_date, 'DeviceId'])
    if not devices_on_report_date:
        return _empty_alerts()
    presence = presence[presence['DeviceId'].isin(devices_on_report_date) & (presence['Date'] <= report_date)]

    calls = hist[(hist['Preempt'] != PRESENCE_PREEMPT) & hist['DeviceId'].isin(devices_on_report_date)]
    if calls.empty:
        return _empty_alerts()
    pairs = calls[['DeviceId', 'Preempt']].drop_duplicates()

    # Every reported day for every pair, zero where the preempt did not fire.
    grid = pairs.merge(presence, on='DeviceId', how='inner')
    grid = grid.merge(
        calls[['DeviceId', 'Preempt', 'Date', 'Count']],
        on=['DeviceId', 'Preempt', 'Date'],
        how='left',
    )
    grid['Count'] = grid['Count'].fillna(0).astype(int)
    grid = grid.sort_values(['DeviceId', 'Preempt', 'Date']).reset_index(drop=True)

    pair_cols = ['DeviceId', 'Preempt']
    grid['DaysFromEnd'] = grid.groupby(pair_cols).cumcount(ascending=False)
    baseline = grid[grid['DaysFromEnd'] >= recent_days]
    recent = grid[grid['DaysFromEnd'] < recent_days]

    stats = (
        baseline.groupby(pair_cols, as_index=False)
        .agg(BaselinePerDay=('Count', 'median'), BaselineDays=('Count', 'size'))
    )
    stats = stats[stats['BaselineDays'] >= min_baseline_days]
    if stats.empty:
        return _empty_alerts()

    stats['Sigma'] = np.sqrt(np.maximum(stats['BaselinePerDay'], 1.0))
    scored = recent.merge(stats, on=pair_cols, how='inner')
    slack = k * scored['Sigma']
    scored['HighExcess'] = np.maximum(0.0, scored['Count'] - scored['BaselinePerDay'] - slack)
    scored['LowExcess'] = np.maximum(0.0, scored['BaselinePerDay'] - scored['Count'] - slack)

    summary = (
        scored.groupby(pair_cols, as_index=False)
        .agg(
            RecentPerDay=('Count', 'mean'),
            RecentDays=('Count', 'size'),
            HighSum=('HighExcess', 'sum'),
            LowSum=('LowExcess', 'sum'),
        )
        .merge(stats, on=pair_cols, how='left')
    )
    summary['IncreaseScore'] = summary['HighSum'] / summary['Sigma']
    summary['DecreaseScore'] = summary['LowSum'] / summary['Sigma']

    increase = summary['IncreaseScore'] > h_increase
    decrease = (
        (summary['DecreaseScore'] > h_decrease)
        & (summary['BaselinePerDay'] >= decrease_min_baseline)
        & ~increase
    )
    summary['Direction'] = np.select(
        [increase, decrease], [DIRECTION_INCREASE, DIRECTION_DECREASE], default=None,
    )
    summary['CusumScore'] = np.where(increase, summary['IncreaseScore'], summary['DecreaseScore'])
    alerts = summary[summary['Direction'].notna()].copy()
    if alerts.empty:
        return _empty_alerts()

    daily_series = (
        grid.groupby(pair_cols)['Count']
        .agg(lambda counts: [int(c) for c in counts])
        .rename('DailyCounts')
        .reset_index()
    )
    alerts = alerts.merge(daily_series, on=pair_cols, how='left')
    alerts['Date'] = report_date
    alerts['BaselinePerDay'] = alerts['BaselinePerDay'].astype(float)
    alerts['RecentPerDay'] = alerts['RecentPerDay'].astype(float)
    alerts['BaselineDays'] = alerts['BaselineDays'].astype(int)
    alerts['RecentDays'] = alerts['RecentDays'].astype(int)
    alerts['CusumScore'] = alerts['CusumScore'].astype(float)

    return (
        alerts.sort_values(['CusumScore'], ascending=False)
        [PREEMPT_ALERT_COLUMNS]
        .reset_index(drop=True)
    )
