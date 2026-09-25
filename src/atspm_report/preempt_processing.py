"""Preempt frequency monitoring.

Preempt calls come from raw EventIds 102/104 (call input on/off), which the
ATSPM timeline query pairs into one ``Preempt`` interval per call with
``EventValue`` holding the preempt number. This module counts those calls per
signal, preempt number and day, and flags signal/preempt pairs whose recent
call frequency has shifted away from their own baseline.

Daily counts are sparse integers (most pairs fire a few times a day at most)
and overdispersed: emergency runs cluster, and neighbouring signals on one
route rise and fall together, so day-to-day variance runs well above the
Poisson value. A preempt's call rate also drifts with traffic, which is not an
equipment problem. The check is for broken detection, so each pair's
recent-week total is tested against a negative binomial fitted to its own
baseline days (mean and variance, never narrower than Poisson), and an alert
also needs a change only a fault explains: a busy preempt going silent, or an
input firing erratically at several times its usual rate.

Timeline only ever covers a single day, so daily counts are accumulated across
runs in a history file, exactly like controller alarms. The history also
records which devices reported data each day (rows with ``Preempt == 0``) so
that a day with no calls counts as zero rather than as a gap, and so a preempt
number that first appears at a signal is compared against a baseline of zeros.
"""

import math
from typing import Optional

import numpy as np
import pandas as pd

from .utils import log_message


PREEMPT_HISTORY_COLUMNS = [
    'DeviceId', 'Preempt', 'Date', 'Count', 'ValidCount', 'TotalDuration', 'MaxDuration',
]
PREEMPT_ALERT_COLUMNS = [
    'DeviceId', 'Preempt', 'Date', 'Direction', 'BaselinePerDay', 'RecentPerDay',
    'BaselineDays', 'RecentDays', 'Score', 'DailyCounts',
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

# Most recent days of data that form the test window. Everything older forms
# the baseline, which must have at least PREEMPT_MIN_BASELINE_DAYS.
PREEMPT_RECENT_DAYS = 7
PREEMPT_MIN_BASELINE_DAYS = 10

# Chance of the recent week's total (or a more extreme one) under the baseline
# that raises an alert. Every signal/preempt pair is tested every day, so this
# is kept small. Backtested on six weeks of a city's preempts, together with
# the size gates below, it flagged only a busy preempt that went silent for
# six days.
PREEMPT_MAX_P_VALUE = 1e-3

# Baseline rate assumed for a preempt that barely fired before, so a new
# preempt number is judged against about one call every four days, not zero.
PREEMPT_MIN_BASELINE_PER_DAY = 0.25

# An increase must triple the baseline and add at least this many calls a day,
# which an erratic input does (3 a day jumping to 20) but busy weeks on an
# emergency route (1 a day rising to 3 or 4) do not.
PREEMPT_INCREASE_MIN_RATIO = 3.0
PREEMPT_INCREASE_MIN_EXTRA_PER_DAY = 5.0

# A decrease must drop to a tenth of the baseline or less, i.e. the preempt
# has all but stopped, and is only considered for preempts that normally fire
# at least daily; a quiet week from a preempt that fires every few days, or a
# few quiet days in a week, is not evidence of a fault.
PREEMPT_DECREASE_MAX_RATIO = 0.1
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
    device_days: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Collapse timeline preempt intervals into one row per device, preempt and day.

    Returns the PREEMPT_HISTORY_COLUMNS. Count is the number of calls that
    started that day; ValidCount, TotalDuration and MaxDuration only cover
    calls whose 102/104 pair closed cleanly, since an unmatched call's
    duration is just the gap to the next call. A ``Preempt == 0`` row is added
    for every device/day the device reported data: the rows of ``device_days``
    (columns DeviceId, Date) when given, otherwise every device whose timeline
    data spans at least ``min_coverage_hours`` that day.
    """
    if timeline is None or timeline.empty or 'EventClass' not in timeline.columns:
        return _empty_history()

    starts = pd.to_datetime(timeline['StartTime'], errors='coerce')
    device_ids = timeline['DeviceId'].astype(str)

    if device_days is not None and not device_days.empty:
        presence = pd.DataFrame({
            'DeviceId': device_days['DeviceId'].astype(str),
            'Date': pd.to_datetime(device_days['Date'], errors='coerce'),
        }).dropna(subset=['Date'])
        presence['Date'] = presence['Date'].dt.normalize()
        presence = presence.drop_duplicates().reset_index(drop=True)
    else:
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


def _count_cdf(count: int, mean: float, var: float) -> float:
    """P(X <= count) for the negative binomial with this mean and variance.

    Poisson when the variance is not above the mean. Terms are built by the
    ratio P(k+1)/P(k) in log space, which stays accurate when the variance is
    barely above the mean (a huge NB size, where lgamma differences lose all
    precision) and for large means (where P(0) underflows).
    """
    if count < 0:
        return 0.0
    if var <= mean:
        log_term = -mean
        ratio = lambda k: math.log(mean) - math.log(k + 1)
    else:
        size = mean * mean / (var - mean)
        log_q = math.log(mean / (size + mean))  # log(1 - p)
        log_term = size * math.log1p(-mean / (size + mean))  # size * log(p)
        ratio = lambda k: math.log(k + size) - math.log(k + 1) + log_q
    total = 0.0
    for k in range(count + 1):
        total += math.exp(log_term)
        log_term += ratio(k)
    return min(1.0, total)


def build_preempt_alerts(
    history: pd.DataFrame,
    report_date=None,
    recent_days: int = PREEMPT_RECENT_DAYS,
    min_baseline_days: int = PREEMPT_MIN_BASELINE_DAYS,
    max_p_value: float = PREEMPT_MAX_P_VALUE,
    min_baseline: float = PREEMPT_MIN_BASELINE_PER_DAY,
    increase_min_ratio: float = PREEMPT_INCREASE_MIN_RATIO,
    increase_min_extra: float = PREEMPT_INCREASE_MIN_EXTRA_PER_DAY,
    decrease_max_ratio: float = PREEMPT_DECREASE_MAX_RATIO,
    decrease_min_baseline: float = PREEMPT_DECREASE_MIN_BASELINE_PER_DAY,
) -> pd.DataFrame:
    """Flag signal/preempt pairs whose recent call frequency shifted from baseline.

    Only devices that reported data on ``report_date`` (default: the latest
    date in the history) are evaluated, so a pair is judged on fresh data.
    For each pair, days the device reported but the preempt did not fire count
    as zero. The most recent ``recent_days`` reported days form the test
    window; all earlier reported days form the baseline, which needs at least
    ``min_baseline_days`` days.

    With mu = max(baseline mean, ``min_baseline``) and v = max(baseline
    variance, mu), the recent total S over n days is compared with a negative
    binomial of mean n*mu and variance n*v:

        increase: recent/day >= increase_min_ratio * mu,
                  recent/day >= mu + increase_min_extra, and P(X >= S) < max_p_value
        decrease: baseline mean >= decrease_min_baseline,
                  recent/day <= decrease_max_ratio * baseline mean, and P(X <= S) < max_p_value

    Returns one row per alerting pair with the PREEMPT_ALERT_COLUMNS; Score is
    -log10 of the p-value (higher is more certain) and DailyCounts holds the
    pair's full daily series for plotting.
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
        .agg(
            BaselinePerDay=('Count', 'mean'),
            BaselineVar=('Count', 'var'),
            BaselineDays=('Count', 'size'),
        )
    )
    stats = stats[stats['BaselineDays'] >= min_baseline_days]
    if stats.empty:
        return _empty_alerts()

    summary = (
        recent.groupby(pair_cols, as_index=False)
        .agg(RecentTotal=('Count', 'sum'), RecentDays=('Count', 'size'))
        .merge(stats, on=pair_cols, how='inner')
    )
    summary['RecentPerDay'] = summary['RecentTotal'] / summary['RecentDays']
    summary['Rate'] = np.maximum(summary['BaselinePerDay'], min_baseline)

    # Size-of-change gates first; the tail probability is only computed for
    # the few pairs that pass them.
    increase = (
        (summary['RecentPerDay'] >= increase_min_ratio * summary['Rate'])
        & (summary['RecentPerDay'] >= summary['Rate'] + increase_min_extra)
    )
    decrease = (
        ~increase
        & (summary['BaselinePerDay'] >= decrease_min_baseline)
        & (summary['RecentPerDay'] <= decrease_max_ratio * summary['BaselinePerDay'])
    )
    summary['Direction'] = np.select(
        [increase, decrease], [DIRECTION_INCREASE, DIRECTION_DECREASE], default=None,
    )
    summary = summary[summary['Direction'].notna()].copy()
    if summary.empty:
        return _empty_alerts()

    def p_value(row) -> float:
        mean = row['RecentDays'] * row['Rate']
        var = row['RecentDays'] * max(row['BaselineVar'], row['Rate'])
        total = int(row['RecentTotal'])
        if row['Direction'] == DIRECTION_INCREASE:
            return max(0.0, 1.0 - _count_cdf(total - 1, mean, var))
        return _count_cdf(total, mean, var)

    summary['PValue'] = summary.apply(p_value, axis=1)
    alerts = summary[summary['PValue'] < max_p_value].copy()
    if alerts.empty:
        return _empty_alerts()
    alerts['Score'] = -np.log10(np.maximum(alerts['PValue'], 1e-15))

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
    alerts['Score'] = alerts['Score'].astype(float)

    return (
        alerts.sort_values(['Score'], ascending=False)
        [PREEMPT_ALERT_COLUMNS]
        .reset_index(drop=True)
    )
