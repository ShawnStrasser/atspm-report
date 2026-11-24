"""
Whether each problem of the report day still shows today: the 'Today' column of signal_problems.

A report covers its report day (yesterday in a daily run). Run in the morning with the data since midnight
(generate's as_of), it can also tell which of yesterday's problems are still there, so a max out that cleared
overnight is not read the next morning as a current problem. Today's rows only ever answer that question: they
never change what the report finds, its alerts or its state.

Today's hours are set against the same clock hours of the report day (a run at 6 AM compares midnight to 6 AM
with midnight to 6 AM), since a quiet night says nothing about the morning peak:

* Max outs: a phase that maxed out in those hours of the report day still does today when today's max-out share
  there is at least TODAY_SHARE of the report day's; it has cleared when it is under it.
* Phase skips: a phase that skipped in those hours of the report day still does when it skips today; cleared when
  it served with no skip.
* Missing data: still missing when today's hours miss at least MISSING_SHARE of their 15-minute records.

True: still there today. False: cleared today. None: today's hours cannot tell (too little of the day yet, no
rows those hours on the report day to set against, or a kind of problem this does not judge).
"""

from typing import Optional

import pandas as pd

from .duckdb_utils import is_empty, run_sql

TODAY_COLUMN = 'Today'
# Less of today than this: no judgement.
MIN_HOURS = 1.0
# Still maxing out: today's max-out share at least this share of the report day's over the same hours.
TODAY_SHARE = 0.5
# A max-out share under this is no max-out problem in those hours (nothing to set today against).
MIN_MAXOUT_SHARE = 0.2
# Still missing: at least this share of today's 15-minute records missing.
MISSING_SHARE = 0.2
BIN_MINUTES = 15

MAX_OUTS, SKIPS, MISSING = 'Max outs', 'Phase skips', 'Missing data'


def window(report_end, as_of):
    """(start, end) of today's hours (midnight to as_of, on the 15-minute bin), or None when too short."""
    if report_end is None or as_of is None:
        return None
    start = pd.Timestamp(report_end)
    end = pd.Timestamp(as_of).floor(f'{BIN_MINUTES}min')
    if (end - start) < pd.Timedelta(hours=MIN_HOURS):
        return None
    return start, end


def _maxout_shares(terminations, start, end) -> dict:
    """{(DeviceId, Phase): max-out share} over [start, end)."""
    if is_empty(terminations):
        return {}
    rows = run_sql("""
        SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(Phase AS INTEGER) AS Phase,
               SUM(CASE WHEN PerformanceMeasure IN ('MaxOut', 'ForceOff') THEN Total ELSE 0 END) / SUM(Total) AS Share
        FROM t
        WHERE TimeStamp >= $start AND TimeStamp < $end AND PerformanceMeasure IN ('MaxOut', 'ForceOff', 'GapOut')
        GROUP BY ALL
        HAVING SUM(Total) > 0
    """, params={'start': start.to_pydatetime(), 'end': end.to_pydatetime()}, t=terminations)
    return {(d, int(p)): float(s) for d, p, s in zip(rows['DeviceId'], rows['Phase'], rows['Share'])}


def _skips(phase_wait, start, end) -> dict:
    """{(DeviceId, Phase): skips} over [start, end) for the phases with rows there."""
    if is_empty(phase_wait):
        return {}
    rows = run_sql("""
        SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(Phase AS INTEGER) AS Phase, SUM(COALESCE(TotalSkips, 0)) AS Skips
        FROM t
        WHERE TimeStamp >= $start AND TimeStamp < $end
        GROUP BY ALL
    """, params={'start': start.to_pydatetime(), 'end': end.to_pydatetime()}, t=phase_wait)
    return {(d, int(p)): float(s) for d, p, s in zip(rows['DeviceId'], rows['Phase'], rows['Skips'])}


def _missing_share(has_data, start, end) -> dict:
    """{DeviceId: share of the 15-minute records missing over [start, end)} for the signals with any record since
    (a signal silent all along is the missing-data check's, not this one's)."""
    if is_empty(has_data):
        return {}
    bins = max(int((end - start) / pd.Timedelta(minutes=BIN_MINUTES)), 1)
    rows = run_sql("""
        SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, COUNT(DISTINCT TimeStamp) AS Seen
        FROM t
        WHERE TimeStamp >= $start AND TimeStamp < $end
        GROUP BY ALL
    """, params={'start': start.to_pydatetime(), 'end': end.to_pydatetime()}, t=has_data)
    return {d: max(0.0, 1.0 - float(seen) / bins) for d, seen in zip(rows['DeviceId'], rows['Seen'])}


def today_flags(problems: pd.DataFrame, report_end, as_of, terminations=None, phase_wait=None,
                has_data=None) -> pd.Series:
    """The Today value of each problem row (True, False or None), indexed like problems. terminations, phase_wait and
    has_data hold the report day and today (at least the same clock hours of both)."""
    out = pd.Series([None] * len(problems), index=problems.index, dtype=object)
    span = window(report_end, as_of)
    if span is None or problems is None or problems.empty:
        return out
    start, end = span
    before = (start - pd.Timedelta(days=1), end - pd.Timedelta(days=1))
    categories = set(problems['Category'])
    maxouts = ((_maxout_shares(terminations, *before), _maxout_shares(terminations, start, end))
               if MAX_OUTS in categories else ({}, {}))
    skips = (_skips(phase_wait, *before), _skips(phase_wait, start, end)) if SKIPS in categories else ({}, {})
    missing = (_missing_share(has_data, start, end) if MISSING in categories else {})
    for idx, row in problems.iterrows():
        device, category = str(row['DeviceId']), row['Category']
        phases = [int(p) for p in (row.get('OnPhases') or [])]
        if category == MAX_OUTS:
            out[idx] = _phase_verdict(device, phases, maxouts, lambda then, now: (
                None if then < MIN_MAXOUT_SHARE else now >= TODAY_SHARE * then))
        elif category == SKIPS:
            out[idx] = _phase_verdict(device, phases, skips, lambda then, now: None if then <= 0 else now > 0)
        elif category == MISSING:
            share = missing.get(device)
            out[idx] = None if share is None else bool(share >= MISSING_SHARE)
    return out


def _phase_verdict(device, phases, values, judge) -> Optional[bool]:
    """True when any of the problem's phases still shows it, False when all that can tell have cleared, else None."""
    before, now = values
    verdicts = []
    for phase in phases:
        key = (device, phase)
        if key not in before or key not in now:
            continue
        verdict = judge(before[key], now[key])
        if verdict is not None:
            verdicts.append(bool(verdict))
    if not verdicts:
        return None
    return any(verdicts)
