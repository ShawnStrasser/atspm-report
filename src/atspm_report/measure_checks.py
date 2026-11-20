"""Checks on atspm's per-phase measures (see measure_inputs): a sudden rise in delay per vehicle, and two trends, rising
split failures and a falling platoon ratio. Each has its own switch, off by default, and its own report section; none is
part of Signal Focus. Each judges every day from the first day the last report did not cover to the report day
(check_days: a Monday run judges Friday, Saturday and Sunday); a phase flagged on several of them is told once, on its
latest flagged day, and the text names the day when it is not the report day.

Sudden rise in delay per vehicle (delay_rise_alerts). Each hour of a judged day, per phase, against the same hour on
earlier days of the same kind (weekdays, Saturdays, Sundays; holidays left out, and no check on a holiday). An hour is
flagged when delay per vehicle is STEP_MIN_RISE_SECONDS and STEP_MIN_RISE_SHARE over the average of those days, on
STEP_MIN_BASELINE_DAYS or more of them, and STEP_MIN_SD standard deviations above them (the spread taken as at least
STEP_SD_FLOOR_SECONDS). A phase alerts with STEP_MIN_FLAGGED_HOURS or more flagged hours in the day. Arrivals on green and
the platoon ratio are told as supporting evidence when the platoon ratio fell too (STEP_PLATOON_DROP and STEP_MIN_SD, its
spread at least STEP_PLATOON_SD_FLOOR) in half or more of the flagged hours: alone it flagged 8 to 10 signals a day, so it
never raises the alert. These are the study's numbers (2026-11-16, ODOT data 11-09 to 11-15). It needs 3 to 5 days of the
same kind before it can judge an hour, so weekends start after three weekends of data.
Two rules from the review of 11-13 and 11-16 (2026-11-17): an earlier day's hour counts only when it ran the same timing
plan as the judged day (coordination_agg's Pattern; a Friday afternoon plan was flagged against Monday to Thursday plans;
about 2 % of hours differ), and a phase is judged only in hours it usually gets STEP_MIN_GREEN_SECONDS of green (a phase
almost never served had a delay figure near zero, then 64 s on the day it was served; 1.9 % of phase-hours). Known
detector faults on another phase of the signal, STEP_OTHER_FAULT_MINUTES or more in the flagged hours, are told in the
text, not left out (leaving those hours out would blank about a quarter of all signal-hours): a held or lost call on one
phase changes how the others are served. A few minutes of erratic calls are not told.

Trends (trend_alerts). One value per phase and day: the share of cycles that failed, or the platoon ratio. Each judged
day is compared with the phase's earlier days of the same kind only (weekdays with weekdays, weekend days with weekend
days; holidays left out) inside the history window: its latest TREND_RECENT_DAYS days of the kind (ending on the judged
day) against the days before them (TREND_MIN_BASELINE_DAYS or more). A day is flagged either way:
- one day: the day alone is `jump` over the earlier days' average and `zscore` of their spread above it;
- several days: the recent days' average is `rise` over the earlier days' average and `zscore` standard errors above it
  (the spread times the square root of 1/recent + 1/earlier days), with the day itself still `rise` above it. A step that
  lasted a few days and a steady climb are caught; one bad day echoing in the recent average is not.
The spread is the day-to-day one, from each earlier day to the next (so a steady climb does not hide itself by widening
it), taken as at least rise / zscore: for a steady phase the rise decides, for a noisy one the z-score. The
rises are the study's (+10 points of split failures, 0.2 of platoon ratio). The one-day jump is twice that: on 11-13 and
11-16, judged against the weekdays before, a one-day +0.10 passed at 4 to 7 phases a day for split failures and a 0.2 fall
at 20 to 39 for platoon ratio; twice those, 0 to 3 and 8 to 12. Provisional until about three weeks of data are reviewed.
Left out: hours with a known fault on the zones the measure comes from (measure_inputs), and then the whole phase-day,
since a day with hours missing is not comparable with full days; days with too few cycles or arrivals; and a phase's
split failures for the whole day when its zones fail overnight (TREND_NIGHT_SPLIT_FAIL_SHARE of cycles from 0 to 5 h:
a zone held on fails every cycle).
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from . import measure_inputs as mi
from .duckdb_utils import columns as table_columns
from .statistical_analysis import PLATOON_FALL_COLUMN, SPLIT_FAIL_COLUMN

DELAY_RISE = 'delay_rise'
SPLIT_FAIL_TREND = 'split_failure_trend'
PLATOON_TREND = 'platoon_ratio_trend'
SECTION_TITLES = {DELAY_RISE: 'Sudden Rise in Delay', SPLIT_FAIL_TREND: 'Rising Split Failures',
                  PLATOON_TREND: 'Falling Platoon Ratio'}
ID_COLUMNS = ['DeviceId', 'Phase']

# ---- Sudden rise in delay per vehicle: the study's proposal (2026-11-16, ODOT data 11-09 to 11-15) ------------------------
STEP_MIN_RISE_SECONDS = 5.0
STEP_MIN_RISE_SHARE = 0.5
STEP_MIN_SD = 3.0
STEP_SD_FLOOR_SECONDS = 1.5
STEP_MIN_BASELINE_DAYS = 3
STEP_MAX_BASELINE_DAYS = 5          # the latest days of the kind
STEP_LOOKBACK_DAYS = 35             # five weeks: five Saturdays
STEP_MIN_FLAGGED_HOURS = 4
STEP_MIN_ARRIVALS_PER_HOUR = 30     # 08043's side streets carry 30 to 90 an hour
STEP_MIN_GREEN_SECONDS = 60.0       # green in the hour, on average over the earlier days
STEP_OTHER_FAULT_MINUTES = 15.0     # another phase's faults in the flagged hours worth telling (one 15-minute bin)
STEP_PLATOON_DROP = 0.2
STEP_PLATOON_SD_FLOOR = 0.05

# ---- Trends -----------------------------------------------------------------------------------------------------------
WEEKDAY, WEEKEND = 'weekdays', 'weekend days'
SPLIT_FAIL_TREND_DEFAULTS = {'rise': 0.10, 'jump': 0.20, 'zscore': 4.0, 'min_volume': 30}
PLATOON_TREND_DEFAULTS = {'rise': 0.2, 'jump': 0.4, 'zscore': 4.0, 'min_volume': 300}
TREND_RECENT_DAYS = {WEEKDAY: 3, WEEKEND: 2}
TREND_MIN_BASELINE_DAYS = {WEEKDAY: 8, WEEKEND: 3}
TREND_HISTORY_DAYS = 21
TREND_NIGHT_SPLIT_FAIL_SHARE = 0.3
TREND_NIGHT_MIN_CYCLES = 20

ALERT_COLUMNS = ['DeviceId', 'Date', 'Phase', 'Text', 'Hours', 'Value', 'Usual']


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=ALERT_COLUMNS)


def _hour_spans(hours) -> str:
    """'8:00-10:00, 13:00-17:00' from a list of hours of the day."""
    hours = sorted(int(h) for h in hours)
    spans, start = [], None
    for k, h in enumerate(hours):
        if start is None:
            start = h
        if k + 1 == len(hours) or hours[k + 1] != h + 1:
            spans.append(f"{start}:00-{h + 1}:00")
            start = None
    return ', '.join(spans)


def _kind_days(day, earlier: list) -> str:
    kind = mi.day_kind(day).lower()
    n = len(earlier)
    return f"the last {n} {kind}" if n != 1 else f"the last {kind[:-1]}"


def _day_label(day) -> str:
    day = pd.Timestamp(day)
    return f"{day:%a} {day.month}/{day.day}"


def judged_days(report_day, check_days: int = 1) -> list:
    """The check_days days ending on report_day, oldest first."""
    day = pd.Timestamp(report_day).normalize()
    return list(pd.date_range(day - pd.Timedelta(days=max(int(check_days), 1) - 1), day))


def _latest_per_phase(frames: list, report_day) -> pd.DataFrame:
    """One row per phase, its latest flagged day; the text names the day when it is not the report day."""
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return _empty()
    out = pd.concat(frames, ignore_index=True)
    out = out.assign(Date=pd.to_datetime(out['Date']).dt.normalize()).sort_values('Date')
    out = out.drop_duplicates(['DeviceId', 'Phase'], keep='last').reset_index(drop=True)
    earlier = out['Date'] < pd.Timestamp(report_day).normalize()
    out.loc[earlier, 'Text'] = [f"On {_day_label(d)}: {t}" for d, t in zip(out.loc[earlier, 'Date'], out.loc[earlier, 'Text'])]
    return out.sort_values(['DeviceId', 'Phase']).reset_index(drop=True)[ALERT_COLUMNS]


# ======================================================================================================================
# Sudden rise in delay per vehicle
# ======================================================================================================================

def _hourly_plans(coordination, start, end, devices, days) -> pd.DataFrame:
    """The timing plan (Pattern) each signal ran most of each hour (DeviceId, hr, pat); empty without the column."""
    empty = pd.DataFrame({'DeviceId': pd.Series(dtype=str), 'hr': pd.Series(dtype='datetime64[us]'),
                          'pat': pd.Series(dtype='float64')})
    if not mi.present(coordination) or 'Pattern' not in table_columns(coordination):
        return empty
    plans = mi._rollup(coordination, f"CAST(DeviceId AS VARCHAR) AS DeviceId, {mi.HOUR} AS hr, "
                       "CAST(mode(Pattern) AS DOUBLE) AS pat", '1, 2', start, end, devices, days)
    return plans if not plans.empty else empty


def _other_phase_faults(faults: Optional[pd.DataFrame], device, phase, day, hours) -> list:
    """Other phases of the signal with known detector faults of STEP_OTHER_FAULT_MINUTES or more in these hours of the
    day (a few minutes of erratic calls do not explain hours of delay)."""
    if faults is None or faults.empty:
        return []
    f = faults[(faults['DeviceId'].astype(str) == str(device)) & (faults['Phase'] != int(phase))]
    minutes = {}
    for h in hours if not f.empty else []:
        a = pd.Timestamp(day) + pd.Timedelta(hours=int(h))
        z = a + pd.Timedelta(hours=1)
        hit = f[(f['s'] < z) & (f['e'] > a)]
        overlap = (hit['e'].clip(upper=z) - hit['s'].clip(lower=a)).dt.total_seconds() / 60
        for p, m in overlap.groupby(hit['Phase']).sum().items():
            minutes[int(p)] = minutes.get(int(p), 0.0) + m
    return sorted(p for p, m in minutes.items() if m >= STEP_OTHER_FAULT_MINUTES)


def _delay_rise_day(approach_delay, arrival_on_green, day, faults, platoon_ratio, devices, coordination) -> pd.DataFrame:
    """delay_rise_alerts for one judged day."""
    if day.date() in mi.holidays(day, day):
        return _empty()
    start, end = day - pd.Timedelta(days=STEP_LOOKBACK_DAYS), day + pd.Timedelta(days=1)
    kind = mi.day_kind(day)
    off = set(mi.holidays(start, end))
    candidates = [d for d in pd.date_range(start, day - pd.Timedelta(days=1)) if mi.day_kind(d) == kind and d.date() not in off]
    baseline = candidates[-STEP_MAX_BASELINE_DAYS:]
    if len(baseline) < STEP_MIN_BASELINE_DAYS:
        return _empty()
    days = pd.DataFrame({'day': [d.date() for d in baseline + [day]], 'test': [False] * len(baseline) + [True]})
    judged = baseline + [day]
    hours = mi.hourly_delay(approach_delay, arrival_on_green, judged[0], end, devices, judged)
    if hours.empty:
        return _empty()
    platoon = mi.hourly_platoon(platoon_ratio, judged[0], end, devices, judged)
    plans = _hourly_plans(coordination, judged[0], end, devices, judged)
    faults = faults if faults is not None else mi.fault_spans()
    con = mi.connect()
    try:
        con.register('h_in', hours)
        con.register('p_in', platoon)
        con.register('plan_in', plans)
        con.register('days_in', days)
        con.register('faults', faults)
        found = con.execute(f"""
            WITH h AS (
                SELECT h.*, CAST(h.hr AS DATE) AS day, hour(h.hr) AS hh, h.dsum / h.veh AS dpv, 100.0 * h.gre / h.veh AS aog
                FROM h_in h
                WHERE h.bins = 4 AND h.veh >= {STEP_MIN_ARRIVALS_PER_HOUR} AND NOT {mi.masked_sql('h', mi.ADVANCE)}
            ), p AS (
                SELECT p.DeviceId, p.Phase, p.hr, p.gre / p.arr / (p.gs / (p.bins * {mi.BIN_SECONDS}.0)) AS pr,
                       p.arr AS p_arr, p.gre AS p_gre, p.gs AS p_gs, p.bins AS p_bins
                FROM p_in p WHERE p.bins = 4 AND p.arr >= {STEP_MIN_ARRIVALS_PER_HOUR} AND p.gs > 0
                  AND NOT {mi.masked_sql('p', mi.ADVANCE)}
            ), x AS (
                SELECT h.*, p.pr, p.p_arr, p.p_gre, p.p_gs, p.p_bins, g.gs AS green, pl.pat, d.test
                FROM h JOIN days_in d USING (day) LEFT JOIN p USING (DeviceId, Phase, hr)
                LEFT JOIN p_in g USING (DeviceId, Phase, hr) LEFT JOIN plan_in pl USING (DeviceId, hr)
            ), b AS (
                SELECT t.DeviceId, t.Phase, t.hh, any_value(t.dpv) AS dpv, any_value(t.aog) AS aog, any_value(t.pr) AS pr,
                       avg(o.dpv) AS dm, stddev_samp(o.dpv) AS ds, count(o.dpv) AS nb, avg(o.aog) AS am,
                       avg(o.pr) AS pm, stddev_samp(o.pr) AS ps, count(o.pr) AS np, avg(o.green) AS gm,
                       -- the sums behind them, to tell the flagged hours together: summed, then divided once
                       any_value(t.dsum) AS t_d, any_value(t.veh) AS t_v, any_value(t.gre) AS t_g,
                       sum(o.dsum) AS o_d, sum(o.veh) AS o_v, sum(o.gre) AS o_g,
                       any_value(t.p_arr) AS t_pa, any_value(t.p_gre) AS t_pg, any_value(t.p_gs) AS t_ps, any_value(t.p_bins) AS t_pb,
                       sum(o.p_arr) AS o_pa, sum(o.p_gre) AS o_pg, sum(o.p_gs) AS o_ps, sum(o.p_bins) AS o_pb
                FROM x t JOIN x o ON o.DeviceId = t.DeviceId AND o.Phase = t.Phase AND o.hh = t.hh AND NOT o.test
                     -- an earlier day's hour counts only when it ran the judged day's plan (either unknown: it counts)
                     AND (t.pat IS NULL OR o.pat IS NULL OR o.pat = t.pat)
                WHERE t.test GROUP BY t.DeviceId, t.Phase, t.hh
            ), f AS (
                SELECT *, nb >= {STEP_MIN_BASELINE_DAYS} AND dpv - dm >= {STEP_MIN_RISE_SECONDS}
                          AND dpv - dm >= {STEP_MIN_RISE_SHARE} * dm
                          AND (dpv - dm) / greatest(coalesce(ds, 0), {STEP_SD_FLOOR_SECONDS}) >= {STEP_MIN_SD}
                          AND coalesce(gm, {STEP_MIN_GREEN_SECONDS}) >= {STEP_MIN_GREEN_SECONDS} AS flag,
                          coalesce(np >= {STEP_MIN_BASELINE_DAYS} AND pm - pr >= {STEP_PLATOON_DROP}
                          AND (pm - pr) / greatest(coalesce(ps, 0), {STEP_PLATOON_SD_FLOOR}) >= {STEP_MIN_SD}, false) AS pflag
                FROM b
            )
            SELECT DeviceId, Phase, list(hh ORDER BY hh) AS Hours, sum(t_d) / sum(t_v) AS Value, sum(o_d) / sum(o_v) AS Usual,
                   100.0 * sum(t_g) / sum(t_v) AS Aog, 100.0 * sum(o_g) / sum(o_v) AS AogUsual,
                   sum(t_pg) / sum(t_pa) / (sum(t_ps) / (sum(t_pb) * {mi.BIN_SECONDS}.0)) AS Pr,
                   sum(o_pg) / sum(o_pa) / (sum(o_ps) / (sum(o_pb) * {mi.BIN_SECONDS}.0)) AS PrUsual,
                   count(*) FILTER (WHERE pflag) AS platoon_hours, max(nb) AS days
            FROM f WHERE flag GROUP BY DeviceId, Phase HAVING count(*) >= {STEP_MIN_FLAGGED_HOURS}
            ORDER BY DeviceId, Phase
        """).df()
    finally:
        con.close()
    if found.empty:
        return _empty()
    texts = []
    for row in found.itertuples():
        text = (f"Delay per vehicle on phase {int(row.Phase)} rose to {row.Value:.0f} s from {row.Usual:.0f} s in the same "
                f"hours of {_kind_days(day, baseline[-int(row.days):])} ({len(row.Hours)} hours: {_hour_spans(row.Hours)}).")
        if row.platoon_hours * 2 >= len(row.Hours) and pd.notna(row.Aog) and pd.notna(row.AogUsual):
            text += (f" Arrivals on green fell too, from {row.AogUsual:.0f} % to {row.Aog:.0f} % "
                     f"(platoon ratio {row.PrUsual:.2f} to {row.Pr:.2f}).")
        others = _other_phase_faults(faults, row.DeviceId, row.Phase, day, row.Hours)
        if others:
            text += (f" A detector of phase{'s' if len(others) > 1 else ''} {' and '.join(map(str, others))} had a known "
                     f"fault in some of these hours, which can change how this phase is served.")
        texts.append(text)
    return pd.DataFrame({'DeviceId': found['DeviceId'].astype(str), 'Date': day, 'Phase': found['Phase'].astype(int),
                         'Text': texts, 'Hours': [list(map(int, h)) for h in found['Hours']],
                         'Value': found['Value'], 'Usual': found['Usual']})[ALERT_COLUMNS]


def delay_rise_alerts(approach_delay, arrival_on_green, report_day, faults: Optional[pd.DataFrame] = None,
                      platoon_ratio=None, devices: Optional[list] = None, coordination=None,
                      check_days: int = 1) -> pd.DataFrame:
    """One row per phase whose delay per vehicle rose suddenly on one of the check_days days ending on report_day, on
    its latest such day (ALERT_COLUMNS; Hours the flagged hours, Value and Usual the delay per vehicle in them on the day
    and on the earlier days). coordination: coordination_agg with Pattern, for the same-plan rule (without it every
    earlier hour counts). Empty without the two tables, on holidays, or without enough earlier days of the kind."""
    if report_day is None or not (mi.present(approach_delay) and mi.present(arrival_on_green)):
        return _empty()
    return _latest_per_phase([_delay_rise_day(approach_delay, arrival_on_green, day, faults, platoon_ratio, devices,
                                              coordination) for day in judged_days(report_day, check_days)], report_day)


# ======================================================================================================================
# Trends
# ======================================================================================================================

def _daily_split_failures(split_failures, start, end, faults, devices) -> pd.DataFrame:
    hours = mi.hourly_split_failures(split_failures, start, end, devices)
    if hours.empty:
        return pd.DataFrame()
    con = mi.connect()
    try:
        con.register('h', hours)
        con.register('faults', faults if faults is not None else mi.fault_spans())
        return con.execute(f"""
            WITH x AS (SELECT h.*, CAST(h.hr AS DATE) AS Date, {mi.masked_sql('h', mi.STOP_BAR)} AS bad FROM h),
            night AS (SELECT DeviceId, Phase, Date FROM x WHERE hour(hr) < 5 GROUP BY ALL
                      HAVING sum(cycles) >= {TREND_NIGHT_MIN_CYCLES} AND sum(sf) >= {TREND_NIGHT_SPLIT_FAIL_SHARE} * sum(cycles))
            SELECT DeviceId, Phase, Date, sum(sf) / sum(cycles) AS "{SPLIT_FAIL_COLUMN}", sum(cycles) AS Volume
            FROM x ANTI JOIN night USING (DeviceId, Phase, Date)
            GROUP BY ALL HAVING NOT bool_or(bad) AND sum(cycles) > 0
        """).df()
    finally:
        con.close()


def _daily_platoon(platoon_ratio, start, end, faults, devices) -> pd.DataFrame:
    hours = mi.hourly_platoon(platoon_ratio, start, end, devices)
    if hours.empty:
        return pd.DataFrame()
    con = mi.connect()
    try:
        con.register('h', hours)
        con.register('faults', faults if faults is not None else mi.fault_spans())
        return con.execute(f"""
            WITH x AS (SELECT h.*, CAST(h.hr AS DATE) AS Date, {mi.masked_sql('h', mi.ADVANCE)} AS bad FROM h)
            SELECT DeviceId, Phase, Date,
                   -(sum(gre) / sum(arr) / (sum(gs) / (sum(bins) * {mi.BIN_SECONDS}.0))) AS "{PLATOON_FALL_COLUMN}",
                   sum(arr) AS Volume
            FROM x GROUP BY ALL HAVING NOT bool_or(bad) AND sum(arr) > 0 AND sum(gs) > 0
        """).df()
    finally:
        con.close()


def _trend_kind(day) -> str:
    return WEEKEND if pd.Timestamp(day).dayofweek >= 5 else WEEKDAY


def trend_alerts(kind: str, table, report_day, faults: Optional[pd.DataFrame] = None, thresholds: Optional[dict] = None,
                 history_days: int = TREND_HISTORY_DAYS, recency_days: int = 0, devices: Optional[list] = None,
                 check_days: int = 1) -> pd.DataFrame:
    """Phases whose split failures rose (kind SPLIT_FAIL_TREND, table split_failures) or whose platoon ratio fell (kind
    PLATOON_TREND, table platoon_ratio) on one of the judged days (check_days, or recency_days + 1, ending on report_day),
    on their latest such day: ALERT_COLUMNS, Value the day's value (one day) or the recent days' average (several days),
    Usual the earlier days' average. Quietly empty without the table or until a phase has enough earlier days of the kind."""
    if report_day is None or not mi.present(table):
        return _empty()
    report = pd.Timestamp(report_day).normalize()
    judged = [d for d in judged_days(report, max(int(check_days), int(recency_days) + 1))
              if d.date() not in mi.holidays(d, d)]
    if not judged:
        return _empty()
    start, end = judged[0] - pd.Timedelta(days=history_days - 1), report + pd.Timedelta(days=1)
    split = kind == SPLIT_FAIL_TREND
    limits = {**(SPLIT_FAIL_TREND_DEFAULTS if split else PLATOON_TREND_DEFAULTS), **(thresholds or {})}
    daily = (_daily_split_failures if split else _daily_platoon)(table, start, end, faults, devices)
    if daily.empty:
        return _empty()
    column = SPLIT_FAIL_COLUMN if split else PLATOON_FALL_COLUMN
    off = set(mi.holidays(start, end))
    daily = daily.assign(Date=pd.to_datetime(daily['Date']).astype('datetime64[us]'), Phase=daily['Phase'].astype(int),
                         DeviceId=daily['DeviceId'].astype(str))
    daily = daily[(daily['Volume'] >= limits['min_volume']) & ~daily['Date'].dt.date.isin(off)]
    if daily.empty:
        return _empty()
    # Larger is worse: the split failure share, or the platoon ratio negated.
    daily = daily.assign(v=daily[column].astype(float), kind=daily['Date'].map(_trend_kind))
    targets = pd.DataFrame({'Date': pd.to_datetime(judged).astype('datetime64[us]'), 'kind': [_trend_kind(d) for d in judged],
                            'recent': [TREND_RECENT_DAYS[_trend_kind(d)] for d in judged],
                            'min_base': [TREND_MIN_BASELINE_DAYS[_trend_kind(d)] for d in judged]})
    con = mi.connect()
    try:
        con.register('d', daily[['DeviceId', 'Phase', 'Date', 'v', 'kind']])
        con.register('j', targets)
        scored = con.execute(f"""
            WITH t AS (SELECT d.DeviceId, d.Phase, d.Date, d.v AS dv, d.kind, j.recent, j.min_base FROM d JOIN j USING (Date, kind)),
            o AS (
                SELECT t.*, o.v, row_number() OVER (w ORDER BY o.Date DESC) AS rn, lag(o.v) OVER (w ORDER BY o.Date) AS pv
                FROM t JOIN d o ON o.DeviceId = t.DeviceId AND o.Phase = t.Phase AND o.kind = t.kind
                 AND o.Date <= t.Date AND o.Date > t.Date - INTERVAL {int(history_days)} DAY
                WINDOW w AS (PARTITION BY t.DeviceId, t.Phase, t.Date)
            ), s AS (
                SELECT DeviceId, Phase, Date, any_value(dv) AS dv, any_value(recent) AS recent, any_value(min_base) AS min_base,
                       avg(v) FILTER (WHERE rn <= recent) AS rm, count(*) FILTER (WHERE rn <= recent) AS nr,
                       avg(v) FILTER (WHERE rn > recent) AS bm, count(*) FILTER (WHERE rn > recent) AS nb,
                       -- the day-to-day spread from one earlier day to the next (a steady climb does not widen it)
                       sqrt(sum((v - pv) * (v - pv)) FILTER (WHERE rn > recent AND pv IS NOT NULL)
                            / (2 * greatest(count(*) FILTER (WHERE rn > recent AND pv IS NOT NULL), 1))) AS bs
                FROM o GROUP BY DeviceId, Phase, Date
            )
            SELECT * FROM s WHERE nb >= min_base AND nr = recent
        """).df()
    finally:
        con.close()
    if scored.empty:
        return _empty()
    z, rise, jump = limits['zscore'], limits['rise'], limits['jump']
    sd = np.maximum(scored['bs'].fillna(0).to_numpy(float), rise / z)
    up_day, up_recent = scored['dv'] - scored['bm'], scored['rm'] - scored['bm']
    one_day = (up_day >= jump) & (up_day / sd >= z)
    several = (up_recent >= rise) & (up_recent / (sd * np.sqrt(1.0 / scored['nr'] + 1.0 / scored['nb'])) >= z) & (up_day >= rise)
    hits = scored.assign(one_day=one_day)[one_day | several]
    if hits.empty:
        return _empty()
    sign = 1.0 if split else -1.0
    texts, values = [], []
    for row in hits.itertuples():
        days_kind, usual = _trend_kind(row.Date), sign * row.bm
        before = (f"against {100 * usual:.0f} % on the {int(row.nb)} {days_kind} before" if split
                  else f"against {usual:.2f} on the {int(row.nb)} {days_kind} before")
        note = '' if split else ' (1.0 is traffic arriving at random; lower, more of it arrives on red)'
        if row.one_day:
            value = sign * row.dv
            texts.append(f"Phase {int(row.Phase)} left a queue on {100 * value:.0f} % of cycles, {before}." if split else
                         f"Phase {int(row.Phase)} platoon ratio fell to {value:.2f}, {before}{note}.")
        else:
            value = sign * row.rm
            texts.append(f"Phase {int(row.Phase)} left a queue on {100 * value:.0f} % of cycles over its last {int(row.nr)} "
                         f"{days_kind} ({100 * sign * row.dv:.0f} % on the last), {before}." if split else
                         f"Phase {int(row.Phase)} platoon ratio averaged {value:.2f} over its last {int(row.nr)} {days_kind} "
                         f"({sign * row.dv:.2f} on the last), {before}{note}.")
        values.append(value)
    out = pd.DataFrame({'DeviceId': hits['DeviceId'].astype(str).values, 'Date': pd.to_datetime(hits['Date']).values,
                        'Phase': hits['Phase'].astype(int).values, 'Text': texts, 'Hours': [[] for _ in texts],
                        'Value': values, 'Usual': (sign * hits['bm']).values})
    return _latest_per_phase([out[ALERT_COLUMNS]], report)


# ======================================================================================================================
# Tables
# ======================================================================================================================

def prepare_measure_table(alerts: Optional[pd.DataFrame], signals_df: Optional[pd.DataFrame], region=None,
                          max_rows: int = 10) -> tuple:
    """(table rows, total) of one report's alerts of one of these checks: Signal, Phase and What Happened, by signal name,
    with the Ongoing date for held-back repeats."""
    from .table_generation import _add_ongoing_column
    if alerts is None or alerts.empty or signals_df is None:
        return pd.DataFrame(), 0
    info = signals_df.assign(DeviceId=signals_df['DeviceId'].astype(str))[['DeviceId', 'Name', 'Region']].drop_duplicates('DeviceId')
    rows = alerts.assign(DeviceId=alerts['DeviceId'].astype(str)).merge(info, on='DeviceId', how='inner')
    if region and region != 'All Regions':
        rows = rows[rows['Region'] == region]
    if rows.empty:
        return pd.DataFrame(), 0
    total = len(rows)
    rows = rows.sort_values(['Name', 'Phase']).head(max_rows if max_rows > 0 else len(rows))
    rows = rows.assign(Signal=rows['Name'], Phase=rows['Phase'].astype(int), **{'What Happened': rows['Text']})
    rows, columns = _add_ongoing_column(rows, ['Signal', 'Phase', 'What Happened'])
    return rows[columns].reset_index(drop=True), total
