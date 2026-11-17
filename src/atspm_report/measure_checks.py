"""Checks on atspm's per-phase measures (see measure_inputs): a sudden rise in delay per vehicle, and two trends, rising
split failures and a falling platoon ratio. Each has its own switch, off by default, and its own report section; none is
part of Signal Focus.

Sudden rise in delay per vehicle (delay_rise_alerts). Each hour of the report day, per phase, against the same hour on
earlier days of the same kind (weekdays, Saturdays, Sundays; holidays left out, and no check on a holiday). An hour is
flagged when delay per vehicle is STEP_MIN_RISE_SECONDS and STEP_MIN_RISE_SHARE over the average of those days, on
STEP_MIN_BASELINE_DAYS or more of them, and STEP_MIN_SD standard deviations above them (the spread taken as at least
STEP_SD_FLOOR_SECONDS). A phase alerts with STEP_MIN_FLAGGED_HOURS or more flagged hours in the day. Arrivals on green and
the platoon ratio are told as supporting evidence when the platoon ratio fell too (STEP_PLATOON_DROP and STEP_MIN_SD, its
spread at least STEP_PLATOON_SD_FLOOR) in half or more of the flagged hours: alone it flagged 8 to 10 signals a day, so it
never raises the alert. These are the study's numbers (2026-11-16, ODOT data 11-09 to 11-15): on 11-12 and 11-13 the
rule flagged 2 to 4 signals a day. It needs 3 to 5 days of the same kind before it can judge an hour, so weekends start
after three weekends of data.

Trends (trend_alerts). The max-out alert's method (statistical_analysis.cusum and alert) on one value per phase and day:
the share of cycles that failed, and the platoon ratio. A day is flagged when the 7-day CUSUM, the z-score against the
phase's own days, the rise over its average and the day's cycles (or arrivals) all pass their limits
(SPLIT_FAIL_TREND_DEFAULTS, PLATOON_TREND_DEFAULTS). The z-score includes the day itself, so a z of 4 cannot be reached
with fewer than 18 days (TREND_MIN_DAYS): until a phase has them the checks quietly do nothing.
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
from .statistical_analysis import PLATOON_FALL_COLUMN, SPLIT_FAIL_COLUMN, alert, cusum

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
STEP_PLATOON_DROP = 0.2
STEP_PLATOON_SD_FLOOR = 0.05

# ---- Trends: the max-out method; provisional limits (the study's starting points; the minimum rise of split failures
# is its +10 points). The CUSUM and z-score limits are the max-out alert's. --------------------------------------------
SPLIT_FAIL_TREND_DEFAULTS = {'cusum': 0.25, 'zscore': 4.0, 'rise': 0.10, 'min_volume': 30}
PLATOON_TREND_DEFAULTS = {'cusum': 0.25, 'zscore': 4.0, 'rise': 0.2, 'min_volume': 300}
TREND_MIN_DAYS = 18
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


# ======================================================================================================================
# Sudden rise in delay per vehicle
# ======================================================================================================================

def delay_rise_alerts(approach_delay, arrival_on_green, report_day, faults: Optional[pd.DataFrame] = None,
                      platoon_ratio=None, devices: Optional[list] = None) -> pd.DataFrame:
    """One row per phase whose delay per vehicle rose suddenly on report_day (ALERT_COLUMNS; Hours the flagged hours,
    Value and Usual the delay per vehicle in them on the day and on the earlier days). Empty without the two tables, on
    a holiday, or without enough earlier days of the kind."""
    if report_day is None or not (mi.present(approach_delay) and mi.present(arrival_on_green)):
        return _empty()
    day = pd.Timestamp(report_day).normalize()
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
    con = mi.connect()
    try:
        con.register('h_in', hours)
        con.register('p_in', platoon)
        con.register('days_in', days)
        con.register('faults', faults if faults is not None else mi.fault_spans())
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
                SELECT h.*, p.pr, p.p_arr, p.p_gre, p.p_gs, p.p_bins, d.test FROM h JOIN days_in d USING (day) LEFT JOIN p USING (DeviceId, Phase, hr)
            ), b AS (
                SELECT t.DeviceId, t.Phase, t.hh, any_value(t.dpv) AS dpv, any_value(t.aog) AS aog, any_value(t.pr) AS pr,
                       avg(o.dpv) AS dm, stddev_samp(o.dpv) AS ds, count(o.dpv) AS nb, avg(o.aog) AS am,
                       avg(o.pr) AS pm, stddev_samp(o.pr) AS ps, count(o.pr) AS np,
                       -- the sums behind them, to tell the flagged hours together: summed, then divided once
                       any_value(t.dsum) AS t_d, any_value(t.veh) AS t_v, any_value(t.gre) AS t_g,
                       sum(o.dsum) AS o_d, sum(o.veh) AS o_v, sum(o.gre) AS o_g,
                       any_value(t.p_arr) AS t_pa, any_value(t.p_gre) AS t_pg, any_value(t.p_gs) AS t_ps, any_value(t.p_bins) AS t_pb,
                       sum(o.p_arr) AS o_pa, sum(o.p_gre) AS o_pg, sum(o.p_gs) AS o_ps, sum(o.p_bins) AS o_pb
                FROM x t JOIN x o ON o.DeviceId = t.DeviceId AND o.Phase = t.Phase AND o.hh = t.hh AND NOT o.test
                WHERE t.test GROUP BY t.DeviceId, t.Phase, t.hh
            ), f AS (
                SELECT *, nb >= {STEP_MIN_BASELINE_DAYS} AND dpv - dm >= {STEP_MIN_RISE_SECONDS}
                          AND dpv - dm >= {STEP_MIN_RISE_SHARE} * dm
                          AND (dpv - dm) / greatest(coalesce(ds, 0), {STEP_SD_FLOOR_SECONDS}) >= {STEP_MIN_SD} AS flag,
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
        texts.append(text)
    return pd.DataFrame({'DeviceId': found['DeviceId'].astype(str), 'Date': day, 'Phase': found['Phase'].astype(int),
                         'Text': texts, 'Hours': [list(map(int, h)) for h in found['Hours']],
                         'Value': found['Value'], 'Usual': found['Usual']})[ALERT_COLUMNS]


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


def trend_alerts(kind: str, table, report_day, faults: Optional[pd.DataFrame] = None, thresholds: Optional[dict] = None,
                 history_days: int = TREND_HISTORY_DAYS, recency_days: int = 0, devices: Optional[list] = None) -> pd.DataFrame:
    """Phases whose split failures rose (kind SPLIT_FAIL_TREND, table split_failures) or whose platoon ratio fell (kind
    PLATOON_TREND, table platoon_ratio) on report_day, by the max-out method. ALERT_COLUMNS, Value and Usual the day's
    value and the phase's average. Quietly empty without the table or until TREND_MIN_DAYS days exist."""
    if report_day is None or not mi.present(table):
        return _empty()
    day = pd.Timestamp(report_day).normalize()
    start, end = day - pd.Timedelta(days=history_days - 1), day + pd.Timedelta(days=1)
    split = kind == SPLIT_FAIL_TREND
    limits = {**(SPLIT_FAIL_TREND_DEFAULTS if split else PLATOON_TREND_DEFAULTS), **(thresholds or {})}
    daily = (_daily_split_failures if split else _daily_platoon)(table, start, end, faults, devices)
    if daily.empty:
        return _empty()
    column = SPLIT_FAIL_COLUMN if split else PLATOON_FALL_COLUMN
    # A phase is scored on its days with enough cycles (or arrivals) only, and only once it has TREND_MIN_DAYS of them.
    daily = daily[daily['Volume'] >= limits['min_volume']]
    daily = daily[daily.groupby(['DeviceId', 'Phase'])['Date'].transform('nunique') >= TREND_MIN_DAYS]
    if daily.empty:
        return _empty()
    daily = daily.assign(Date=pd.to_datetime(daily['Date']), Phase=daily['Phase'].astype(int))
    flagged = alert(cusum(daily[['DeviceId', 'Phase', 'Date', column, 'Volume']], k_value=1), limits, recency_days)
    if flagged.empty:
        return _empty()
    # Only the report day (or recency_days before it): a phase whose report day was left out has its own latest day older.
    hits = flagged[(flagged['Alert'] == 1) & (flagged['days_from_max'] <= recency_days)
                   & (flagged['Date'] >= day - pd.Timedelta(days=recency_days))]
    hits = hits.sort_values('Date').drop_duplicates(['DeviceId', 'Phase'], keep='last')
    sign = 1.0 if split else -1.0
    value, usual = sign * hits[column], sign * hits['_Average']
    if split:
        texts = [f"Phase {int(p)} left a queue on {100 * v:.0f} % of cycles, against {100 * u:.0f} % on average over the last "
                 f"{history_days} days." for p, v, u in zip(hits['Phase'], value, usual)]
    else:
        texts = [f"Phase {int(p)} platoon ratio fell to {v:.2f}, against {u:.2f} on average over the last {history_days} days "
                 f"(1.0 is traffic arriving at random; lower, more of it arrives on red)." for p, v, u in zip(hits['Phase'], value, usual)]
    return pd.DataFrame({'DeviceId': hits['DeviceId'].astype(str).values, 'Date': hits['Date'].dt.normalize().values,
                         'Phase': hits['Phase'].astype(int).values, 'Text': texts, 'Hours': [[] for _ in texts],
                         'Value': value.values, 'Usual': usual.values})[ALERT_COLUMNS]


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
