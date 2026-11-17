"""Phase timing check from the 15-minute tables: split failures without max outs.

The user's words: "a phase that has split failures without max out means its gap timing is off, the phase is gapping
out prematurely despite continued demand." Maxing out without leaving a queue is fine, the user decided (2026-11-16), and
is not checked.

Split failures without max outs (CHECK_SPLIT_FAIL). In each 15-minute bin, failed cycles above the bin's max outs and
force offs must have been greens that gapped out (a lower bound on failed gap-outs). An hour is flagged when that lower
bound is SPLIT_FAIL_GAP_OUT_PERCENT of the phase's measured cycles or more. In the study every flagged case with enough
greens was real, but about 4 in 10 were a stop-bar zone that did not extend the green rather than a short gap time,
and these tables cannot tell the two apart, so the alert names both.

Exclusions: an hour counts only with MIN_MEASURED_SHARE of its cycles measured for split failures (a
dead zone is not read as no failures) and MIN_CYCLES cycles; the coordinated phase while the signal runs coordinated; an
hour with a known fault on the phase's stop-bar or advance zones (no measure from a detector while it is faulting), and
bins where the stop-bar zone never cleared (green and red occupancy both NEVER_CLEAR or more); a phase resting in green
(average phase wait under MIN_PHASE_WAIT_SECONDS); the whole signal saturated (SATURATED_PHASES or more phases at
SATURATED_SPLIT_FAIL percent split failures in the hour); a phase whose zones fail overnight (split failures on
NIGHT_SPLIT_FAIL percent or more of its cycles from midnight to NIGHT_END_HOUR drops its split failures all day); and a
phase on max recall all day (maxing out NIGHT_RECALL_MAX_OUT percent or more in 2+ night hours while served about as
often as the busiest phase, or the whole day). The reference also reads phase calls for recall; the report's inputs
have none, so that test is left out. Flagged hours make an alert in runs of MIN_SPAN_HOURS or more, and only when the
same phase was flagged by the same check on REPEAT_DAYS of the last REPEAT_OF days of the same kind (weekdays,
Saturdays, Sundays; holidays left out), the report day one of them.

Coordinated phase: the report has no green times, so a phase is the coordinated one on a day when, in bins the signal
ran a cycle, it was served on nearly every cycle (median services x cycle / 900 of COORD_SERVED or more) and gapped out on
at most COORD_GAP_SHARE of its services, at most one per ring (the one gapping out least).

Numbers: study of 2026-11-16, ODOT data 11-09 to 11-15.
All set-based SQL on one private connection; Python only words and draws the few alerts.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from . import measure_inputs
from .duckdb_utils import columns as table_columns
from .utils import log_message

ALERT_TYPE = 'phase_timing'
ID_COLUMNS = ['DeviceId', 'Phase', 'Check']
CHECK_SPLIT_FAIL = 'Split failures without max outs'
SECTION_TITLES = {CHECK_SPLIT_FAIL: 'Phases Ending Green Too Early'}
CONFIG_SWITCHES = {CHECK_SPLIT_FAIL: 'split_failures_no_maxout_check_enabled'}

# ---- Thresholds: study of 2026-11-16, ODOT data 11-09 to 11-15 -----------------------------------------------------------
SPLIT_FAIL_GAP_OUT_PERCENT = 15.0     # lower bound on failed gap-outs, share of measured cycles
# The phase must mostly gap out in a flagged hour (the reference rule set's "gaps out early" line, 50 %): a phase
# that mostly runs to its limit and still leaves a queue needs more green, which is another question. 16 November
# 2026: 03061 phase 5 was flagged while running to its limit on 59 % of greens.
MIN_GAP_OUT_PERCENT = 50.0
MIN_SPAN_HOURS = 2
MIN_CYCLES = 20
MIN_MEASURED_SHARE = 0.8
MIN_PHASE_WAIT_SECONDS = 15.0
SATURATED_PHASES = 3
SATURATED_SPLIT_FAIL = 15.0
NIGHT_END_HOUR = 5
NIGHT_SPLIT_FAIL = 30.0
NIGHT_RECALL_MAX_OUT = 95.0
NIGHT_RECALL_HOURS = 2
FIXED_DAY_MAX_OUT, FIXED_DAY_GAP_OUT, FIXED_DAY_MIN_SERVICES = 95.0, 2.0, 50
NEVER_CLEAR = 0.99
COORD_SERVED = 0.95
COORD_GAP_SHARE = 0.25
COORD_MIN_BINS = 8
# Repeat rule: flagged on REPEAT_DAYS of the last REPEAT_OF days of the same kind, the report day one of them.
REPEAT_DAYS = 2
REPEAT_OF = 3
# How far back to look for the earlier days of the same kind.
REPEAT_LOOKBACK_DAYS = 21

ALERT_COLUMNS = ['DeviceId', 'Date', 'Phase', 'Check', 'Text', 'FromHour', 'ToHour', 'DaysSeen', 'DaysJudged',
                 'ChartSplitFail', 'ChartMaxOut', 'ChartFlagged']

GREY, STRONG, SHADE = '#5b7fa6', '#c4302b', '#f2d7d5'
CHART_SIZE = (7.2, 3.0)
CHART_HEIGHT_INCHES = 6.5 * CHART_SIZE[1] / CHART_SIZE[0]


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=ALERT_COLUMNS)


def judged_days(report_day, repeat_of: int = REPEAT_OF) -> list:
    """The report day and up to repeat_of - 1 earlier days of the same kind, newest first, holidays left out. None when
    the report day is a holiday (traffic on it is not an ordinary day)."""
    day = pd.Timestamp(report_day).normalize()
    start = day - pd.Timedelta(days=REPEAT_LOOKBACK_DAYS)
    off = set(measure_inputs.holidays(start, day))
    if day.date() in off:
        return []
    kind = measure_inputs.day_kind(day)
    days = [day]
    probe = day - pd.Timedelta(days=1)
    while len(days) < repeat_of and probe >= start:
        if measure_inputs.day_kind(probe) == kind and probe.date() not in off:
            days.append(probe)
        probe -= pd.Timedelta(days=1)
    return days


def _read(table, wanted: list, days: list, devices) -> pd.DataFrame:
    """The rows of these days with the wanted columns that the table has."""
    have = set(table_columns(table))
    chosen = ', '.join(c for c in wanted if c in have)
    return measure_inputs.rows_on_days(table, chosen, days, devices)


def _hours_sql(has_occupancy: bool, has_samples: bool, has_pattern: bool) -> list:
    """The statements that build table x: one row per signal, phase, day and hour with the measures and exclusions."""
    never_clear = (f"coalesce(s.Green_Occupancy >= {NEVER_CLEAR} AND s.Red_Occupancy >= {NEVER_CLEAR}, FALSE)"
                   if has_occupancy else 'FALSE')
    wait = ("sum(AvgPhaseWait * Samples) / nullif(sum(Samples), 0)" if has_samples else "avg(AvgPhaseWait)")
    free_pattern = " AND Pattern NOT IN (254, 255)" if has_pattern else ''
    return [
        # termination bins
        """CREATE TEMP TABLE tb AS
           SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(Phase AS INTEGER) AS Phase, TimeStamp AS ts,
                  CAST(TimeStamp AS DATE) AS day, hour(TimeStamp) AS h, sum(Total) AS services,
                  coalesce(sum(Total) FILTER (WHERE PerformanceMeasure = 'GapOut'), 0) AS gaps,
                  coalesce(sum(Total) FILTER (WHERE PerformanceMeasure IN ('MaxOut', 'ForceOff')), 0) AS lims,
                  coalesce(sum(Total) FILTER (WHERE PerformanceMeasure = 'MaxOut'), 0) AS maxouts
           FROM term_in GROUP BY ALL""",
        # bins the signal ran a cycle
        f"""CREATE TEMP TABLE cb AS
            SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, time_bucket(INTERVAL 15 MINUTE, TimeStamp) AS ts, max(CycleLength) AS cl
            FROM co_in WHERE CycleLength > 0{free_pattern} GROUP BY ALL""",
        # the coordinated phase of each signal-day: served every cycle, rarely gapping out; one per ring
        f"""CREATE TEMP TABLE cp AS
            SELECT DeviceId, day, Phase FROM (
              SELECT t.DeviceId, t.day, t.Phase, (t.Phase - 1) // 4 AS ring,
                     median((t.gaps + t.lims) * c.cl / 900.0) AS served,
                     sum(t.gaps) * 1.0 / nullif(sum(t.gaps + t.lims), 0) AS gshare
              FROM tb t JOIN cb c USING (DeviceId, ts) WHERE t.Phase BETWEEN 1 AND 8
              GROUP BY ALL HAVING count(*) >= {COORD_MIN_BINS})
            WHERE served >= {COORD_SERVED} AND gshare <= {COORD_GAP_SHARE}
            QUALIFY row_number() OVER (PARTITION BY DeviceId, day, ring ORDER BY gshare, served DESC) = 1""",
        # hours with a known fault on the phase's zones
        """CREATE TEMP TABLE fh AS
           SELECT DISTINCT f.DeviceId, f.Phase, CAST(b.hr AS DATE) AS day, hour(b.hr) AS h
           FROM faults f, generate_series(time_bucket(INTERVAL 1 HOUR, f.s), f.e - INTERVAL 1 MICROSECOND, INTERVAL 1 HOUR) b(hr)
           WHERE f.e > f.s AND CAST(b.hr AS DATE) IN (SELECT day FROM days)""",
        f"""CREATE TEMP TABLE sfh AS
            SELECT CAST(s.DeviceId AS VARCHAR) AS DeviceId, CAST(s.Phase AS INTEGER) AS Phase, CAST(s.TimeStamp AS DATE) AS day,
                   hour(s.TimeStamp) AS h,
                   sum(s.Cycles) FILTER (WHERE NOT {never_clear}) AS n_meas,
                   sum(s.Split_Failure) FILTER (WHERE NOT {never_clear}) AS sf,
                   sum(greatest(0, s.Split_Failure - coalesce(t.lims, 0))) FILTER (WHERE NOT {never_clear}) AS go_lb
            FROM sf_in s LEFT JOIN tb t ON t.DeviceId = CAST(s.DeviceId AS VARCHAR) AND t.Phase = s.Phase AND t.ts = s.TimeStamp
            GROUP BY ALL""",
        f"""CREATE TEMP TABLE pw AS
            SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(Phase AS INTEGER) AS Phase, CAST(TimeStamp AS DATE) AS day,
                   hour(TimeStamp) AS h, {wait} AS wait_s
            FROM pw_in GROUP BY ALL""",
        f"""CREATE TEMP TABLE r AS
            WITH t AS (
              SELECT tb.DeviceId, tb.Phase, tb.day, tb.h, sum(services) AS services, sum(gaps) AS gaps, sum(lims) AS lims,
                     coalesce(sum(maxouts) FILTER (WHERE cb.ts IS NULL), 0) AS free_maxouts,
                     bool_or(cb.ts IS NOT NULL) AS coord_any
              FROM tb LEFT JOIN cb USING (DeviceId, ts) GROUP BY ALL)
            SELECT t.*, s.n_meas, s.sf, s.go_lb, pw.wait_s,
                   100.0 * t.free_maxouts / t.services AS mo, 100.0 * t.lims / t.services AS lim,
                   100.0 * t.gaps / t.services AS gap,
                   100.0 * s.sf / nullif(s.n_meas, 0) AS sfp, 100.0 * s.go_lb / nullif(s.n_meas, 0) AS go_lb_p,
                   coalesce(s.n_meas, 0) >= {MIN_MEASURED_SHARE} * t.services AS measured,
                   t.coord_any AND cp.Phase IS NOT NULL AS coord_hour,
                   fh.Phase IS NOT NULL AS fault
            FROM t LEFT JOIN sfh s USING (DeviceId, Phase, day, h) LEFT JOIN pw USING (DeviceId, Phase, day, h)
            LEFT JOIN cp USING (DeviceId, day, Phase) LEFT JOIN fh USING (DeviceId, Phase, day, h)
            WHERE t.services >= {MIN_CYCLES}""",
        f"""CREATE TEMP TABLE sat AS
            SELECT DeviceId, day, h FROM r WHERE Phase BETWEEN 1 AND 8 AND measured AND NOT fault
            GROUP BY ALL HAVING count(*) FILTER (WHERE sfp >= {SATURATED_SPLIT_FAIL}) >= {SATURATED_PHASES}""",
        f"""CREATE TEMP TABLE night_sf AS
            SELECT DeviceId, day, Phase FROM r WHERE h < {NIGHT_END_HOUR} AND sfp IS NOT NULL GROUP BY ALL
            HAVING sum(services) >= {MIN_CYCLES} AND sum(sfp * services) / sum(services) >= {NIGHT_SPLIT_FAIL}""",
        f"""CREATE TEMP TABLE recall AS
            SELECT DeviceId, day, Phase FROM (
              SELECT a.DeviceId, a.day, a.Phase FROM r a
              WHERE a.h < {NIGHT_END_HOUR} AND a.mo >= {NIGHT_RECALL_MAX_OUT}
                AND a.services >= 0.9 * (SELECT max(b.services) FROM r b WHERE b.DeviceId = a.DeviceId AND b.day = a.day AND b.h = a.h)
              GROUP BY ALL HAVING count(*) >= {NIGHT_RECALL_HOURS}
              UNION
              SELECT DeviceId, day, Phase FROM r GROUP BY ALL
              HAVING sum(services) >= {FIXED_DAY_MIN_SERVICES} AND sum(mo * services) / sum(services) >= {FIXED_DAY_MAX_OUT}
                 AND sum(gap * services) / sum(services) <= {FIXED_DAY_GAP_OUT})""",
        f"""CREATE TEMP TABLE x AS
            SELECT r.*, sat.h IS NOT NULL AS saturated, ns.Phase IS NOT NULL AS night_sf, rc.Phase IS NOT NULL AS recall,
                   r.measured AND NOT r.coord_hour AND NOT r.fault AND ns.Phase IS NULL AND rc.Phase IS NULL
                     AND coalesce(r.wait_s, 99) >= {MIN_PHASE_WAIT_SECONDS} AND sat.h IS NULL AS judged
            FROM r LEFT JOIN sat USING (DeviceId, day, h) LEFT JOIN night_sf ns USING (DeviceId, day, Phase)
            LEFT JOIN recall rc USING (DeviceId, day, Phase)""",
        f"""CREATE TEMP TABLE flags AS
            SELECT DeviceId, Phase, day, h, '{CHECK_SPLIT_FAIL}' AS "Check" FROM x
              WHERE judged AND go_lb_p >= {SPLIT_FAIL_GAP_OUT_PERCENT} AND gap >= {MIN_GAP_OUT_PERCENT}""",
        # runs of MIN_SPAN_HOURS or more flagged hours
        f"""CREATE TEMP TABLE spans AS
            SELECT DeviceId, Phase, day, "Check", min(h) AS from_h, max(h) + 1 AS to_h FROM (
              SELECT *, h - row_number() OVER (PARTITION BY DeviceId, Phase, day, "Check" ORDER BY h) AS g FROM flags)
            GROUP BY DeviceId, Phase, day, "Check", g HAVING count(*) >= {MIN_SPAN_HOURS}""",
    ]


def build_timing_alerts(split_failures, terminations, phase_wait, coordination_agg, faults: Optional[pd.DataFrame],
                        report_day, checks: set, signals: Optional[pd.DataFrame] = None, repeat_days: int = REPEAT_DAYS,
                        repeat_of: int = REPEAT_OF, verbosity: int = 0) -> pd.DataFrame:
    """The alerts of the chosen checks (CHECK_SPLIT_FAIL) for the report day: ALERT_COLUMNS,
    one row per signal, phase and check. Empty, quietly, without the four tables or a report day."""
    checks = set(checks or ())
    if not checks or report_day is None or not all(measure_inputs.present(t) for t in
                                                   (split_failures, terminations, phase_wait, coordination_agg)):
        return _empty()
    days = judged_days(report_day, repeat_of)
    if not days:
        return _empty()
    devices = None
    if signals is not None and 'DeviceId' in signals.columns:
        devices = sorted(signals['DeviceId'].astype(str).unique())
    sf = _read(split_failures, ['TimeStamp', 'DeviceId', 'Phase', 'Split_Failure', 'Cycles', 'Green_Occupancy', 'Red_Occupancy'],
               days, devices)
    term = _read(terminations, ['TimeStamp', 'DeviceId', 'Phase', 'PerformanceMeasure', 'Total'], days, devices)
    wait = _read(phase_wait, ['TimeStamp', 'DeviceId', 'Phase', 'AvgPhaseWait', 'Samples'], days, devices)
    coord = _read(coordination_agg, ['TimeStamp', 'DeviceId', 'CycleLength', 'Pattern'], days, devices)
    if sf.empty or term.empty:
        return _empty()
    faults = faults if faults is not None and not faults.empty else measure_inputs.fault_spans()
    con = measure_inputs.connect()
    try:
        for name, frame in (('sf_in', sf), ('term_in', term), ('pw_in', wait), ('co_in', coord), ('faults', faults)):
            con.register(name, frame)
        con.register('days', pd.DataFrame({'day': [d.date() for d in days]}))
        for statement in _hours_sql({'Green_Occupancy', 'Red_Occupancy'} <= set(sf.columns), 'Samples' in wait.columns,
                                    'Pattern' in coord.columns):
            con.execute(statement)
        chosen = ', '.join(f"'{c}'" for c in checks)
        report = pd.Timestamp(days[0]).date()
        found = con.execute(f"""
            WITH seen AS (SELECT DeviceId, Phase, "Check", count(DISTINCT day) AS n FROM spans GROUP BY ALL),
            judged AS (SELECT DeviceId, Phase, count(DISTINCT day) AS n FROM x WHERE judged GROUP BY ALL),
            today AS (SELECT * FROM spans WHERE day = $day AND "Check" IN ({chosen}))
            SELECT t.DeviceId, t.Phase, t."Check", list(t.from_h ORDER BY t.from_h) AS starts, list(t.to_h ORDER BY t.from_h) AS ends,
                   any_value(s.n) AS seen, any_value(j.n) AS judged
            FROM today t JOIN seen s USING (DeviceId, Phase, "Check") JOIN judged j USING (DeviceId, Phase)
            GROUP BY ALL HAVING any_value(s.n) >= $need""", {'day': report, 'need': repeat_days}).df()
        if found.empty:
            return _empty()
        con.register('found', found[['DeviceId', 'Phase', 'Check']])
        hours = con.execute("""
            SELECT x.DeviceId, x.Phase, f."Check", x.h, x.judged, x.sfp, x.mo, x.lim, x.go_lb, x.n_meas, x.sf, x.services,
                   x.free_maxouts, x.lims, EXISTS (
                       SELECT 1 FROM spans sp WHERE sp.DeviceId = x.DeviceId AND sp.Phase = x.Phase AND sp.day = x.day
                       AND sp."Check" = f."Check" AND x.h >= sp.from_h AND x.h < sp.to_h) AS flagged
            FROM x JOIN found f USING (DeviceId, Phase)
            WHERE x.day = $day""", {'day': report}).df()
    finally:
        con.close()
    alerts = _describe(found, hours, days[0])
    log_message(f"Phase timing checks: {len(alerts)} alerts on {days[0]:%Y-%m-%d} ({len(days)} days of the same kind judged).",
                1, verbosity)
    return alerts


def _clock(hour: int) -> str:
    return f"{int(hour) % 24}:00"


def _when(starts, ends) -> str:
    """'between 9:00 and 19:00' for one span, 'at 7:00-9:00, 11:00-13:00 and 15:00-18:00' for several."""
    if len(starts) == 1:
        return f"between {_clock(starts[0])} and {_clock(ends[0])}"
    parts = [f"{_clock(s)}-{_clock(e)}" for s, e in zip(starts, ends)]
    return 'at ' + ', '.join(parts[:-1]) + ' and ' + parts[-1]


def _percent(top: float, bottom: float) -> float:
    return 100.0 * top / bottom if bottom else float('nan')


def alert_text(check: str, phase: int, starts, ends, stats: dict, seen: int, judged: int, kind: str) -> str:
    """The plain sentence of one alert."""
    when = _when(starts, ends)
    # go_lb counts greens that gapped out with a queue left; lim is a separate share of all greens, so no "only".
    limit = ('it never runs to its limit' if round(stats['lim']) == 0 else
             f"it runs to its limit on {stats['lim']:.0f} %")
    text = (f"Phase {phase} gaps out and leaves a queue on {stats['go_lb']:.0f} % of greens {when} ({limit}): "
            f"the gap time is too short, or a stop-bar zone is not extending the green.")
    if judged > 1:
        text += f" Seen on {seen} of the last {judged} {'weekdays' if kind == 'Weekdays' else kind}."
    return text


def _describe(found: pd.DataFrame, hours: pd.DataFrame, day) -> pd.DataFrame:
    kind = measure_inputs.day_kind(day)
    rows = []
    for alert in found.itertuples(index=False):
        mine = hours[(hours['DeviceId'] == alert.DeviceId) & (hours['Phase'] == alert.Phase) & (hours['Check'] == alert.Check)]
        flagged = mine[mine['flagged'].astype(bool)]
        stats = {
            'go_lb': _percent(flagged['go_lb'].sum(), flagged['n_meas'].sum()),
            'lim': _percent(flagged['lims'].sum(), flagged['services'].sum()),
        }
        judged_hours = mine[mine['judged'].astype(bool)]
        split = np.full(24, np.nan)
        limit = np.full(24, np.nan)
        marks = np.zeros(24, dtype=bool)
        hrs = judged_hours['h'].astype(int).to_numpy()
        split[hrs] = judged_hours['sfp'].to_numpy(float)
        limit[hrs] = judged_hours['lim'].to_numpy(float)
        marks[flagged['h'].astype(int).to_numpy()] = True
        starts, ends = [int(s) for s in alert.starts], [int(e) for e in alert.ends]
        rows.append({
            'DeviceId': str(alert.DeviceId), 'Date': pd.Timestamp(day).normalize(), 'Phase': int(alert.Phase),
            'Check': alert.Check, 'Text': alert_text(alert.Check, int(alert.Phase), starts, ends, stats, int(alert.seen), int(alert.judged), kind),
            'FromHour': min(starts), 'ToHour': max(ends), 'DaysSeen': int(alert.seen), 'DaysJudged': int(alert.judged),
            'ChartSplitFail': split.tolist(), 'ChartMaxOut': limit.tolist(), 'ChartFlagged': marks.tolist(),
        })
    frame = pd.DataFrame(rows, columns=ALERT_COLUMNS)
    frame = frame.sort_values(['Check', 'DeviceId', 'Phase'])
    frame['Date'] = frame['Date'].astype('datetime64[us]')
    return frame.reset_index(drop=True)


# ======================================================================================================================
# Report: tables and charts
# ======================================================================================================================

def prepare_timing_table(alerts: Optional[pd.DataFrame], signals_df: Optional[pd.DataFrame], region=None,
                         max_rows: int = 10, check: str = CHECK_SPLIT_FAIL) -> tuple:
    """(table rows, total) of one report's alerts of one section: Signal, Phase and What Is Wrong, by signal name, and
    the Ongoing date for held-back repeats."""
    from .table_generation import _add_ongoing_column
    if alerts is None or alerts.empty or signals_df is None or 'Check' not in alerts.columns:
        return pd.DataFrame(), 0
    info = signals_df.assign(DeviceId=signals_df['DeviceId'].astype(str))[['DeviceId', 'Name', 'Region']].drop_duplicates('DeviceId')
    rows = alerts[alerts['Check'] == check]
    rows = rows.assign(DeviceId=rows['DeviceId'].astype(str)).merge(info, on='DeviceId', how='inner')
    if region and region != 'All Regions':
        rows = rows[rows['Region'] == region]
    if rows.empty:
        return pd.DataFrame(), 0
    total = len(rows)
    rows = rows.sort_values(['Name', 'Phase'])
    rows = rows.head(max_rows if max_rows > 0 else len(rows))
    rows = rows.assign(Signal=rows['Name'], Phase=rows['Phase'].astype(int), **{'What Is Wrong': rows['Text']})
    rows, out_columns = _add_ongoing_column(rows, ['Signal', 'Phase', 'What Is Wrong'])
    return rows[out_columns].reset_index(drop=True), total


def _timing_chart(alert, name):
    from matplotlib.patches import Patch
    from .coordination_check import signal_number
    from .visualization import _legend_above, new_figure
    figure = new_figure(CHART_SIZE, dpi=110)
    ax = figure.add_subplot(111)
    x = np.arange(24) + 0.5
    flagged = np.asarray(alert['ChartFlagged'], dtype=bool)
    for h in np.where(flagged)[0]:
        ax.axvspan(h, h + 1, color=SHADE, lw=0, zorder=0)
    split = np.asarray(alert['ChartSplitFail'], dtype=float)
    limit = np.asarray(alert['ChartMaxOut'], dtype=float)
    limit_label = 'Ran to its limit'
    ax.plot(x, split, color=STRONG, lw=2.2, marker='o', ms=3, label='Split failures', zorder=3)
    ax.plot(x, limit, color=GREY, lw=2.0, marker='s', ms=3, label=limit_label, zorder=2)
    ax.set_xlim(0, 24)
    ax.set_xticks(range(0, 25, 3))
    ax.set_xticklabels([f'{h}:00' for h in range(0, 25, 3)])
    ax.set_ylim(0, 105)
    ax.set_ylabel('% of cycles', fontsize=8)
    ax.tick_params(labelsize=8)
    ax.grid(axis='y', color='#e6e6e6', lw=0.8)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)
    day = pd.Timestamp(alert['Date'])
    ax.set_title(f"{signal_number(name)} Ph{int(alert['Phase'])} by hour, {day:%a} {day.month}/{day.day}", fontsize=10)
    handles, labels = ax.get_legend_handles_labels()
    handles.append(Patch(color=SHADE, label='Flagged hours'))
    labels.append('Flagged hours')
    figure.tight_layout()
    _legend_above(ax, handles, labels)
    figure.tight_layout()
    return figure


def create_timing_plots(alerts: Optional[pd.DataFrame], signals: Optional[pd.DataFrame], num_figures: int = 10) -> list:
    """One chart per alert, as (figure, scope) for 'All Regions' and its region, in the tables' order (section, then
    signal name), at most num_figures per scope and section: split failure share and the share running to its limit by
    hour of day for that phase, the flagged hours shaded."""
    if alerts is None or alerts.empty or signals is None or 'ChartFlagged' not in alerts.columns:
        return []
    info = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')
    alerts = alerts.assign(_name=alerts['DeviceId'].astype(str).map(info['Name']),
                           _section=alerts['Check'])
    alerts = alerts[alerts['_name'].notna()].sort_values(['_section', '_name', 'Phase'])
    regions = alerts['DeviceId'].astype(str).map(info['Region'])
    drawn, figures = {}, []
    for scope in ['All Regions'] + sorted(regions.dropna().unique().tolist()):
        chosen = alerts if scope == 'All Regions' else alerts[regions == scope]
        for _, section in chosen.groupby('_section', sort=False):
            for idx, alert in section.head(num_figures).iterrows():
                if idx not in drawn:
                    drawn[idx] = _timing_chart(alert, alert['_name'])
                    drawn[idx].timing_section = alert['_section']
                figures.append((drawn[idx], scope))
    return figures


def section_figures(figures: list, check: str) -> list:
    """The (figure, scope) pairs of one section (CHECK_SPLIT_FAIL) from create_timing_plots' list."""
    return [pair for pair in figures if getattr(pair[0], 'timing_section', None) == check]
