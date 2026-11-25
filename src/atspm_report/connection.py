"""
Connection mode: the report run on a DuckDB connection that holds atspm's tables, the way atspm 3 works in conn mode.

atspm writes its measures and atspm-detector's health into tables on the caller's connection. Here the report reads
its inputs from those tables (load_inputs), keeps its own state between runs in tables of its own (load_state,
save_state, schema STATE_SCHEMA) and writes each signal's problems to a table (write_problems), so an application
needs no data plumbing of its own. run() does all of it.

Two kinds of run:
* a report run (the daily one): the report for report_date, its PDFs in the result, and its state saved for the next
  report (save_state, which run() calls unless save=False: a caller that emails the PDFs can save after the email, as
  a deployment that writes its state only once a report went out does).
* an analytics run (analytics_only=True), any time between reports: what a report would find for report_date, with
  the rows since its end up to as_of telling which problems still show today (signal_problems 'Today'). Nothing is
  drawn and no state is touched, so it never files, suppresses or rotates anything before the next report.

ReportGenerator.generate() taking frames is unchanged; this module only feeds it.
"""

import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from .alarm_processing import ALARM_EVENT_CLASSES, ALARM_HISTORY_DAYS, FLASH_EVENT_IDS
from . import coordination_holding
from .detector_triage import usual_detector_counts
from .preempt_processing import PREEMPT_HISTORY_DAYS
from . import today as today_check

# atspm's tables (atspm 3, conn mode) and the raw events table: override any with run(tables=...).
TABLES = {
    'raw_events': 'HiResEvents',
    'has_data': 'has_data',
    'terminations': 'terminations',
    'ped': 'ped',
    'phase_wait': 'phase_wait',
    'coordination_agg': 'coordination_agg',
    'timeline': 'timeline',
    'actuations': 'actuations',
    'split_failures': 'split_failures',
    'platoon_ratio': 'platoon_ratio',
    'approach_delay': 'approach_delay',
    'detector_health_findings': 'detector_health_findings',
}
# The report's own tables.
STATE_SCHEMA = 'atspm_report'
PAST_ALERTS_PREFIX = 'past_alerts_'
DETECTOR_MINUTES = 'detector_minutes'
PREEMPT_CALLS = 'preempt_calls'
ONGOING_LAST_SHOWN = 'ongoing_last_shown'
SIGNAL_PROBLEMS = 'signal_problems'

# The report day's timeline in full, plus this many days of alarm and preempt rows before it, so the six-week alarm
# and preempt counts are rebuilt from the table every run (nothing to carry between runs).
TIMELINE_HISTORY_DAYS = max(ALARM_HISTORY_DAYS, PREEMPT_HISTORY_DAYS)
TIMELINE_HISTORY_CLASSES = list(ALARM_EVENT_CLASSES) + ['Preempt']
# A signal counts as having reported a day when has_data covers this much of it.
DEVICE_DAY_MIN_MINUTES = 12 * 60
# Days atspm-detector's whole-day check judges (its horizon_days), and so the days of counts its charts need.
DETECTOR_HEALTH_DAYS = 3
# What a detector usually counts (detector_usual_counts, the traffic weights of the triage): its counts over this many days.
USUAL_COUNT_DAYS = 21
# Raw events that class preempt calls: preempt input on/off, preempt entry, TSP check-in; and a ped button press.
PREEMPT_EVENT_IDS = (102, 104, 105, 112)
PED_PRESS_EVENT_ID = 90


def _midnight(day) -> datetime:
    return datetime.combine(pd.Timestamp(day).date(), datetime.min.time())


def _wall_clock(frame, timezone: Optional[str]):
    """frame with any timezone-aware timestamp column as naive wall-clock time in timezone (atspm-report works in naive
    wall-clock time; a TIMESTAMPTZ column would otherwise print as UTC)."""
    if not isinstance(frame, pd.DataFrame) or timezone is None:
        return frame
    out = None
    for col in frame.columns:
        if isinstance(frame[col].dtype, pd.DatetimeTZDtype):
            if out is None:
                out = frame.copy()
            out[col] = frame[col].dt.tz_convert(timezone).dt.tz_localize(None)
    return out if out is not None else frame


def _cycle_lengths(frame):
    """ActualCycleLength, with the plan's CycleLength where it is unknown (0 there: free running, the phase skip check then
    uses its assumed cycle). CycleLength stays as stored, NULL included, which the coordination plan check leaves out."""
    if frame is None or frame.empty:
        return frame
    out = frame.copy()
    for col in ('ActualCycleLength', 'CycleLength'):
        if col not in out.columns:
            out[col] = 0
    actual = pd.to_numeric(out['ActualCycleLength'], errors='coerce').fillna(0)
    cycle = pd.to_numeric(out['CycleLength'], errors='coerce').fillna(0)
    out['ActualCycleLength'] = np.where(actual > 0, actual, cycle)
    return out


def _frame(con, sql, params, timezone=None):
    """A query's rows as a DataFrame, None when the table is missing or nothing matched."""
    try:
        frame = con.execute(sql, params).df()
    except Exception:
        return None
    return None if frame.empty else _wall_clock(frame, timezone)


def load_inputs(con, report_date, *, signals: pd.DataFrame, config: Optional[dict] = None, as_of=None,
                detector_config: Optional[pd.DataFrame] = None, tables: Optional[dict] = None,
                timezone: Optional[str] = None, detector_health_days: int = DETECTOR_HEALTH_DAYS) -> Dict[str, Any]:
    """
    generate()'s inputs for report_date from atspm's tables on con (a DuckDB connection or cursor): every table a check
    reads, over the days it reads, up to the end of report_date, or up to as_of for an analytics run that tells what
    still shows today. signals: the signal table (DeviceId, Name, Region, optional Latitude, Longitude and a corridor
    column), which is the application's, not atspm's. detector_config: the detector table atspm-detector's health read
    (DeviceId, Detector, phase, function, lanes). timezone: the IANA name the data's wall clock is in (TIMESTAMPTZ
    columns are converted to it). Missing tables are left out, as generate() takes every input but signals as optional.
    """
    from .generator import ReportGenerator
    names = {**TABLES, **(tables or {})}
    settings = ReportGenerator(config or {}).config
    report_end = _midnight(report_date) + timedelta(days=1)
    end = report_end
    if as_of is not None and pd.Timestamp(as_of) > pd.Timestamp(report_end):
        end = pd.Timestamp(as_of).to_pydatetime()
    start = report_end - timedelta(days=int(settings['historical_window_days']))
    skip_start = report_end - timedelta(days=int(settings['phase_skip_retention_days']))
    history_start = report_end - timedelta(days=TIMELINE_HISTORY_DAYS)
    report_day = report_end - timedelta(days=1)
    if timezone:
        try:
            con.execute(f"SET TimeZone = '{timezone}'")
        except Exception:
            pass
    t = names
    data: Dict[str, Any] = {'signals': signals}
    data['terminations'] = _frame(con, f'SELECT TimeStamp, DeviceId, Phase, PerformanceMeasure, Total FROM "{t["terminations"]}" '
                                       'WHERE TimeStamp >= ? AND TimeStamp < ? ORDER BY TimeStamp', [start, end], timezone)
    data['has_data'] = _frame(con, f'SELECT TimeStamp, DeviceId FROM "{t["has_data"]}" WHERE TimeStamp >= ? AND TimeStamp < ? '
                                   'ORDER BY TimeStamp', [start, end], timezone)
    data['pedestrian'] = _frame(con, f'SELECT TimeStamp, DeviceId, Phase, PedActuation, PedServices FROM "{t["ped"]}" '
                                     'WHERE TimeStamp >= ? AND TimeStamp < ? ORDER BY TimeStamp', [start, end], timezone)
    data['phase_wait'] = _frame(con, f'SELECT TimeStamp, DeviceId, Phase, AvgPhaseWait, MaxPhaseWait, TotalSkips '
                                     f'FROM "{t["phase_wait"]}" WHERE TimeStamp >= ? AND TimeStamp < ? ORDER BY TimeStamp',
                                [skip_start, end], timezone)
    data['coordination_agg'] = _cycle_lengths(_frame(
        con, f'SELECT TimeStamp, DeviceId, Pattern, CycleLength, ActualCycleLength, ActualOffset FROM "{t["coordination_agg"]}" '
             'WHERE TimeStamp >= ? AND TimeStamp < ? ORDER BY TimeStamp', [skip_start, end], timezone))
    classes = ', '.join('?' * len(TIMELINE_HISTORY_CLASSES))
    data['timeline'] = _frame(
        con, f'SELECT DeviceId, StartTime, EndTime, Duration, IsValid, EventClass, EventValue FROM "{t["timeline"]}" '
             f'WHERE (StartTime >= ? AND StartTime < ?) OR (StartTime >= ? AND StartTime < ? AND EventClass IN ({classes})) '
             'ORDER BY StartTime', [report_day, report_end, history_start, report_day, *TIMELINE_HISTORY_CLASSES], timezone)
    data['preempt_events'] = _frame(
        con, f'SELECT DeviceId, TimeStamp, EventId, Parameter FROM "{t["raw_events"]}" WHERE TimeStamp >= ? AND TimeStamp < ? '
             f'AND EventId IN ({", ".join(str(e) for e in PREEMPT_EVENT_IDS)}) ORDER BY TimeStamp',
        [history_start, report_end], timezone)
    data['device_days'] = _frame(
        con, f'SELECT DeviceId, CAST(TimeStamp AS DATE) AS Date FROM "{t["has_data"]}" WHERE TimeStamp >= ? AND TimeStamp < ? '
             'GROUP BY ALL HAVING COUNT(*) * 15 >= ? ORDER BY Date, DeviceId', [history_start, report_end, DEVICE_DAY_MIN_MINUTES],
        timezone)
    data.update(_detector_health_inputs(con, t, report_end, detector_health_days, detector_config, timezone))
    for name, query in {
        'split_failures': f'SELECT TimeStamp, DeviceId, Phase, Split_Failure, Cycles, Green_Occupancy, Red_Occupancy FROM "{t["split_failures"]}"',
        'arrival_on_green': f'SELECT TimeStamp, DeviceId, Phase, Total_Actuations, Percent_AOG FROM "{t["platoon_ratio"]}"',
        'platoon_ratio': f'SELECT TimeStamp, DeviceId, Phase, Total_Actuations, Green_Actuations, Green_Seconds FROM "{t["platoon_ratio"]}"',
        'approach_delay': f'SELECT TimeStamp, DeviceId, Phase, Delay_Seconds, Queued_Delay_Seconds, Queue_Delay_Seconds FROM "{t["approach_delay"]}"',
    }.items():
        try:    # relations: generate() filters them to the signals and days it needs before reading
            data[name] = con.sql(f"{query} WHERE TimeStamp >= $start AND TimeStamp < $end", params={'start': start, 'end': end})
        except Exception:
            data[name] = None
    events = _controller_events(con, t['raw_events'], settings, report_day, report_end, end)
    if events is not None:
        data['controller_events'] = events
    data['expected_data_end'] = report_end
    return {key: value for key, value in data.items() if value is not None}


def _controller_events(con, table, settings, report_day, report_end, end):
    """The raw events the checks on them read, as a relation on con (nothing is read here), or None when none of them is
    switched on: flash_verification_enabled reads 1, 173 and 174 over the report day and on to as_of (for Today);
    coordination_holding_enabled its EVENT_IDS over the report day and the plan events of the PLAN_LEAD_DAYS before."""
    flash, holding = settings.get('flash_verification_enabled'), settings.get(coordination_holding.CONFIG_SWITCH)
    if not (flash or holding):
        return None
    ids = sorted(set(FLASH_EVENT_IDS if flash else ()) | set(coordination_holding.EVENT_IDS if holding else ()))
    plan_ids = ', '.join(str(i) for i in coordination_holding.PLAN_EVENT_IDS)
    lead = report_day - timedelta(days=coordination_holding.PLAN_LEAD_DAYS)
    where = f"(TimeStamp >= $start AND TimeStamp < $end AND EventId IN ({', '.join(str(i) for i in ids)}))"
    if holding:
        where += f" OR (TimeStamp >= $lead AND TimeStamp < $start AND EventId IN ({plan_ids}))"
    try:
        return con.sql(f'SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(TimeStamp AS TIMESTAMP) AS TimeStamp, EventId, '
                       f'Parameter FROM "{table}" WHERE {where}',
                       params={'start': report_day, 'end': end if flash else report_end, **({'lead': lead} if holding else {})})
    except Exception:
        return None


def _detector_health_inputs(con, t, report_end, days, detector_config, timezone) -> Dict[str, Any]:
    """The newest whole-day check of each signal that covers the report day (its findings as atspm-detector's health()
    returns them), and the counts its charts and triage read. No check covering the report day leaves the detector
    sections out (no input), rather than reading as a clean run that would lower the running totals."""
    report_day = report_end - timedelta(days=1)
    out: Dict[str, Any] = {}
    findings = _frame(con, f'''
        WITH checks AS (
            SELECT DeviceId, MAX(TimeStamp) AS checked FROM "{t["detector_health_findings"]}"
            WHERE TimeStamp > ? AND TimeStamp <= ? GROUP BY DeviceId
        )
        SELECT f.DeviceId, f.DetectorType, f.Detector, f."Start", f."End", f.Label, f.Confidence, f.Detail
        FROM "{t["detector_health_findings"]}" f JOIN checks c ON c.DeviceId = f.DeviceId AND c.checked = f.TimeStamp
        WHERE f.Detector IS NOT NULL
        ORDER BY f.DeviceId, f.DetectorType, f.Detector, f."Start"
    ''', [report_day, report_end], timezone)
    checked = _frame(con, f'SELECT DISTINCT DeviceId FROM "{t["detector_health_findings"]}" WHERE TimeStamp > ? AND TimeStamp <= ?',
                     [report_day, report_end])
    if checked is None:
        return out
    out['atspm_detector_findings'] = findings if findings is not None else pd.DataFrame(
        columns=['DeviceId', 'DetectorType', 'Detector', 'Start', 'End', 'Label', 'Confidence', 'Detail'])
    ids = sorted({int(d) for d in checked['DeviceId']})
    marks = ', '.join('?' * len(ids))
    window_start = report_end - timedelta(days=days)
    out['detector_counts'] = _frame(con, f'SELECT TimeStamp, DeviceId, Detector, Total FROM "{t["actuations"]}" '
                                         f'WHERE TimeStamp >= ? AND TimeStamp < ? AND DeviceId IN ({marks}) ORDER BY TimeStamp',
                                    [window_start, report_end, *ids], timezone)
    usual = _frame(con, f'SELECT TimeStamp, DeviceId, Detector, Total FROM "{t["actuations"]}" '
                        f'WHERE TimeStamp >= ? AND TimeStamp < ? AND DeviceId IN ({marks})',
                   [report_end - timedelta(days=USUAL_COUNT_DAYS), report_end, *ids], timezone)
    if usual is not None:
        out['detector_usual_counts'] = usual_detector_counts(usual)
    out['ped_press_counts'] = _frame(con, f'''
        SELECT time_bucket(INTERVAL 15 MINUTE, TimeStamp) AS TimeStamp, DeviceId, Parameter AS Detector, COUNT(*) AS Total
        FROM "{t["raw_events"]}"
        WHERE TimeStamp >= ? AND TimeStamp < ? AND EventId = {PED_PRESS_EVENT_ID} AND DeviceId IN ({marks})
        GROUP BY ALL ORDER BY 1
    ''', [window_start, report_end, *ids], timezone)
    if detector_config is not None and not detector_config.empty:
        out['atspm_detector_config'] = detector_config[detector_config['DeviceId'].astype(int).isin(ids)]
    return {key: value for key, value in out.items() if value is not None}


# ---------------------------------------------------------------------------
# The report's own state and outputs
# ---------------------------------------------------------------------------

def _exists(con, schema, table) -> bool:
    try:
        return bool(con.execute("SELECT 1 FROM duckdb_tables() WHERE database_name = current_database() AND schema_name = ? "
                                "AND table_name = ?", [schema, table]).fetchall())
    except Exception:
        return False


def _read(con, schema, table) -> pd.DataFrame:
    if not _exists(con, schema, table):
        return pd.DataFrame()
    return con.execute(f'SELECT * FROM "{schema}"."{table}"').df()


def _replace(con, schema, table, frame) -> None:
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    con.register('_atspm_report_state', frame)
    try:
        con.execute(f'CREATE OR REPLACE TABLE "{schema}"."{table}" AS SELECT * FROM _atspm_report_state')
    finally:
        con.unregister('_atspm_report_state')


def load_state(con, schema: str = STATE_SCHEMA, *, drop_from_date=None) -> Dict[str, Any]:
    """The state the last report saved: past_alerts (by alert type), atspm_detector_minutes, preempt_calls and
    ongoing_last_shown, as generate() takes them. drop_from_date leaves out past alerts filed for that day or later: a
    report re-run for a day it already filed would otherwise suppress its own alerts."""
    from .generator import ReportGenerator
    past = {}
    for alert_type in ReportGenerator.ALERT_TYPES:
        history = _read(con, schema, PAST_ALERTS_PREFIX + alert_type)
        if drop_from_date is not None and not history.empty and 'Date' in history.columns:
            dates = pd.to_datetime(history['Date'], errors='coerce').dt.tz_localize(None)
            history = history[dates < pd.Timestamp(drop_from_date)]
        past[alert_type] = history
    return {
        'past_alerts': past,
        'atspm_detector_minutes': _read(con, schema, DETECTOR_MINUTES),
        'preempt_calls': _read(con, schema, PREEMPT_CALLS),
        'ongoing_last_shown': _read(con, schema, ONGOING_LAST_SHOWN),
    }


def save_state(con, result: Dict[str, Any], schema: str = STATE_SCHEMA) -> None:
    """Save a report run's state for the next report (each table replaced whole, as returned). An empty frame is not
    written (the package prunes a stale one next run)."""
    def keep(table, frame):
        if frame is not None and not getattr(frame, 'empty', True):
            _replace(con, schema, table, frame)
    for alert_type, frame in (result.get('updated_past_alerts') or {}).items():
        keep(PAST_ALERTS_PREFIX + alert_type, frame)
    keep(DETECTOR_MINUTES, result.get('updated_atspm_detector_minutes'))
    keep(PREEMPT_CALLS, result.get('updated_preempt_calls'))
    keep(ONGOING_LAST_SHOWN, result.get('updated_ongoing_last_shown'))


def _json(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return json.dumps(value, default=str)


def write_problems(con, result: Dict[str, Any], report_date, *, as_of=None, analytics_only: bool = False,
                   schema: str = STATE_SCHEMA) -> int:
    """
    Replace the signal_problems table with this run's: one row per signal and area (signal_focus.PROBLEM_COLUMNS), plus
    ReportDate, AsOf (None for a report run without one), Mode ('report' or 'analytics'), RunAt and Today (whether it still
    shows today; None when that cannot be told). Lines, Harm and Phases are JSON. Returns the rows written.
    """
    problems = result.get('signal_problems')
    frame = problems.copy() if problems is not None else pd.DataFrame()
    for col in ('Lines', 'Harm', 'Phases', 'OnPhases'):
        if col in frame.columns:
            frame[col] = frame[col].map(_json)
    if today_check.TODAY_COLUMN not in frame.columns:
        frame[today_check.TODAY_COLUMN] = None
    frame[today_check.TODAY_COLUMN] = frame[today_check.TODAY_COLUMN].astype(object)
    frame['DeviceId'] = frame['DeviceId'].astype(str) if 'DeviceId' in frame.columns else pd.Series(dtype=str)
    frame['ReportDate'] = pd.Timestamp(report_date).date()
    frame['AsOf'] = pd.Timestamp(as_of).to_pydatetime() if as_of is not None else None
    frame['Mode'] = 'analytics' if analytics_only else 'report'
    frame['RunAt'] = datetime.now()
    _replace(con, schema, SIGNAL_PROBLEMS, frame)
    return len(frame)


def run(con, report_date, *, signals: pd.DataFrame, config: Optional[dict] = None, as_of=None, analytics_only: bool = False,
        detector_config: Optional[pd.DataFrame] = None, tables: Optional[dict] = None, timezone: Optional[str] = None,
        schema: str = STATE_SCHEMA, save: bool = True, **extra) -> Dict[str, Any]:
    """
    A report (or, analytics_only, an analytics run) for report_date on con: inputs from atspm's tables (load_inputs), the
    state from the report's own tables (load_state), generate(), and signal_problems written (write_problems). A report run
    saves its state (save_state) unless save is False; an analytics run never does. extra: further generate() arguments
    (ped inputs, alarm history, ...). Returns generate()'s result.
    """
    from .generator import ReportGenerator
    inputs = load_inputs(con, report_date, signals=signals, config=config, as_of=as_of, detector_config=detector_config,
                         tables=tables, timezone=timezone)
    state = load_state(con, schema, drop_from_date=None if analytics_only else report_date)
    result = ReportGenerator(config or {}).generate(**inputs, **state, **extra, report_date=report_date, as_of=as_of,
                                                    analytics_only=analytics_only)
    write_problems(con, result, report_date, as_of=as_of, analytics_only=analytics_only, schema=schema)
    if save and not analytics_only:
        save_state(con, result, schema)
    return result
