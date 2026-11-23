"""atspm's per-phase measures as report inputs: split_failures, arrival_on_green, platoon_ratio and approach_delay.

All four are optional. Each is read the way atspm's README says to roll it up: platoon ratio and delay per vehicle are
never averaged over bins; the raw columns are summed and divided once.

- arrivals on green = SUM(Percent_AOG * Total_Actuations) / SUM(Total_Actuations), from arrival_on_green
- platoon ratio = SUM(Green_Actuations) / SUM(Total_Actuations) / (SUM(Green_Seconds) / (bins * 900)), from platoon_ratio
- delay per vehicle = SUM(delay) / SUM(arrival_on_green.Total_Actuations), the two tables joined bin by bin first
  (approach_delay can end a bin earlier than arrival_on_green). The delay is Queue_Delay_Seconds, the whole delay by
  atspm's Queue Model (atspm 3.1, queue_model=True), when approach_delay has that column; a row where it is NULL (a day
  from before the Queue Model was on), or a table without it, takes Delay_Seconds + Queued_Delay_Seconds instead
- split failure share = SUM(Split_Failure) / SUM(Cycles)

No measure from a detector while it is faulting (the user's rule, 2026-11-16). Split failures come from a phase's stop-bar
zones (function Presence), the other three from its advance zones (function Advance). A bin or hour in which one of
those zones has a known fault is not used: the detector health findings (Confidence 0.5 or more, fault labels only),
the timeline's Stuck On and Erratic intervals, and, for days before the findings reach, a day on which the detector
had MASK_MINUTES_PER_DAY or more bad or suspect minutes in the stored daily minutes (the whole day goes).

Inputs may be pandas DataFrames or DuckDB relations. A relation is filtered and rolled up on its own connection and only
the small result comes back, so a caller can hand over weeks of the tables without them being read into pandas.
"""

from __future__ import annotations

from typing import Optional

import duckdb
import numpy as np
import pandas as pd

from .duckdb_utils import is_relation, to_pandas

STOP_BAR, ADVANCE, COUNT = 'stop bar', 'advance', 'count'
ZONE_OF_FUNCTION = {'Presence': STOP_BAR, 'Advance': ADVANCE}
# Timing Insights' Downstream Bottleneck also reads the stop-bar count zones.
WITH_COUNT_ZONES = {**ZONE_OF_FUNCTION, 'Count': COUNT}
# The detector health labels that are faults (lane closures, timing plans and notes are not).
FAULT_LABELS = ('stuck on', 'stuck on (together)', 'count zone held ON', 'silent', 'silent at night', 'counts dropped',
                'misses vehicles at night', 'too-fast actuations', 'too many for the traffic', 'counts too high',
                'spiky counts', 'busy at night', 'no daily pattern')
FAULT_MIN_CONFIDENCE = 0.5
TIMELINE_FAULT_CLASSES = ('Stuck On', 'Erratic')
# A day before the findings reach is masked whole when a detector had this many bad or suspect minutes (one bin).
MASK_MINUTES_PER_DAY = 15.0
BIN_SECONDS = 900
# Threads of the private DuckDB connections: the default (all cores) made repeat calls slower (see coordination_check).
DUCKDB_THREADS = 4

SPAN_COLUMNS = ['DeviceId', 'Phase', 'Zone', 's', 'e']


def _typed_empty(columns: list) -> pd.DataFrame:
    """No rows, with the column types the roll-ups give (so SQL over it binds)."""
    kinds = {'DeviceId': pd.Series(dtype=str), 'Phase': pd.Series(dtype='int64'), 'hr': pd.Series(dtype='datetime64[us]'),
             'bins': pd.Series(dtype='int64')}
    return pd.DataFrame({column: kinds.get(column, pd.Series(dtype=float)) for column in columns})


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    return con


def present(table) -> bool:
    """True for a DataFrame with rows or any relation (a relation is not counted here: that would read it)."""
    if table is None:
        return False
    if isinstance(table, pd.DataFrame):
        return not table.empty
    return is_relation(table)


# ---- Day kinds and holidays --------------------------------------------------------------------------------------------

def day_kind(day) -> str:
    """'Weekdays', 'Saturdays' or 'Sundays'."""
    weekday = pd.Timestamp(day).day_of_week
    return 'Saturdays' if weekday == 5 else 'Sundays' if weekday == 6 else 'Weekdays'


def _nth_weekday(year, month, weekday, n):
    first = pd.Timestamp(year=year, month=month, day=1)
    return first + pd.Timedelta(days=(weekday - first.day_of_week) % 7 + 7 * (n - 1))


def _last_weekday(year, month, weekday):
    last = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
    return last - pd.Timedelta(days=(last.day_of_week - weekday) % 7)


def holidays(start, end) -> list:
    """Oregon state holidays (the federal ones plus the day after Thanksgiving) from start to end, as dates; a holiday on
    a Saturday is observed the Friday before, on a Sunday the Monday after. Traffic on them is not an ordinary day."""
    days = set()
    for year in range(pd.Timestamp(start).year, pd.Timestamp(end).year + 1):
        fixed = [(1, 1), (6, 19), (7, 4), (11, 11), (12, 25)]
        for month, day in fixed:
            date = pd.Timestamp(year=year, month=month, day=day)
            days.add(date)
            if date.day_of_week == 5:
                days.add(date - pd.Timedelta(days=1))
            elif date.day_of_week == 6:
                days.add(date + pd.Timedelta(days=1))
        thanksgiving = _nth_weekday(year, 11, 3, 4)
        days.update({_nth_weekday(year, 1, 0, 3), _nth_weekday(year, 2, 0, 3), _last_weekday(year, 5, 0),
                     _nth_weekday(year, 9, 0, 1), thanksgiving, thanksgiving + pd.Timedelta(days=1)})
    return sorted(d.date() for d in days if pd.Timestamp(start).normalize() <= d <= pd.Timestamp(end))


# ---- Faults -------------------------------------------------------------------------------------------------------------

def fault_spans(findings=None, detectors: Optional[pd.DataFrame] = None, timeline=None, minutes=None,
                zones_of: Optional[dict] = None) -> pd.DataFrame:
    """When each phase's stop-bar or advance zones had a known fault: SPAN_COLUMNS (Zone 'stop bar' or 'advance', s and
    e the fault's start and end). detectors: detector_info() rows (DeviceId, Detector, Phase, Function). Without them no
    zone is known and nothing is masked. zones_of: Function -> Zone (default ZONE_OF_FUNCTION; WITH_COUNT_ZONES adds the
    count zones)."""
    empty = pd.DataFrame({'DeviceId': pd.Series(dtype=str), 'Phase': pd.Series(dtype='int64'), 'Zone': pd.Series(dtype=str),
                          's': pd.Series(dtype='datetime64[us]'), 'e': pd.Series(dtype='datetime64[us]')})
    if detectors is None or len(detectors) == 0 or not {'DeviceId', 'Detector', 'Phase', 'Function'} <= set(detectors.columns):
        return empty
    zones = detectors.assign(DeviceId=detectors['DeviceId'].astype(str), Zone=detectors['Function'].map(zones_of or ZONE_OF_FUNCTION))
    zones = zones[zones['Zone'].notna() & zones['Phase'].notna()]
    zones = zones.assign(Detector=pd.to_numeric(zones['Detector']).astype('int64'), Phase=zones['Phase'].astype('int64'))
    parts = []
    findings = to_pandas(findings)
    if findings is not None and not findings.empty and {'Start', 'End', 'Label', 'Detector'} <= set(findings.columns):
        f = findings
        if 'DetectorType' in f.columns:
            f = f[f['DetectorType'].astype(str) != 'Ped']
        if 'Confidence' in f.columns:
            f = f[pd.to_numeric(f['Confidence'], errors='coerce').fillna(0) >= FAULT_MIN_CONFIDENCE]
        f = f[f['Label'].isin(FAULT_LABELS) & f['Detector'].notna()]
        parts.append(pd.DataFrame({'DeviceId': f['DeviceId'].astype(str), 'Detector': pd.to_numeric(f['Detector']).astype('int64'),
                                   's': pd.to_datetime(f['Start']), 'e': pd.to_datetime(f['End'])}))
    timeline = to_pandas(timeline)
    if timeline is not None and not timeline.empty and 'EventClass' in timeline.columns:
        t = timeline[timeline['EventClass'].isin(TIMELINE_FAULT_CLASSES)]
        value = pd.to_numeric(t['EventValue'], errors='coerce')
        t, value = t[value.notna()], value[value.notna()]
        parts.append(pd.DataFrame({'DeviceId': t['DeviceId'].astype(str), 'Detector': value.astype('int64'),
                                   's': pd.to_datetime(t['StartTime']), 'e': pd.to_datetime(t['EndTime'])}))
    minutes = to_pandas(minutes)
    if minutes is not None and not minutes.empty and {'Date', 'BadMinutes'} <= set(minutes.columns):
        m = minutes
        if 'DetectorType' in m.columns:
            m = m[m['DetectorType'].astype(str) != 'Ped']
        total = pd.to_numeric(m['BadMinutes'], errors='coerce').fillna(0) + \
            pd.to_numeric(m.get('SuspectMinutes', 0), errors='coerce').fillna(0)
        m = m[total >= MASK_MINUTES_PER_DAY]
        day = pd.to_datetime(m['Date']).dt.tz_localize(None).dt.normalize() if len(m) else pd.to_datetime(m['Date'])
        parts.append(pd.DataFrame({'DeviceId': m['DeviceId'].astype(str), 'Detector': pd.to_numeric(m['Detector']).astype('int64'),
                                   's': day, 'e': day + pd.Timedelta(days=1)}))
    parts = [p for p in parts if not p.empty]
    if not parts:
        return empty
    spans = pd.concat(parts, ignore_index=True).dropna()
    spans = spans[spans['e'] > spans['s']]
    out = spans.merge(zones[['DeviceId', 'Detector', 'Phase', 'Zone']], on=['DeviceId', 'Detector'])
    out = out[SPAN_COLUMNS].drop_duplicates().reset_index(drop=True)
    out['s'] = out['s'].astype('datetime64[us]')
    out['e'] = out['e'].astype('datetime64[us]')
    return out


def masked_sql(alias: str, zone: str, width: str = '1 HOUR', time_column: str = 'hr') -> str:
    """SQL condition: the row (alias with DeviceId, Phase and a time column) overlaps a fault of its zone. Needs table
    `faults` (SPAN_COLUMNS) on the connection."""
    return (f"EXISTS (SELECT 1 FROM faults f WHERE f.DeviceId = {alias}.DeviceId AND f.Phase = {alias}.Phase "
            f"AND f.Zone = '{zone}' AND f.s < {alias}.{time_column} + INTERVAL {width} AND f.e > {alias}.{time_column})")


# ---- Reading the tables --------------------------------------------------------------------------------------------------

def _where(start, end, devices, days=None) -> str:
    cond = [f"TimeStamp >= TIMESTAMP '{pd.Timestamp(start)}'", f"TimeStamp < TIMESTAMP '{pd.Timestamp(end)}'"]
    if days is not None:
        cond.append("CAST(TimeStamp AS DATE) IN (" + ', '.join(f"DATE '{pd.Timestamp(d).date()}'" for d in days) + ")")
    if devices is not None:
        ids = ', '.join("'" + str(d).replace("'", "''") + "'" for d in devices) or "''"
        cond.append(f"CAST(DeviceId AS VARCHAR) IN ({ids})")
    return ' AND '.join(cond)


def rows(table, columns: str, start, end, devices=None, days=None) -> pd.DataFrame:
    """The rows of [start, end) (of these devices, on these days) with these columns, filtered before anything is read."""
    where = _where(start, end, devices, days)
    if is_relation(table):
        return table.filter(where).project(columns).df()
    con = connect()
    try:
        con.register('t', to_pandas(table))
        return con.execute(f"SELECT {columns} FROM t WHERE {where}").df()
    finally:
        con.close()


def rows_on_days(table, columns: str, days: list, devices=None) -> pd.DataFrame:
    """The rows of these days (of these devices) with these columns, in one read of the table."""
    days = sorted(pd.Timestamp(d).normalize() for d in days)
    where = _where(days[0], days[-1] + pd.Timedelta(days=1), devices, days)
    if is_relation(table):
        return table.filter(where).project(columns).df()
    con = connect()
    try:
        con.register('t', to_pandas(table))
        return con.execute(f"SELECT {columns} FROM t WHERE {where}").df()
    finally:
        con.close()


def _rollup(table, select: str, group: str, start, end, devices=None, days=None) -> pd.DataFrame:
    where = _where(start, end, devices, days)
    if is_relation(table):
        return table.filter(where).aggregate(select, group).df()
    con = connect()
    try:
        con.register('t', to_pandas(table))
        return con.execute(f"SELECT {select} FROM t WHERE {where} GROUP BY {group}").df()
    finally:
        con.close()


HOUR = "time_bucket(INTERVAL 1 HOUR, TimeStamp)"
QUEUE_DELAY = 'Queue_Delay_Seconds'


def has_queue_delay(approach_delay) -> bool:
    """True when approach_delay has atspm's Queue Model delay (Queue_Delay_Seconds, atspm 3.1 with queue_model=True)."""
    return approach_delay is not None and QUEUE_DELAY in approach_delay.columns


def delay_sql(approach_delay, alias: str = '') -> str:
    """The SQL of one approach_delay row's delay in seconds: the Queue Model's whole delay where approach_delay has it, else
    (and on a row where it is NULL) the measured plus the estimated queued delay."""
    a = f"{alias}." if alias else ''
    measured = f"({a}Delay_Seconds + {a}Queued_Delay_Seconds)"
    return f"COALESCE({a}{QUEUE_DELAY}, {measured})" if has_queue_delay(approach_delay) else measured


def delay_rows(approach_delay, start, end, devices=None, days=None) -> pd.DataFrame:
    """DeviceId, Phase, TimeStamp and Delay (seconds, delay_sql) of the approach_delay rows of [start, end)."""
    return rows(approach_delay, f"DeviceId, Phase, TimeStamp, {delay_sql(approach_delay)}::DOUBLE AS Delay", start, end, devices, days)


def hourly_delay(approach_delay, arrival_on_green, start, end, devices=None, days=None) -> pd.DataFrame:
    """Per DeviceId, Phase and hour (hr): veh (arrivals, from arrival_on_green), gre (of them on green), dsum (seconds of
    delay, delay_sql: the Queue Model's where there is one, else measured plus estimated) and bins (15-minute bins with
    both). Bins are joined before summing."""
    columns = ['DeviceId', 'Phase', 'hr', 'veh', 'gre', 'dsum', 'bins']
    if not (present(approach_delay) and present(arrival_on_green)):
        return _typed_empty(columns)
    where = _where(start, end, devices, days)

    def select(delay: str) -> str:
        return (f"CAST(g.DeviceId AS VARCHAR) AS DeviceId, CAST(g.Phase AS INTEGER) AS Phase, "
                f"time_bucket(INTERVAL 1 HOUR, g.TimeStamp) AS hr, sum(g.Total_Actuations)::DOUBLE AS veh, "
                f"sum(g.Percent_AOG * g.Total_Actuations)::DOUBLE AS gre, sum({delay})::DOUBLE AS dsum, count(*) AS bins")
    if is_relation(approach_delay) and is_relation(arrival_on_green):
        try:
            g = arrival_on_green.filter(where).set_alias('g')
            a = approach_delay.filter(where).set_alias('a')
            joined = g.join(a, 'g.DeviceId = a.DeviceId AND g.Phase = a.Phase AND g.TimeStamp = a.TimeStamp')
            return joined.aggregate(select(delay_sql(approach_delay, 'a')), '1, 2, 3').df()[columns]
        except duckdb.Error:
            pass   # relations of different connections: read the window and join here
    con = connect()
    try:
        con.register('g_in', rows(arrival_on_green, 'DeviceId, Phase, TimeStamp, Total_Actuations, Percent_AOG', start, end, devices, days))
        con.register('a_in', delay_rows(approach_delay, start, end, devices, days))
        return con.execute(f"SELECT {select('a.Delay')} FROM g_in g JOIN a_in a ON g.DeviceId = a.DeviceId AND g.Phase = a.Phase "
                           f"AND g.TimeStamp = a.TimeStamp GROUP BY 1, 2, 3").df()[columns]
    finally:
        con.close()


def hourly_platoon(platoon_ratio, start, end, devices=None, days=None) -> pd.DataFrame:
    """Per DeviceId, Phase and hour: arr, gre, gs (green seconds) and bins, to divide once: gre / arr / (gs / (bins * 900))."""
    columns = ['DeviceId', 'Phase', 'hr', 'arr', 'gre', 'gs', 'bins']
    if not present(platoon_ratio):
        return _typed_empty(columns)
    return _rollup(platoon_ratio, f"CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(Phase AS INTEGER) AS Phase, {HOUR} AS hr, "
                   "sum(Total_Actuations)::DOUBLE AS arr, sum(Green_Actuations)::DOUBLE AS gre, "
                   "sum(Green_Seconds)::DOUBLE AS gs, count(*) AS bins", '1, 2, 3', start, end, devices, days)[columns]


def hourly_split_failures(split_failures, start, end, devices=None) -> pd.DataFrame:
    """Per DeviceId, Phase and hour: sf (failed cycles) and cycles."""
    columns = ['DeviceId', 'Phase', 'hr', 'sf', 'cycles']
    if not present(split_failures):
        return _typed_empty(columns)
    return _rollup(split_failures, f"CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(Phase AS INTEGER) AS Phase, {HOUR} AS hr, "
                   "sum(Split_Failure)::DOUBLE AS sf, sum(Cycles)::DOUBLE AS cycles", '1, 2, 3', start, end, devices)[columns]


def unmasked(hours: pd.DataFrame, faults: pd.DataFrame, zone: str, width: str = '1 HOUR', time_column: str = 'hr') -> pd.DataFrame:
    """hours without the rows whose zone had a known fault in them."""
    if hours is None or hours.empty or faults is None or faults.empty:
        return hours
    con = connect()
    try:
        con.register('h', hours)
        con.register('faults', faults)
        return con.execute(f"SELECT * FROM h WHERE NOT {masked_sql('h', zone, width, time_column)}").df()
    finally:
        con.close()


def chart_window(day, days: int):
    """[start, end) of the `days` days up to and including day."""
    end = pd.Timestamp(day).normalize() + pd.Timedelta(days=1)
    return end - pd.Timedelta(days=days), end


def hours_of_day(values: pd.Series) -> np.ndarray:
    return pd.to_datetime(values).dt.hour.to_numpy()
