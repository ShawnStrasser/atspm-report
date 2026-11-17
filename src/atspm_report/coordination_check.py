"""Coordination plans or clocks set wrong.

Signals along a corridor are coordinated: they run the same cycle length (or half or double it) at the same times of day,
so traffic leaving one on green reaches the next on green. A signal that runs another plan, runs free, or changes plans at
other hours than the signals around it breaks the corridor at that point. Two checks look for it, each with its own
switch and its own alerts:

Plan check (plan_findings). Which signals are coordinated together is learned from coordination_agg history and the
signals' coordinates, in two steps.

By what they run: two signals within NEIGHBOUR_MAX_MILES that both run coordinated plans are neighbours when, in the bins
where one of them is coordinated, they run matching cycles (equal, half or double) at least NEIGHBOUR_MIN_MATCH of the
time. A pair is judged on NEIGHBOUR_MIN_BINS shared bins, about five days of data.

Signals this leaves without a group (fewer than CONSENSUS_MIN_AGREE neighbours) are attached to one, one way:
- part time: to a signal within PART_TIME_MAX_MILES whose cycle it runs in the hours the one of them that coordinates
  less is coordinated (03021 runs with 03026 at Woodburn about four hours a day; 2026-11-16);
- by position: to two neighbours it sits between, or to the next signals along the line past the signal it is attached
  to part time (POSITION_MAX_DETOUR). 2B418 on OR8 ran free all day between 2B414 and 2B419, which run 100 to 135 s
  plans together (user, 2026-11-16: a signal in the middle of a group is part of that group).
An attached signal is judged against the signals it is attached to, but they are not judged against it: it has shown
it does not run their plans, so it neither counts towards what the group runs nor makes another signal's difference a
shared one. Letting such signals count broke the 2/3 agreement in small groups and hid findings already confirmed.

The check needs MIN_REPEAT_DAYS of each kind of day on top, so weekend findings start after three weekends.
COORDINATION_HISTORY_DAYS (six weeks) is the design; with less, groups still form but fewer kinds of day can be judged.

For each 15-minute bin a signal's neighbours agree on a state (CONSENSUS_MIN_AGREE of them and CONSENSUS_MIN_SHARE of
those reporting). The signal differs alone when its own state is another one and no neighbour shares it: a sub-system at
the edge of a group, with a neighbour running what it runs, is not alone. A bin is a finding for one kind of day
(weekdays, Saturdays, Sundays) when the signal differed alone in it on MIN_SHARE of its recent days of that kind
(RECENT_DAYS) and on at least MIN_REPEAT_DAYS of them; runs of at least MIN_RUN_BINS such bins (30 min) are told, and only
while the latest day of that kind judged in those hours still showed it. A difference on weekdays, or on every day, is
told too (user, 2026-11-16: such a signal may be set up that way, or set wrong; the review decides).

The check reads the controller's own log: times are as each controller stamped them. A signal whose clock is wrong but
whose log shows its plans at the scheduled times is the clock check's to find, not this one's.

Outages. atspm carries a signal's last plan forward across a gap in its data, so a plan change lost in the gap leaves the
old plan in the record until the next change: the false cause of most lone differences in the study. Bins after a gap
are not trusted until the signal records a plan change or the day ends (a signal holding one plan for days would
otherwise stay untrusted for days; one stale day cannot make a finding, which needs MIN_REPEAT_DAYS), and a day needs
EVALUABLE_DAY_MIN_BINS bins judged to count.

Clock check (preempt_clock_offsets). An emergency vehicle preempts one signal after another within seconds to a minute,
and each controller stamps its calls with its own clock. A signal whose calls keep lining up with its neighbours' calls a
fixed time apart, and no longer at the same time, has a clock off by that much from the day the lining up moved. It reads
the preempt calls the report already keeps (six weeks), so it runs every report.

Both run as set-based SQL on one in-memory DuckDB connection; Python only words and draws the few findings.
"""

from __future__ import annotations

import math
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

from .duckdb_utils import to_pandas
from .preempt_cascade import CLASS_RAIL, TSP_CLASSES, signal_neighbours
from .utils import log_message

ALERT_TYPE = 'coordination'
CAUSE_SCHEDULE, CAUSE_CLOCK = 'Schedule', 'Clock'
SECTION_TITLE = 'Coordination Plans and Clocks'
ALERT_COLUMNS = ['DeviceId', 'Date', 'Cause', 'Text', 'Group', 'Runs', 'Matched', 'ClockOffsetSeconds', 'ClockSince',
                 'ChartDay', 'ChartOwn', 'ChartGroup', 'ChartCalls', 'Confidence']
# High: a clock alert, or a signal out of step that sits inside its group (between two of its signals) while no
# nearby signal outside the group runs what it runs. On the 2026-11-16 review every such signal was called a real
# problem and every signal at an end of its group 'probably'. Only High alerts go on to the Ongoing list.
CONFIDENCE_HIGH, CONFIDENCE_CHECK = 'High', 'Check'
# High also needs this many hours out of step, added over its findings: the signals confirmed on the review had six
# or more, and the 30 to 60 minute ones (06044, 2B331, 2B422) were never reviewed.
CONFIDENCE_MIN_HOURS = 2.0
WITH_MIN_SHARE = 0.6

# ---- States -----------------------------------------------------------------------------------------------------------
BIN_MINUTES = 15
BINS_PER_DAY = 24 * 60 // BIN_MINUTES
PATTERN_UNKNOWN, PATTERN_FREE, PATTERN_FLASH = 0, 254, 255
FREE, FLASH = 0.0, -1.0
# Pattern 255 is not taken as flashing: its bins are left unjudged, like pattern 0. From 2026-11-06 two Bend signals
# (10017, 10026) logged 255 all day while every green ended in a force-off, so they were cycling under a plan the
# log does not name. A signal that does flash is the flash alarm's to tell.
# A coordinated pattern sometimes logs cycle length 0 while it runs its normal cycle (the event at the pattern change
# says 0; actual cycle and offset carry on). Without a correction such signals look free. So each signal's pattern gets
# the cycle it usually logs, and a pattern logging a cycle in less than this share of its bins is a free pattern (21, 22
# and 23 on most ODOT controllers). Study of 2026-11-16.
FREE_PATTERN_MAX_SHARE = 0.2
# Six weeks: four weekends to judge, and groups learned from more than the weeks judged.
COORDINATION_HISTORY_DAYS = 42

# ---- Groups -----------------------------------------------------------------------------------------------------------
NEIGHBOUR_MAX_MILES = 1.5
COORDINATED_MIN_BINS = 3 * BINS_PER_DAY   # three days' worth of coordinated bins
NEIGHBOUR_MIN_BINS = 400                  # bins both reported and one ran coordinated
NEIGHBOUR_MIN_MATCH = 0.5                 # ...with matching cycles in at least this share
# A signal left without a group is attached to a signal this close when they run matching cycles in NEIGHBOUR_MIN_MATCH
# of the bins the one that coordinates less is coordinated (at least NEIGHBOUR_MIN_COORDINATED of them). Without the
# distance, two corridors that happen to use the same cycle were joined (2B052 on OR224 with OR99E, 2026-11-16).
PART_TIME_MAX_MILES = 0.5
NEIGHBOUR_MIN_COORDINATED = 200
# A signal is in line with two others when going by way of it adds no more than this to the straight distance.
POSITION_MAX_DETOUR = 1.05
LEARNED, PART_TIME, BY_POSITION = 'learned', 'part time', 'position'
PAIR_COLUMNS = ['DeviceId', 'NextDeviceId', 'Miles', 'Bins', 'Match', 'Kind']

# ---- Plan check -------------------------------------------------------------------------------------------------------
CONSENSUS_MIN_AGREE = 2
CONSENSUS_MIN_SHARE = 2 / 3
WEEKDAYS, SATURDAYS, SUNDAYS = 'Weekdays', 'Saturdays', 'Sundays'
DAY_TYPES = (WEEKDAYS, SATURDAYS, SUNDAYS)
# Recent days of each kind judged: two weeks of weekdays, four weekends.
RECENT_DAYS = {WEEKDAYS: 10, SATURDAYS: 4, SUNDAYS: 4}
MIN_REPEAT_DAYS = 3
MIN_SHARE = 0.75
MIN_RUN_BINS = 2
EVALUABLE_DAY_MIN_BINS = 32
# Of a finding's bins judged, the share the latest day of that kind judged there must still show. Asking only one of
# the last two let 2C023's Sunday finding run on for a week after its odd cycles stopped on 11-02 (study of 2026-11-16).
CURRENT_MIN_SHARE = 0.5
# A signal agreed with its group in a finding's hours on a day when it did in this share of the bins judged.
MATCHED_MIN_SHARE = 0.75

# ---- Clock check ------------------------------------------------------------------------------------------------------
CLOCK_MAX_MILES = 1.0
# Calls at two signals this close in time are the same vehicle (travel between neighbours within a mile, and detection).
ALIGN_SECONDS = 120
# Offsets searched: up to 75 minutes either way, from 5 minutes (closer than that is travel time).
CLOCK_SEARCH_SECONDS = 4500
CLOCK_MIN_OFFSET_SECONDS = 300
CLOCK_MIN_CALLS = 3
CLOCK_MIN_NEIGHBOURS = 2
# Any other offset: a vehicle on a regular round trip (out past one signal, back past the next a set time later) can
# line up a few calls by itself. Over six weeks of ODOT calls one signal (03096) lined up 5 calls at 11 to 12 minutes
# with two neighbours for two days; the missed clock change at 2B095 lined up 9 at an hour with five within a week.
CLOCK_MIN_CALLS_OTHER = 8
CLOCK_MIN_NEIGHBOURS_OTHER = 3
# Since the offset began, calls at the offset must outnumber calls at the same time, and calls at the mirror offset (a
# vehicle's travel time between two signals gives both), this many times over.
CLOCK_MIN_RATIO = 3.0
CLOCK_MAX_P_VALUE = 1e-3
# One vehicle can call several inputs of a signal; calls this close together at a signal are one.
CALL_GAP_SECONDS = 30
# An offset this close to an hour, starting at a clock change, is a missed clock change.
HOUR_SLACK_SECONDS = 600

# Threads of the checks' DuckDB connections. On a 28-core machine the default (all of them) made the plan check slower
# each time it ran in one process (0.7 s to 2.7 s over five runs, 2026-11-16); four held it at about 1.4 s.
DUCKDB_THREADS = 4

GREY, STRONG, SHADE = '#9a9a9a', '#c4302b', '#fbe3df'
CHART_SIZE = (7.2, 3.0)
# Drawn at 6.5 inches wide in the PDF.
CHART_HEIGHT_INCHES = 6.5 * CHART_SIZE[1] / CHART_SIZE[0]


def _connect():
    """An in-memory DuckDB connection on DUCKDB_THREADS threads."""
    con = duckdb.connect()
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    return con


def _cycle_key_sql(x: str) -> str:
    """SQL for the key equal for matching states: a cycle halved (while even and 40 s or more after halving) twice."""
    once = f"CASE WHEN {x} > 0 AND {x} / 2 >= 40 AND CAST(round({x}) AS BIGINT) % 2 = 0 THEN {x} / 2 ELSE {x} END"
    return f"CASE WHEN ({once}) > 0 AND ({once}) / 2 >= 40 AND CAST(round({once}) AS BIGINT) % 2 = 0 THEN ({once}) / 2 ELSE ({once}) END"


def us_clock_changes(start, end) -> list:
    """(local time, hours) of US clock changes from start to end: +1 at 2:00 on the second Sunday of March, -1 at 2:00 on
    the first Sunday of November."""
    changes = []
    for year in range(pd.Timestamp(start).year, pd.Timestamp(end).year + 1):
        march = pd.Timestamp(year=year, month=3, day=1)
        november = pd.Timestamp(year=year, month=11, day=1)
        spring = march + pd.Timedelta(days=(6 - march.dayofweek) % 7 + 7, hours=2)
        fall = november + pd.Timedelta(days=(6 - november.dayofweek) % 7, hours=2)
        changes += [(spring, 1), (fall, -1)]
    return [(when, hours) for when, hours in changes if pd.Timestamp(start) <= when <= pd.Timestamp(end)]


# ======================================================================================================================
# Plan check
# ======================================================================================================================

def _load_states(con, coordination, start, end) -> None:
    """Temp table st (DeviceId, ts, State, CycleKey): each signal's state per bin, stale bins after gaps left out."""
    con.register('coord', coordination)
    con.execute(f"""
        CREATE TEMP TABLE raw AS
        SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, time_bucket(INTERVAL {BIN_MINUTES} MINUTE, TimeStamp) AS ts,
               max(CAST(Pattern AS INTEGER)) AS Pattern, max(CAST(CycleLength AS DOUBLE)) AS CycleLength
        FROM coord WHERE TimeStamp >= $start AND TimeStamp < $end AND Pattern IS NOT NULL GROUP BY ALL
    """, {'start': start.to_pydatetime(), 'end': end.to_pydatetime()})
    con.execute(f"""
        CREATE TEMP TABLE st AS
        WITH u AS (
            SELECT DeviceId, Pattern, avg(CAST(CycleLength > 0 AS INTEGER)) AS share,
                   mode(CycleLength) FILTER (CycleLength > 0) AS usual
            FROM raw WHERE Pattern NOT IN ({PATTERN_UNKNOWN}, {PATTERN_FREE}, {PATTERN_FLASH}) GROUP BY ALL
        ), s AS (
            SELECT r.DeviceId, r.ts, CASE
                WHEN r.Pattern = {PATTERN_UNKNOWN} THEN NULL
                WHEN r.Pattern = {PATTERN_FLASH} THEN NULL
                WHEN r.Pattern = {PATTERN_FREE} THEN {FREE}
                WHEN coalesce(u.share, 0) < {FREE_PATTERN_MAX_SHARE} THEN {FREE}
                WHEN r.CycleLength > 0 THEN r.CycleLength
                ELSE coalesce(u.usual, {FREE}) END AS State,
                r.ts - lag(r.ts) OVER w > INTERVAL {BIN_MINUTES} MINUTE AS gap,
                r.Pattern <> lag(r.Pattern) OVER w AS changed
            FROM raw r LEFT JOIN u USING (DeviceId, Pattern)
            WINDOW w AS (PARTITION BY r.DeviceId ORDER BY r.ts)
        ), f AS (
            SELECT *, max(CASE WHEN gap THEN ts END) OVER w2 AS last_resume,
                      max(CASE WHEN changed THEN ts END) OVER w2 AS last_change
            FROM s WINDOW w2 AS (PARTITION BY DeviceId ORDER BY ts ROWS UNBOUNDED PRECEDING)
        )
        SELECT DeviceId, ts, State, {_cycle_key_sql('State')} AS CycleKey FROM f
        WHERE State IS NOT NULL AND NOT coalesce(last_resume > coalesce(last_change, TIMESTAMP '1900-01-01')
                                                 AND CAST(last_resume AS DATE) = CAST(ts AS DATE), false)
    """)


def _learn_neighbours(con, signals, max_miles, by_position: bool = False) -> pd.DataFrame:
    """Temp table nb (DeviceId, NextDeviceId): each signal and the signals it is judged against. Returns the pairs
    (PAIR_COLUMNS): Kind 'learned' pairs hold both ways round and are listed once; Kind 'part time' and 'position' rows
    are one way, DeviceId being the signal attached to NextDeviceId's group."""
    near = signal_neighbours(signals, max_miles)
    if near.empty:
        near = pd.DataFrame({'DeviceId': pd.Series(dtype=str), 'NextDeviceId': pd.Series(dtype=str),
                             'Miles': pd.Series(dtype=float)})
    con.register('near', near)
    con.execute(f"""
        CREATE TEMP TABLE matches AS
        WITH co AS (SELECT DeviceId FROM st GROUP BY ALL HAVING count(*) FILTER (State > 0) >= {COORDINATED_MIN_BINS}),
        n AS (SELECT * FROM near WHERE DeviceId < NextDeviceId
              AND DeviceId IN (SELECT DeviceId FROM co) AND NextDeviceId IN (SELECT DeviceId FROM co))
        SELECT n.DeviceId, n.NextDeviceId, any_value(n.Miles) AS Miles,
               count(*) FILTER (a.State > 0 OR b.State > 0) AS Bins,
               least(count(*) FILTER (a.State > 0), count(*) FILTER (b.State > 0)) AS Fewer,
               count(*) FILTER (a.State > 0 AND b.State > 0 AND (abs(a.State / b.State - 1) < 0.02
                   OR abs(a.State / b.State - 2) < 0.04 OR abs(a.State / b.State - 0.5) < 0.01)) AS Matched
        FROM n JOIN st a ON a.DeviceId = n.DeviceId JOIN st b ON b.DeviceId = n.NextDeviceId AND b.ts = a.ts
        GROUP BY n.DeviceId, n.NextDeviceId HAVING Bins >= {NEIGHBOUR_MIN_BINS}
    """)
    con.execute(f"CREATE TEMP TABLE pairs AS SELECT * FROM matches WHERE Matched >= {NEIGHBOUR_MIN_MATCH} * Bins")
    con.execute("""CREATE TEMP TABLE lnb AS SELECT DeviceId, NextDeviceId FROM pairs
                   UNION ALL SELECT NextDeviceId, DeviceId FROM pairs""")
    # Signals with data that what they run left without a group, and what they are attached to: part time to a close
    # signal A; by position between a learned pair A and B, or to B past A on the line when attached to A part time.
    con.execute(f"""
        CREATE TEMP TABLE lone AS
        SELECT DeviceId FROM st GROUP BY ALL HAVING count(*) >= {COORDINATED_MIN_BINS}
        EXCEPT SELECT DeviceId FROM lnb GROUP BY ALL HAVING count(*) >= {CONSENSUS_MIN_AGREE}
    """)
    con.execute(f"""
        CREATE TEMP TABLE part AS
        WITH ways AS (SELECT DeviceId, NextDeviceId, Miles, Fewer, Matched FROM matches
                      UNION ALL SELECT NextDeviceId, DeviceId, Miles, Fewer, Matched FROM matches)
        SELECT DeviceId, NextDeviceId FROM ways
        WHERE DeviceId IN (SELECT DeviceId FROM lone) AND Miles <= {PART_TIME_MAX_MILES}
          AND Fewer >= {NEIGHBOUR_MIN_COORDINATED} AND Matched >= {NEIGHBOUR_MIN_MATCH} * Fewer
        EXCEPT SELECT DeviceId, NextDeviceId FROM lnb
    """)
    con.execute("""
        CREATE TEMP TABLE tri AS
        SELECT sa.DeviceId AS S, p.DeviceId AS A, p.NextDeviceId AS B, sa.Miles AS SA, sb.Miles AS SB, ab.Miles AS AB
        FROM lnb p JOIN near ab ON ab.DeviceId = p.DeviceId AND ab.NextDeviceId = p.NextDeviceId
        JOIN near sa ON sa.NextDeviceId = p.DeviceId
        JOIN near sb ON sb.DeviceId = sa.DeviceId AND sb.NextDeviceId = p.NextDeviceId
        WHERE sa.DeviceId IN (SELECT DeviceId FROM lone)
    """)
    # Past the end: S runs with A part time (or is its one learned neighbour) and B is next along the line beyond A.
    con.execute(f"""
        CREATE TEMP TABLE ext AS
        SELECT DISTINCT t.S AS DeviceId, t.B AS NextDeviceId FROM tri t
        JOIN (SELECT * FROM part UNION ALL SELECT * FROM lnb) l ON l.DeviceId = t.S AND l.NextDeviceId = t.A
        WHERE t.SA + t.AB <= {POSITION_MAX_DETOUR} * t.SB
        EXCEPT (SELECT DeviceId, NextDeviceId FROM lnb UNION ALL SELECT DeviceId, NextDeviceId FROM part)
    """)
    # In the middle: S between a learned pair A and B (its own switch, see by_position).
    con.execute(f"""
        CREATE TEMP TABLE pos AS
        SELECT DISTINCT S AS DeviceId, unnest([A, B]) AS NextDeviceId FROM tri
        WHERE {'true' if by_position else 'false'} AND SA + SB <= {POSITION_MAX_DETOUR} * AB
        EXCEPT (SELECT DeviceId, NextDeviceId FROM lnb UNION ALL SELECT DeviceId, NextDeviceId FROM part
                UNION ALL SELECT DeviceId, NextDeviceId FROM ext)
    """)
    con.execute("""CREATE TEMP TABLE nb AS SELECT * FROM lnb UNION ALL SELECT * FROM part
                   UNION ALL SELECT * FROM ext UNION ALL SELECT * FROM pos""")
    return con.execute(f"""
        SELECT DeviceId, NextDeviceId, Miles, Bins, Matched / Bins AS Match, '{LEARNED}' AS Kind FROM pairs
        UNION ALL
        SELECT p.DeviceId, p.NextDeviceId, n.Miles, NULL, NULL, '{PART_TIME}'
        FROM (SELECT * FROM part UNION ALL SELECT * FROM ext) p JOIN near n USING (DeviceId, NextDeviceId)
        UNION ALL
        SELECT p.DeviceId, p.NextDeviceId, n.Miles, NULL, NULL, '{BY_POSITION}'
        FROM pos p JOIN near n USING (DeviceId, NextDeviceId)
    """).df()


def group_of(pairs: pd.DataFrame, device) -> list:
    """The signals one signal is judged against: its learned neighbours, and for a signal attached to a group (part
    time or by position), the signals it is attached to."""
    if pairs is None or pairs.empty:
        return []
    learned = pairs['Kind'] == LEARNED if 'Kind' in pairs.columns else pd.Series(True, index=pairs.index)
    return sorted(set(pairs.loc[pairs['DeviceId'] == device, 'NextDeviceId'])
                  | set(pairs.loc[learned & (pairs['NextDeviceId'] == device), 'DeviceId']))


def attached_to(pairs: pd.DataFrame, device) -> list:
    """The signals attached to this signal's group (they are judged against it, not it against them)."""
    if pairs is None or pairs.empty or 'Kind' not in pairs.columns:
        return []
    return sorted(set(pairs.loc[(pairs['Kind'] != LEARNED) & (pairs['NextDeviceId'] == device), 'DeviceId']))


def _judge(con) -> None:
    """Temp tables j (each signal's judged bins against its neighbours' consensus) and chosen (its recent days)."""
    con.execute("""
        CREATE TEMP TABLE nk AS
        SELECT p.DeviceId, s.ts, s.CycleKey, count(*) AS n, mode(s.State) AS State
        FROM nb p JOIN st s ON s.DeviceId = p.NextDeviceId GROUP BY ALL
    """)
    con.execute(f"""
        CREATE TEMP TABLE j AS
        WITH cons AS (
            SELECT DeviceId, ts, arg_max(CycleKey, n) AS Cons, arg_max(State, n) AS ConsState, max(n) AS Agree
            FROM nk GROUP BY ALL HAVING max(n) >= {CONSENSUS_MIN_AGREE} AND max(n) >= {CONSENSUS_MIN_SHARE} * sum(n)
        )
        SELECT s.DeviceId, CAST(s.ts AS DATE) AS Day,
               CAST((hour(s.ts) * 60 + minute(s.ts)) // {BIN_MINUTES} AS INTEGER) AS Bin,
               CASE isodow(s.ts) WHEN 6 THEN '{SATURDAYS}' WHEN 7 THEN '{SUNDAYS}' ELSE '{WEEKDAYS}' END AS DayType,
               s.State, s.CycleKey, c.Cons, c.ConsState, c.Agree,
               s.CycleKey <> c.Cons AND sh.n IS NULL AS Alone, s.CycleKey = c.Cons AS Agreeing
        FROM st s JOIN cons c USING (DeviceId, ts)
        LEFT JOIN nk sh ON sh.DeviceId = s.DeviceId AND sh.ts = s.ts AND sh.CycleKey = s.CycleKey
    """)
    con.execute(f"""
        CREATE TEMP TABLE chosen AS
        SELECT * FROM (
            SELECT DeviceId, Day, DayType,
                   row_number() OVER (PARTITION BY DeviceId, DayType ORDER BY Day DESC) AS Recent
            FROM j GROUP BY DeviceId, Day, DayType HAVING count(*) >= {EVALUABLE_DAY_MIN_BINS})
        WHERE Recent <= CASE DayType WHEN '{WEEKDAYS}' THEN {RECENT_DAYS[WEEKDAYS]}
                                     WHEN '{SATURDAYS}' THEN {RECENT_DAYS[SATURDAYS]} ELSE {RECENT_DAYS[SUNDAYS]} END
    """)


def _repeat_bins(con) -> pd.DataFrame:
    """Bins of the day a signal differed alone in on most of its recent days of a kind (DeviceId, DayType, Bin)."""
    return con.execute(f"""
        WITH enough AS (SELECT DeviceId, DayType FROM chosen GROUP BY ALL HAVING count(*) >= {MIN_REPEAT_DAYS})
        SELECT j.DeviceId, j.DayType, j.Bin
        FROM j JOIN chosen USING (DeviceId, Day, DayType) JOIN enough USING (DeviceId, DayType)
        GROUP BY ALL
        HAVING sum(CAST(Alone AS INTEGER)) >= {MIN_REPEAT_DAYS} AND sum(CAST(Alone AS INTEGER)) >= {MIN_SHARE} * count(*)
    """).df()


def _runs(mask: np.ndarray) -> list:
    """(start, end) of runs of True, holes of one bin filled, at least MIN_RUN_BINS long."""
    m = mask.copy()
    m[1:-1] |= ~m[1:-1] & m[:-2] & m[2:]
    edges = np.diff(np.r_[0, m.astype(int), 0])
    return [(s, e) for s, e in zip(np.where(edges == 1)[0], np.where(edges == -1)[0]) if e - s >= MIN_RUN_BINS]


def _mode(values) -> float:
    values = np.asarray(values, dtype=float)
    values = values[~np.isnan(values)]
    if values.size == 0:
        return np.nan
    uniq, counts = np.unique(values, return_counts=True)
    return float(uniq[np.argmax(counts)])


FINDING_COLUMNS = ['DeviceId', 'DayType', 'Start', 'End', 'Own', 'Cons', 'OwnStates', 'Edge', 'Agree', 'Seen', 'Judged',
                   'SeenDays', 'MatchedDays', 'OwnTypical', 'ConsStates']


def _kind(value) -> float:
    """Free, a cycle or flash, whatever the cycle length (nan when unknown)."""
    return np.nan if np.isnan(value) else float(np.sign(value))


def _pieces(start: int, end: int, own: np.ndarray, cons: np.ndarray) -> list:
    """A run of bins cut where what the signal or its group usually runs changes between free and a cycle, so one
    sentence never covers hours the group ran a plan and hours it ran free (2B147, 2026-11-16: its neighbours ran
    110 s until 9:00 and free until 14:30, and both were told as free). Pieces under MIN_RUN_BINS are dropped."""
    pieces, first, last = [], start, None
    for b in range(start, end):
        kinds = (_kind(own[b]), _kind(cons[b]))
        if np.isnan(kinds[0]) or np.isnan(kinds[1]):       # a filled hole goes with the bins before it
            continue
        if last is not None and kinds != last:
            pieces.append((first, b))
            first = b
        last = kinds
    pieces.append((first, end))
    return [(s, e) for s, e in pieces if e - s >= MIN_RUN_BINS]


def _describe(rows: pd.DataFrame, chosen: pd.DataFrame, repeat: pd.DataFrame) -> pd.DataFrame:
    """The findings of the few signals with repeating bins, from their judged bins (rows) and recent days (chosen)."""
    found = []
    for (device, day_type), bins in repeat.groupby(['DeviceId', 'DayType']):
        mask = np.zeros(BINS_PER_DAY, dtype=bool)
        mask[bins['Bin'].to_numpy()] = True
        mine = rows[rows['DeviceId'] == device]
        recent = sorted(chosen.loc[(chosen['DeviceId'] == device) & (chosen['DayType'] == day_type), 'Day'])
        of_type = mine[mine['Day'].isin(recent)]

        def grid(column):
            return of_type.pivot_table(index='Day', columns='Bin', values=column, aggfunc='first').reindex(
                index=recent, columns=range(BINS_PER_DAY)).to_numpy(float)

        alone, key, cons, state = grid('Alone'), grid('CycleKey'), grid('Cons'), grid('State')
        cons_state, agree = grid('ConsState'), grid('Agree')
        judged = ~np.isnan(key)
        alone = np.nan_to_num(alone, nan=0).astype(bool)
        all_days = mine.pivot_table(index='Day', columns='Bin', values='Agreeing', aggfunc='first').reindex(
            columns=range(BINS_PER_DAY))
        # What the signal and its group usually ran in each bin, on the days it differed alone there.
        own_usual = np.array([_mode(state[:, b][alone[:, b]]) for b in range(BINS_PER_DAY)])
        cons_usual = np.array([_mode(cons_state[:, b][alone[:, b]]) for b in range(BINS_PER_DAY)])
        for s, e in [piece for run in _runs(mask) for piece in _pieces(*run, own_usual, cons_usual)]:
            def showed(d):
                n = judged[d, s:e].sum()
                return n > 0 and alone[d, s:e].sum() >= CURRENT_MIN_SHARE * n
            judged_days = [d for d in range(len(recent)) if judged[d, s:e].any()]
            if not judged_days or not showed(judged_days[-1]):
                continue
            cells = alone[:, s:e]
            own_key, cons_key = _mode(key[:, s:e][cells]), _mode(cons[:, s:e][cells])
            own_states = state[:, s:e][cells]

            def typical(grid_values, b):
                return np.nan if b < 0 or b >= BINS_PER_DAY else _mode(grid_values[:, b])
            before, after = typical(key, s - 1), typical(key, e)
            cons_before, cons_after = typical(cons, s - 1), typical(cons, e)
            own_typical = sorted(set(own_usual[s:e][~np.isnan(own_usual[s:e])].tolist()))
            cons_states = sorted(set(cons_usual[s:e][~np.isnan(cons_usual[s:e])].tolist()))
            # An early or late change is one plan against one plan; anything else is told by its hours.
            steady = len(own_typical) <= 1 and len(cons_states) <= 1
            edge = ('early' if steady and before == cons_key and cons_after == own_key else
                    'late' if steady and cons_before == own_key and after == cons_key else 'whole')
            window = all_days.loc[:, s:e - 1]
            judged_all = window.notna().sum(axis=1)
            agreed = window.fillna(False).astype(bool).sum(axis=1)
            matched = [pd.Timestamp(d) for d in window.index[(judged_all > 0) & (agreed >= MATCHED_MIN_SHARE * judged_all)]]
            found.append({
                'DeviceId': device, 'DayType': day_type, 'Start': int(s), 'End': int(e),
                'Own': _mode(own_states), 'Cons': _mode(cons_state[:, s:e][cells]),
                'OwnStates': sorted(set(own_states[~np.isnan(own_states)].tolist())), 'Edge': edge,
                'Agree': int(np.nanmedian(agree[:, s:e][cells])) if cells.any() else 0,
                'Seen': int(sum(showed(d) for d in range(len(recent)))),
                'Judged': int(sum(judged[d, s:e].any() for d in range(len(recent)))),
                'SeenDays': [pd.Timestamp(recent[d]) for d in range(len(recent)) if showed(d)],
                'MatchedDays': matched, 'OwnTypical': own_typical, 'ConsStates': cons_states,
            })
    return pd.DataFrame(found, columns=FINDING_COLUMNS)


def plan_findings(coordination, signals: Optional[pd.DataFrame], report_date,
                  history_days: int = COORDINATION_HISTORY_DAYS, max_miles: float = NEIGHBOUR_MAX_MILES,
                  by_position: bool = False) -> dict:
    """The plan check for one report day.

    coordination: coordination_agg (TimeStamp, DeviceId, Pattern, CycleLength) of the history_days up to report_date.
    Returns a dict: 'findings' (FINDING_COLUMNS: one row per signal, kind of day and run of bins; Start and End are
    bins of the day, Own and Cons the cycle it and its group ran, 0 free, -1 flash; Edge 'early', 'late' or 'whole';
    Inside and With as _position_facts gives them),
    'pairs' (PAIR_COLUMNS: the neighbour pairs learned and the signals attached to a group one way) and 'charts' ({(DeviceId, day): {DeviceId: 96 states}} for each finding's
    signal and group on its latest day seen). by_position also attaches signals that sit between two signals of a
    group while running none of its plans (held back until reviewed).
    """
    empty = {'findings': pd.DataFrame(columns=FINDING_COLUMNS + ['Inside', 'With']),
             'pairs': pd.DataFrame(columns=PAIR_COLUMNS), 'charts': {}}
    coordination = to_pandas(coordination)
    if coordination is None or coordination.empty or not {'Pattern', 'CycleLength'} <= set(coordination.columns):
        return empty
    end = pd.Timestamp(report_date).normalize() + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=history_days)
    con = _connect()
    try:
        _load_states(con, coordination, start, end)
        pairs = _learn_neighbours(con, signals, max_miles, by_position)
        if pairs.empty:
            return {**empty, 'pairs': pairs}
        _judge(con)
        repeat = _repeat_bins(con)
        if repeat.empty:
            return {**empty, 'pairs': pairs}
        con.register('cand', repeat[['DeviceId']].drop_duplicates())
        rows = con.execute("SELECT * FROM j WHERE DeviceId IN (SELECT DeviceId FROM cand)").df()
        chosen = con.execute("SELECT * FROM chosen WHERE DeviceId IN (SELECT DeviceId FROM cand)").df()
        for frame in (rows, chosen):
            frame['Day'] = pd.to_datetime(frame['Day'])
        findings = _position_facts(con, _describe(rows, chosen, repeat), pairs, end)
        charts = _chart_states(con, findings, pairs)
    finally:
        con.close()
    return {'findings': findings, 'pairs': pairs, 'charts': charts}


def _position_facts(con, findings: pd.DataFrame, pairs: pd.DataFrame, end) -> pd.DataFrame:
    """findings with Inside (the signal sits between two signals of its group, not at an end of it) and With (signals
    within PART_TIME_MAX_MILES outside its group that run the cycle it runs in a finding's hours: it may be set to run
    with them then, as where two corridors cross)."""
    findings = findings.assign(Inside=False, With=[[] for _ in range(len(findings))])
    if findings.empty:
        return findings
    con.register('found_devices', findings[['DeviceId']].drop_duplicates())
    near = con.execute("SELECT * FROM near WHERE DeviceId IN (SELECT DeviceId FROM found_devices)").df()
    between = con.execute("SELECT DeviceId, NextDeviceId, Miles FROM near").df() if not near.empty else near
    miles = {(a, b): m for a, b, m in between.itertuples(index=False)}
    groups = {device: group_of(pairs, device) for device in findings['DeviceId'].unique()}
    inside = {device: any(miles.get((device, a), np.inf) + miles.get((device, b), np.inf)
                          <= POSITION_MAX_DETOUR * miles.get((a, b), 0.0)
                          for k, a in enumerate(group) for b in group[k + 1:]) for device, group in groups.items()}
    findings['Inside'] = findings['DeviceId'].map(inside).astype(bool)
    runs = findings.loc[findings['Own'] > 0, ['DeviceId', 'DayType', 'Start', 'End', 'Own']].reset_index(names='Row')
    if runs.empty:
        return findings
    con.register('runs', runs)
    shared = con.execute(f"""
        SELECT r.Row, n.NextDeviceId AS Other
        FROM runs r JOIN near n ON n.DeviceId = r.DeviceId AND n.Miles <= {PART_TIME_MAX_MILES}
        JOIN st s ON s.DeviceId = n.NextDeviceId AND s.ts >= TIMESTAMP '{end - pd.Timedelta(days=28)}'
            AND CASE isodow(s.ts) WHEN 6 THEN '{SATURDAYS}' WHEN 7 THEN '{SUNDAYS}' ELSE '{WEEKDAYS}' END = r.DayType
            AND (hour(s.ts) * 60 + minute(s.ts)) // {BIN_MINUTES} >= r.Start
            AND (hour(s.ts) * 60 + minute(s.ts)) // {BIN_MINUTES} < r.End
        GROUP BY ALL HAVING avg(CAST(abs(s.State / r.Own - 1) < 0.02 AS INTEGER)) >= {WITH_MIN_SHARE}
    """).df()
    for row, others in shared.groupby('Row')['Other']:
        device = findings.at[row, 'DeviceId']
        findings.at[row, 'With'] = sorted(o for o in others if o not in groups[device])
    return findings


def _chart_states(con, findings: pd.DataFrame, pairs: pd.DataFrame) -> dict:
    """{(DeviceId, day): {signal: 96 states}} of each signal with a finding and its group, on the latest day seen."""
    if findings.empty:
        return {}
    picks = []
    for device, mine in findings.groupby('DeviceId'):
        main = mine.assign(length=mine['End'] - mine['Start']).sort_values('length', ascending=False).iloc[0]
        day = max(main['SeenDays'])
        group = group_of(pairs, device)
        picks += [(device, day, member) for member in [device] + group]
    wanted = pd.DataFrame(picks, columns=['Alert', 'Day', 'DeviceId'])
    con.register('wanted', wanted)
    states = con.execute(f"""
        SELECT w.Alert, w.Day, s.DeviceId, CAST((hour(s.ts) * 60 + minute(s.ts)) // {BIN_MINUTES} AS INTEGER) AS Bin, s.State
        FROM wanted w JOIN st s ON s.DeviceId = w.DeviceId AND CAST(s.ts AS DATE) = CAST(w.Day AS DATE)
    """).df()
    charts = {}
    for (alert, day), one in wanted.groupby(['Alert', 'Day']):
        mine = states[(states['Alert'] == alert)]
        series = {}
        for member in one['DeviceId']:
            values = np.full(BINS_PER_DAY, np.nan)
            rows = mine[mine['DeviceId'] == member]
            values[rows['Bin'].to_numpy()] = rows['State'].to_numpy(float)
            series[member] = values.tolist()
        charts[(alert, pd.Timestamp(day))] = series
    return charts


# ======================================================================================================================
# Clock check
# ======================================================================================================================

def _poisson_sf(k: int, lam: float) -> float:
    """P(X >= k) for a Poisson count with mean lam."""
    if k <= 0:
        return 1.0
    if lam <= 0:
        return 0.0
    term, total = math.exp(-lam), 0.0
    for n in range(k):
        total += term
        term *= lam / (n + 1)
    return max(0.0, 1.0 - total)


CLOCK_COLUMNS = ['DeviceId', 'OffsetSeconds', 'Since', 'Calls', 'SameTime', 'Neighbours', 'ClockChange']


def preempt_clock_offsets(calls: pd.DataFrame, signals: Optional[pd.DataFrame], report_date=None,
                          max_miles: float = CLOCK_MAX_MILES) -> dict:
    """Signals whose clock is off against their neighbours', from emergency vehicle preempt calls.

    calls: preempt calls (DeviceId, StartTime, and Class when known; bus and rail calls are left out: a train's travel
    between crossings looks like an offset of minutes). An offset c means the signal stamps events c seconds after true
    time (positive: its clock is fast). Returns a dict: 'offsets' (CLOCK_COLUMNS; ClockChange is the clock change it
    missed, -1 fall back or +1 spring forward, else 0), 'calls' (for each signal found, its calls that lined up with a
    neighbour's: DeviceId, StartTime, Minutes apart) and 'judged' (signals with CLOCK_MIN_CALLS or more calls lined up
    at the same time with calls at CLOCK_MIN_NEIGHBOURS or more neighbours: the ones the check can vouch for).
    """
    empty = {'offsets': pd.DataFrame(columns=CLOCK_COLUMNS),
             'calls': pd.DataFrame(columns=['DeviceId', 'StartTime', 'Minutes']), 'judged': pd.Series(dtype=str)}
    calls = to_pandas(calls)
    near = signal_neighbours(signals, max_miles)
    if calls is None or calls.empty or near.empty:
        return empty
    keep = ~calls['Class'].isin(list(TSP_CLASSES) + [CLASS_RAIL]) if 'Class' in calls.columns else np.ones(len(calls), bool)
    calls = pd.DataFrame({'DeviceId': calls.loc[keep, 'DeviceId'].astype(str),
                          'StartTime': pd.to_datetime(calls.loc[keep, 'StartTime'])})
    if report_date is not None:
        calls = calls[calls['StartTime'] < pd.Timestamp(report_date).normalize() + pd.Timedelta(days=1)]
    reach = ALIGN_SECONDS // 60
    con = _connect()
    try:
        con.register('calls_in', calls)
        con.register('near', near)
        con.execute(f"""
            CREATE TEMP TABLE e AS
            WITH c AS (
                SELECT DeviceId, StartTime FROM (
                    SELECT DeviceId, StartTime, lag(StartTime) OVER (PARTITION BY DeviceId ORDER BY StartTime) AS previous
                    FROM calls_in WHERE StartTime IS NOT NULL)
                WHERE previous IS NULL OR StartTime - previous > INTERVAL {CALL_GAP_SECONDS} SECOND
            )
            SELECT a.DeviceId, a.StartTime, n.NextDeviceId, epoch(a.StartTime) - epoch(b.StartTime) AS Apart
            FROM c a JOIN near n ON n.DeviceId = a.DeviceId
            JOIN c b ON b.DeviceId = n.NextDeviceId
                AND b.StartTime BETWEEN a.StartTime - INTERVAL {CLOCK_SEARCH_SECONDS} SECOND
                                    AND a.StartTime + INTERVAL {CLOCK_SEARCH_SECONDS} SECOND
        """)
        judged = con.execute(f"""
            SELECT DeviceId FROM e WHERE abs(Apart) <= {ALIGN_SECONDS}
            GROUP BY ALL HAVING count(DISTINCT StartTime) >= {CLOCK_MIN_CALLS}
                            AND count(DISTINCT NextDeviceId) >= {CLOCK_MIN_NEIGHBOURS}
        """).df()['DeviceId']
        # The best offset of each signal, in whole minutes: the most calls lined up within ALIGN_SECONDS of it.
        con.execute(f"""
            CREATE TEMP TABLE best AS
            -- The middle of the minutes that tie for the most calls: a vehicle's travel spreads the calls over a
            -- minute or two, and the edge of that spread would leave some of them outside the window.
            WITH m AS (
                SELECT DeviceId, CAST(round(Apart / 60) AS INTEGER) + o AS Minute, count(DISTINCT StartTime) AS n
                FROM e, range(-{reach}, {reach + 1}) r(o)
                WHERE abs(Apart) > {CLOCK_MIN_OFFSET_SECONDS - ALIGN_SECONDS} GROUP BY ALL
            ), top AS (
                SELECT * FROM m WHERE abs(Minute) * 60 >= {CLOCK_MIN_OFFSET_SECONDS} AND n >= {CLOCK_MIN_CALLS}
                QUALIFY n = max(n) OVER (PARTITION BY DeviceId)
            ), side AS (
                SELECT DeviceId, arg_min(sign(Minute), abs(Minute)) AS side FROM top GROUP BY ALL
            )
            SELECT DeviceId, CAST(round(median(Minute)) AS INTEGER) AS Minute
            FROM top JOIN side USING (DeviceId) WHERE sign(Minute) = side GROUP BY DeviceId
        """)
        # Each call of those signals: lined up at the same time (z), at the offset (k), at the mirror offset (m).
        con.execute(f"""
            CREATE TEMP TABLE lab AS
            SELECT e.DeviceId, e.StartTime, b.Minute,
                   bool_or(abs(Apart) <= {ALIGN_SECONDS}) AS z,
                   bool_or(abs(Apart - 60 * b.Minute) <= {ALIGN_SECONDS}) AS k,
                   bool_or(abs(Apart + 60 * b.Minute) <= {ALIGN_SECONDS}) AS m,
                   arg_min(Apart, abs(Apart - 60 * b.Minute)) FILTER (abs(Apart - 60 * b.Minute) <= {ALIGN_SECONDS}) AS ko,
                   arg_min(Apart, abs(Apart)) FILTER (abs(Apart) <= {ALIGN_SECONDS}) AS zo,
                   count(*) FILTER (abs(Apart - 60 * b.Minute) > {ALIGN_SECONDS}) AS far
            FROM e JOIN best b USING (DeviceId) GROUP BY ALL
        """)
        # Since: the call that best splits same-time calls before it from offset calls after it.
        stats = con.execute(f"""
            WITH seq AS (
                SELECT *, coalesce(sum(CAST(z AND NOT k AS INTEGER)) OVER (PARTITION BY DeviceId ORDER BY StartTime
                              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                        + sum(CAST(k AND NOT z AS INTEGER)) OVER (PARTITION BY DeviceId ORDER BY StartTime
                              ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS score,
                          sum(CAST(z AND NOT k AS INTEGER)) OVER (PARTITION BY DeviceId) AS all_same
                FROM lab WHERE z OR k
            ), split AS (
                SELECT DeviceId, StartTime AS Since FROM seq WHERE score > all_same
                QUALIFY row_number() OVER (PARTITION BY DeviceId ORDER BY score DESC, StartTime) = 1
            ), nbrs AS (
                -- The neighbours whose calls line up at the offset since then.
                SELECT e.DeviceId, count(DISTINCT e.NextDeviceId) AS Neighbours
                FROM e JOIN best b USING (DeviceId) JOIN split s USING (DeviceId)
                WHERE abs(e.Apart - 60 * b.Minute) <= {ALIGN_SECONDS} AND e.StartTime >= s.Since GROUP BY ALL
            )
            SELECT l.DeviceId, any_value(nb.Neighbours) AS Neighbours, any_value(s.Since) AS Since, any_value(l.Minute) AS Minute,
                   count(*) FILTER (l.k AND l.StartTime >= s.Since) AS Calls,
                   count(*) FILTER (l.z AND NOT l.k AND l.StartTime >= s.Since) AS SameTime,
                   count(*) FILTER (l.m AND l.StartTime >= s.Since) AS Mirror,
                   median(l.ko) FILTER (l.k AND l.StartTime >= s.Since) AS OffsetSeconds,
                   -- Chance lining up: for each call, the chance one of its neighbours' other calls within the
                   -- search (at least one, so a quiet signal is not taken as certain) falls in a given window.
                   sum(least(1.0, greatest(l.far, 1) * {2 * ALIGN_SECONDS} / {2 * CLOCK_SEARCH_SECONDS}))
                       FILTER (l.StartTime >= s.Since) AS Expected,
                   max(l.StartTime) FILTER (l.z AND NOT l.k AND l.StartTime < s.Since) AS SameBefore,
                   max(l.StartTime) FILTER (l.z AND NOT l.k) AS LastSame,
                   max(l.StartTime) FILTER (l.k) AS LastShifted
            FROM lab l JOIN split s USING (DeviceId) LEFT JOIN nbrs nb USING (DeviceId) GROUP BY l.DeviceId
        """).df()
        if stats.empty:
            return {**empty, 'judged': judged}
        # Windows searched: the chance of the best one is that many times the chance of one. An hour either way (a
        # missed clock change) is the one offset looked for in advance, so it counts as two windows.
        hour = (stats['Minute'].abs() - 60).abs() * 60 <= ALIGN_SECONDS
        windows = np.where(hour, 2, (CLOCK_SEARCH_SECONDS - CLOCK_MIN_OFFSET_SECONDS) / ALIGN_SECONDS)
        p = np.array([_poisson_sf(int(k), float(x)) for k, x in zip(stats['Calls'], stats['Expected'].fillna(0))]) * windows
        stats['P'] = p
        current = stats['LastSame'].isna() | (stats['LastShifted'] > stats['LastSame'])
        # An offset other than an hour was not looked for in advance and needs more evidence.
        stats['MinCalls'] = np.where(hour, CLOCK_MIN_CALLS, CLOCK_MIN_CALLS_OTHER)
        stats['MinNeighbours'] = np.where(hour, CLOCK_MIN_NEIGHBOURS, CLOCK_MIN_NEIGHBOURS_OTHER)
        found = stats[(stats['Calls'] >= stats['MinCalls']) & (stats['Neighbours'] >= stats['MinNeighbours'])
                      & (stats['Calls'] >= CLOCK_MIN_RATIO * stats['SameTime'])
                      & (stats['Calls'] >= CLOCK_MIN_RATIO * stats['Mirror']) & (p <= CLOCK_MAX_P_VALUE) & current
                      & (stats['OffsetSeconds'].abs() >= CLOCK_MIN_OFFSET_SECONDS)].copy()
        lined = pd.DataFrame(columns=['DeviceId', 'StartTime', 'Minutes'])
        if not found.empty:
            con.register('found', found[['DeviceId']])
            lined = con.execute("""
                SELECT DeviceId, StartTime, coalesce(CASE WHEN k THEN ko END, zo) / 60 AS Minutes
                FROM lab WHERE (z OR k) AND DeviceId IN (SELECT DeviceId FROM found) ORDER BY DeviceId, StartTime
            """).df()
    finally:
        con.close()
    found['ClockChange'] = 0
    if not found.empty:
        found['Since'] = pd.to_datetime(found['Since'])
        changes = us_clock_changes(calls['StartTime'].min(), calls['StartTime'].max())
        for idx, row in found.iterrows():
            for when, hours in changes:
                after_same = pd.isna(row['SameBefore']) or pd.Timestamp(row['SameBefore']) < when
                # A clock that did not fall back is an hour fast; one that did not spring forward is an hour slow.
                if (after_same and when <= row['Since'] and abs(abs(row['OffsetSeconds']) - 3600) <= HOUR_SLACK_SECONDS
                        and -hours * row['OffsetSeconds'] > 0):
                    found.at[idx, 'Since'], found.at[idx, 'ClockChange'] = when, hours
    judged = pd.Series(sorted(set(judged) | set(found['DeviceId'])), dtype=str)
    return {'offsets': found[CLOCK_COLUMNS].reset_index(drop=True), 'calls': lined, 'judged': judged}


# ======================================================================================================================
# Alerts
# ======================================================================================================================

NUMBER_WORDS = ['no', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten']


def signal_number(name) -> str:
    """The signal number at the front of its name ('08043-OR99 at Pioneer St' gives 08043)."""
    return str(name).split('-')[0].strip()


def _count(n: int) -> str:
    return NUMBER_WORDS[n] if 0 <= n < len(NUMBER_WORDS) else str(n)


def _clock(b: int) -> str:
    minutes = int(b) * BIN_MINUTES
    return f"{minutes // 60 % 24}:{minutes % 60:02d}"


def _span(bins: int) -> str:
    hours, rest = divmod(int(bins) * BIN_MINUTES, 60)
    if not hours:
        return f"{rest} min"
    return ('an hour' if hours == 1 else f"{hours} h") if not rest else f"{hours} h {rest} min"


def state_words(value) -> str:
    """Plain words for a state: free, flash or a cycle length."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return 'unknown'
    return 'free' if value == FREE else 'flash' if value == FLASH else f"{value:g} s"


def finding_text(row, others: str) -> str:
    """What one finding says, as the review sheet put it: its hours, what the signal ran and what its group ran."""
    s, e = int(row['Start']), int(row['End'])
    own, cons = row['Own'], row['Cons']
    if row['Edge'] == 'early':
        return (f"it changes from {state_words(cons)} to {state_words(own)} at {_clock(s)}, {_span(e - s)} before "
                f"{others} ({_clock(e)})")
    if row['Edge'] == 'late':
        return (f"it changes from {state_words(own)} to {state_words(cons)} at {_clock(e)}, {_span(e - s)} after "
                f"{others} ({_clock(s)})")
    hours = 'all day' if (s, e) == (0, BINS_PER_DAY) else f"{_clock(s)}-{_clock(e)}"
    def listed(name):
        values = row.get(name)
        return [v for v in values if v > 0] if isinstance(values, list) else []
    cycles, usual, group = listed('OwnStates'), listed('OwnTypical'), listed('ConsStates')
    if own > 0 and len(usual) >= 2:            # more than one plan in these hours
        own_text = f"runs {min(usual):g} to {max(usual):g} s"
    elif own > 0 and len(cycles) >= 3:         # no steady plan at all
        own_text = f"runs cycles from {min(cycles):g} to {max(cycles):g} s"
    else:
        own_text = 'runs free' if own == FREE else 'flashes' if own == FLASH else f"runs {state_words(own)}"
    if cons > 0 and len(group) >= 2:
        cons_text = f"run {min(group):g} to {max(group):g} s"
    else:
        cons_text = 'run free' if cons == FREE else 'flash' if cons == FLASH else f"run {state_words(cons)}"
    return f"{hours} it {own_text} while {others} {cons_text}"


def _told(findings: pd.DataFrame) -> list:
    """(label, rows): a finding on Saturdays and Sundays alike is told once for Weekends, on all three kinds of day for
    Every day."""
    told, used = [], set()
    keys = ['Start', 'End', 'Own', 'Cons', 'Edge']
    for idx, row in findings.iterrows():
        if idx in used:
            continue
        same = findings[(findings[keys] == row[keys]).all(axis=1) & ~findings.index.isin(used)]
        used.update(same.index)
        kinds = set(same['DayType'])
        label = ('Every day' if kinds == set(DAY_TYPES) else 'Weekends' if kinds == {SATURDAYS, SUNDAYS}
                 else ' and '.join(t for t in DAY_TYPES if t in kinds))
        told.append((label, same))
    return told


def plan_alert_text(findings: pd.DataFrame, group_size: int, names=None) -> str:
    others = f"the other {_count(group_size)}" if group_size > 1 else 'its neighbour'
    order = {t: n for n, t in enumerate(DAY_TYPES)}
    findings = findings.assign(_o=findings['DayType'].map(order)).sort_values(['_o', 'Start']).drop(columns='_o')
    parts = []
    for label, rows in _told(findings):
        counts = '; '.join(f"{int(r.Seen)} of the last {int(r.Judged)} {r.DayType}" for r in rows.itertuples())
        parts.append(f"{label} {finding_text(rows.iloc[0], others)} ({counts}).")
    nearby = sorted({o for row in findings['With'] for o in row}) if 'With' in findings.columns else []
    if nearby:
        numbers = sorted(signal_number(names.get(o, o)) if names is not None else str(o) for o in nearby)
        listed = ', '.join(numbers[:4]) + (f" and {len(numbers) - 4} more" if len(numbers) > 4 else '')
        parts.append(f"{listed} nearby {'runs' if len(numbers) == 1 else 'run'} the same then, so it may be set to "
                     f"run with {'it' if len(numbers) == 1 else 'them'}.")
    return ' '.join(parts)


def clock_alert_text(clock) -> str:
    """What a clock offset says, with its evidence."""
    offset = float(clock['OffsetSeconds'])
    since = pd.Timestamp(clock['Since'])
    fast = offset > 0
    amount = 'an hour' if abs(abs(offset) - 3600) <= HOUR_SLACK_SECONDS else f"{abs(offset) / 60:.0f} minutes"
    missed = int(clock.get('ClockChange', 0) or 0)
    lead = f"Its clock is {amount} {'fast' if fast else 'slow'}"
    if missed:
        lead += (f" since the clock change on {since.month}/{since.day} "
                 f"(it did not {'fall back' if missed < 0 else 'spring forward'})")
    else:
        lead += f" since {since.month}/{since.day}"
    return (lead + f": {int(clock['Calls'])} of its emergency vehicle calls since then are stamped {amount} "
            f"{'later' if fast else 'earlier'} than the same vehicles' calls at {_count(int(clock['Neighbours']))} "
            f"nearby signals. Every plan, log entry and count it records is off by that much.")


def build_coordination_alerts(signals: pd.DataFrame, report_date, coordination=None, preempt_calls=None,
                              plan_check: bool = True, clock_check: bool = True,
                              history_days: int = COORDINATION_HISTORY_DAYS, verbosity: int = 1,
                              by_position: bool = False) -> dict:
    """Alerts of the plan check (Cause 'Schedule') and the clock check (Cause 'Clock') for one report day.

    signals: DeviceId, Name, Latitude, Longitude. coordination: coordination_agg with Pattern and CycleLength over the
    history_days up to report_date. preempt_calls: the stored preempt calls (DeviceId, StartTime, Class).
    Returns a dict: 'alerts' (ALERT_COLUMNS, one row per signal and Cause), 'findings', 'pairs', 'clock' (offsets) and
    'clock_judged' (the signals the clock check could judge).
    """
    report_date = pd.Timestamp(report_date).normalize()
    signals = to_pandas(signals)
    names = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')['Name'] \
        if signals is not None and 'Name' in signals.columns else pd.Series(dtype=object)
    rows = []
    plan = {'findings': pd.DataFrame(columns=FINDING_COLUMNS), 'pairs': pd.DataFrame(), 'charts': {}}
    if plan_check and coordination is not None:
        plan = plan_findings(coordination, signals, report_date, history_days, by_position=by_position)
        findings, pairs = plan['findings'], plan['pairs']
        attached = set(pairs.loc[pairs['Kind'] != LEARNED, 'DeviceId']) if not pairs.empty else set()
        for device, mine in findings.groupby('DeviceId'):
            group = group_of(pairs, device)
            chart_key = next((key for key in plan['charts'] if key[0] == device), None)
            series = plan['charts'].get(chart_key, {})
            rows.append({
                'DeviceId': device, 'Date': report_date, 'Cause': CAUSE_SCHEDULE,
                'Text': plan_alert_text(mine, len(group), names), 'Group': group,
                'Confidence': CONFIDENCE_HIGH if (
                    device not in attached and bool(mine['Inside'].all()) and not any(len(w) for w in mine['With'])
                    and (mine['End'] - mine['Start']).sum() * BIN_MINUTES / 60 >= CONFIDENCE_MIN_HOURS) else CONFIDENCE_CHECK,
                'Runs': [{'DayType': r.DayType, 'Start': _clock(r.Start), 'End': _clock(r.End), 'Days': list(r.SeenDays)}
                         for r in mine.itertuples()],
                'Matched': [{'DayType': r.DayType, 'Start': _clock(r.Start), 'End': _clock(r.End),
                             'Days': list(r.MatchedDays)} for r in mine.itertuples()],
                'ClockOffsetSeconds': np.nan, 'ClockSince': pd.NaT,
                'ChartDay': chart_key[1] if chart_key else pd.NaT, 'ChartOwn': series.get(device, []),
                'ChartGroup': {signal_number(names.get(m, m)): v for m, v in series.items() if m != device},
                'ChartCalls': None,
            })
    clock = {'offsets': pd.DataFrame(columns=CLOCK_COLUMNS), 'calls': pd.DataFrame(), 'judged': pd.Series(dtype=str)}
    if clock_check and preempt_calls is not None and not to_pandas(preempt_calls).empty:
        clock = preempt_clock_offsets(preempt_calls, signals, report_date)
        for _, offset in clock['offsets'].iterrows():
            calls = clock['calls'][clock['calls']['DeviceId'] == offset['DeviceId']]
            rows.append({
                'DeviceId': offset['DeviceId'], 'Date': report_date, 'Cause': CAUSE_CLOCK,
                'Text': clock_alert_text(offset), 'Group': [], 'Runs': [], 'Matched': [],
                'ClockOffsetSeconds': float(offset['OffsetSeconds']), 'ClockSince': pd.Timestamp(offset['Since']),
                'ChartDay': pd.NaT, 'ChartOwn': [], 'ChartGroup': {},
                'ChartCalls': list(zip(pd.to_datetime(calls['StartTime']), calls['Minutes'].astype(float))),
                'Confidence': CONFIDENCE_HIGH,
            })
        # A signal with its clock off runs its plans at the wrong hours too: the clock alert tells it once.
        off = set(clock['offsets']['DeviceId'])
        rows = [row for row in rows if row['Cause'] == CAUSE_CLOCK or row['DeviceId'] not in off]
    log_message(f"Coordination: {len(plan['findings'])} plan findings, {len(clock['offsets'])} clock offsets.", 1, verbosity)
    return {'alerts': pd.DataFrame(rows, columns=ALERT_COLUMNS), 'findings': plan['findings'], 'pairs': plan['pairs'],
            'clock': clock['offsets'], 'clock_judged': clock['judged']}


# ======================================================================================================================
# Charts
# ======================================================================================================================

def _style(ax):
    ax.tick_params(labelsize=8)
    ax.grid(axis='y', color='#e6e6e6', lw=0.8)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)


def _plan_chart(alert, name):
    from .visualization import _legend_above, new_figure
    figure = new_figure(CHART_SIZE, dpi=110)
    ax = figure.add_subplot(111)
    x = np.arange(BINS_PER_DAY + 1) / 4
    group = alert['ChartGroup'] or {}
    for k, number in enumerate(sorted(group)):
        y = np.array(group[number], dtype=float)
        ax.step(x, np.r_[y, y[-1]], where='post', color=GREY, lw=1.6, alpha=0.8,
                label=('Group: ' + ', '.join(sorted(group))) if k == 0 else None)
    y = np.array(alert['ChartOwn'], dtype=float)
    ax.step(x, np.r_[y, y[-1]], where='post', color=STRONG, lw=2.2, label=signal_number(name))
    # The hours the alert is about, on this kind of day: other differences on the chart are ones a neighbour shares.
    day = pd.Timestamp(alert['ChartDay'])
    kind = SATURDAYS if day.dayofweek == 5 else SUNDAYS if day.dayofweek == 6 else WEEKDAYS
    shaded = False
    for run in alert['Runs'] if isinstance(alert['Runs'], (list, tuple, np.ndarray)) else []:
        if run['DayType'] != kind:
            continue
        start, end = (int(t.split(':')[0]) + int(t.split(':')[1]) / 60 for t in (run['Start'], run['End']))
        ax.axvspan(start, end if end > start else 24, color=SHADE, lw=0, zorder=0,
                   label=None if shaded else 'Hours it is out of step')
        shaded = True
    ax.set_xlim(0, 24)
    ax.set_xticks(range(0, 25, 3))
    ax.set_xticklabels([f'{h}:00' for h in range(0, 25, 3)])
    values = np.r_[y, *[np.array(v, dtype=float) for v in group.values()]] if group else y
    top = np.nanmax(values) if np.isfinite(values).any() else 100
    ax.set_ylim(-5, top * 1.12 + 5)
    ax.set_ylabel('Cycle (s), 0 = free', fontsize=8)
    _style(ax)
    ax.set_title(f"{signal_number(name)} cycle length by time of day, {day:%a} {day.month}/{day.day}", fontsize=10)
    figure.tight_layout()
    _legend_above(ax)
    figure.tight_layout()
    return figure


def _clock_chart(alert, name):
    from .visualization import _legend_above, new_figure
    figure = new_figure(CHART_SIZE, dpi=110)
    ax = figure.add_subplot(111)
    points = alert['ChartCalls'] or []
    times = [t for t, _ in points]
    minutes = [m for _, m in points]
    ax.scatter(times, minutes, s=16, color=STRONG, label=f"{signal_number(name)} emergency vehicle calls")
    ax.axhline(0, color=GREY, lw=1.2, label='Same time as at nearby signals')
    ax.set_ylabel("Minutes after nearby signals' call", fontsize=8)
    lo, hi = min(minutes + [0]), max(minutes + [0])
    ax.set_ylim(lo - 10, hi + 10)
    import matplotlib.dates as mdates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    _style(ax)
    ax.set_title(f"{signal_number(name)} call times against nearby signals", fontsize=10)
    figure.tight_layout()
    _legend_above(ax)
    figure.tight_layout()
    return figure


def in_report_order(alerts: pd.DataFrame, names) -> pd.DataFrame:
    """Alerts in the order tables and charts show them: clock alerts, then High confidence, then by signal name.
    names: the signal names by DeviceId, or one per row."""
    name = names if len(names) == len(alerts) and names.index.equals(alerts.index) \
        else alerts['DeviceId'].astype(str).map(names)
    confidence = alerts['Confidence'] if 'Confidence' in alerts.columns else CONFIDENCE_HIGH
    return alerts.assign(_clock=alerts['Cause'] != CAUSE_CLOCK, _check=confidence != CONFIDENCE_HIGH, _name=name) \
        .sort_values(['_clock', '_check', '_name']).drop(columns=['_clock', '_check', '_name'])


def listed_alerts(alerts: Optional[pd.DataFrame], signals: Optional[pd.DataFrame], per_region: int) -> pd.DataFrame:
    """The alerts a run's reports list: for each region the first per_region in report order, and only alerts with a
    chart, so every row of a table has its chart. The rest are not told in this run and must not be filed as told:
    they come up again on the next run, ahead of nothing but what is still left."""
    if alerts is None or alerts.empty or signals is None or per_region <= 0:
        return alerts.head(0) if alerts is not None else pd.DataFrame(columns=ALERT_COLUMNS)
    info = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')
    charted = alerts[[bool(own) or bool(calls) for own, calls in zip(alerts['ChartOwn'], alerts['ChartCalls'])]]
    ordered = in_report_order(charted, info['Name'])
    region = ordered['DeviceId'].astype(str).map(info['Region'])
    # RegionTotal: all the region has, so its table can say '5 of 26'.
    ordered = ordered.assign(RegionTotal=ordered.groupby(region)['DeviceId'].transform('size'))
    return ordered[region.notna() & (ordered.groupby(region).cumcount() < per_region)]


def create_coordination_plots(alerts: pd.DataFrame, signals: pd.DataFrame, num_figures: int = 10) -> list:
    """One chart per alert, as (figure, region) for its region and for All Regions. Schedule: cycle length by time of
    day on the latest day it showed, the signal in a strong colour against its group in grey. Clock: its emergency
    vehicle calls against the matching calls at nearby signals, minutes apart by date."""
    if alerts is None or alerts.empty or signals is None:
        return []
    info = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')
    drawn = {}
    for idx, alert in alerts.iterrows():
        name = info['Name'].get(str(alert['DeviceId']), alert['DeviceId'])
        if alert['Cause'] == CAUSE_CLOCK and alert['ChartCalls']:
            drawn[idx] = _clock_chart(alert, name)
        elif alert['Cause'] == CAUSE_SCHEDULE and alert['ChartOwn']:
            drawn[idx] = _plan_chart(alert, name)
    figures = []
    alerts = in_report_order(alerts, info['Name'])
    regions = alerts['DeviceId'].astype(str).map(info['Region'])
    for scope in ['All Regions'] + sorted(regions.dropna().unique().tolist()):
        chosen = alerts.index if scope == 'All Regions' else alerts.index[regions == scope]
        figures += [(drawn[idx], scope) for idx in list(chosen)[:num_figures] if idx in drawn]
    return figures


def prepare_coordination_table(alerts: Optional[pd.DataFrame], signals_df: Optional[pd.DataFrame], region=None,
                               max_rows: int = 10) -> tuple:
    """(table rows, total) of one report's coordination alerts: Signal and What Is Wrong, clock alerts first, and the
    Ongoing date for held-back repeats."""
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
    if 'RegionTotal' in rows.columns and rows['RegionTotal'].notna().all():
        total = int(rows.drop_duplicates('Region')['RegionTotal'].sum())    # with the ones waiting for a later run
    rows = in_report_order(rows, rows['Name']).head(max_rows if max_rows > 0 else len(rows))
    rows = rows.assign(Signal=rows['Name'], **{'What Is Wrong': rows['Text']})
    rows, columns = _add_ongoing_column(rows, ['Signal', 'What Is Wrong'])
    return rows[columns].reset_index(drop=True), total


def create_coordination_impact_chart(alert, group: list, mismatched: list, matched: list, measures: Optional[dict] = None):
    """The impact chart of a Schedule or Clock alert: arrivals on green and delay per vehicle by hour of day on the days
    it was out of step against the days it was not (see impact_chart, which draws it).

    alert: the alert row (DeviceId, Cause, ClockOffsetSeconds, ClockSince...). group: its group's DeviceIds (for a Clock
    alert, its neighbours). mismatched: the alert's Runs (DayType, Start, End, Days it differed; unused for a Clock
    alert, which takes the days since ClockSince). matched: the alert's Matched (days it ran with its group in those
    hours; for a Clock alert, the days before ClockSince are taken). measures: {'hourly': measure_inputs.hourly_delay
    rows of the signal and its group, faulting hours left out; 'signals': the signals table}.
    Returns a figure, or None when there is not enough data on both sides or nothing to see.
    """
    from .impact_chart import coordination_impact_chart
    return coordination_impact_chart(alert, group, mismatched, matched, measures)
