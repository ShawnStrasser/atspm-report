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
"Between" means within POSITION_MAX_OFFSET_MILES of the line from one group signal to the other; a signal between the
group and a part-time signal is measured against the group signal nearest to that one (2B080, at a ramp, sat on the
line from 2B063 to 2B060 while OR224 curved round through 2B062 and 2B061). A signal is never attached to signals of another system where the signals table names them
(SYSTEM_COLUMN), nor part time when a signal with no data is nearer than the one it would be attached to.
A group has no holes: a signal is attached only when no signal with data outside the group sits between it and the
group, and a signal between the group and a part-time signal is attached by position like one in the middle (user,
2026-11-17: 2B013 had been attached past 2B012, which ran free and was in no group).
An attached signal is judged against the signals it is attached to, but they are not judged against it: it has shown
it does not run their plans, so it neither counts towards what the group runs nor makes another signal's difference a
shared one. Letting such signals count broke the 2/3 agreement in small groups and hid findings already confirmed.

The check needs MIN_REPEAT_DAYS of each kind of day on top, so weekend findings start after three weekends.
COORDINATION_HISTORY_DAYS (six weeks) is the design; with less, groups still form but fewer kinds of day can be judged.

For each 15-minute bin a signal's neighbours agree on a state (CONSENSUS_MIN_AGREE of them and CONSENSUS_MIN_SHARE of
those reporting). The signal differs alone when its own state is another one and no neighbour shares it: a sub-system at
the edge of a group, with a neighbour running what it runs, is not alone. A neighbour that is itself out of step does
not count as sharing it (_repeat_alone). A bin is a finding for one kind of day
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
import re
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
# ChartMembers: the group shown with the signal (whole_group within CHART_GROUP_MILES). ChartWeek: {day: {DeviceId: 96
# states}} of the signal and ChartMembers over the last CHART_DAYS days. ChartNoData: signals near it with no
# coordination data (drawn hollow on its map). ChartCalls: a Clock alert's calls against its neighbours'.
ALERT_COLUMNS = ['DeviceId', 'Date', 'Cause', 'Text', 'Group', 'Runs', 'Matched', 'ClockOffsetSeconds', 'ClockSince',
                 'ChartMembers', 'ChartWeek', 'ChartNoData', 'ChartCalls', 'Confidence']
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
# ...and it is no further than this off the straight line between them (about 260 ft). The detour alone let in signals
# on other roads: 12051 on Main St beside US 395, 03108 beside OR99W, 2B048 on OR224 beside OR99E and 2B525 on Edy Rd
# were 0.09 to 0.19 mi off the line, while the signals on the group's own road measured 0.05 or less (2026-11-17).
POSITION_MAX_OFFSET_MILES = 0.05
# Optional column of the signals table: the agency's own name for the system or corridor a signal belongs to. ODOT's
# central system has one; it told OR99E from OR43 and OR224 where coordinates could not (2026-11-17).
SYSTEM_COLUMN = 'group_name'
# A signal more than this from every other signal of a group is not in it (user, 2026-11-17: signals over half a mile
# apart were drawn as one group). Groups are chains of signals that run together, each link no longer than this; a
# signal is judged against the signals of its own chain within NEIGHBOUR_MAX_MILES.
GROUP_MAX_GAP_MILES = 0.5
# Optional column of the signals table: the coordinated group (corridor) each signal belongs to, kept by the agency. Where
# a signal has one, it is used as given and nothing is learned for it: signals with the same value are a group, and a
# value no other signal has means the signal runs on its own (never judged). Signals with no value (missing or blank)
# are grouped from what they run, as above, and only with each other.
CORRIDOR_COLUMN = 'corridor'
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
# Rounds of taking a signal found out of step off the list of those that excuse their neighbours (ODOT needed three).
EXCUSE_ROUNDS = 6
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

# ---- Group charts and maps --------------------------------------------------------------------------------------------
# A group's alerts are shown together: one chart of cycle length by time of day for each recent day that shows them
# differing from their group, and one map of the signals around it (user, 2026-11-17).
CHART_DAYS = 7               # the days looked at: the last week
SAME_PLAN_BINS = 3           # two days' lines are the same when they differ in no more than 45 minutes...
ENOUGH_BINS = 48             # ...judged on at least half a day both have data for
CHART_GROUP_MILES = 2.5      # how far along its group a chart and map show
DAY_CHART_SIZE = (5.0, 3.4)  # drawn two to a row in the PDF
# The group's parts by what they ran on the chart days: black for a group that ran as one, else blue, green, brown,
# olive (not grey: grey is the signals outside the group).
ONE_GROUP = '#111111'
PART_COLOURS = ['#1f5fae', '#2a8f5a', '#8a5a2b', '#9a8f1f']
# The signals drawn on their own: the alerted ones, then group signals that ran with no other signal.
OWN_COLOURS = ['#c4302b', '#8e24aa', '#e07b00', '#00838f', '#d81b60', '#6d4c41', '#7cb342', '#283593', '#c0a000',
               '#00695c']
# Lines are drawn widest first and thinner over them, so signals running the same plan all show.
OWN_WIDTHS = [6.5, 5.2, 4.2, 3.4, 2.9, 2.6, 2.4, 2.2, 2.1, 2.0]
PART_WIDTHS = [1.8, 1.2, 0.9, 0.7]
# The base map: a plain light-grey road map (no key needed). Set MAP_TILES to None to draw maps without it. A tile
# request that fails, or takes longer than MAP_TILE_SECONDS, turns the base map off for the rest of the run; the maps
# are then drawn on a plain background.
MAP_TILES = 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}'
MAP_CREDIT = 'Base map: Esri, HERE, Garmin, (c) OpenStreetMap contributors'
MAP_TILE_SECONDS = 10
MAP_MAX_ZOOM = 16
MAP_PX = (840, 552)
MAP_MARGIN_MILES = 0.6


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


def _in_line(a: str, b: str, base: str) -> str:
    """SQL: a point lies between the two ends of a line of length base, at distances a and b from them: going by way
    of it adds little (POSITION_MAX_DETOUR) and it is not far off the line (POSITION_MAX_OFFSET_MILES, by Heron's
    formula for the height of the triangle)."""
    half = f"(({a} + {b} + {base}) / 2)"
    height = (f"2 * sqrt(greatest(0, {half} * ({half} - {a}) * ({half} - {b}) * ({half} - {base}))) / "
              f"greatest({base}, 1e-9)")
    return f"({a} + {b} <= {POSITION_MAX_DETOUR} * {base} AND {height} <= {POSITION_MAX_OFFSET_MILES})"


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
    # Signals the signals table puts in a corridor (CORRIDOR_COLUMN) are grouped as it says, further down; nothing below
    # learns or attaches anything for them.
    corridors = _corridors(signals)
    con.register('corridors', corridors)
    con.execute(f"""CREATE TEMP TABLE pairs AS SELECT * FROM matches WHERE Matched >= {NEIGHBOUR_MIN_MATCH} * Bins
                    AND DeviceId NOT IN (SELECT DeviceId FROM corridors)
                    AND NextDeviceId NOT IN (SELECT DeviceId FROM corridors)""")
    # Signals that run together, whatever the chains: a signal between two of them is in the middle of a group even
    # where they are in two chains (2B418, free 0.24 mi from 2B414 and 0.51 mi from 2B419, 0.75 mi apart).
    con.execute("""CREATE TEMP TABLE together AS SELECT DeviceId, NextDeviceId FROM pairs
                   UNION ALL SELECT NextDeviceId, DeviceId FROM pairs""")
    # Groups are chains of links no longer than GROUP_MAX_GAP_MILES: a pair further apart is kept only inside one chain.
    # A pair further apart is a link too when a signal with data sits in line between them within that distance of
    # both: a signal in the middle of a group is part of it, whatever it runs.
    chain = _chains(con.execute(f"""
        SELECT p.DeviceId, p.NextDeviceId, p.Miles <= {GROUP_MAX_GAP_MILES} OR EXISTS (
            SELECT 1 FROM near sa JOIN near sb ON sb.DeviceId = sa.DeviceId AND sb.NextDeviceId = p.NextDeviceId
            WHERE sa.NextDeviceId = p.DeviceId AND sa.Miles <= {GROUP_MAX_GAP_MILES} AND sb.Miles <= {GROUP_MAX_GAP_MILES}
              AND sa.DeviceId IN (SELECT DeviceId FROM st GROUP BY ALL HAVING count(*) >= {COORDINATED_MIN_BINS})
              AND {_in_line('sa.Miles', 'sb.Miles', 'p.Miles')}) AS Link
        FROM pairs p""").df())
    con.register('chain', pd.DataFrame({'DeviceId': list(chain), 'Chain': list(chain.values())}, columns=['DeviceId', 'Chain']))
    con.execute("""DELETE FROM pairs p WHERE NOT EXISTS (
                       SELECT 1 FROM chain a JOIN chain b ON a.Chain = b.Chain
                       WHERE a.DeviceId = p.DeviceId AND b.DeviceId = p.NextDeviceId)""")
    con.execute("""CREATE TEMP TABLE lnb AS SELECT DeviceId, NextDeviceId FROM pairs
                   UNION ALL SELECT NextDeviceId, DeviceId FROM pairs""")
    # Signals with data that what they run left without a group, and what they are attached to: part time to a close
    # signal A; by position between two group signals; or to B past A on the line when attached to A part time.
    con.execute(f"""
        CREATE TEMP TABLE lone AS
        SELECT DeviceId FROM st GROUP BY ALL HAVING count(*) >= {COORDINATED_MIN_BINS}
        EXCEPT SELECT DeviceId FROM lnb GROUP BY ALL HAVING count(*) >= {CONSENSUS_MIN_AGREE}
        EXCEPT SELECT DeviceId FROM corridors
    """)
    con.execute(f"CREATE TEMP TABLE has_data AS SELECT DeviceId FROM st GROUP BY ALL HAVING count(*) >= {COORDINATED_MIN_BINS}")
    # The agency's own name for each signal's system, where the signals table has one (SYSTEM_COLUMN): a signal is only
    # attached to signals of its own system. 'other' lists the pairs that are named and named differently.
    named = signals is not None and SYSTEM_COLUMN in signals.columns
    systems = signals.assign(DeviceId=signals['DeviceId'].astype(str))[['DeviceId', SYSTEM_COLUMN]].dropna() if named \
        else pd.DataFrame({'DeviceId': pd.Series(dtype=str), SYSTEM_COLUMN: pd.Series(dtype=str)})
    con.register('systems', systems.rename(columns={SYSTEM_COLUMN: 'SystemName'}).drop_duplicates('DeviceId'))
    con.execute("""
        CREATE TEMP TABLE other AS
        SELECT n.DeviceId, n.NextDeviceId FROM near n JOIN systems x ON x.DeviceId = n.DeviceId
        JOIN systems y ON y.DeviceId = n.NextDeviceId WHERE trim(x.SystemName) <> trim(y.SystemName)
    """)
    con.execute(f"""
        CREATE TEMP TABLE part AS
        WITH ways AS (SELECT DeviceId, NextDeviceId, Miles, Fewer, Matched FROM matches
                      UNION ALL SELECT NextDeviceId, DeviceId, Miles, Fewer, Matched FROM matches)
        SELECT DeviceId, NextDeviceId FROM ways w
        WHERE DeviceId IN (SELECT DeviceId FROM lone) AND Miles <= {PART_TIME_MAX_MILES}
          AND NextDeviceId NOT IN (SELECT DeviceId FROM corridors)
          AND Fewer >= {NEIGHBOUR_MIN_COORDINATED} AND Matched >= {NEIGHBOUR_MIN_MATCH} * Fewer
          AND (DeviceId, NextDeviceId) NOT IN (SELECT (DeviceId, NextDeviceId) FROM other)
          -- not when a signal with no data is nearer: it may be the one this signal really runs with
          AND NOT EXISTS (SELECT 1 FROM near q WHERE q.DeviceId = w.DeviceId AND q.Miles < w.Miles
                          AND q.NextDeviceId NOT IN (SELECT DeviceId FROM has_data))
        EXCEPT SELECT DeviceId, NextDeviceId FROM lnb
    """)
    # Triangles of a lone signal S and two signals A and B that go together (a learned pair): the three distances.
    con.execute("""
        CREATE TEMP TABLE tri AS
        SELECT sa.DeviceId AS S, p.DeviceId AS A, p.NextDeviceId AS B, sa.Miles AS SA, sb.Miles AS SB, ab.Miles AS AB
        FROM together p JOIN near ab ON ab.DeviceId = p.DeviceId AND ab.NextDeviceId = p.NextDeviceId
        JOIN near sa ON sa.NextDeviceId = p.DeviceId
        JOIN near sb ON sb.DeviceId = sa.DeviceId AND sb.NextDeviceId = p.NextDeviceId
        WHERE sa.DeviceId IN (SELECT DeviceId FROM lone)
          AND (sa.DeviceId, p.DeviceId) NOT IN (SELECT (DeviceId, NextDeviceId) FROM other)
          AND (sa.DeviceId, p.NextDeviceId) NOT IN (SELECT (DeviceId, NextDeviceId) FROM other)
    """)
    # Past the end: S runs with A part time (or is its one learned neighbour) and B is next along the line beyond A.
    con.execute(f"""
        CREATE TEMP TABLE ext AS
        SELECT DISTINCT t.S AS DeviceId, t.B AS NextDeviceId FROM tri t
        JOIN (SELECT * FROM part UNION ALL SELECT * FROM lnb) l ON l.DeviceId = t.S AND l.NextDeviceId = t.A
        WHERE t.SA + t.AB <= {POSITION_MAX_DETOUR} * t.SB       -- it runs with A, so the road may bend: no offset limit
        EXCEPT (SELECT DeviceId, NextDeviceId FROM lnb UNION ALL SELECT DeviceId, NextDeviceId FROM part)
    """)
    # In the middle (its own switch, see by_position): S between two group signals...
    con.execute(f"""
        CREATE TEMP TABLE pos AS
        SELECT DISTINCT S AS DeviceId, unnest([A, B]) AS NextDeviceId FROM tri
        WHERE {'true' if by_position else 'false'} AND {_in_line('SA', 'SB', 'AB')}
          AND least(SA, SB) <= {GROUP_MAX_GAP_MILES}
        EXCEPT (SELECT DeviceId, NextDeviceId FROM lnb UNION ALL SELECT DeviceId, NextDeviceId FROM part
                UNION ALL SELECT DeviceId, NextDeviceId FROM ext)
    """)
    # ...or between a signal T attached part time and the group signal nearest to T: judged against all T is.
    con.execute(f"""
        INSERT INTO pos
        WITH attached AS (SELECT e.DeviceId AS T, e.NextDeviceId AS B, n.Miles AS TB
                          FROM (SELECT * FROM part UNION ALL SELECT * FROM ext) e JOIN near n USING (DeviceId, NextDeviceId)),
        first AS (SELECT T, arg_min(B, TB) AS B, min(TB) AS TB FROM attached GROUP BY ALL)
        SELECT DISTINCT sx.DeviceId, a.B
        FROM first f JOIN near sx ON sx.NextDeviceId = f.T
        JOIN near sb ON sb.DeviceId = sx.DeviceId AND sb.NextDeviceId = f.B
        JOIN attached a ON a.T = f.T
        WHERE {'true' if by_position else 'false'} AND sx.DeviceId IN (SELECT DeviceId FROM lone)
          AND {_in_line('sx.Miles', 'sb.Miles', 'f.TB')} AND least(sx.Miles, sb.Miles) <= {GROUP_MAX_GAP_MILES}
          AND (sx.DeviceId, a.B) NOT IN (SELECT (DeviceId, NextDeviceId) FROM other)
          AND (sx.DeviceId, a.B) NOT IN (
              SELECT (DeviceId, NextDeviceId) FROM lnb UNION ALL SELECT (DeviceId, NextDeviceId) FROM part
              UNION ALL SELECT (DeviceId, NextDeviceId) FROM ext UNION ALL SELECT (DeviceId, NextDeviceId) FROM pos)
    """)
    con.execute(f"""CREATE TEMP TABLE one_way AS
                    SELECT *, '{PART_TIME}' AS Kind FROM part UNION ALL SELECT *, '{PART_TIME}' FROM ext
                    UNION ALL SELECT *, '{BY_POSITION}' FROM pos""")
    # A group has no holes. An attached signal S is kept only when no signal with data outside its group sits between
    # it and the nearest signal it is attached to (2B013 was attached past 2B012, which ran free and was in no group;
    # user, 2026-11-17). Its group: what it is attached to, their learned neighbours, and the other signals attached to
    # any of those. Dropping one can leave a hole behind another, so this repeats until nothing more goes.
    for _ in range(5):
        dropped = con.execute(f"""
            WITH nearest AS (
                SELECT o.DeviceId AS S, arg_min(o.NextDeviceId, n.Miles) AS A, min(n.Miles) AS SA
                FROM one_way o JOIN near n USING (DeviceId, NextDeviceId) GROUP BY ALL
            ), grp AS (
                SELECT DeviceId AS S, NextDeviceId AS Member FROM one_way
                UNION SELECT o.DeviceId, l.NextDeviceId FROM one_way o JOIN lnb l ON l.DeviceId = o.NextDeviceId
                UNION SELECT o.DeviceId, w.DeviceId FROM one_way o JOIN one_way w ON w.NextDeviceId = o.NextDeviceId
            ), holes AS (
                SELECT DISTINCT q.S
                FROM nearest q JOIN near sx ON sx.DeviceId = q.S
                JOIN near xa ON xa.DeviceId = sx.NextDeviceId AND xa.NextDeviceId = q.A
                WHERE sx.NextDeviceId IN (SELECT DeviceId FROM has_data) AND {_in_line('sx.Miles', 'xa.Miles', 'q.SA')}
                  AND (q.S, sx.NextDeviceId) NOT IN (SELECT (S, Member) FROM grp)
            )
            DELETE FROM one_way WHERE DeviceId IN (SELECT S FROM holes) RETURNING DeviceId
        """).fetchall()
        if not dropped:
            break
    # The corridors as given: signals with data and the same corridor, each judged against the others within
    # NEIGHBOUR_MAX_MILES (all of them where coordinates are missing).
    located = not near.empty
    con.execute(f"""
        CREATE TEMP TABLE given AS
        SELECT a.DeviceId, b.DeviceId AS NextDeviceId, n.Miles
        FROM corridors a JOIN corridors b ON a.Corridor = b.Corridor AND a.DeviceId < b.DeviceId
        LEFT JOIN near n ON n.DeviceId = a.DeviceId AND n.NextDeviceId = b.DeviceId
        WHERE a.DeviceId IN (SELECT DeviceId FROM has_data) AND b.DeviceId IN (SELECT DeviceId FROM has_data)
          AND (n.Miles IS NOT NULL OR NOT {'true' if located else 'false'})
    """)
    con.execute("""CREATE TEMP TABLE nb AS SELECT * FROM lnb UNION ALL SELECT DeviceId, NextDeviceId FROM one_way
                   UNION ALL SELECT DeviceId, NextDeviceId FROM given UNION ALL SELECT NextDeviceId, DeviceId FROM given""")
    return con.execute(f"""
        SELECT DeviceId, NextDeviceId, Miles, Bins, Matched / Bins AS Match, '{LEARNED}' AS Kind FROM pairs
        UNION ALL
        SELECT g.DeviceId, g.NextDeviceId, g.Miles, m.Bins, m.Matched / m.Bins, '{LEARNED}'
        FROM given g LEFT JOIN matches m USING (DeviceId, NextDeviceId)
        UNION ALL
        SELECT o.DeviceId, o.NextDeviceId, n.Miles, NULL, NULL, o.Kind
        FROM one_way o JOIN near n USING (DeviceId, NextDeviceId)
    """).df()


def _corridors(signals) -> pd.DataFrame:
    """(DeviceId, Corridor) of the signals the signals table puts in a corridor (CORRIDOR_COLUMN, blanks left out)."""
    if signals is None or CORRIDOR_COLUMN not in signals.columns:
        return pd.DataFrame({'DeviceId': pd.Series(dtype=str), 'Corridor': pd.Series(dtype=str)})
    given = pd.DataFrame({'DeviceId': signals['DeviceId'].astype(str),
                          'Corridor': signals[CORRIDOR_COLUMN].astype('string').str.strip()})
    return given[given['Corridor'].notna() & (given['Corridor'] != '')].drop_duplicates('DeviceId').astype(str) \
        .reset_index(drop=True)


def _chains(pairs: pd.DataFrame) -> dict:
    """{DeviceId: chain}: the signals of the pairs, linked through any number of pairs with Link true."""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for a, b, link in pairs[['DeviceId', 'NextDeviceId', 'Link']].itertuples(index=False):
        find(a), find(b)
        if link:
            parent[find(b)] = find(a)
    return {x: find(x) for x in parent}


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
        SELECT s.DeviceId, s.ts, CAST(s.ts AS DATE) AS Day,
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


def _repeat_alone(con) -> pd.DataFrame:
    """_repeat_bins, where a neighbour that is itself out of step does not excuse a signal running what it runs.

    A signal is not alone in a bin where a neighbour shares what it runs. But two signals side by side can both be
    set wrong and cover for each other: 2B050 ran free on weekend mornings and evenings beside 2B052, free all
    weekend, while the six signals around them ran their plan (user, 2026-11-17: both are to be told). So a signal
    found out of step stops counting as cover for its neighbours, and the search runs again until no more turn up.
    Signals that only ever differ together stay untold: they may be a group of their own."""
    repeat = _repeat_bins(con)
    if repeat.empty:
        return repeat
    # who shares what a signal runs, in the bins it differs from its group without being alone
    con.execute("""
        CREATE TEMP TABLE sharers AS
        SELECT j.DeviceId, j.ts, p.NextDeviceId AS Sharer
        FROM j JOIN nb p ON p.DeviceId = j.DeviceId
        JOIN st s ON s.DeviceId = p.NextDeviceId AND s.ts = j.ts AND s.CycleKey = j.CycleKey
        WHERE j.CycleKey <> j.Cons AND NOT j.Alone
    """)
    told = set(repeat['DeviceId'])
    for _ in range(EXCUSE_ROUNDS):
        con.register('out_of_step', pd.DataFrame({'DeviceId': sorted(told)}))
        con.execute("""
            UPDATE j SET Alone = true
            WHERE CycleKey <> Cons AND NOT Alone AND NOT EXISTS (
                SELECT 1 FROM sharers x WHERE x.DeviceId = j.DeviceId AND x.ts = j.ts
                AND x.Sharer NOT IN (SELECT DeviceId FROM out_of_step))
        """)
        repeat = _repeat_bins(con)
        more = set(repeat['DeviceId']) - told
        if not more:
            break
        told |= more
    return repeat


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
    'pairs' (PAIR_COLUMNS: the neighbour pairs learned and the signals attached to a group one way), 'charts'
    ({DeviceId: {'members': its whole group (whole_group, within CHART_GROUP_MILES), 'week': {day: {DeviceId: 96
    states}}}} of each signal with a finding, its group included, over the last CHART_DAYS days) and 'with_data' (the
    signals with enough coordination data to be judged). by_position also attaches signals that sit between two
    signals of a group while running none of its plans.
    """
    empty = {'findings': pd.DataFrame(columns=FINDING_COLUMNS + ['Inside', 'With']),
             'pairs': pd.DataFrame(columns=PAIR_COLUMNS), 'charts': {}, 'with_data': set()}
    coordination = to_pandas(coordination)
    if coordination is None or coordination.empty or not {'Pattern', 'CycleLength'} <= set(coordination.columns):
        return empty
    end = pd.Timestamp(report_date).normalize() + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=history_days)
    con = _connect()
    try:
        _load_states(con, coordination, start, end)
        pairs = _learn_neighbours(con, signals, max_miles, by_position)
        with_data = set(con.execute("SELECT DeviceId FROM has_data").df()['DeviceId'])
        if pairs.empty:
            return {**empty, 'pairs': pairs, 'with_data': with_data}
        _judge(con)
        repeat = _repeat_alone(con)
        if repeat.empty:
            return {**empty, 'pairs': pairs, 'with_data': with_data}
        con.register('cand', repeat[['DeviceId']].drop_duplicates())
        rows = con.execute("SELECT * FROM j WHERE DeviceId IN (SELECT DeviceId FROM cand)").df()
        chosen = con.execute("SELECT * FROM chosen WHERE DeviceId IN (SELECT DeviceId FROM cand)").df()
        for frame in (rows, chosen):
            frame['Day'] = pd.to_datetime(frame['Day'])
        findings = _position_facts(con, _describe(rows, chosen, repeat), pairs, end)
        charts = _week_states(con, sorted(findings['DeviceId'].unique()), pairs, signals, end)
    finally:
        con.close()
    return {'findings': findings, 'pairs': pairs, 'charts': charts, 'with_data': with_data}


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
    # for With, the signals attached to its group are in the group too, not nearby signals outside it
    inside_group = {device: set(group) | set(attached_to(pairs, device)) for device, group in groups.items()}
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
        findings.at[row, 'With'] = sorted(o for o in others if o not in inside_group[device])
    return findings


def whole_group(pairs: pd.DataFrame, device) -> list:
    """The signals shown with a signal on its charts and map: its chain (the learned pairs it is linked into, through
    any number of them; for an attached signal, the chain of the signals it is attached to) and every signal attached
    to the chain. Wider than group_of, which is only the signals it is judged against."""
    if pairs is None or pairs.empty:
        return []
    learned = pairs[pairs['Kind'] == LEARNED]
    links = {}
    for a, b in learned[['DeviceId', 'NextDeviceId']].itertuples(index=False):
        links.setdefault(a, set()).add(b)
        links.setdefault(b, set()).add(a)
    seen, todo = set(), [device] + group_of(pairs, device)
    while todo:
        signal = todo.pop()
        if signal not in seen:
            seen.add(signal)
            todo.extend(links.get(signal, ()))
    attached = pairs[(pairs['Kind'] != LEARNED) & pairs['NextDeviceId'].isin(seen)]
    return sorted((seen | set(attached['DeviceId'])) - {device})


def _coordinates(signals) -> Optional[pd.DataFrame]:
    """Latitude and Longitude in degrees by DeviceId (signals with both), or None when the signals table has none."""
    from .preempt_cascade import _coordinate_columns
    if signals is None or signals.empty:
        return None
    lat_col, lon_col = _coordinate_columns(signals)
    if lat_col is None or lon_col is None:
        return None
    where = pd.DataFrame({'DeviceId': signals['DeviceId'].astype(str),
                          'Latitude': pd.to_numeric(signals[lat_col], errors='coerce'),
                          'Longitude': pd.to_numeric(signals[lon_col], errors='coerce')}).dropna()
    where = where[(where['Latitude'] != 0) & (where['Longitude'] != 0)].drop_duplicates('DeviceId').set_index('DeviceId')
    return where if not where.empty else None


def _miles_from(where: pd.DataFrame, device) -> pd.Series:
    """Miles from one signal to every signal in where (_coordinates)."""
    lat, lon = np.radians(where['Latitude'].to_numpy()), np.radians(where['Longitude'].to_numpy())
    la, lo = np.radians(where.at[device, 'Latitude']), np.radians(where.at[device, 'Longitude'])
    h = np.sin((lat - la) / 2) ** 2 + np.cos(la) * np.cos(lat) * np.sin((lon - lo) / 2) ** 2
    return pd.Series(3958.8 * 2 * np.arcsin(np.sqrt(np.clip(h, 0, 1))), index=where.index)


def _shown_group(pairs, device, where) -> list:
    """whole_group within CHART_GROUP_MILES of the signal, where coordinates allow (all of it where they do not)."""
    members = whole_group(pairs, device)
    if where is None or device not in where.index:
        return members
    miles = _miles_from(where, device)
    return [m for m in members if miles.get(m, 0.0) <= CHART_GROUP_MILES]


def _week_states(con, devices: list, pairs: pd.DataFrame, signals, end) -> dict:
    """{DeviceId: {'members': _shown_group, 'week': {day: {DeviceId: 96 states}}}} of each signal and its group over
    the CHART_DAYS days up to end; bins with no trusted state are NaN, and a signal with none on a day is left out."""
    if not devices:
        return {}
    where = _coordinates(signals)
    charts = {device: {'members': _shown_group(pairs, device, where), 'week': {}} for device in devices}
    wanted = pd.DataFrame([(device, member) for device in devices for member in [device] + charts[device]['members']],
                          columns=['Alert', 'DeviceId'])
    con.register('wanted', wanted)
    states = con.execute(f"""
        SELECT w.Alert, CAST(s.ts AS DATE) AS Day, s.DeviceId,
               CAST((hour(s.ts) * 60 + minute(s.ts)) // {BIN_MINUTES} AS INTEGER) AS Bin, s.State
        FROM wanted w JOIN st s ON s.DeviceId = w.DeviceId
        WHERE s.ts >= $first AND s.ts < $end
    """, {'first': (end - pd.Timedelta(days=CHART_DAYS)).to_pydatetime(), 'end': end.to_pydatetime()}).df()
    for (alert, day, member), rows in states.groupby(['Alert', 'Day', 'DeviceId']):
        values = np.full(BINS_PER_DAY, np.nan)
        values[rows['Bin'].to_numpy()] = rows['State'].to_numpy(float)
        charts[alert]['week'].setdefault(pd.Timestamp(day), {})[member] = values.tolist()
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
    plan = {'findings': pd.DataFrame(columns=FINDING_COLUMNS), 'pairs': pd.DataFrame(), 'charts': {}, 'with_data': set()}
    where = _coordinates(signals)

    def no_data_near(device):
        """Signals within reach of the signal's map that have no coordination data (unknown until the plan check ran)."""
        if where is None or device not in where.index or not plan['with_data']:
            return []
        miles = _miles_from(where, device)
        return sorted(set(miles.index[miles <= CHART_GROUP_MILES + MAP_MARGIN_MILES]) - plan['with_data'] - {device})
    if plan_check and coordination is not None:
        plan = plan_findings(coordination, signals, report_date, history_days, by_position=by_position)
        findings, pairs = plan['findings'], plan['pairs']
        attached = set(pairs.loc[pairs['Kind'] != LEARNED, 'DeviceId']) if not pairs.empty else set()
        for device, mine in findings.groupby('DeviceId'):
            group = group_of(pairs, device)
            chart = plan['charts'].get(device, {'members': [], 'week': {}})
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
                'ChartMembers': chart['members'], 'ChartWeek': chart['week'], 'ChartNoData': no_data_near(device),
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
                'ChartMembers': _shown_group(plan['pairs'], offset['DeviceId'], where), 'ChartWeek': {},
                'ChartNoData': no_data_near(offset['DeviceId']),
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
    # With no table beside it, the title says what was found: the alert's first clause ("clock is an hour fast since
    # the clock change on 11/1").
    said = re.sub(r' \([^)]*\)', '', str(alert['Text']).split(':')[0])
    ax.set_title(f"{signal_number(name)} {said[4:] if said.startswith('Its ') else said}", fontsize=10)
    figure.tight_layout()
    _legend_above(ax)
    figure.tight_layout()
    return figure


# ---- Group rows -------------------------------------------------------------------------------------------------------

def _series(week: dict, day, device) -> np.ndarray:
    """A signal's 96 states on a day of a ChartWeek (all NaN without any)."""
    values = (week.get(day) or {}).get(device)
    return np.array(values, dtype=float) if values is not None else np.full(BINS_PER_DAY, np.nan)


def _same_lines(x: np.ndarray, y: np.ndarray):
    """True when two day-long lines are the same (SAME_PLAN_BINS), False when they differ, None with too little data
    on both to tell."""
    both = ~np.isnan(x) & ~np.isnan(y)
    if both.sum() < ENOUGH_BINS:
        return None
    return bool((x[both] != y[both]).sum() <= SAME_PLAN_BINS)


def _alike(verdicts) -> bool:
    """Some line the same and none different."""
    verdicts = list(verdicts)
    return any(v for v in verdicts) and not any(v is False for v in verdicts)


def _chart_days(week: dict, devices: list) -> list:
    """The days a group gets a chart for, the most recent first: going back through the week, a day is kept unless
    every signal ran on it what it ran on a day already kept (Monday is not drawn again when Tuesday shows the same
    lines). A day with data for under two thirds of the signals is skipped. User, 2026-11-17."""
    kept = []
    for day in sorted(week, reverse=True):
        lines = {d: _series(week, day, d) for d in devices}
        if sum((~np.isnan(v)).sum() >= ENOUGH_BINS for v in lines.values()) < 2 / 3 * len(devices):
            continue
        if not any(_alike(_same_lines(lines[d], _series(week, other, d)) for d in devices) for other in kept):
            kept.append(day)
        if len(kept) == CHART_DAYS:
            break
    return kept


def _parts(members: list, week: dict, days: list, miles, alerted: list) -> tuple:
    """(parts, own): the group's signals other than the alerted ones, by what they ran on the chart days.

    Two signals run together when their lines are the same on every chart day both have data for. Signals that run
    together are a part (Group A, Group B...), the biggest first and of equals the one nearer the alerted signals; a
    part is never one signal. own: the signals that ran with no other signal, each drawn as a line of its own like an
    alerted one. A signal with no data on any chart day goes with the part of the signal nearest to it. With no two
    signals running together they are all one part."""
    parent = {m: m for m in members}

    def find(m):
        while parent[m] != m:
            parent[m] = parent[parent[m]]
            m = parent[m]
        return m
    for i, a in enumerate(members):
        for b in members[i + 1:]:
            if _alike(_same_lines(_series(week, day, a), _series(week, day, b)) for day in days):
                parent[find(b)] = find(a)
    clusters = {}
    for m in members:
        clusters.setdefault(find(m), []).append(m)
    big = sorted((c for c in clusters.values() if len(c) >= 2),
                 key=lambda c: (-len(c), min((miles(a, m) for a in alerted for m in c), default=0.0), c[0]))
    if not big:
        return [list(members)], []
    own = []
    for single in sorted(c[0] for c in clusters.values() if len(c) == 1):
        if any((~np.isnan(_series(week, day, single))).sum() >= ENOUGH_BINS for day in days):
            own.append(single)
        else:
            nearest = min((m for c in big for m in c), key=lambda m: miles(single, m))
            next(c for c in big if nearest in c).append(single)
    if len(big) > len(PART_COLOURS):
        big = big[:len(PART_COLOURS) - 1] + [[m for c in big[len(PART_COLOURS) - 1:] for m in c]]
    return big, own


def _shows_difference(week: dict, day, drawn: list, parts: list, home) -> bool:
    """A day is worth a chart only when a signal drawn on its own ran something else than its part of the group (what
    most of the part ran) for 30 minutes or more (user, 2026-11-17: a chart with every line together shows nothing)."""
    for d in drawn:
        part = [m for m in parts[home(d)] if np.isfinite(_series(week, day, m)).any()]
        if not part:
            continue
        mine = _series(week, day, d)
        lines = np.array([_series(week, day, m) for m in part])
        line = np.array([_mode(lines[:, b]) for b in range(BINS_PER_DAY)])
        both = np.isfinite(mine) & np.isfinite(line)
        if both.sum() >= ENOUGH_BINS and _runs(both & (mine != line)):
            return True
    return False


def group_rows(alerts: Optional[pd.DataFrame], signals: Optional[pd.DataFrame] = None) -> list:
    """The alerts as the report shows them: a clock alert alone, plan alerts one row per group (two alerted signals
    share a row when one is in the other's ChartMembers), most important first (clock, then any High, then by signal
    number). Each row: 'alerts' (index labels), 'devices' (the alerted signals by number), 'clock', 'members' (the rest
    of the group), 'week', 'days' (its chart days, most recent first), 'parts' and 'own' (_parts), 'high'. A plan
    row with no day showing a difference is left out: it would show nothing."""
    if alerts is None or alerts.empty:
        return []
    where = _coordinates(signals)
    names = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')['Name'] \
        if signals is not None and 'Name' in signals.columns else pd.Series(dtype=object)

    def number(device):
        return signal_number(names.get(device, device))

    def miles(a, b):
        if where is None or a not in where.index or b not in where.index:
            return 0.0
        return float(_miles_from(where.loc[[a, b]], a)[b])
    rows = []
    for idx, alert in alerts[alerts['Cause'] == CAUSE_CLOCK].iterrows():
        members = sorted(alert['ChartMembers'] or [], key=number)
        rows.append({'alerts': [idx], 'devices': [alert['DeviceId']], 'clock': True, 'members': members, 'week': {},
                     'days': [], 'parts': [members], 'own': [], 'high': True})
    plan = alerts[alerts['Cause'] == CAUSE_SCHEDULE]
    group = {alert['DeviceId']: set(alert['ChartMembers'] or []) for _, alert in plan.iterrows()}
    parent = {d: d for d in group}

    def find(d):
        while parent[d] != d:
            parent[d] = parent[parent[d]]
            d = parent[d]
        return d
    for a in group:
        for b in group:
            if a < b and (a in group[b] or b in group[a]):
                parent[find(b)] = find(a)
    for lead in sorted({find(d) for d in group}):
        mine = plan[plan['DeviceId'].map(find) == lead]
        devices = sorted(mine['DeviceId'], key=number)
        members = sorted(set().union(*(group[d] for d in devices)) - set(devices), key=number)
        week = {}
        for chart in mine['ChartWeek']:
            for day, lines in (chart or {}).items():
                week.setdefault(pd.Timestamp(day), {}).update(lines)
        days = _chart_days(week, devices + members)
        # the parts come from the chart days, and a day stays only while it shows a difference from them: settle both
        while True:
            parts, own = _parts(members, week, days, miles, devices) if members else ([[]], [])
            spread = [m for part in parts for m in part]

            def home(d, parts=parts, spread=spread):
                nearest = min(spread, key=lambda m: miles(d, m))
                return next(k for k, part in enumerate(parts) if nearest in part)
            showing = [day for day in days if spread and _shows_difference(week, day, devices + own, parts, home)]
            if showing == days:
                break
            days = showing
        if days:
            rows.append({'alerts': list(mine.index), 'devices': devices, 'clock': False, 'members': members,
                         'week': week, 'days': days, 'parts': parts, 'own': own,
                         'high': bool((mine['Confidence'] == CONFIDENCE_HIGH).any())})
    return sorted(rows, key=lambda row: (not row['clock'], not row['high'], number(row['devices'][0])))


def listed_alerts(alerts: Optional[pd.DataFrame], signals: Optional[pd.DataFrame], per_region: int) -> pd.DataFrame:
    """The alerts a run's reports show: for each region the first per_region groups of group_rows, every alert of a
    group with them. The rest are not told in this run and must not be filed as told: they come up again on the next
    run. Adds Row (the group's first alerted signal) and RegionTotal (the groups the region has, shown or not)."""
    if alerts is None or alerts.empty or signals is None or per_region <= 0:
        return alerts.head(0) if alerts is not None else pd.DataFrame(columns=ALERT_COLUMNS)
    info = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')
    rows = group_rows(alerts, signals)
    regions = [info['Region'].get(str(row['devices'][0])) if 'Region' in info.columns else None for row in rows]
    totals = pd.Series(regions, dtype=object).value_counts().to_dict()
    kept, seen = [], {}
    for row, region in zip(rows, regions):
        if region is None or pd.isna(region):
            continue
        seen[region] = seen.get(region, 0) + 1
        if seen[region] <= per_region:
            kept += [(idx, row['devices'][0], totals[region]) for idx in row['alerts']]
    if not kept:
        return alerts.head(0)
    out = alerts.loc[[idx for idx, _, _ in kept]].copy()
    out['Row'] = [lead for _, lead, _ in kept]
    out['RegionTotal'] = [total for _, _, total in kept]
    return out


# ---- Drawing ----------------------------------------------------------------------------------------------------------

class CoordinationGroup:
    """One group as the report shows it: title (the alerted signals' names), map (a figure, None without coordinates)
    and charts (one per chart day of a plan row; the calls chart of a clock alert)."""

    def __init__(self, title: str, map_figure, charts: list, devices: list, region):
        self.title, self.map, self.charts, self.devices, self.region = title, map_figure, charts, devices, region

    @property
    def figures(self) -> list:
        return ([self.map] if self.map is not None else []) + list(self.charts)


def _day_chart(day, coloured: list, parts: list, week: dict, number):
    """One day of a group: each signal drawn on its own (coloured: [(DeviceId, colour)], wide lines, widest first) and
    each part of the group (thin lines over them). Nothing marks the hours a signal is out of step: the reader sees
    the difference in the lines (user, 2026-11-17)."""
    import textwrap
    from .visualization import new_figure
    figure = new_figure(DAY_CHART_SIZE, dpi=120)
    ax = figure.add_subplot(111)
    x = np.arange(BINS_PER_DAY + 1) / 4
    top = 100.0
    for d in [d for d, _ in coloured] + [d for part in parts for d in part]:
        y = _series(week, day, d)
        if np.isfinite(y).any():
            top = max(top, float(np.nanmax(y)))
    for k, (d, colour) in enumerate(coloured):
        y = _series(week, day, d)
        label = number(d) if np.isfinite(y).any() else f'{number(d)} (not shown: plan unknown after a data gap this day)'
        ax.step(x, np.r_[y, y[-1]], where='post', color=colour, lw=OWN_WIDTHS[min(k, len(OWN_WIDTHS) - 1)], alpha=0.85,
                zorder=2, label=label)
    one = len(parts) == 1
    for k, part in enumerate(parts):
        if not part:
            continue
        label = ('Its group' if one else f"Group {'ABCD'[k]}") + ': ' + \
            textwrap.fill(', '.join(sorted(number(d) for d in part)), 48, subsequent_indent='   ')
        for j, d in enumerate(part):
            y = _series(week, day, d)
            ax.step(x, np.r_[y, y[-1]], where='post', color=ONE_GROUP if one else PART_COLOURS[k], lw=PART_WIDTHS[k],
                    zorder=10 + k, label=label if j == 0 else None)
    ax.set_xlim(0, 24)
    ax.set_xticks(range(0, 25, 3))
    ax.set_xticklabels([f'{h}:00' for h in range(0, 25, 3)])
    ax.set_ylim(-5, top * 1.12 + 5)
    ax.set_ylabel('Cycle (s), 0 = free', fontsize=8)
    _style(ax)
    handles, labels = ax.get_legend_handles_labels()
    legend = figure.legend(handles, labels, loc='upper left', bbox_to_anchor=(0.01, 0.925), ncol=2 if len(coloured) > 4 else 1,
                           frameon=False, fontsize=8, borderaxespad=0, labelspacing=0.3)
    figure.suptitle(f"Cycle length by time of day, {day:%a} {day.month}/{day.day}", fontsize=10, y=0.98)
    figure.canvas.draw()
    used = legend.get_window_extent().height / figure.get_window_extent().height
    figure.subplots_adjust(left=0.11, right=0.97, bottom=0.1, top=max(0.35, 0.925 - used - 0.03))
    return figure


_tiles = {}
_basemap = {'on': True}


def _world_px(lat, lon, z):
    n = 256 * 2 ** z
    return (lon + 180) / 360 * n, (1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n


def _tile(z, x, y):
    """One base-map tile, or None once a request has failed this run (the maps are then drawn without one)."""
    if not _basemap['on'] or not MAP_TILES:
        return None
    if (z, x, y) not in _tiles:
        import urllib.request
        from io import BytesIO
        from PIL import Image
        try:
            request = urllib.request.Request(MAP_TILES.format(z=z, x=x, y=y), headers={'User-Agent': 'atspm-report'})
            with urllib.request.urlopen(request, timeout=MAP_TILE_SECONDS) as answer:
                if not answer.headers.get('Content-Type', '').startswith('image'):
                    raise ValueError('not an image')
                _tiles[(z, x, y)] = Image.open(BytesIO(answer.read())).convert('RGB')
        except Exception as error:      # no base map: the map is drawn without one
            if _basemap['on']:
                _basemap['on'] = False
                log_message(f"Coordination maps: no base map ({type(error).__name__}: {error}); drawn without it.", 1, 1)
            return None
    return _tiles[(z, x, y)]


def _group_map(where: pd.DataFrame, number, alerted: list, parts: list, no_data=()):
    """All signals around a group on a plain road map. parts: [(label, DeviceIds, colour)]. alerted: [(DeviceId, the
    colour of its part, the colour of its line on the charts)]: a dot of its part's colour outlined in its line's
    colour. Every other signal nearby is grey, hollow where it has no data. None when the group has no coordinates."""
    import matplotlib.patheffects as pe
    from .visualization import new_figure
    subjects = [d for d, _, _ in alerted if d in where.index]
    members = [d for _, ids, _ in parts for d in ids if d in where.index and d not in subjects]
    focus = subjects + members
    if not subjects:
        return None
    lat, lon = where.loc[focus, 'Latitude'], where.loc[focus, 'Longitude']
    per_lat, per_lon = 69.0, 69.0 * math.cos(math.radians(lat.mean()))
    wide = MAP_PX[0] / MAP_PX[1]
    half = max(0.45, MAP_MARGIN_MILES + 0.5 * max((lat.max() - lat.min()) * per_lat, (lon.max() - lon.min()) * per_lon / wide))
    cy, cx = (lat.max() + lat.min()) / 2, (lon.max() + lon.min()) / 2
    west, east = cx - half * wide / per_lon, cx + half * wide / per_lon
    south, north = cy - half / per_lat, cy + half / per_lat
    z = MAP_MAX_ZOOM        # the deepest zoom whose picture of this view is no wider than the drawn map needs twice over
    while z > 9 and _world_px(north, east, z)[0] - _world_px(north, west, z)[0] > 2 * MAP_PX[0]:
        z -= 1
    x0, y0 = _world_px(north, west, z)
    x1, y1 = _world_px(south, east, z)
    figure = new_figure((MAP_PX[0] / 130, MAP_PX[1] / 130), dpi=130)
    ax = figure.add_axes([0.005, 0.005, 0.99, 0.925])
    base = None
    if _basemap['on'] and MAP_TILES:
        from concurrent.futures import ThreadPoolExecutor
        from PIL import Image, ImageEnhance
        tx0, ty0, tx1, ty1 = int(x0 // 256), int(y0 // 256), int(x1 // 256), int(y1 // 256)
        # fetched eight at a time: one by one they were most of the report's coordination time
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda xy: _tile(z, *xy), [(tx, ty) for tx in range(tx0, tx1 + 1) for ty in range(ty0, ty1 + 1)]))
        base = Image.new('RGB', ((tx1 - tx0 + 1) * 256, (ty1 - ty0 + 1) * 256))
        for tx in range(tx0, tx1 + 1):
            for ty in range(ty0, ty1 + 1):
                piece = _tile(z, tx, ty)
                if piece is None:
                    base = None
                    break
                base.paste(piece, ((tx - tx0) * 256, (ty - ty0) * 256))
            if base is None:
                break
        if base is not None:
            base = base.crop((int(x0 - tx0 * 256), int(y0 - ty0 * 256), int(x1 - tx0 * 256), int(y1 - ty0 * 256)))
            base = ImageEnhance.Contrast(base).enhance(1.7)      # the roads are white on pale grey: make them stand out
            ax.imshow(base, extent=(x0, x1, y1, y0), interpolation='lanczos', zorder=0)
    if base is None:
        ax.set_facecolor('#f4f4f4')
    ax.set_xlim(x0, x1)
    ax.set_ylim(y1, y0)
    ax.set_xticks([])
    ax.set_yticks([])
    for side in ax.spines.values():
        side.set_color('#999999')
    view = where[where['Latitude'].between(south, north) & where['Longitude'].between(west, east)]
    xy = {d: _world_px(r.Latitude, r.Longitude, z) for d, r in view.iterrows()}
    others = [d for d in view.index if d not in focus]
    known = [d for d in others if d not in set(no_data)]
    silent = [d for d in others if d in set(no_data)]
    ax.scatter([xy[d][0] for d in known], [xy[d][1] for d in known], s=22, color=GREY, edgecolors='white',
               linewidths=0.4, label='Other signals', zorder=2)
    if silent:      # nothing is known of what these run
        ax.scatter([xy[d][0] for d in silent], [xy[d][1] for d in silent], s=22, facecolors='white', edgecolors=GREY,
                   linewidths=1.0, label='No data', zorder=2)
    colour_of = {d: colour for _, ids, colour in parts for d in ids}
    for label, ids, colour in parts:
        ids = [d for d in ids if d in xy and d not in subjects]
        ax.scatter([xy[d][0] for d in ids], [xy[d][1] for d in ids], s=38, color=colour, edgecolors='white',
                   linewidths=0.5, label=label, zorder=3)
    for d, fill, outline in alerted:
        if d in xy:
            ax.scatter(*xy[d], s=74, color=fill, edgecolors=outline, linewidths=2.2, zorder=4)
    # Labels, the alerted signals and their group first: each takes the spot by its dot that overlaps the dots and
    # labels placed least.
    figure.canvas.draw()
    crowded = len(members) > 8          # a downtown grid: a little smaller so the numbers do not run together
    points = ([(d, outline, 9.0, 'bold') for d, _, outline in alerted if d in xy]
              + [(d, colour_of[d], 6.6 if crowded else 7.5, 'bold') for d in members if d in xy]
              + [(d, GREY, 6.0 if crowded else 6.5, 'normal') for d in others])
    px = {d: ax.transData.transform(xy[d]) for d, *_ in points}
    taken = [(x - 4, y - 4, x + 4, y + 4) for x, y in px.values()]
    spots = [(5, 2), (5, -8), (-5, 2), (-5, -8), (0, 6), (0, -13), (5, 10), (-5, -16), (5, -17), (-5, 10),
             (0, 14), (0, -21), (12, 14), (-12, 14), (12, -21), (-12, -21)]
    scale = 130 / 72
    for d, colour, size, weight in points:
        text = number(d)
        w, h = 0.62 * size * len(text) * scale, size * scale
        best = None
        for dx, dy in spots:
            left = px[d][0] + dx * scale - (w if dx < 0 else w / 2 if dx == 0 else 0)
            low = px[d][1] + dy * scale
            box = (left, low, left + w, low + h)
            cost = sum(max(0, min(box[2], t[2]) - max(box[0], t[0])) * max(0, min(box[3], t[3]) - max(box[1], t[1]))
                       for t in taken)
            if best is None or cost < best[0] - 1e-9:
                best = (cost, dx, dy, box)
            if cost == 0:
                break
        _, dx, dy, box = best
        taken.append(box)
        ax.annotate(text, xy[d], xytext=(dx, dy), textcoords='offset points', fontsize=size, color=colour,
                    fontweight=weight, ha='right' if dx < 0 else 'center' if dx == 0 else 'left', zorder=5,
                    path_effects=[pe.withStroke(linewidth=2.2, foreground='white')])
    miles_px = (x1 - x0) / (2 * half * wide)
    bar = 0.5 if half * wide > 1.2 else 0.25
    bx, by = x0 + 0.03 * (x1 - x0), y1 - 0.05 * (y1 - y0)
    ax.plot([bx, bx + bar * miles_px], [by, by], color='#222222', lw=2, zorder=6)
    ax.annotate(f'{bar:g} mi', (bx, by), xytext=(0, 3), textcoords='offset points', fontsize=6.5, color='#222222',
                zorder=6, path_effects=[pe.withStroke(linewidth=1.6, foreground='white')])
    if base is not None:
        ax.annotate(MAP_CREDIT, (1, 0), xycoords='axes fraction', xytext=(-2, 2), textcoords='offset points', ha='right',
                    fontsize=4.5, color='#444444', zorder=6, path_effects=[pe.withStroke(linewidth=1.4, foreground='white')])
    handles, labels = ax.get_legend_handles_labels()
    order = [labels.index(x) for x in [label for label, _, _ in parts] + ['Other signals', 'No data'] if x in labels]
    figure.legend([handles[i] for i in order], [labels[i] for i in order], loc='upper left', bbox_to_anchor=(0.0, 1.0),
                  ncol=6, frameon=False, fontsize=8, handletextpad=0.1, columnspacing=1.0, borderaxespad=0.2)
    figure.report_own_proportions = True
    return figure


def _draw_row(row: dict, alerts: pd.DataFrame, info: pd.DataFrame, where) -> CoordinationGroup:
    """The CoordinationGroup of one group_rows row."""
    def number(device):
        return signal_number(info['Name'].get(str(device), device)) if 'Name' in info.columns else str(device)
    devices, parts, own = row['devices'], row['parts'], row['own']
    one = len(parts) == 1

    def part_colour(k):
        return ONE_GROUP if one else PART_COLOURS[k]
    if row['clock']:
        alert = alerts.loc[row['alerts'][0]]
        charts = [_clock_chart(alert, info['Name'].get(str(alert['DeviceId']), alert['DeviceId']))]
        coloured = [(devices[0], STRONG)]
    else:
        coloured = [(d, OWN_COLOURS[k % len(OWN_COLOURS)]) for k, d in enumerate(devices + own)]
        charts = [_day_chart(day, coloured, parts, row['week'], number) for day in row['days']]
    map_figure = None
    if where is not None:
        spread = [d for part in parts for d in part if d in where.index]
        outlined = []
        for d, colour in coloured:
            home = 0
            if spread and d in where.index:
                miles = _miles_from(where, d)
                nearest = min(spread, key=lambda m: miles.get(m, np.inf))
                home = next(k for k, part in enumerate(parts) if nearest in part)
            outlined.append((d, part_colour(home), colour))
        labelled = [('Its group' if one else f"Group {'ABCD'[k]}", part, part_colour(k)) for k, part in enumerate(parts) if part]
        no_data = set().union(*[set(alerts.at[idx, 'ChartNoData'] or []) for idx in row['alerts']])
        map_figure = _group_map(where, number, outlined, labelled, no_data)
    title = '; '.join(str(info['Name'].get(str(d), d)) if 'Name' in info.columns else str(d) for d in devices)
    region = info['Region'].get(str(devices[0])) if 'Region' in info.columns else None
    return CoordinationGroup(title, map_figure, charts, devices, region)


def create_coordination_plots(alerts: pd.DataFrame, signals: pd.DataFrame, num_figures: int = 5) -> list:
    """The groups of the alerts (group_rows) drawn, as (CoordinationGroup, report) for each region's report and for
    All Regions: the first num_figures groups of each. A plan group: its map and one chart per chart day. A clock
    alert: its map and its emergency vehicle calls against the matching calls at nearby signals."""
    if alerts is None or alerts.empty or signals is None:
        return []
    info = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')
    where = _coordinates(signals)
    rows = group_rows(alerts, signals)
    drawn = [_draw_row(row, alerts, info, where) for row in rows]
    out = [(group, 'All Regions') for group in drawn[:num_figures]]
    for region in sorted({group.region for group in drawn if group.region is not None and not pd.isna(group.region)}):
        out += [(group, region) for group in [g for g in drawn if g.region == region][:num_figures]]
    return out


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
