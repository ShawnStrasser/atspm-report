"""Timing Insights: how each phase uses its green, from the 15-minute tables (study of 2026-11-19 to 11-23, reviewed by the
user on the gap timing work list).

Six checks, each its own table at the bottom of the report:

  Premature Gap-Out           the phase gaps out with a split failure: the gap time is too short or the detection is not
                              working. In each 15-minute bin, the bin's split failures (at most its gap-outs) count as at
                              a gap-out in the share its gap-outs have of its greens; a bin of gap-outs only counts them
                              all. Listed when PREMATURE_SHARE of its gap-outs end in a split failure over the judged days
                              and on PREMATURE_DAYS of them (each with PREMATURE_MIN_GAPOUTS gap-outs or more), and
                              PREMATURE_PER_DAY or more of them a day.
  Maxing Out despite Low Demand  the phase runs to its limit without the traffic for it.
  Max Recall despite Detection  the phase is on max recall although its detection works and the traffic is light.
  Needs More Green            the phase runs to its limit with split failures while phases that can give it time have
                              time to spare.
  Max Capacity                one row per signal: phases short of green with none to spare anywhere.
  Downstream Bottleneck       the green is occupied but few vehicles leave (count zones): traffic backed up from beyond.

The four green-use checks judge each signal, phase and hour (HOUR_FROM to HOUR_TO on the judged weekdays) and list a
phase in that state STATE_HOURS hours a day or more on STATE_DAYS of the days:

  on recall     the phase ran to its limit on RECALL_LIMIT of its greens with ped service on under half of them. Data
                only: the controllers' recall settings are never read.
  coordinated   phases 2 and 6 while the signal runs a pattern with a cycle length. A coordinated phase is held to its
                force-off by design, so Maxing Out despite Low Demand and Max Recall leave it out (Premature Gap-Out does not).
  barrier partner  phases 2 and 6, 4 and 8 end at the barrier together (standard ring and barrier phasing: ring 1 is
                phases 1 to 4, ring 2 is 5 to 8; 1, 2, 5, 6 one side of the barrier, 3, 4, 7, 8 the other). A phase
                held by a busier partner is not running long by itself, so both must meet the low-demand limits.
  max recall    on recall, not coordinated, green occupancy above 0 and under LOW_OCCUPANCY, split failures under
                LOW_SPLIT_FAIL, the partner under the same limits.
  max-out, low demand  not coordinated, not on recall, no ped service, runs to its limit on LIMIT_SHARE of greens,
                occupancy under LOW_OCCUPANCY, split failures under LOW_SPLIT_FAIL; the partner too (and not on recall).
  short         runs to its limit on LIMIT_SHARE of greens and SHORT_SPLIT_FAIL of its cycles fail.
  spare         split failures under LOW_SPLIT_FAIL and occupancy under SPARE_OCCUPANCY.
  needs more green  short, and a phase with spare can give it time: one of its own ring and side; in a single ring any
                phase; else across the barrier, where every ring serving the other side has a phase with spare.
  at capacity   short, no measured phase of the signal has spare and TIGHT_SHARE of the phases it serves have
                TIGHT_SPLIT_FAIL split failures. A signal with a Downstream Bottleneck is not listed: it is not the
                signal's own capacity.

Downstream Bottleneck (per phase, stop-bar count zones of the detector table): a 15-minute bin of the phase's green
lasting BLOCK_MIN_GREEN seconds or more on average is blocked when its green occupancy is BLOCK_OCCUPANCY or more, a split
failure ended BLOCK_SPLIT_FAIL of its cycles, a vehicle was counted, and the vehicles counted are under BLOCK_SHARE of what
the green carries at its usual best (the phase's BLOCK_TOP_QUANTILE of vehicles over (green - START_UP_LOSS) /
HEADWAY per cycle per lane, at most 1). Listed when blocked BLOCK_BINS_A_DAY bins on STATE_DAYS days, and in under half
of its bins at that occupancy (always low when occupied points to the count zone, not to a blockage).

No measure from a faulting detector: hours with a known fault on the phase's stop-bar zone are left out, and so are hours
whose green occupancy never dropped under STUCK_OCCUPANCY (a zone held on). Judged days: the last JUDGED_DAYS weekdays up
to the report day, holidays left out.

All set-based SQL on one private connection; Python only finds the phases that can give time, words the alerts and draws
the few charts.
"""

from __future__ import annotations

import json
from typing import Optional

import numpy as np
import pandas as pd

from . import measure_inputs
from .duckdb_utils import columns as table_columns, is_relation, to_pandas
from .utils import log_message

ALERT_TYPE = 'timing_insights'
ID_COLUMNS = ['DeviceId', 'Phase', 'Check']
CONFIG_SWITCH = 'timing_insights_enabled'
ROWS_CONFIG = 'timing_insights_rows'
SECTION_TITLE = 'Timing Insights'

PREMATURE = 'Premature Gap-Out'
MAXOUT_LOW = 'Maxing Out despite Low Demand'
MAX_RECALL = 'Max Recall despite Detection'
NEEDS_GREEN = 'Needs More Green'
CAPACITY = 'Max Capacity'
BOTTLENECK = 'Downstream Bottleneck'
# Report order, the first kept when a report is long (user, 2026-11-23: Downstream Bottleneck, then Max Capacity, last).
CHECKS = (PREMATURE, MAXOUT_LOW, MAX_RECALL, NEEDS_GREEN, BOTTLENECK, CAPACITY)

# The user's own words for each table (2026-11-23).
NOTES = {
    PREMATURE: 'If a gap-out happens with a split failure, the phase gapped out despite demand. Either the gap timing is '
               'too aggressive or the detection is not working.',
    MAXOUT_LOW: 'The phase is extending despite a lack of demand, shown by high max-outs with low split failures and low '
                'green occupancy. The gap timing is too lenient, or the detection is malfunctioning or misconfigured.',
    MAX_RECALL: 'The phase is on max recall despite working detection and a lack of demand. Check the timing for max recall '
                '(is it intentional?). If it is not the timing, a broken detector is forcing the phase into recall while '
                "the phase's other detection still works.",
    NEEDS_GREEN: 'Phases that need more green time AND have potential donor phases with time to spare.',
    CAPACITY: 'Signals that are at capacity on all phases.',
    BOTTLENECK: 'Phases that are getting backed up by a downstream bottleneck. They are wasting green time by giving it to '
                'vehicles that have nowhere to go.',
}

# ---- Thresholds: study of 2026-11-19 to 11-23, ODOT weekdays 11-09 to 11-17 -------------------------------------------
JUDGED_DAYS = 6
LOOKBACK_DAYS = 21
HOUR_FROM, HOUR_TO = 7, 19           # 7:00 to 19:00
MIN_HOUR_CYCLES = 12
STUCK_OCCUPANCY = 0.97
MIN_SERVED = 40                      # services over the judged days for a phase to count as served
STATE_HOURS, STATE_DAYS = 2, 3
RECALL_LIMIT = 0.98
LIMIT_SHARE = 0.5
LOW_OCCUPANCY = 0.40
LOW_SPLIT_FAIL = 0.02
SHORT_SPLIT_FAIL = 0.20
SPARE_OCCUPANCY = 0.50
TIGHT_SPLIT_FAIL, TIGHT_SHARE = 0.10, 0.75
DONOR_SHARE = 0.25                   # a set of donor phases named when it gave time in this share of the hours
ALL_RECALL_SHARE = 0.5               # a signal whose every measured phase is on recall this share of hours goes last
# Study of 2026-11-23 (weekdays 11-13 to 11-20): 5 % flagged 545 phases, a sixth of those measured (the median phase ends
# 1 % of its gap-outs in a split failure). 15 % and 20 a day flags 78 (2.3 %); 83 % of those flagged on three of the days
# were flagged again on the other three.
PREMATURE_SHARE = 0.15
PREMATURE_PER_DAY = 20
PREMATURE_DAYS = 3
PREMATURE_MIN_GAPOUTS = 20
START_UP_LOSS, HEADWAY = 5.0, 2.0
BLOCK_MIN_GREEN = 10.0
BLOCK_OCCUPANCY = 0.85
BLOCK_SPLIT_FAIL = 0.20
BLOCK_SHARE = 0.5
BLOCK_TOP_QUANTILE = 0.95
BLOCK_MIN_BINS, BLOCK_MIN_VEHICLES = 200, 500
BLOCK_BINS_A_DAY = 4
IDEAL_OCCUPANCY = (40, 70)           # % of green; split failures under 1 % of cycles below 40 %, over 5 % from 70 %

ALERT_COLUMNS = ['DeviceId', 'Date', 'Phase', 'Check', 'Severity', 'Rank', 'Detail', 'Donors', 'DaysSeen', 'DaysJudged',
                 'ChartData']

PHASE_COLOURS = {1: '#0072B2', 2: '#E69F00', 3: '#009E73', 4: '#D55E00', 5: '#CC79A7', 6: '#56B4E9', 7: '#8C6D1F',
                 8: '#555555'}
INK, MUTED, BLUE, RED, BAND = '#222222', '#666666', '#2a78d6', '#d04a3c', '#3a9d5d'
MAXOUT_TITLE, SPLIT_TITLE, GOR_TITLE = 'Percent MaxOut/ForceOff', 'Percent Split Failures', 'Green Occupancy Ratio'
CHART_WIDTH = 7.5


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=ALERT_COLUMNS)


def judged_days(report_day, n: int = JUDGED_DAYS) -> list:
    """The last n weekdays up to the report day (within LOOKBACK_DAYS), holidays left out, newest first."""
    day = pd.Timestamp(report_day).normalize()
    start = day - pd.Timedelta(days=LOOKBACK_DAYS)
    off = set(measure_inputs.holidays(start, day))
    days, probe = [], day
    while len(days) < n and probe >= start:
        if probe.day_of_week < 5 and probe.date() not in off:
            days.append(probe)
        probe -= pd.Timedelta(days=1)
    return days


def _read(table, wanted: list, days: list, devices, required: list = ()) -> pd.DataFrame:
    have = set(table_columns(table))
    missing = [c for c in required if c not in have]
    if missing:
        raise ValueError(f"timing insights: a table lacks columns {missing}")
    return measure_inputs.rows_on_days(table, ', '.join(c for c in wanted if c in have), days, devices)


def _read_counts(actuations, zones: pd.DataFrame, days: list) -> pd.DataFrame:
    """The 15-minute counts of the count zones on these days, filtered before anything is read."""
    days = sorted(pd.Timestamp(d).normalize() for d in days)
    keys = ', '.join("'" + f"{d}|{int(z)}".replace("'", "''") + "'" for d, z in zip(zones['DeviceId'], zones['Detector']))
    where = (measure_inputs._where(days[0], days[-1] + pd.Timedelta(days=1), None, days)
             + f" AND (CAST(DeviceId AS VARCHAR) || '|' || CAST(CAST(Detector AS INTEGER) AS VARCHAR)) IN ({keys})")
    columns = 'TimeStamp, DeviceId, Detector, Total'
    if is_relation(actuations):
        return actuations.filter(where).project(columns).df()
    con = measure_inputs.connect()
    try:
        con.register('t', to_pandas(actuations))
        return con.execute(f"SELECT {columns} FROM t WHERE {where}").df()
    finally:
        con.close()


def _hours_sql(has_pattern: bool) -> list:
    """Statements building s (one row per signal, phase, day and hour with its measures and states) and its inputs."""
    free_pattern = " AND Pattern NOT IN (254, 255)" if has_pattern else ''
    return [
        """CREATE TEMP TABLE tb AS
           SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(Phase AS INTEGER) AS Phase, TimeStamp AS ts,
                  coalesce(sum(Total) FILTER (WHERE PerformanceMeasure = 'GapOut'), 0) AS gap,
                  coalesce(sum(Total) FILTER (WHERE PerformanceMeasure = 'MaxOut'), 0) AS mx,
                  coalesce(sum(Total) FILTER (WHERE PerformanceMeasure = 'ForceOff'), 0) AS fo
           FROM term_in GROUP BY ALL""",
        f"""CREATE TEMP TABLE cb AS
            SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, time_bucket(INTERVAL 15 MINUTE, TimeStamp) AS ts,
                   bool_or(coalesce(CycleLength, 0) > 0{free_pattern}) AS coord
            FROM co_in GROUP BY ALL""",
        """CREATE TEMP TABLE pb AS
           SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(Phase AS INTEGER) AS Phase,
                  time_bucket(INTERVAL 15 MINUTE, TimeStamp) AS ts, sum(PedServices) AS ped
           FROM ped_in GROUP BY ALL""",
        # every 15-minute bin of the judged days (Downstream Bottleneck reads them all)
        """CREATE TEMP TABLE bins AS
           SELECT CAST(s.DeviceId AS VARCHAR) AS DeviceId, CAST(s.Phase AS INTEGER) AS Phase, s.TimeStamp AS ts,
                  CAST(s.TimeStamp AS DATE) AS day, hour(s.TimeStamp) AS hr, s.Green_Time AS green, s.Green_Occupancy AS gor,
                  s.Split_Failure AS sf, s.Cycles AS cyc, coalesce(t.gap, 0) AS gap, coalesce(t.mx, 0) AS mx,
                  coalesce(t.fo, 0) AS fo, coalesce(p.ped, 0) AS ped, NOT coalesce(c.coord, FALSE) AS free
           FROM sf_in s
           LEFT JOIN tb t ON t.DeviceId = CAST(s.DeviceId AS VARCHAR) AND t.Phase = s.Phase AND t.ts = s.TimeStamp
           LEFT JOIN pb p ON p.DeviceId = CAST(s.DeviceId AS VARCHAR) AND p.Phase = s.Phase AND p.ts = s.TimeStamp
           LEFT JOIN cb c ON c.DeviceId = CAST(s.DeviceId AS VARCHAR) AND c.ts = s.TimeStamp
           WHERE s.Cycles > 0""",
        f"""CREATE TEMP TABLE h AS
            SELECT DeviceId, Phase, day, hr, time_bucket(INTERVAL 1 HOUR, min(ts)) AS hts,
                   sum(cyc) AS cyc, sum(gap) AS gap, sum(mx) AS mx, sum(fo) AS fo, sum(sf) AS sf, sum(ped) AS ped,
                   sum(gor * cyc) / sum(cyc) AS gor, min(gor) AS gor_min, avg(free::INT) >= 0.5 AS free,
                   sum(least(sf, gap) * gap / nullif(gap + mx + fo, 0)) AS sf_go
            FROM bins WHERE hr >= {HOUR_FROM} AND hr < {HOUR_TO}
            GROUP BY DeviceId, Phase, day, hr HAVING sum(cyc) >= {MIN_HOUR_CYCLES}""",
        f"DELETE FROM h WHERE gor_min >= {STUCK_OCCUPANCY}",
        f"DELETE FROM h WHERE {measure_inputs.masked_sql('h', measure_inputs.STOP_BAR, time_column='hts')}",
        f"""CREATE TEMP TABLE served AS
            SELECT DeviceId, Phase, (Phase - 1) // 4 AS ring, ((Phase - 1) % 4) // 2 AS side FROM tb
            WHERE Phase BETWEEN 1 AND 8 GROUP BY ALL HAVING sum(gap + mx + fo) >= {MIN_SERVED}""",
        """CREATE TEMP TABLE sig AS SELECT DeviceId, count(*) AS n_served, count(DISTINCT ring) AS rings FROM served GROUP BY ALL""",
        # standard phasing: 2 and 6, 4 and 8 end at the barrier together
        """CREATE TEMP TABLE s AS
           SELECT h.*, (h.mx + h.fo) / nullif(h.gap + h.mx + h.fo, 0) AS lim, h.sf / h.cyc AS sfr,
                  h.ped < 0.5 * h.cyc AND (h.mx + h.fo) / nullif(h.gap + h.mx + h.fo, 0) >= {recall} AS on_recall,
                  NOT h.free AND h.Phase IN (2, 6) AS coord,
                  CASE WHEN h.Phase IN (2, 4, 6, 8) AND v.Phase IS NOT NULL THEN v.Phase END AS tied,
                  (h.Phase - 1) // 4 AS ring, ((h.Phase - 1) % 4) // 2 AS side
           FROM h LEFT JOIN served v ON v.DeviceId = h.DeviceId AND v.Phase = CASE h.Phase WHEN 2 THEN 6 WHEN 6 THEN 2
                                                                                     WHEN 4 THEN 8 WHEN 8 THEN 4 END""".format(recall=RECALL_LIMIT),
        f"""CREATE TEMP TABLE s1 AS
            SELECT a.*,
               a.Phase BETWEEN 1 AND 8 AND a.on_recall AND NOT a.coord AND a.gor > 0 AND a.gor < {LOW_OCCUPANCY}
                 AND a.sfr < {LOW_SPLIT_FAIL}
                 AND (a.tied IS NULL OR coalesce(p.gor < {LOW_OCCUPANCY} AND p.sfr < {LOW_SPLIT_FAIL}, FALSE)) AS max_recall,
               a.Phase BETWEEN 1 AND 8 AND NOT a.coord AND NOT a.on_recall AND a.ped = 0 AND a.lim >= {LIMIT_SHARE}
                 AND a.gor < {LOW_OCCUPANCY} AND a.sfr < {LOW_SPLIT_FAIL}
                 AND (a.tied IS NULL OR coalesce(NOT p.on_recall AND p.ped = 0 AND p.gor < {LOW_OCCUPANCY}
                                                 AND p.sfr < {LOW_SPLIT_FAIL}, FALSE)) AS maxout_low,
               a.Phase BETWEEN 1 AND 8 AND a.lim >= {LIMIT_SHARE} AND a.sfr >= {SHORT_SPLIT_FAIL} AS short,
               a.Phase BETWEEN 1 AND 8 AND a.sfr < {LOW_SPLIT_FAIL} AND a.gor < {SPARE_OCCUPANCY} AS spare
            FROM s a LEFT JOIN s p ON p.DeviceId = a.DeviceId AND p.day = a.day AND p.hr = a.hr AND p.Phase = a.tied""",
    ]


# The phases with spare that can give each short phase time, by the ring rules: in a single ring any of them; else those of
# its own ring and side; else those across the barrier, when every ring serving the other side has one there.
DONORS_SQL = """CREATE TEMP TABLE dn AS
WITH need AS (SELECT DeviceId, day, hr, Phase, ring, side FROM s1 WHERE short),
     give AS (SELECT DeviceId, day, hr, Phase, ring, side FROM s1 WHERE spare),
     far_rings AS (SELECT DeviceId, side, count(DISTINCT ring) AS n FROM served GROUP BY ALL),
     pairs AS (SELECT n.DeviceId, n.day, n.hr, n.Phase, n.ring, n.side, g.Phase AS d, g.ring AS dr, g.side AS ds
               FROM need n JOIN give g ON g.DeviceId = n.DeviceId AND g.day = n.day AND g.hr = n.hr AND g.Phase <> n.Phase),
     sets AS (SELECT DeviceId, day, hr, Phase, side,
                     string_agg(d::VARCHAR, ',' ORDER BY d) AS anyone,
                     string_agg(d::VARCHAR, ',' ORDER BY d) FILTER (WHERE dr = ring AND ds = side) AS same,
                     string_agg(d::VARCHAR, ',' ORDER BY d) FILTER (WHERE ds <> side) AS far,
                     count(DISTINCT dr) FILTER (WHERE ds <> side) AS far_n
              FROM pairs GROUP BY DeviceId, day, hr, Phase, side)
SELECT x.DeviceId, x.day, x.hr, x.Phase,
       CASE WHEN coalesce(sig.rings, 2) = 1 THEN x.anyone
            WHEN x.same IS NOT NULL THEN x.same
            WHEN x.far_n > 0 AND x.far_n = fr.n THEN x.far END AS donors
FROM sets x LEFT JOIN sig USING (DeviceId)
LEFT JOIN far_rings fr ON fr.DeviceId = x.DeviceId AND fr.side = 1 - x.side"""


def build_insight_alerts(split_failures, terminations, report_day, signals: Optional[pd.DataFrame] = None,
                         pedestrian=None, coordination_agg=None, actuations=None, detectors: Optional[pd.DataFrame] = None,
                         faults: Optional[pd.DataFrame] = None, verbosity: int = 0) -> pd.DataFrame:
    """The Timing Insights of the report day: ALERT_COLUMNS, one row per signal, phase and check (Phase 0 for Max
    Capacity, which is about the whole signal). Empty, quietly, without split_failures and terminations or a report day.
    detectors: detector_info() rows (DeviceId, Detector, Phase, Function, Lanes), for the count zones of Downstream
    Bottleneck, which also needs actuations."""
    if report_day is None or not measure_inputs.present(split_failures) or not measure_inputs.present(terminations):
        return _empty()
    days = judged_days(report_day)
    if not days:
        return _empty()
    devices = sorted(signals['DeviceId'].astype(str).unique()) if signals is not None and 'DeviceId' in signals.columns else None
    sf = _read(split_failures, ['TimeStamp', 'DeviceId', 'Phase', 'Green_Time', 'Green_Occupancy', 'Split_Failure', 'Cycles'],
               days, devices, required=['Green_Time', 'Green_Occupancy', 'Split_Failure', 'Cycles'])
    term = _read(terminations, ['TimeStamp', 'DeviceId', 'Phase', 'PerformanceMeasure', 'Total'], days, devices)
    if sf.empty or term.empty:
        return _empty()
    ped = (_read(pedestrian, ['TimeStamp', 'DeviceId', 'Phase', 'PedServices'], days, devices, required=['PedServices'])
           if measure_inputs.present(pedestrian) else
           pd.DataFrame({'TimeStamp': pd.Series(dtype='datetime64[us]'), 'DeviceId': pd.Series(dtype=str),
                         'Phase': pd.Series(dtype='int64'), 'PedServices': pd.Series(dtype=float)}))
    coord = (_read(coordination_agg, ['TimeStamp', 'DeviceId', 'CycleLength', 'Pattern'], days, devices, required=['CycleLength'])
             if measure_inputs.present(coordination_agg) else
             pd.DataFrame({'TimeStamp': pd.Series(dtype='datetime64[us]'), 'DeviceId': pd.Series(dtype=str),
                           'CycleLength': pd.Series(dtype=float)}))
    faults = faults if faults is not None else measure_inputs.fault_spans()
    zones = pd.DataFrame()
    if detectors is not None and len(detectors) and measure_inputs.present(actuations) \
            and {'DeviceId', 'Detector', 'Phase', 'Function'} <= set(detectors.columns):
        zones = detectors[(detectors['Function'] == 'Count') & detectors['Phase'].notna()]
        if devices is not None:
            zones = zones[zones['DeviceId'].astype(str).isin(devices)]
        zones = pd.DataFrame({'DeviceId': zones['DeviceId'].astype(str), 'Detector': pd.to_numeric(zones['Detector']).astype('int64'),
                              'Phase': zones['Phase'].astype('int64'),
                              'lanes': pd.to_numeric(zones.get('Lanes', 1), errors='coerce').fillna(1).clip(lower=1)})
    counts = _read_counts(actuations, zones, days) if not zones.empty else pd.DataFrame()

    con = measure_inputs.connect()
    try:
        for name, frame in (('sf_in', sf), ('term_in', term), ('ped_in', ped), ('co_in', coord), ('faults', faults)):
            con.register(name, frame)
        for statement in _hours_sql('Pattern' in coord.columns):
            con.execute(statement)
        served = con.execute("SELECT * FROM served").df()
        con.execute(DONORS_SQL)
        con.execute(f"""CREATE TEMP TABLE sh AS
            SELECT DeviceId, day, hr, sum(spare::INT) AS n_spare, sum((sfr >= {TIGHT_SPLIT_FAIL})::INT) AS n_tight, count(*) AS n
            FROM s1 GROUP BY ALL""")
        con.execute(f"""CREATE TEMP TABLE s2 AS
            SELECT s1.*, dn.donors, s1.short AND dn.donors IS NOT NULL AS needs_green,
                   s1.short AND sh.n_spare = 0 AND sh.n_tight >= {TIGHT_SHARE} * coalesce(sig.n_served, sh.n) AS capacity
            FROM s1 JOIN sh USING (DeviceId, day, hr) LEFT JOIN sig USING (DeviceId)
            LEFT JOIN dn ON dn.DeviceId = s1.DeviceId AND dn.day = s1.day AND dn.hr = s1.hr AND dn.Phase = s1.Phase""")
        flags = []
        for state, check in (('maxout_low', MAXOUT_LOW), ('max_recall', MAX_RECALL), ('needs_green', NEEDS_GREEN),
                             ('capacity', CAPACITY)):
            found = con.execute(f"""
                WITH d AS (SELECT DeviceId, Phase, day, count(*) AS hrs FROM s2 WHERE {state} GROUP BY ALL HAVING count(*) >= {STATE_HOURS})
                SELECT DeviceId, Phase, count(*) AS days_seen, sum(hrs) AS hours FROM d GROUP BY ALL HAVING count(*) >= {STATE_DAYS}""").df()
            flags.append(found.assign(Check=check))
        premature = con.execute(f"""
            WITH d AS (SELECT DeviceId, Phase, day, sum(gap) AS n, sum(sf_go) AS k FROM s GROUP BY ALL)
            SELECT DeviceId, Phase, count(*) FILTER (WHERE n >= {PREMATURE_MIN_GAPOUTS} AND k / n >= {PREMATURE_SHARE}) AS days_seen,
                   sum(k) AS k, sum(n) AS n
            FROM d GROUP BY ALL
            HAVING sum(k) / nullif(sum(n), 0) >= {PREMATURE_SHARE} AND sum(k) >= {PREMATURE_PER_DAY * len(days)}
               AND count(*) FILTER (WHERE n >= {PREMATURE_MIN_GAPOUTS} AND k / n >= {PREMATURE_SHARE}) >= {PREMATURE_DAYS}""").df()
        donors = con.execute("SELECT DeviceId, Phase, donors FROM s2 WHERE needs_green").df()
        # the hours a day the whole signal was at capacity (any phase), on its days with STATE_HOURS of them
        capacity_hours = con.execute(f"""
            WITH d AS (SELECT DeviceId, day, count(DISTINCT hr) AS hrs FROM s2 WHERE capacity GROUP BY ALL
                       HAVING count(DISTINCT hr) >= {STATE_HOURS})
            SELECT DeviceId, sum(hrs) AS hours, count(*) AS days_seen FROM d GROUP BY ALL""").df()
        recall_share = con.execute("SELECT DeviceId, Phase, avg(on_recall::INT) AS r FROM s GROUP BY ALL").df()
        hourly = con.execute("""
            SELECT DeviceId, Phase, hr, sum(mx + fo) AS lim_n, sum(gap + mx + fo) AS all_n, sum(gor * cyc) AS occ,
                   sum(cyc) AS cyc, sum(sf) AS sf, sum(sf_go) AS sf_go
            FROM s GROUP BY ALL""").df()
        block = _bottleneck(con, counts, zones) if not counts.empty else (pd.DataFrame(), pd.DataFrame())
    finally:
        con.close()

    nd = len(days)
    states = pd.concat(flags, ignore_index=True)
    states = states.assign(Severity=states['hours'] / nd)
    blocked, points = block
    block_devices = set(blocked['DeviceId']) if not blocked.empty else set()
    rows = []
    # Max recall at a signal whose every measured phase is on recall (fixed time with detection) goes last.
    rs = recall_share.assign(on=recall_share['r'] >= ALL_RECALL_SHARE).groupby('DeviceId')['on'].agg(['all', 'size'])
    all_recall = set(rs[rs['all'] & (rs['size'] >= 2)].index)
    for r in states[states['Check'] != CAPACITY].itertuples(index=False):
        rank = 1 if r.Check == MAX_RECALL and r.DeviceId in all_recall else 0
        donor_text = ''
        if r.Check == NEEDS_GREEN:
            sets = donors[(donors['DeviceId'] == r.DeviceId) & (donors['Phase'] == r.Phase)]['donors'].value_counts(normalize=True)
            donor_text = '; or '.join('Ph ' + d.replace(',', ' and ') for d in sets[sets >= DONOR_SHARE].index[:3])
        rows.append(dict(DeviceId=r.DeviceId, Phase=int(r.Phase), Check=r.Check, Severity=float(r.Severity), Rank=rank,
                         Detail=_state_detail(r.Check, r.Severity, int(r.days_seen), nd), Donors=donor_text,
                         DaysSeen=int(r.days_seen)))
    cap = states[(states['Check'] == CAPACITY) & ~states['DeviceId'].isin(block_devices)]
    signal_hours = capacity_hours.set_index('DeviceId')
    for dev, grp in cap.groupby('DeviceId'):
        severity = float(signal_hours.at[dev, 'hours'] / nd)
        seen = int(signal_hours.at[dev, 'days_seen'])
        phases = ' and '.join(', '.join(str(int(p)) for p in sorted(grp['Phase'])).rsplit(', ', 1))
        rows.append(dict(DeviceId=dev, Phase=0, Check=CAPACITY, Severity=severity, Rank=0,
                         Detail=f"Phase{'s' if len(grp) > 1 else ''} {phases} short of green with no time to spare elsewhere, {severity:.1f} hours a day "
                                f"on {seen} of {nd} weekdays.",
                         Donors='', DaysSeen=seen))
    for r in premature.itertuples(index=False):
        per_day = r.k / nd
        rows.append(dict(DeviceId=r.DeviceId, Phase=int(r.Phase), Check=PREMATURE, Severity=float(per_day), Rank=0,
                         Detail=f"{per_day:.0f} gap-outs a day end in a split failure ({100 * r.k / r.n:.0f} % of its gap-outs).",
                         Donors='', DaysSeen=int(r.days_seen)))
    for r in (blocked.itertuples(index=False) if not blocked.empty else ()):
        rows.append(dict(DeviceId=r.DeviceId, Phase=int(r.Phase), Check=BOTTLENECK, Severity=float(r.hours / nd), Rank=0,
                         Detail=f"Green occupied but few vehicles leaving, {r.hours / nd:.1f} hours a day on {int(r.days_seen)} of {nd} weekdays.",
                         Donors='', DaysSeen=int(r.days_seen)))
    if not rows:
        return _empty()
    alerts = pd.DataFrame(rows)
    missing = served.groupby('DeviceId')['Phase'].apply(set).to_dict()
    by_signal = dict(tuple(hourly[hourly['Phase'].between(1, 8)].groupby('DeviceId')))
    by_phase = dict(tuple(points.groupby(['DeviceId', 'Phase']))) if not points.empty else {}
    alerts['ChartData'] = [_chart_data(by_signal.get(a.DeviceId, hourly.iloc[:0]), by_phase.get((a.DeviceId, a.Phase)), a.Check,
                                       missing.get(a.DeviceId, set()))
                           for a in alerts.itertuples(index=False)]
    alerts = alerts.assign(Date=pd.Timestamp(days[0]).normalize(), DaysJudged=nd)
    alerts['Date'] = alerts['Date'].astype('datetime64[us]')
    alerts = alerts.sort_values(['Check', 'Rank', 'Severity'], ascending=[True, True, False])[ALERT_COLUMNS].reset_index(drop=True)
    log_message(f"Timing insights: {len(alerts)} alerts on {days[0]:%Y-%m-%d} ({nd} weekdays judged): "
                + ', '.join(f"{c} {int((alerts['Check'] == c).sum())}" for c in CHECKS), 1, verbosity)
    return alerts


def _state_detail(check: str, severity: float, seen: int, nd: int) -> str:
    what = {MAXOUT_LOW: 'Runs to its limit with little traffic', MAX_RECALL: 'On max recall with light traffic',
            NEEDS_GREEN: 'Runs to its limit with split failures'}[check]
    return f"{what}, {severity:.1f} hours a day on {seen} of {nd} weekdays."


def _bottleneck(con, counts: pd.DataFrame, zones: pd.DataFrame) -> tuple:
    """(listed phases: DeviceId, Phase, days_seen, hours; their 15-minute points for the chart)."""
    con.register('cnt_in', counts)
    con.register('zones', zones)
    con.execute("""CREATE TEMP TABLE zl AS SELECT DeviceId, Phase, sum(lanes) AS lanes FROM zones GROUP BY ALL""")
    con.execute("""CREATE TEMP TABLE vb AS
        SELECT CAST(c.DeviceId AS VARCHAR) AS DeviceId, z.Phase, c.TimeStamp AS ts, sum(c.Total) AS veh
        FROM cnt_in c JOIN zones z ON z.DeviceId = CAST(c.DeviceId AS VARCHAR) AND z.Detector = CAST(c.Detector AS INTEGER)
        GROUP BY ALL""")
    con.execute(f"""CREATE TEMP TABLE b2 AS
        SELECT b.DeviceId, b.Phase, b.ts, b.day, b.hr, b.cyc, b.sf, b.gor AS occ, b.green AS G, coalesce(v.veh, 0) AS veh, l.lanes,
               coalesce(v.veh, 0) / (b.cyc * (b.green - {START_UP_LOSS}) / {HEADWAY} * l.lanes) AS x,
               coalesce(v.veh, 0) / (b.green * b.cyc * l.lanes) * 3600 AS vph
        FROM bins b JOIN zl l USING (DeviceId, Phase) LEFT JOIN vb v USING (DeviceId, Phase, ts)
        WHERE b.green >= {BLOCK_MIN_GREEN} AND NOT EXISTS (
            SELECT 1 FROM faults f WHERE f.DeviceId = b.DeviceId AND f.Phase = b.Phase
              AND f.s < b.ts + INTERVAL 15 MINUTE AND f.e > b.ts)""")
    con.execute(f"""CREATE TEMP TABLE bp AS SELECT DeviceId, Phase, quantile_cont(x, {BLOCK_TOP_QUANTILE}) AS top FROM b2
        GROUP BY ALL HAVING count(*) >= {BLOCK_MIN_BINS} AND sum(veh) >= {BLOCK_MIN_VEHICLES}""")
    con.execute(f"""CREATE TEMP TABLE bf AS
        SELECT b.*, b.occ >= {BLOCK_OCCUPANCY} AS hi,
               b.occ >= {BLOCK_OCCUPANCY} AND b.sf >= {BLOCK_SPLIT_FAIL} * b.cyc AND b.veh > 0
                 AND b.x < {BLOCK_SHARE} * least(p.top, 1) AS blocked
        FROM b2 b JOIN bp p USING (DeviceId, Phase)""")
    listed = con.execute(f"""
        WITH day_counts AS (SELECT DeviceId, Phase, day, count(*) AS nb FROM bf
                            WHERE blocked AND hr >= {HOUR_FROM} AND hr < {HOUR_TO} GROUP BY ALL HAVING count(*) >= {BLOCK_BINS_A_DAY}),
             totals AS (SELECT DeviceId, Phase, count(*) FILTER (WHERE blocked) AS nb, count(*) FILTER (WHERE hi) AS nh
                        FROM bf WHERE hr >= {HOUR_FROM} AND hr < {HOUR_TO} GROUP BY ALL)
        SELECT d.DeviceId, d.Phase, count(*) AS days_seen, sum(d.nb) / 4.0 AS hours
        FROM day_counts d JOIN totals t USING (DeviceId, Phase)
        GROUP BY ALL HAVING count(*) >= {STATE_DAYS} AND any_value(t.nb) < 0.5 * any_value(t.nh)""").df()
    if listed.empty:
        return listed, pd.DataFrame()
    con.register('listed', listed[['DeviceId', 'Phase']])
    points = con.execute("""SELECT DeviceId, Phase, occ, vph, blocked AND hr >= {f} AND hr < {t} AS blocked
                            FROM bf JOIN listed USING (DeviceId, Phase)""".format(f=HOUR_FROM, t=HOUR_TO)).df()
    return listed, points


def _series(values: pd.Series) -> list:
    return [None if pd.isna(v) else round(float(v), 1) for v in values]


def _chart_data(mine: pd.DataFrame, points: Optional[pd.DataFrame], check: str, served: set) -> str:
    """The chart's numbers as JSON (kept with the alert, so an ongoing alert is drawn from what was judged): by hour of
    day for every measured phase of the signal (mine: its hourly rows), and for a Downstream Bottleneck the phase's
    15-minute points."""
    hours = range(HOUR_FROM, HOUR_TO)
    data = {'hours': list(hours), 'phases': {}}
    for ph, grp in mine.groupby('Phase'):
        g = grp.set_index('hr').reindex(hours)
        data['phases'][str(int(ph))] = {
            'mo': _series(100 * g['lim_n'] / g['all_n'].where(g['all_n'] > 0)),
            'sf': _series(100 * g['sf'] / g['cyc']),
            'gor': _series(100 * g['occ'] / g['cyc']),
            'sfgo': _series(100 * g['sf_go'] / g['cyc']) if check == PREMATURE else None,
        }
    data['missing'] = sorted(int(p) for p in served - {int(p) for p in mine['Phase']})
    if check == BOTTLENECK and points is not None and not points.empty:
        data['points'] = [[round(float(o) * 100, 1), round(float(v)), bool(b)]
                          for o, v, b in zip(points['occ'], points['vph'], points['blocked'])]
    return json.dumps(data, separators=(',', ':'))


# ======================================================================================================================
# Report: which alerts each report lists, tables and charts
# ======================================================================================================================

def _ordered(alerts: pd.DataFrame) -> pd.DataFrame:
    return alerts.sort_values(['Rank', 'Severity'], ascending=[True, False], kind='stable')


def listed_alerts(alerts: Optional[pd.DataFrame], signals: Optional[pd.DataFrame], rows: int) -> pd.DataFrame:
    """The alerts some report lists: per check, the first `rows` of All Regions and of each region. Only these are filed
    as told; the others come up on a later run. rows 0: all."""
    if alerts is None or alerts.empty or signals is None or not rows or rows <= 0:
        return alerts
    regions = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')['Region']
    frame = alerts.assign(_region=alerts['DeviceId'].astype(str).map(regions))
    frame = frame[frame['_region'].notna()]
    keep = set()
    for _, grp in frame.groupby('Check', sort=False):
        grp = _ordered(grp)
        keep.update(grp.head(rows).index)
        for _, part in grp.groupby('_region'):
            keep.update(part.head(rows).index)
    return alerts.loc[sorted(keep)]


def prepare_insight_table(alerts: Optional[pd.DataFrame], signals_df: Optional[pd.DataFrame], region, max_rows: int,
                          check: str) -> tuple:
    """(table rows, total) of one check in one report, worst first: Signal, Phase (not for Max Capacity), Phases with
    Extra Time (Needs More Green) and Details, with the Ongoing date for held-back repeats."""
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
    rows = _ordered(rows).head(max_rows if max_rows > 0 else len(rows))
    columns = ['Signal'] + ([] if check == CAPACITY else ['Phase']) + (['Phases with Extra Time'] if check == NEEDS_GREEN else []) + ['Details']
    rows = rows.assign(Signal=rows['Name'], Phase=rows['Phase'].astype(int), Details=rows['Detail'],
                       **{'Phases with Extra Time': rows['Donors']})
    rows, out_columns = _add_ongoing_column(rows, columns)
    return rows[out_columns].reset_index(drop=True), total


LEGEND_ROW = 0.3        # inches per legend row above the panels
TITLE_H = 0.62          # title and detail line
PANEL_TITLE_H = 0.3     # room for the panels' own titles under the legend
PANEL_H = 1.65          # one row of panels
AXIS_H = 0.45           # x labels under a row


def _style(ax, title: str, xlabel: str, ylabel: str):
    """The report's chart look: light grey plot area, dashed grid, titles and labels as the detector charts have them."""
    ax.set_facecolor('#fafafa')
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_title(title, fontsize=10, loc='left', color=INK)
    ax.set_xlabel(xlabel, fontsize=9.5)
    ax.set_ylabel(ylabel, fontsize=9.5)
    ax.tick_params(labelsize=9)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)


def _panel(ax, title: str, series: dict, me, scale: str, dashed: Optional[dict] = None):
    if scale == 'gor':
        ax.axhspan(*IDEAL_OCCUPANCY, color=BAND, alpha=0.15, lw=0, zorder=1)
    for k, block in enumerate([series] + ([dashed] if dashed else [])):
        for ph, values in block.items():
            focus = me is None or ph == me
            lw = 1.6 if me is None else (3.0 if ph == me else 1.2)
            ax.plot(range(HOUR_FROM, HOUR_TO), [np.nan if v is None else v for v in values], color=PHASE_COLOURS.get(ph, '#999999'),
                    lw=lw * (0.85 if k else 1), ls='--' if k else '-', alpha=1.0 if focus else 0.8, zorder=3 if ph == me else 2)
    ax.set_xticks([7, 10, 13, 16])
    ax.set_xticklabels(['7:00', '10:00', '13:00', '16:00'])
    ax.set_xlim(HOUR_FROM - 0.3, HOUR_TO - 0.7)
    _style(ax, title, '', '% of green' if scale == 'gor' else '% of cycles')


def _scatter(ax, points: list):
    p = np.array([[o, v, b] for o, v, b in points], dtype=float) if points else np.zeros((0, 3))
    if len(p):
        free, blocked = p[p[:, 2] == 0], p[p[:, 2] == 1]
        ax.scatter(free[:, 0], free[:, 1], color='#9a9a9a', s=5, alpha=0.5, linewidth=0)
        ax.scatter(blocked[:, 0], blocked[:, 1], color=RED, s=9, linewidth=0, zorder=3)
        levels = np.minimum(np.floor(p[:, 0] / 10), 9)
        xs, ys = [], []
        for lv in range(10):
            sel = p[levels == lv]
            if len(sel) >= 8:
                xs.append(lv * 10 + 5)
                ys.append(sel[:, 1].mean())
        ax.plot(xs, ys, color=BLUE, lw=2.2, zorder=4)
        ax.set_ylim(0, max(600.0, float(np.quantile(p[:, 1], 0.995)) * 1.1))
    ax.set_xlim(0, 100)
    _style(ax, 'Volume against Occupancy', 'Green occupancy, %', 'Vehicles / hour of green / lane')


def insight_chart(alert, name: str):
    """By hour of day, one line per phase, the phase in question thick: Premature Gap-Out shows split failures (dashed:
    with a gap-out) and max-outs; the other checks max-outs, split failures and green occupancy with its ideal range;
    Downstream Bottleneck adds volume against occupancy, with its own legend."""
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
    from .visualization import new_figure
    data = json.loads(alert['ChartData'])
    phases = {int(p): v for p, v in sorted(data['phases'].items(), key=lambda kv: int(kv[0]))}
    check = alert['Check']
    me = None if check == CAPACITY else int(alert['Phase'])
    mo = {p: v['mo'] for p, v in phases.items()}
    sf = {p: v['sf'] for p, v in phases.items()}
    gor = {p: v['gor'] for p, v in phases.items()}
    if check == PREMATURE:
        panels = [(SPLIT_TITLE, sf, 'sf', {p: v['sfgo'] for p, v in phases.items() if v.get('sfgo')}), (MAXOUT_TITLE, mo, 'full', None)]
    else:
        panels = [(MAXOUT_TITLE, mo, 'full', None), (SPLIT_TITLE, sf, 'sf', None), (GOR_TITLE, gor, 'gor', None)]
    bottleneck = check == BOTTLENECK
    rows = 2 if bottleneck else 1
    legends = 2 if bottleneck else 1
    height = TITLE_H + legends * LEGEND_ROW + rows * (PANEL_TITLE_H + PANEL_H + AXIS_H)
    figure = new_figure((CHART_WIDTH, height), dpi=110)
    columns = 2 if bottleneck else len(panels)
    grid = figure.add_gridspec(rows, columns, left=0.08, right=0.985, bottom=AXIS_H / height,
                               top=1 - (TITLE_H + legends * LEGEND_ROW + PANEL_TITLE_H) / height,
                               hspace=(AXIS_H + PANEL_TITLE_H) / PANEL_H, wspace=0.3)
    axes = [figure.add_subplot(grid[k // columns, k % columns]) for k in range(rows * columns)]
    if bottleneck:
        _scatter(axes[0], data.get('points') or [])
        chart_axes = axes[1:]
    else:
        chart_axes = axes
    values = [v for _, block, sc, dashed in panels if sc == 'sf' for b in (block, dashed or {}) for s in b.values() for v in s
              if v is not None]
    sf_top = max([10.0] + ([max(values) * 1.15] if values else []))
    for ax, (title, block, scale, dashed) in zip(chart_axes, panels):
        _panel(ax, title, block, me, scale, dashed)
        ax.set_ylim(0, sf_top if scale == 'sf' else 103)
    if data.get('missing'):
        chart_axes[-1].set_xlabel(f"Not measured: Ph {', '.join(map(str, data['missing']))}", fontsize=9, color=MUTED)

    # the title and the alert's own words, as the detector charts have them
    figure.text(0.01, 1 - 0.08 / height, name + ('' if me is None else f"  ·  Ph{me}"), fontsize=11, fontweight='bold', va='top')
    day = pd.Timestamp(alert['Date'])
    figure.text(0.01, 1 - 0.34 / height, f"{alert['Detail']}  Weekdays to {day:%a} {day.month}/{day.day}.", fontsize=9.5,
                color='#333333', va='top')
    blank = Line2D([], [], alpha=0)
    handles = [Line2D([0], [0], color=PHASE_COLOURS.get(p, '#999999'), lw=1.6 if me is None else (3.0 if p == me else 1.2),
                      alpha=1.0 if me is None or p == me else 0.8) for p in phases]
    labels = [f'Ph {p}' for p in phases]
    if check == PREMATURE:
        handles += [blank, Line2D([0], [0], color=INK, lw=1.4), Line2D([0], [0], color=INK, lw=1.4, ls='--')]
        labels += ['(', 'All Split Fail', 'Split Fail with Gap Out)']
    else:
        handles.append(Patch(color=BAND, alpha=0.3, lw=0))
        labels.append(f'Ideal Occupancy {IDEAL_OCCUPANCY[0]}-{IDEAL_OCCUPANCY[1]}%')
    row_y = 1 - TITLE_H / height
    figure.legend(handles, labels, loc='upper left', bbox_to_anchor=(0.01, row_y), ncol=len(labels), fontsize=9,
                  frameon=False, handlelength=1.8, columnspacing=0.9, handletextpad=0.4)
    if bottleneck:
        dot = lambda colour: Line2D([0], [0], ls='', marker='o', ms=5, color=colour)
        figure.legend([Line2D([0], [0], color=BLUE, lw=2.2), dot('#9a9a9a'), dot(RED)],
                      ['Average at each occupancy', '15 minutes', '15 minutes backed up'], loc='upper left',
                      bbox_to_anchor=(0.01, row_y - LEGEND_ROW / height), ncol=3, fontsize=9, frameon=False,
                      handlelength=1.8, columnspacing=1.2, handletextpad=0.4)
    figure.report_own_proportions = True     # drawn page-wide at its own height
    return figure


def create_insight_plots(alerts: Optional[pd.DataFrame], signals: Optional[pd.DataFrame], rows: int) -> list:
    """One chart per listed alert, as (figure, scope, check) for 'All Regions' and each region, in the tables' order."""
    if alerts is None or alerts.empty or signals is None or 'ChartData' not in alerts.columns:
        return []
    info = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')
    alerts = alerts.assign(_name=alerts['DeviceId'].astype(str).map(info['Name']))
    alerts = alerts[alerts['_name'].notna()]
    regions = alerts['DeviceId'].astype(str).map(info['Region'])
    drawn, figures = {}, []
    for scope in ['All Regions'] + sorted(regions.dropna().unique().tolist()):
        chosen = alerts if scope == 'All Regions' else alerts[regions == scope]
        for check, section in chosen.groupby('Check', sort=False):
            for idx, alert in _ordered(section).head(rows if rows and rows > 0 else len(section)).iterrows():
                if idx not in drawn:
                    drawn[idx] = insight_chart(alert, alert['_name'])
                figures.append((drawn[idx], scope, check))
    return figures


def chart_height_inches(check: str) -> float:
    """The chart's height on the page, drawn 6.5 inches wide."""
    rows = 2 if check == BOTTLENECK else 1
    return (TITLE_H + rows * LEGEND_ROW + rows * (PANEL_TITLE_H + PANEL_H + AXIS_H)) * 6.5 / CHART_WIDTH
