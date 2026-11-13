"""How urgent each flagged detector or ped button is: the order and the Impact text of the Detector Health table.

The fault's hours are compared, phase by phase, with the same clock hours on other days of the same day type. Each row gets a tier:

- A, service lost: during the fault its own phase got far fewer greens, or ped walks, than normal for those hours.
- B, harm to other phases: during the fault other phases waited longer or maxed out more, weighted by the traffic on them at
  those hours, so night effects count for less, and the fault is the likelier cause of the rise (see below).
- C, data only: no operational effect measured. The detector still needs fixing for its data.
- Not measured: phase unknown, no normal to compare, too few services, or the whole signal stopped running.

Max outs on the detector's own phase do not count: a phase that maxes out from its own detector is getting more time.

Is the rise the fault, or an ordinary day? Waits differ from day to day with no fault at all, so a rise over the usual day proves
little. For every fault day the traffic-weighted change of the other phases is worked out, and the same for each normal day in
the same clock hours. Bayes' rule then weighs the explanations of the fault days (effect_probability): one more ordinary day,
drawn from the spread of the normal days; an unusual day for another reason, as often as the normal days had one; something
else that changed for all the fault days, when none of the report's own days is a normal day that would have shown it; or an
effect of the fault. It starts from how likely a fault of that kind is to hurt other phases at all (HARM_PRIOR, times the
health check's confidence in the fault, raised when the fault visibly made its own phase run more). A row is tier B only when
the chance that the fault caused a rise of HARM_WAIT_SECONDS or more (or HARM_MAX_OUT_POINTS for max outs) reaches
HARM_PROBABILITY. A few seconds count when the normal days agree closely; a large rise does not when they are few or scattered.

The assumptions are cautious, so the chances run low. They were checked on 5 to 9 November 2026 with placebo faults: the real
fault times moved to signals with no fault then. Of 1,591 placebo events 0.6% were called harm at HARM_PROBABILITY, against
1.3% for the fixed rise of 5 s this replaced; of 764 real events, 6.4% against 9.4%.

Faults that overlap. A change in the hours two faults share cannot be split between them. Faults of one signal with much the
same times (GROUP_OVERLAP) are one event: judged once, together, and carrying the same Group, so the tables tell them as one
row. An event is judged on the hours no other event of its signal was going on, when it has enough of them (SOLO_MIN_BINS,
SOLO_SHARE), and otherwise on all its hours.

This changes the order and the Impact text. It never changes which detectors alert.

Measured: greens, walks, presses, max outs and wait during the fault hours. Inferred: that the detector caused the change, and
the vehicle-hours, which multiply a per-service wait by rough detector counts.
"""

from typing import Optional

import duckdb
import json
import math

import numpy as np
import pandas as pd

from .duckdb_utils import TableLike, to_pandas

MIN_CONFIDENCE = 0.5    # the health check's own suspect limit
MIN_BIN_MINUTES = 5.0   # a 15-minute bin counts as at fault with this many minutes of it at fault
MIN_NORMAL_DAYS = 3
MIN_NORMAL_COVER = 0.8  # a normal day holds at least this share of the fault's clock bins
COFAULT_SHARE = 0.25    # another phase with its own fault in this share of the row's bins is left out: its change may be its own
# Tier A, vehicle: only a phase that almost disappears can be told from chance.
LOST_MIN_NORMAL_SERVICES = 8
LOST_RATIO = 0.3
LOST_RATIO_GAP = 0.5
LOST_MAX_OWN_MAX_OUT_UP = 20.0  # a phase that runs longer, and so less often, has not lost service
# Tier A, ped.
LOST_PED_MIN_NORMAL_WALKS = 3.0
LOST_PED_RATIO = 0.4
# Tier B: a rise this large, this likely to be the fault's, on this much traffic.
HARM_WAIT_SECONDS = 3.0
HARM_VEHICLE_HOURS_PER_DAY = 2.0
HARM_MAX_OUT_POINTS = 15.0
HARM_MAX_OUT_VEHICLES_PER_DAY = 100.0
HARM_PROBABILITY = 0.5
# How likely a fault of each kind is to hurt other phases at all, before looking at the phases. A stuck input holds or calls a
# phase; a detector that counts wrong does so less often (it can: one that sticks, or radar losing vehicles in rain).
HARM_PRIOR = {'Stuck on': 0.3, 'Silent': 0.15, 'Counts low': 0.15, 'Counts high': 0.15, 'No daily pattern': 0.05,
              'Ped stuck on': 0.3, 'Ped walks, no presses': 0.3, 'Ped erratic': 0.15, 'Ped silent': 0.05, 'Ped never calls': 0.05}
# The size of an effect when there is one: unknown, small more likely than large, half of them under this.
TYPICAL_WAIT_EFFECT = 10.0
TYPICAL_MAX_OUT_EFFECT = 20.0
# The day-to-day spread assumed before seeing a signal's normal days, worth NOISE_WEIGHT days: what signals usually show.
# The weighted wait spreads by about WAIT_NOISE_SHARE of its usual value and never under WAIT_NOISE seconds; the weighted
# share of max outs by MAX_OUT_NOISE points. Two normal days that happen to agree do not make a small rise certain.
WAIT_NOISE = 1.0
WAIT_NOISE_SHARE = 0.12
MAX_OUT_NOISE = 6.0
NOISE_WEIGHT = 2.0
# Fault days are compared when they have this many normal days. With one, the assumed spread carries the comparison.
EFFECT_MIN_DAYS = 1
# A normal day this many spreads from the others is unusual and set aside. Before seeing the days, UNUSUAL_SHARE of days are
# taken to be unusual, a belief worth UNUSUAL_PRIOR_DAYS days. An unusual day lands within UNUSUAL_WIDTH typical effects or so.
UNUSUAL_DAY = 4.0
UNUSUAL_SHARE = 0.03
UNUSUAL_PRIOR_DAYS = 3.0
UNUSUAL_WIDTH = 3.0
# The chance that something else changed how the signal ran over all the fault days, when no normal day of the report's own
# days is there to show it, and how large such a change is against a fault's typical effect.
OTHER_CHANGE = 0.1
OTHER_WIDTH = 0.3
# A fault that visibly made its own phase run more (this many times its normal greens or walks, or this many points more max
# outs) is that much more likely to have cost the other phases time.
ACTED_RATIO = 1.5
ACTED_MAX_OUT_UP = 20.0
ACTED_PRIOR = 2.0
MAX_PRIOR = 0.6
# Faults of one signal are one group when they overlap by this share of their time together.
GROUP_OVERLAP = 0.5
# A group is judged on the bins it has to itself when it has this many, and this share of its own.
SOLO_MIN_BINS = 4
SOLO_SHARE = 0.25
UNMEASURED_NORMAL_SERVICES = 4.0
# A phase is named in the Impact text from these changes, largest first, up to NAMED_PHASES.
NAMED_WAIT_SECONDS = 2.0
NAMED_MAX_OUT_POINTS = 5.0
NAMED_PHASES = 3
# Ped buttons silent all window.
SILENT_EXPECTED_PRESSES = 30.0
SILENT_MIN_DAY_BINS = 80

SERVICE_LOST, OTHER_PHASES, DATA_ONLY, NOT_MEASURED = 'Service lost', 'Harm to other phases', 'Data only', 'Not measured'
TIER_ORDER = {SERVICE_LOST: 1, OTHER_PHASES: 2, DATA_ONLY: 3, NOT_MEASURED: 3}

VEHICLE, PED = 'Vehicle', 'Ped'
# What kind of fault each finding is. Lane closures, timing plans, borderline rows and notes are not faults here.
FAULT_KINDS = {'stuck on': 'Stuck on', 'stuck on (together)': 'Stuck on', 'count zone held ON': 'Stuck on',
               'silent': 'Silent', 'silent at night': 'Silent',
               'counts dropped': 'Counts low', 'misses vehicles at night': 'Counts low',
               'too-fast actuations': 'Counts high', 'too many for the traffic': 'Counts high',
               'counts too high': 'Counts high', 'spiky counts': 'Counts high', 'busy at night': 'Counts high',
               'no daily pattern': 'No daily pattern'}
PED_KINDS = {'stuck on': 'Ped stuck on', 'long hold': 'Ped stuck on', 'sudden stop': 'Ped silent',
             'never calls a walk': 'Ped never calls', 'machine-regular': 'Ped erratic', 'sustained pressing': 'Ped erratic',
             'walks without presses': 'Ped walks, no presses'}
# Tier C and Not measured: the kinds most likely to hurt come first (share of each that did, in the study).
KIND_ORDER = {'Stuck on': 1, 'Silent': 2, 'Counts low': 3, 'Counts high': 4, 'No daily pattern': 5,
              'Ped stuck on': 1, 'Ped silent': 2, 'Ped never calls': 2, 'Ped erratic': 4, 'Ped walks, no presses': 4}
KIND_WORDS = {'Stuck on': 'stuck on', 'Silent': 'silent', 'Counts low': 'counts fall', 'Counts high': 'counts too high',
              'No daily pattern': 'no daily pattern', 'Ped stuck on': 'stuck on', 'Ped silent': 'silent',
              'Ped never calls': 'never calls a walk', 'Ped erratic': 'pressing like a machine',
              'Ped walks, no presses': 'walks without presses'}

RANK_COLUMNS = ['DeviceId', 'DetectorType', 'Detector', 'Phase', 'Kind', 'KindOrder', 'FaultHours', 'Tier', 'SignalWide', 'Lost',
                'ExtraVehHoursPerDay', 'MaxOutVehPerDay', 'OtherWaitUp', 'OtherMaxOutUp', 'OwnRatio', 'OtherRatio', 'HurtPhases',
                'ImpactText', 'ImpactLines', 'Coordinated', 'HarmProbability', 'Group']


def _sql_list(values) -> str:
    return ', '.join("'" + str(value).replace("'", "''") + "'" for value in values)


def _case(column: str, mapping: dict) -> str:
    return f"CASE {column} " + ' '.join(f"WHEN '{key}' THEN '{value}'" for key, value in mapping.items()) + ' END'


# The repeated hour of the clock change (first Sunday of November, 01:00 to 02:00) is not stored.
def _clock_change(column: str) -> str:
    return (f"(month({column}) = 11 AND day({column}) <= 7 AND isodow({column}) = 7 AND hour({column}) = 1)")


def usual_detector_counts(counts: TableLike) -> pd.DataFrame:
    """What the detectors usually count (DeviceId, Detector, weekend, clock, Total, Samples), for the traffic weights, from a table with
    TimeStamp, DeviceId, Detector, Total. Read over as many days as there are, so that one fault does not distort it."""
    if counts is None:
        return None
    con = duckdb.connect()
    try:
        con.register('counts', to_pandas(counts))
        return con.execute("""SELECT DeviceId::VARCHAR AS DeviceId, Detector::INT AS Detector, isodow(TimeStamp) >= 6 AS weekend,
                                     cast(TimeStamp AS TIME) AS clock, avg(Total) AS Total, count(*) AS Samples
                              FROM counts GROUP BY ALL""").df()
    finally:
        con.close()


def _groups(rows: list, overlaps: list) -> dict:
    """{row: the lowest row of its group} for rows joined by the overlaps (pairs of rows)."""
    lead = {row: row for row in rows}

    def find(row):
        while lead[row] != row:
            lead[row] = lead[lead[row]]
            row = lead[row]
        return row

    for first, second in overlaps:
        a, b = find(first), find(second)
        lead[max(a, b)] = min(a, b)
    return {row: find(row) for row in rows}


def _judged_bins(con) -> None:
    """jb: the bins each row (fr, fb) is judged on. judged: rid, alone, grp. ex: the bins that stay out of its normal.

    Rows of one signal whose fault times are much the same (GROUP_OVERLAP of their time together) are one group: one event, told
    once. A group is judged on the bins no other group of its signal is at fault in, when it has enough of them (alone), and
    otherwise on all its bins. Every row of a group is judged on the same bins."""
    rows = [row[0] for row in con.execute("SELECT rid FROM fr ORDER BY rid").fetchall()]
    overlaps = con.execute(f"""
        WITH n AS (SELECT rid, count(*) AS n FROM fb GROUP BY 1)
        SELECT a.rid AS first, b.rid AS second
        FROM fb a JOIN fr ra ON ra.rid = a.rid JOIN fr rb ON rb.did = ra.did AND rb.rid > ra.rid JOIN fb b ON b.rid = rb.rid AND b.ts = a.ts
        JOIN n na ON na.rid = a.rid JOIN n nb ON nb.rid = b.rid
        GROUP BY a.rid, b.rid, na.n, nb.n HAVING count(*) >= {GROUP_OVERLAP} * (na.n + nb.n - count(*))""").fetchall()
    groups = _groups(rows, overlaps)
    con.register('groups_in', pd.DataFrame({'rid': rows, 'grp': [groups[row] for row in rows]}, dtype='int64'))
    con.execute("CREATE TEMP TABLE gb AS SELECT DISTINCT g.grp, r.did, b.ts FROM groups_in g JOIN fr r USING (rid) JOIN fb b USING (rid)")
    con.execute("""CREATE TEMP TABLE own AS
        SELECT g.grp, g.ts FROM gb g JOIN (SELECT did, ts, count(*) AS n FROM gb GROUP BY ALL) y USING (did, ts) WHERE y.n = 1""")
    con.execute(f"""CREATE TEMP TABLE judged AS
        SELECT g.rid::INT AS rid, g.grp::INT AS grp, coalesce(o.n, 0) >= {SOLO_MIN_BINS} AND coalesce(o.n, 0) >= {SOLO_SHARE} * a.n AS alone
        FROM groups_in g JOIN (SELECT grp, count(*) AS n FROM gb GROUP BY 1) a USING (grp)
        LEFT JOIN (SELECT grp, count(*) AS n FROM own GROUP BY 1) o USING (grp)""")
    con.execute("""CREATE TEMP TABLE jb AS
        SELECT j.rid, o.ts FROM judged j JOIN own o USING (grp) WHERE j.alone
        UNION ALL
        SELECT j.rid, g.ts FROM judged j JOIN gb g USING (grp) WHERE NOT j.alone""")
    # Every bin a detector of the row's group has any finding, whichever kind, stays out of its normal.
    con.execute("""CREATE TEMP TABLE ex AS
        SELECT DISTINCT j.rid, b.ts
        FROM judged j JOIN judged m ON m.grp = j.grp JOIN fr rm ON rm.rid = m.rid
        JOIN fr r2 ON r2.did = rm.did AND r2.DetectorType = rm.DetectorType AND r2.Detector = rm.Detector
        JOIN fb b ON b.rid = r2.rid""")


def _prepare(con, findings, detectors, ped_inputs, terminations, phase_wait, has_data, pedestrian, usual_counts, coordination,
             window_start, window_end, baseline_days, wait_days):
    """The tables of the comparison on con: fr rows, fb their bins, jb the bins they are judged on, pb the phases' bins, tr
    traffic, coord coordinated hours."""
    start, end = f"TIMESTAMP '{pd.Timestamp(window_start)}'", f"TIMESTAMP '{pd.Timestamp(window_end)}'"
    con.register('findings_in', findings)
    con.register('detectors_in', detectors)
    con.register('ped_inputs_in', ped_inputs)
    con.execute(f"""CREATE TEMP TABLE fx AS
        SELECT DetectorType, DeviceId, Detector::INT AS Detector,
               CASE WHEN DetectorType = '{PED}' THEN {_case('Label', PED_KINDS)} ELSE {_case('Label', FAULT_KINDS)} END AS Kind,
               greatest("Start", {start}) AS s, least("End", {end}) AS e, Confidence
        FROM findings_in
        WHERE Confidence >= {MIN_CONFIDENCE} AND "Start" < {end} AND "End" > {start}
          AND ((DetectorType = '{PED}' AND Label IN ({_sql_list(PED_KINDS)})) OR (DetectorType <> '{PED}' AND Label IN ({_sql_list(FAULT_KINDS)})))""")
    con.execute("""CREATE TEMP TABLE dmap AS SELECT DeviceId, row_number() OVER ()::INT AS did FROM (SELECT DISTINCT DeviceId FROM fx)""")
    con.execute("""CREATE TEMP TABLE fr AS
        SELECT row_number() OVER (ORDER BY f.DeviceId, f.DetectorType, f.Detector, f.Kind)::INT AS rid, f.*, m.did,
               CASE WHEN f.DetectorType = 'Ped' THEN i.Phase ELSE d.Phase END::INT AS Phase, d.Function
        FROM (SELECT DeviceId, DetectorType, Detector, Kind, least(max(Confidence), 1.0) AS Confidence FROM fx GROUP BY ALL) f
        JOIN dmap m USING (DeviceId)
        LEFT JOIN (SELECT DeviceId, Detector::INT AS Detector, any_value(Phase) AS Phase, any_value(Function) AS Function FROM detectors_in GROUP BY ALL) d
               ON d.DeviceId = f.DeviceId AND d.Detector = f.Detector
        LEFT JOIN (SELECT DeviceId, Input::INT AS Input, any_value(Phase) AS Phase FROM ped_inputs_in GROUP BY ALL) i
               ON i.DeviceId = f.DeviceId AND i.Input = f.Detector""")
    con.execute(f"""CREATE TEMP TABLE fb AS
        SELECT rid, ts FROM (
          SELECT r.rid, x.ts, sum(epoch(least(x.e, x.ts + INTERVAL 15 MINUTE) - greatest(x.s, x.ts))) / 60.0 AS mins
          FROM (SELECT *, unnest(generate_series(time_bucket(INTERVAL 15 MINUTE, s), e - INTERVAL 1 MICROSECOND, INTERVAL 15 MINUTE)) AS ts FROM fx WHERE e > s) x
          JOIN fr r USING (DeviceId, DetectorType, Detector, Kind) GROUP BY ALL)
        WHERE mins >= {MIN_BIN_MINUTES} AND NOT {_clock_change('ts')}""")
    _judged_bins(con)

    history = f"TimeStamp >= {start} - INTERVAL {baseline_days} DAY AND TimeStamp < {end}"
    con.register('terminations_in', terminations)
    con.register('wait_in', phase_wait)
    con.register('has_data_in', has_data)
    con.register('ped_in', pedestrian)
    con.register('coordination_in', coordination)
    con.execute(f"""CREATE TEMP TABLE pb AS
      WITH h AS (SELECT DISTINCT TimeStamp AS ts, m.did FROM has_data_in JOIN dmap m USING (DeviceId) WHERE {history}),
      t AS (SELECT TimeStamp AS ts, m.did, Phase, coalesce(sum(Total) FILTER (PerformanceMeasure IN ('MaxOut', 'ForceOff')), 0) AS maxouts,
                   coalesce(sum(Total) FILTER (PerformanceMeasure = 'GapOut'), 0) AS gapouts
            FROM terminations_in JOIN dmap m USING (DeviceId) WHERE {history} GROUP BY ALL),
      w AS (SELECT TimeStamp AS ts, m.did, Phase, avg(AvgPhaseWait) AS wait FROM wait_in JOIN dmap m USING (DeviceId) WHERE {history} GROUP BY ALL),
      p AS (SELECT TimeStamp AS ts, m.did, Phase, sum(PedServices) AS walks, sum(PedActuation) AS presses FROM ped_in JOIN dmap m USING (DeviceId) WHERE {history} GROUP BY ALL),
      c AS (SELECT TimeStamp AS ts, m.did, max(CycleLength) AS cl FROM coordination_in JOIN dmap m USING (DeviceId) WHERE {history} GROUP BY ALL),
      ph AS (SELECT DISTINCT did, Phase FROM (SELECT did, Phase FROM t UNION SELECT did, Phase FROM w UNION SELECT did, Phase FROM p WHERE walks > 0 OR presses > 0)
             WHERE Phase BETWEEN 1 AND 16)
      SELECT h.did, ph.Phase::SMALLINT AS Phase, h.ts, cast(h.ts AS DATE) AS dy, cast(h.ts AS TIME) AS clock, isodow(h.ts) >= 6 AS wk,
             coalesce(t.maxouts, 0)::INT AS maxouts, coalesce(t.gapouts, 0)::INT AS gapouts, w.wait::FLOAT AS wait,
             coalesce(p.walks, 0)::INT AS walks, coalesce(p.presses, 0)::INT AS presses, c.cl::SMALLINT AS cl
      FROM h JOIN ph USING (did) LEFT JOIN t USING (ts, did, Phase) LEFT JOIN w USING (ts, did, Phase) LEFT JOIN p USING (ts, did, Phase) LEFT JOIN c USING (ts, did)
      WHERE NOT {_clock_change('h.ts')}""")
    # Rough vehicles per bin on each phase on a usual day: its largest detector function, leaving out bike detectors.
    con.register('usual_in', usual_counts)
    con.execute("""CREATE TEMP TABLE tr AS
      WITH a AS (SELECT m.did, d.Phase::SMALLINT AS Phase, d.Function, u.weekend AS wk, u.clock, sum(u.Total) AS v
                 FROM usual_in u JOIN dmap m ON m.DeviceId = u.DeviceId
                 JOIN detectors_in d ON d.DeviceId = u.DeviceId AND d.Detector::INT = u.Detector
                 WHERE d.Phase IS NOT NULL AND coalesce(d.Function, '') <> 'Bike' GROUP BY ALL)
      SELECT did, Phase, wk, clock, max(v)::FLOAT AS veh FROM a GROUP BY ALL""")
    # A phase behaves as coordinated at an hour when the signal runs a cycle, it is served every cycle and almost never gaps out.
    con.execute("""CREATE TEMP TABLE coord AS
      SELECT did, Phase, wk, hour(ts) AS hr, avg(CASE WHEN cl > 0 THEN 1 ELSE 0 END) AS cyc_share,
             median((maxouts + gapouts) * cl / 900.0) FILTER (cl > 0) AS served,
             sum(gapouts) FILTER (cl > 0) * 1.0 / nullif(sum(maxouts + gapouts) FILTER (cl > 0), 0) AS gap_share
      FROM pb GROUP BY ALL""")


def _compare(con, wait_start) -> None:
    """pr: one row per fr row and phase of its signal, the hours it is judged on against the same hours on other days.
    days: the same day by day (the fault day and each normal day), for the Bayes test."""
    con.execute("""CREATE TEMP TABLE ep AS
       SELECT r.rid, r.did, b.ts, cast(b.ts AS DATE) AS fday, cast(b.ts AS TIME) AS clock, isodow(b.ts) >= 6 AS wk FROM fr r JOIN jb b USING (rid)""")
    con.execute("""CREATE TEMP TABLE per_day AS
       SELECT e.rid, e.fday, p.Phase, p.dy, count(*) AS nb, sum(maxouts) AS maxouts, sum(maxouts + gapouts) AS services, sum(walks) AS walks,
              sum(presses) AS presses, avg(wait) AS wait
       FROM ep e JOIN pb p ON p.did = e.did AND p.clock = e.clock AND p.wk = e.wk
       ANTI JOIN ex x ON x.rid = e.rid AND x.ts = p.ts AND p.dy <> e.fday
       GROUP BY ALL""")
    con.execute(f"""CREATE TEMP TABLE norm AS
       WITH fd AS (SELECT rid, fday, Phase, nb FROM per_day WHERE dy = fday),
       o AS (SELECT o.* FROM per_day o JOIN fd f USING (rid, fday, Phase) WHERE o.dy <> o.fday AND o.nb >= {MIN_NORMAL_COVER} * f.nb)
       SELECT rid, fday, Phase, count(*) AS ndays,
              median(services * 1.0 / nb) AS s_n, quantile_cont(services * 1.0 / nb, 0.1) AS s_q10,
              median(100.0 * maxouts / nullif(services, 0)) AS pct_n,
              median(walks * 1.0 / nb) AS wk_n, min(walks * 1.0 / nb) AS wk_min, median(presses * 1.0 / nb) AS pr_n,
              median(wait) FILTER (dy >= DATE '{wait_start}') AS wait_n
       FROM o GROUP BY ALL HAVING count(*) >= {MIN_NORMAL_DAYS}""")
    con.execute("""CREATE TEMP TABLE veh AS
       SELECT e.rid, e.fday, t.Phase, sum(t.veh) AS veh FROM ep e JOIN tr t ON t.did = e.did AND t.clock = e.clock AND t.wk = e.wk GROUP BY ALL""")
    con.execute("""CREATE TEMP TABLE cmp AS
       SELECT f.rid, f.fday, f.Phase, f.nb, f.services, f.maxouts, f.wait, f.walks, f.presses, coalesce(v.veh, 0) AS veh, n.ndays,
              n.s_n * f.nb AS sn, n.s_q10 * f.nb AS sq10, n.pct_n, n.wk_n * f.nb AS wkn, n.wk_min * f.nb AS wkmin, n.wait_n
       FROM per_day f LEFT JOIN norm n USING (rid, fday, Phase) LEFT JOIN veh v USING (rid, fday, Phase) WHERE f.dy = f.fday""")
    con.execute(f"""CREATE TEMP TABLE days AS
       SELECT d.rid, d.fday, d.Phase, d.dy, d.dy = d.fday AS fault, d.wait, d.dy >= DATE '{wait_start}' AS waits,
              100.0 * d.maxouts / nullif(d.services, 0) AS pct, coalesce(v.veh, 0) AS veh
       FROM per_day d JOIN (SELECT rid, fday, Phase, nb FROM per_day WHERE dy = fday) f USING (rid, fday, Phase)
       LEFT JOIN veh v USING (rid, fday, Phase)
       WHERE d.dy = d.fday OR d.nb >= {MIN_NORMAL_COVER} * f.nb""")
    con.execute("""CREATE TEMP TABLE pr AS
       SELECT rid, Phase, sum(nb) / 4.0 AS hours, count(ndays) AS mdays,
              sum(services) FILTER (ndays IS NOT NULL) AS services, sum(sn) AS sn, sum(sq10) AS sq10,
              sum(maxouts) FILTER (ndays IS NOT NULL) AS maxouts, sum(services * pct_n / 100.0) FILTER (ndays IS NOT NULL) AS maxouts_n,
              sum(veh) FILTER (ndays IS NOT NULL) AS veh_m,
              sum(nb * wait) FILTER (wait_n IS NOT NULL) / nullif(sum(nb) FILTER (wait_n IS NOT NULL AND wait IS NOT NULL), 0) AS wait,
              sum(nb * wait_n) FILTER (wait IS NOT NULL) / nullif(sum(nb) FILTER (wait_n IS NOT NULL AND wait IS NOT NULL), 0) AS wait_n,
              sum(veh * (wait - wait_n)) FILTER (wait IS NOT NULL AND wait_n IS NOT NULL) AS extra_veh_s,
              sum(veh) FILTER (wait IS NOT NULL AND wait_n IS NOT NULL) AS veh_w,
              sum(walks) FILTER (ndays IS NOT NULL) AS walks, sum(wkn) AS wkn, sum(wkmin) AS wkmin
       FROM cmp GROUP BY ALL""")
    con.execute(f"""CREATE TEMP TABLE cofault AS
       WITH n AS (SELECT rid, count(*) AS nb FROM ep GROUP BY 1),
       other AS (SELECT DISTINCT r2.did, r2.Phase, r2.DetectorType, r2.Detector, b.ts FROM fr r2 JOIN fb b USING (rid) WHERE r2.Phase IS NOT NULL)
       SELECT e.rid, o.Phase, count(DISTINCT e.ts) * 1.0 / n.nb AS share
       FROM ep e JOIN fr r USING (rid) JOIN n USING (rid)
       JOIN other o ON o.did = e.did AND o.ts = e.ts AND NOT (o.DetectorType = r.DetectorType AND o.Detector = r.Detector)
       GROUP BY e.rid, o.Phase, n.nb""")
    con.execute(f"""CREATE TEMP TABLE pr_flag AS
       SELECT p.*, coalesce(c.share, 0) >= {COFAULT_SHARE} AS own_fault FROM pr p LEFT JOIN cofault c USING (rid, Phase)""")
    con.execute("""CREATE TEMP TABLE own_c AS
       SELECT e.rid, avg(CASE WHEN c.cyc_share >= 0.5 AND c.served >= 0.95 AND c.gap_share <= 0.25 THEN 1 ELSE 0 END) AS coord_share
       FROM ep e JOIN fr r USING (rid) LEFT JOIN coord c ON c.did = e.did AND c.Phase = r.Phase AND c.wk = e.wk AND c.hr = hour(e.ts) GROUP BY ALL""")


def _roll_up(group: pd.DataFrame, days: float) -> dict:
    """The row's numbers over a set of phases."""
    with_services = group[group['services'] > 0]
    weight_m = with_services['veh_m'].sum()
    weight_w = group['veh_w'].sum()
    change = (with_services['maxouts'] - with_services['maxouts_n']) / with_services['services']
    return {
        'OtherWaitUp': group['extra_veh_s'].sum() / weight_w if weight_w > 0 else float('nan'),
        'ExtraVehHoursPerDay': group['extra_veh_s'].sum() / 3600.0 / days,
        'OtherMaxOutUp': (with_services['veh_m'] * 100.0 * change).sum() / weight_m if weight_m > 0 else float('nan'),
        'MaxOutVehPerDay': (with_services['veh_m'] * change).sum() / days,
    }


def _weighted_change(table: pd.DataFrame, fault_day, weights: pd.Series):
    """(the fault day's traffic-weighted change from each phase's usual value, the same for each normal day as a Series by day,
    the usual value weighted the same way) from a table of one measure, a row per day and a column per phase. None when too
    little is known to compare."""
    if fault_day not in table.index:
        return None
    fault, normal = table.loc[fault_day], table.drop(index=fault_day)
    phases = [phase for phase in table.columns
              if pd.notna(fault[phase]) and weights.get(phase, 0.0) > 0 and normal[phase].notna().sum() >= EFFECT_MIN_DAYS]
    if not phases:
        return None
    weight = weights[phases].astype(float)
    usual = normal[phases].median()
    change = float(((fault[phases] - usual) * weight).sum() / weight.sum())
    apart = normal[phases] - usual
    known = apart.notna() * weight
    covered = known.sum(axis=1)
    daily = (apart.fillna(0.0) * weight).sum(axis=1)[covered >= MIN_NORMAL_COVER * weight.sum()] / covered[covered >= MIN_NORMAL_COVER * weight.sum()]
    return (change, daily, float((usual * weight).sum() / weight.sum())) if len(daily) >= EFFECT_MIN_DAYS else None


def _day_changes(days: pd.DataFrame, phases, window_start=None) -> tuple:
    """([(change, normal changes, spread assumed before seeing them)] of the wait, the same of the max outs, whether a normal
    day of the report's own days was among them), one per fault day, over these phases. Waits spread more from day to day where
    they are long."""
    waits, max_outs, control = [], [], False
    days = days[days['Phase'].isin(list(phases))]
    for fault_day, one in days.groupby('fday'):
        weights = one[one['fault']].set_index('Phase')['veh']
        for column, rows, found in (('wait', one[one['waits'] | one['fault']], waits), ('pct', one, max_outs)):
            found_change = _weighted_change(rows.pivot(index='dy', columns='Phase', values=column), fault_day, weights)
            if found_change is not None:
                change, normals, usual = found_change
                noise = max(WAIT_NOISE, WAIT_NOISE_SHARE * usual) if column == 'wait' else MAX_OUT_NOISE
                found.append((change, normals.tolist(), noise))
                control = control or (window_start is not None and bool((pd.to_datetime(normals.index) >= window_start).any()))
    return waits, max_outs, control


def _ordinary_days(normals, noise: float) -> tuple:
    """(middle, spread of the next ordinary day, degrees of freedom, share of days that are unusual, the unusual days) from
    the normal days.

    A normal day far from the others (another fault that day, a crash, a closure) is not left to widen what counts as ordinary:
    it is set aside, up to a third of the days, and counted, since the fault day could be such a day too. `noise` is the spread
    assumed before seeing the days, worth NOISE_WEIGHT of them."""
    values = np.asarray(normals, dtype=float)
    kept = values
    while len(kept) > 2 and len(values) - len(kept) < len(values) // 3:
        far = int(np.argmax(np.abs(kept - np.median(kept))))
        rest = np.delete(kept, far)
        spread = math.sqrt((NOISE_WEIGHT * noise ** 2 + ((rest - rest.mean()) ** 2).sum()) / (NOISE_WEIGHT + len(rest) - 1))
        if abs(kept[far] - np.median(rest)) <= UNUSUAL_DAY * spread:
            break
        kept = rest
    freedom = NOISE_WEIGHT + len(kept) - 1
    variance = (NOISE_WEIGHT * noise ** 2 + ((kept - kept.mean()) ** 2).sum()) / freedom
    unusual = (len(values) - len(kept) + UNUSUAL_PRIOR_DAYS * UNUSUAL_SHARE) / (len(values) + UNUSUAL_PRIOR_DAYS)
    aside = values[~np.isin(values, kept)] if len(kept) < len(values) else values[:0]
    return float(kept.mean()), math.sqrt(variance * (1.0 + 1.0 / len(kept))), freedom, unusual, aside


def effect_probability(changes: list, least: float, prior: float, typical: float, other: float = 0.0) -> float:
    """How likely it is that a fault raised a measure by `least` or more, by Bayes' rule.

    changes: one (change on the fault day, [the same change on each normal day], the day-to-day spread assumed before seeing
    them) per fault day. Three explanations of a fault day are weighed.
    - An ordinary day. What an ordinary day gives is predicted from the normal days: a Student t around their middle, wide when
      they are few or disagree (see _ordinary_days).
    - An unusual day for some other reason, as often as the normal days had one (rare unless they show otherwise). It lands
      near the unusual days they had, or almost anywhere.
    - The fault: an ordinary or unusual day moved up by the fault's effect. Its size is unknown, small more likely than large,
      around `typical`, the same on each fault day.
    `prior` is the chance of an effect before looking at the days. `other` is the chance that something else moved all the
    fault days together (a timing change, road works, the season): it explains them as well as the fault does, and is told from
    it only by its size, which is mostly small (OTHER_WIDTH of `typical`). It is for faults with no normal day among the
    report's own days, which would have shown such a change.
    Returns the chance, after the days, of an effect of `least` or more.
    """
    changes = [change for change in changes if len(change[1]) >= EFFECT_MIN_DAYS]
    if not changes or not 0.0 < prior < 1.0 - other:
        return 0.0
    top = max(20.0 * typical, 3.0 * max(value for value, _, _ in changes), 2.0 * least)
    effect = np.linspace(0.0, top, 4001)
    log_likelihood = np.zeros_like(effect)
    for value, normals, noise in changes:
        middle, spread, freedom, unusual, aside = _ordinary_days(normals, noise)
        constant = math.exp(math.lgamma((freedom + 1.0) / 2.0) - math.lgamma(freedom / 2.0)) / math.sqrt(freedom * math.pi) / spread

        def around(centre):
            return constant * (1.0 + ((value - centre - effect) / spread) ** 2 / freedom) ** (-(freedom + 1.0) / 2.0)

        ordinary = around(middle)
        wide = UNUSUAL_WIDTH * typical
        elsewhere = 1.0 / (math.pi * wide * (1.0 + ((value - middle - effect) / wide) ** 2))
        if len(aside):
            elsewhere = 0.5 * elsewhere + 0.5 * sum(around(day) for day in aside) / len(aside)
        log_likelihood += np.log((1.0 - unusual) * ordinary + unusual * elsewhere)
    likelihood = np.exp(log_likelihood - log_likelihood.max())
    def sizes(scale):
        density = 1.0 / (1.0 + (effect / scale) ** 2)
        return likelihood * density / density.sum()

    weighted = sizes(typical)
    with_effect = prior * weighted.sum()
    enough = prior * weighted[effect >= least].sum()
    something_else = other * sizes(OTHER_WIDTH * typical).sum() if other > 0 else 0.0
    return float(enough / ((1.0 - prior - other) * likelihood[0] + with_effect + something_else))


def _plain(number: float) -> str:
    return f"{number:.0f}"


def _phase_list(items: list) -> str:
    shown = ', '.join(items[:NAMED_PHASES])
    return f"{shown} and {len(items) - NAMED_PHASES} more" if len(items) > NAMED_PHASES else shown


def _impact_text(row: pd.Series, phases: pd.DataFrame) -> tuple:
    """(what the fault did, naming its phases; the phases measured to be hurt)."""
    hours = row['FaultHours']
    over = f"over {hours * 60:.0f} min" if hours < 1 else f"over {round(hours * 2) / 2:g} h"
    tier = row['Tier']
    if tier == SERVICE_LOST:
        phase = int(row['Phase']) if pd.notna(row['Phase']) else None
        if row['DetectorType'] == PED:
            if row['Kind'] == 'Ped never calls':
                return f"Ph{phase}: pressed, no walk ever followed" if phase else 'Pressed, no walk ever followed', []
            return f"Ph{phase}: {_plain(row['Walks'])} walks against {_plain(row['NormalWalks'])} normal {over}", []
        greens = 'green' if round(row['Services']) == 1 else 'greens'
        return f"Ph{phase} got {_plain(row['Services'])} {greens} against {_plain(row['NormalServices'])} normal {over}", []
    if tier == NOT_MEASURED:
        return f"Not measured: {row['NotMeasured']}", []
    hurt = []
    if tier == OTHER_PHASES:
        parts = []
        waits = phases[(phases['wait'] - phases['wait_n'] >= NAMED_WAIT_SECONDS) & phases['veh_w'].gt(0)].sort_values('extra_veh_s', ascending=False)
        if row['WaitHarm']:
            named = [f"Ph{int(p.Phase)} {p.wait_n:.0f} to {p.wait:.0f} s" for p in waits.itertuples()]
            subject = 'Its phases wait' if row['SignalWide'] else 'Other phases wait'
            parts.append(f"{subject} longer {over}, {row['OtherWaitUp']:.0f} s a vehicle on average: {_phase_list(named)}")
            hurt += waits['Phase'].astype(int).tolist()
        if row['MaxOutHarm']:
            served = phases[phases['services'] > 0].assign(
                pct_n=lambda t: 100.0 * t['maxouts_n'] / t['services'], pct=lambda t: 100.0 * t['maxouts'] / t['services'])
            moved = served[served['pct'] - served['pct_n'] >= NAMED_MAX_OUT_POINTS].sort_values('pct', ascending=False)
            named = [f"Ph{int(p.Phase)} {p.pct_n:.0f}% to {p.pct:.0f}%" for p in moved.itertuples()]
            if parts:
                parts.append(f"Max outs up on {_phase_list(named)}")
            else:
                subject = 'Its phases max out more' if row['SignalWide'] else 'Other phases max out more'
                parts.append(f"{subject} {over}: {_phase_list(named)}")
            hurt += moved['Phase'].astype(int).tolist()
        text = '. '.join(parts)
        return text, sorted(set(hurt))
    text = f"No effect on other phases {over}"
    if row['Coordinated']:
        text += ', phase behaves as coordinated in these hours'
    return text, []


def _impact_facts(row: pd.Series, phases: pd.DataFrame) -> list:
    """What a fault that hurt service was measured to do, in full, every phase named, with no estimates: one {text, chart, phases} per
    measure, chart being the kind of chart that shows it. Empty for a fault with no measured effect."""
    hours = row['FaultHours']
    during = f"during the fault ({hours * 60:.0f} min)" if hours < 1 else f"during the fault ({round(hours * 2) / 2:g} h)"
    tier = row['Tier']
    if tier == SERVICE_LOST:
        phase = int(row['Phase']) if pd.notna(row['Phase']) else None
        where = f"Ph{phase} " if phase else ''
        known = [phase] if phase else []
        if row['DetectorType'] == PED:
            if row['Kind'] == 'Ped never calls':
                return [{'text': f"{where}pressed, no walk ever followed", 'chart': 'ped', 'phases': known}]
            return [{'text': f"{where}walks: {_plain(row['NormalWalks'])} normal, {_plain(row['Walks'])} {during}", 'chart': 'ped', 'phases': known}]
        return [{'text': f"{where}greens: {_plain(row['NormalServices'])} normal, {_plain(row['Services'])} {during}", 'chart': 'green', 'phases': known}]
    if tier != OTHER_PHASES:
        return []
    facts = []
    if row['WaitHarm']:
        waits = phases[(phases['wait'] - phases['wait_n'] >= NAMED_WAIT_SECONDS) & phases['veh_w'].gt(0)].sort_values('extra_veh_s', ascending=False)
        named = ', '.join(f"Ph{int(p.Phase)} {p.wait_n:.0f} to {p.wait:.0f} s" for p in waits.itertuples())
        facts.append({'text': f"Wait, normal to {during}: {named}", 'chart': 'wait', 'phases': waits['Phase'].astype(int).tolist()})
    if row['MaxOutHarm']:
        served = phases[phases['services'] > 0].assign(
            pct_n=lambda t: 100.0 * t['maxouts_n'] / t['services'], pct=lambda t: 100.0 * t['maxouts'] / t['services'])
        moved = served[served['pct'] - served['pct_n'] >= NAMED_MAX_OUT_POINTS].sort_values('pct', ascending=False)
        named = ', '.join(f"Ph{int(p.Phase)} {p.pct_n:.0f}% to {p.pct:.0f}%" for p in moved.itertuples())
        facts.append({'text': f"Max outs, normal to {during}: {named}", 'chart': 'max out', 'phases': moved['Phase'].astype(int).tolist()})
    return facts


def _harm_priors(rows: pd.DataFrame) -> pd.Series:
    """The chance, before looking at the other phases, that each row's fault hurts them: its kind's share times the health
    check's confidence in it, raised when the fault visibly made its own phase run more (_acted). Rows judged as one group share
    the chance that any of their detectors does."""
    own = rows['Kind'].map(HARM_PRIOR).fillna(min(HARM_PRIOR.values())) * rows['Confidence'].fillna(MIN_CONFIDENCE).clip(0.0, 1.0)
    own = (own * rows['_acted'].map({True: ACTED_PRIOR, False: 1.0})).clip(upper=MAX_PRIOR)
    per_detector = own.groupby([rows['grp'], rows['DetectorType'], rows['Detector']]).max()
    together = (1.0 - (1.0 - per_detector).groupby(level=0).prod()).clip(upper=MAX_PRIOR)
    members = per_detector.groupby(level=0).size()
    return own.where(rows['grp'].map(members) <= 1, rows['grp'].map(together))


def _own_phase(row: dict, mine: pd.DataFrame, days: float) -> dict:
    """What a row's own phase and the phases it is judged on did during the fault."""
    empty = mine.head(0)
    own = mine[mine['Phase'] == row['Phase']] if pd.notna(row['Phase']) else empty
    others = mine[(mine['Phase'] != row['Phase']) & mine['sn'].gt(0)]
    counted = others[~others['own_fault']]
    all_phases = mine[mine['sn'].gt(0)]
    # No other phase free of faults of its own: judge the whole signal, its own phase included.
    signal_wide = counted.empty and len(all_phases) > 1
    judged = all_phases if signal_wide else counted
    first = own.iloc[0] if not own.empty else None
    get = lambda name: float(first[name]) if first is not None and pd.notna(first[name]) else float('nan')
    sn, services, walks, wkn = get('sn'), get('services'), get('walks'), get('wkn')
    maxouts, maxouts_n = get('maxouts'), get('maxouts_n')
    own_ratio = services / sn if sn > 0 else float('nan')
    other_sn = counted['sn'].sum()
    own_up = 100.0 * (maxouts - maxouts_n) / services if services > 0 else float('nan')
    vehicle = row['DetectorType'] != PED
    if vehicle:
        acted = sn >= UNMEASURED_NORMAL_SERVICES and (own_ratio >= ACTED_RATIO or own_up >= ACTED_MAX_OUT_UP)
    else:
        acted = walks >= ACTED_RATIO * max(wkn, 1.0)
    return dict(judged=judged, counted=counted, signal_wide=signal_wide, first=first, numbers=_roll_up(judged, days), sn=sn, services=services,
                sq10=get('sq10'), walks=walks, wkn=wkn, wkmin=get('wkmin'), own_ratio=own_ratio, own_up=own_up, vehicle=vehicle,
                other_ratio=counted['services'].sum() / other_sn if other_sn > 0 else float('nan'),
                measured_days=int(first['mdays']) if first is not None and pd.notna(first['mdays']) else 0, acted=bool(acted))


def _tiers(rows: pd.DataFrame, phases: pd.DataFrame, days: float, daily: pd.DataFrame, window_start=None) -> pd.DataFrame:
    """The tier and the numbers of every row (fr, own_c, judged), from its phases (pr_flag) and their days (days)."""
    records = []
    phases_of = dict(tuple(phases.groupby('rid'))) if not phases.empty else {}
    daily_of = dict(tuple(daily.groupby('rid'))) if not daily.empty else {}
    empty = phases.head(0)
    measured = {row['rid']: _own_phase(row, phases_of.get(row['rid'], empty), days) for row in rows.to_dict('records')}
    rows = rows.assign(_acted=[measured[rid]['acted'] for rid in rows['rid']])
    rows = rows.assign(_prior=_harm_priors(rows))
    members = {group: len(set(zip(found['DetectorType'], found['Detector']))) for group, found in rows.groupby('grp')}
    for row in rows.to_dict('records'):
        found = measured[row['rid']]
        judged, counted, signal_wide, first, numbers = (found[name] for name in ('judged', 'counted', 'signal_wide', 'first', 'numbers'))
        sn, services, sq10, walks, wkn, wkmin = (found[name] for name in ('sn', 'services', 'sq10', 'walks', 'wkn', 'wkmin'))
        own_ratio, other_ratio, own_up, measured_days, vehicle = (found[name] for name in ('own_ratio', 'other_ratio', 'own_up', 'measured_days', 'vehicle'))
        xveh = numbers['ExtraVehHoursPerDay']

        # Is the rise the fault, or an ordinary day?
        wait_days, max_out_days, control = (_day_changes(daily_of[row['rid']], judged['Phase'], window_start)
                                            if row['rid'] in daily_of else ([], [], False))
        other = 0.0 if control else OTHER_CHANGE
        wait_chance = effect_probability(wait_days, HARM_WAIT_SECONDS, row['_prior'], TYPICAL_WAIT_EFFECT, other)
        max_out_chance = effect_probability(max_out_days, HARM_MAX_OUT_POINTS, row['_prior'], TYPICAL_MAX_OUT_EFFECT, other)
        wait_harm = bool(wait_chance >= HARM_PROBABILITY and numbers['OtherWaitUp'] >= HARM_WAIT_SECONDS and xveh >= HARM_VEHICLE_HOURS_PER_DAY)
        max_out_harm = bool(max_out_chance >= HARM_PROBABILITY and numbers['OtherMaxOutUp'] >= HARM_MAX_OUT_POINTS
                            and numbers['MaxOutVehPerDay'] >= HARM_MAX_OUT_VEHICLES_PER_DAY)

        tier, why = DATA_ONLY, ''
        if pd.isna(row['Phase']):
            tier, why = NOT_MEASURED, 'phase unknown'
        elif measured_days == 0:
            tier, why = NOT_MEASURED, 'no normal to compare'
        elif vehicle and (0.0 if pd.isna(sn) else sn) < UNMEASURED_NORMAL_SERVICES and xveh < HARM_VEHICLE_HOURS_PER_DAY:
            tier, why = NOT_MEASURED, 'too few services'
        if wait_harm or max_out_harm:
            tier = OTHER_PHASES
        if vehicle:
            lost = (sn >= LOST_MIN_NORMAL_SERVICES and own_ratio <= LOST_RATIO
                    and (1.0 if pd.isna(other_ratio) else other_ratio) - own_ratio >= LOST_RATIO_GAP
                    and services < sq10 and (0.0 if pd.isna(own_up) else own_up) < LOST_MAX_OWN_MAX_OUT_UP)
        else:
            lost = ((wkn >= LOST_PED_MIN_NORMAL_WALKS and walks <= LOST_PED_RATIO * wkn and walks < wkmin)
                    or (row['Kind'] == 'Ped silent' and wkn >= 1 and walks <= LOST_PED_RATIO * wkn))
        if lost:
            tier = SERVICE_LOST
        if own_ratio <= LOST_RATIO and other_ratio <= LOST_RATIO:
            tier, why = NOT_MEASURED, 'the whole signal stopped or its log was lost'
        if row['Kind'] == 'Ped never calls':
            tier = SERVICE_LOST
        record = {**row, **numbers, 'Tier': tier, 'NotMeasured': why, 'SignalWide': signal_wide, 'OwnRatio': own_ratio, 'OtherRatio': other_ratio,
                  'FaultHours': float(first['hours']) if first is not None else float(row['fault_hours']),
                  'Services': services, 'NormalServices': sn, 'Walks': walks, 'NormalWalks': wkn,
                  'Lost': (wkn - walks if not vehicle else sn - services) / days,
                  'Coordinated': vehicle and pd.notna(row['coord_share']) and row['coord_share'] >= 0.5,
                  'WaitHarm': wait_harm, 'MaxOutHarm': max_out_harm, 'WaitProbability': wait_chance, 'MaxOutProbability': max_out_chance,
                  'HarmProbability': max(wait_chance, max_out_chance),
                  'Group': float(row['grp']) if members.get(row['grp'], 1) > 1 else float('nan')}
        if tier == SERVICE_LOST and pd.isna(record['Lost']):
            record['Lost'] = 0.0
        text, hurt = _impact_text(pd.Series(record), judged)
        record['ImpactText'], record['HurtPhases'] = text, ','.join(str(phase) for phase in hurt)
        record['ImpactLines'] = json.dumps(_impact_facts(pd.Series(record), judged))
        records.append(record)
    return pd.DataFrame(records)


def rank_detectors(findings: pd.DataFrame, detectors: pd.DataFrame, terminations: TableLike, phase_wait: TableLike,
                   has_data: TableLike, report_day, days: int = 3, baseline_days: int = 18, wait_days: int = 11,
                   pedestrian: TableLike = None, ped_inputs: pd.DataFrame = None, usual_counts: pd.DataFrame = None,
                   coordination: TableLike = None) -> pd.DataFrame:
    """One row per flagged detector or ped button at its most urgent fault, most urgent first (RANK_COLUMNS).

    findings: the health findings of the window. detectors: detector_info() rows (DeviceId, Detector, Phase, Function).
    terminations, phase_wait, has_data, pedestrian (PedServices, PedActuation) and coordination (CycleLength): the report's
    15-minute inputs, reaching baseline_days and wait_days before the window. ped_inputs: DeviceId, Input, Phase, the phase each
    button calls. usual_counts: usual_detector_counts(), the traffic weights; without it no harm to other phases can be measured.
    """
    empty = pd.DataFrame(columns=RANK_COLUMNS)
    if findings is None or findings.empty or detectors is None or detectors.empty:
        return empty
    if any(table is None for table in (terminations, phase_wait, has_data)):
        return empty
    window_end = pd.Timestamp(report_day).normalize() + pd.Timedelta(days=1)
    window_start = window_end - pd.Timedelta(days=days)
    frames = dict(
        terminations=to_pandas(terminations), phase_wait=to_pandas(phase_wait), has_data=to_pandas(has_data),
        pedestrian=to_pandas(pedestrian) if pedestrian is not None else pd.DataFrame({
            'TimeStamp': pd.Series(dtype='datetime64[us]'), 'DeviceId': pd.Series(dtype=str), 'Phase': pd.Series(dtype=int),
            'PedServices': pd.Series(dtype=int), 'PedActuation': pd.Series(dtype=int)}),
        ped_inputs=ped_inputs if ped_inputs is not None else pd.DataFrame({
            'DeviceId': pd.Series(dtype=str), 'Input': pd.Series(dtype=int), 'Phase': pd.Series(dtype=float)}),
        usual_counts=usual_counts if usual_counts is not None else pd.DataFrame({
            'DeviceId': pd.Series(dtype=str), 'Detector': pd.Series(dtype=int), 'weekend': pd.Series(dtype=bool),
            'clock': pd.Series(dtype=object), 'Total': pd.Series(dtype=float)}),
        coordination=to_pandas(coordination) if coordination is not None and 'CycleLength' in to_pandas(coordination).columns else pd.DataFrame({
            'TimeStamp': pd.Series(dtype='datetime64[us]'), 'DeviceId': pd.Series(dtype=str), 'CycleLength': pd.Series(dtype=float)}),
    )
    detector_table = detectors.assign(Function=detectors['Function'] if 'Function' in detectors.columns else None)
    wait_start = (window_start - pd.Timedelta(days=wait_days)).date()
    con = duckdb.connect()
    try:
        _prepare(con, findings.assign(DeviceId=findings['DeviceId'].astype(str)), detector_table, frames['ped_inputs'],
                 frames['terminations'], frames['phase_wait'], frames['has_data'], frames['pedestrian'], frames['usual_counts'],
                 frames['coordination'], window_start, window_end, baseline_days, wait_days)
        _compare(con, wait_start)
        rows = con.execute("""SELECT r.DeviceId, r.DetectorType, r.Detector, r.Kind, r.Phase, r.rid, r.Confidence, j.grp, j.alone, c.coord_share,
                                     (SELECT count(*) FROM fb b WHERE b.rid = r.rid) / 4.0 AS fault_hours
                              FROM fr r JOIN judged j USING (rid) LEFT JOIN own_c c USING (rid) WHERE r.rid IN (SELECT rid FROM fb)""").df()
        phases = con.execute("SELECT * FROM pr_flag").df()
        daily = con.execute("SELECT * FROM days").df()
    finally:
        con.close()
    if rows.empty:
        return empty
    ranked = _tiers(rows, phases, float(days), daily, window_start)
    ranked['KindOrder'] = ranked['Kind'].map(KIND_ORDER)
    urgent = ranked['Tier'].isin((SERVICE_LOST, OTHER_PHASES))
    how_much = ranked['Lost'].where(ranked['Tier'] == SERVICE_LOST, ranked['ExtraVehHoursPerDay']).fillna(0.0).where(urgent, 0.0)
    ranked = ranked.assign(_tier=ranked['Tier'].map(TIER_ORDER), _first=-how_much, _second=ranked['KindOrder'].where(~urgent, 0),
                           _third=-ranked['FaultHours'])
    ranked = ranked.sort_values(['_tier', '_first', '_second', '_third', 'DeviceId', 'Detector'], kind='stable')
    return ranked.drop_duplicates(['DeviceId', 'DetectorType', 'Detector'], keep='first')[RANK_COLUMNS].reset_index(drop=True)


def attach_triage(alerts: pd.DataFrame, ranked: Optional[pd.DataFrame]) -> pd.DataFrame:
    """The health alerts with the ranking of their fault rows: columns Tier, Kind, ImpactLabel, TriagePhase, TriageRank and so on.

    TriageRank runs from 0: service lost, then harm to other phases, in ranked order; then the others, bad before suspect, by kind
    and longest fault. A fault without a ranked row comes last of them. Information rows have no rank. Without a ranking the
    alerts come back unchanged.
    """
    if alerts is None or alerts.empty or ranked is None or ranked.empty:
        return alerts
    keys = ['DeviceId', 'DetectorType', 'Detector']
    merged = alerts.merge(ranked.rename(columns={'Phase': 'TriagePhase'}).assign(_order=range(len(ranked))), on=keys, how='left')
    faults = merged['Status'].isin(('bad', 'suspect'))
    ranked_columns = [column for column in merged.columns if column not in alerts.columns]
    merged[ranked_columns] = merged[ranked_columns].astype(object)
    merged.loc[~faults, ranked_columns] = None
    merged['ImpactLabel'] = None
    merged.loc[faults, 'ImpactLabel'] = [
        impact_label(row) for _, row in merged[faults].iterrows()
    ]
    above = merged[faults & merged['Tier'].isin((SERVICE_LOST, OTHER_PHASES))].assign(
        _o=lambda t: pd.to_numeric(t['_order'])).sort_values('_o', kind='stable')
    below = merged[faults & ~merged['Tier'].isin((SERVICE_LOST, OTHER_PHASES))].assign(
        _status=lambda t: (t['Status'] != 'bad').astype(int),
        _kind=lambda t: pd.to_numeric(t['KindOrder'], errors='coerce').fillna(len(KIND_ORDER) + 1),
        _hours=lambda t: -pd.to_numeric(t['FaultHours'], errors='coerce').fillna(0.0),
    ).sort_values(['_status', '_kind', '_hours', 'DeviceId', 'Detector'], kind='stable')
    merged['TriageRank'] = float('nan')
    merged.loc[list(above.index) + list(below.index), 'TriageRank'] = [float(rank) for rank in range(len(above) + len(below))]
    for column in ('FaultHours', 'KindOrder', 'ExtraVehHoursPerDay', 'MaxOutVehPerDay', 'OtherWaitUp', 'OtherMaxOutUp', 'OwnRatio', 'OtherRatio', 'Lost', 'TriagePhase',
                   'HarmProbability', 'Group'):
        if column in merged:
            merged[column] = pd.to_numeric(merged[column], errors='coerce')
    return merged.drop(columns='_order')


def impact_label(row) -> str:
    """The Impact column of a ranked row: its tier in bold and what it did, naming the phases."""
    tier = row.get('Tier')
    text = row.get('ImpactText')
    if not isinstance(tier, str) or not isinstance(text, str):
        return 'Not measured'
    if tier == NOT_MEASURED:
        return text
    return f"<b>{tier}.</b> {text}"


def silent_ped_phases(pedestrian: TableLike, terminations: TableLike, has_data: TableLike, report_day, days: int = 3) -> pd.DataFrame:
    """Ped phases with no press in the window that normally get SILENT_EXPECTED_PRESSES or more. A button with no press at all is
    not on the ped health list, so this looks at the phases instead. Rows for review, not alerts: DeviceId, Phase, Presses, Walks,
    Services, ExpectedPresses, ExpectedWalks, Tier, Note."""
    columns = ['DeviceId', 'Phase', 'Presses', 'Walks', 'Services', 'ExpectedPresses', 'ExpectedWalks', 'Tier', 'Note']
    if pedestrian is None or terminations is None or has_data is None:
        return pd.DataFrame(columns=columns)
    window_end = pd.Timestamp(report_day).normalize() + pd.Timedelta(days=1)
    window_start = window_end - pd.Timedelta(days=days)
    start, end = f"TIMESTAMP '{window_start}'", f"TIMESTAMP '{window_end}'"
    con = duckdb.connect()
    try:
        con.register('ped_in', to_pandas(pedestrian))
        con.register('terminations_in', to_pandas(terminations))
        con.register('has_data_in', to_pandas(has_data))
        found = con.execute(f"""
          WITH pb AS (
            SELECT h.ts, h.DeviceId, ph.Phase, cast(h.ts AS DATE) AS dy, isodow(h.ts) >= 6 AS wk, coalesce(p.walks, 0) AS walks, coalesce(p.presses, 0) AS presses, coalesce(t.services, 0) AS services
            FROM (SELECT DISTINCT TimeStamp AS ts, DeviceId FROM has_data_in WHERE TimeStamp < {end}) h
            JOIN (SELECT DISTINCT DeviceId, Phase FROM ped_in WHERE PedServices > 0 OR PedActuation > 0) ph USING (DeviceId)
            LEFT JOIN (SELECT TimeStamp AS ts, DeviceId, Phase, sum(PedServices) AS walks, sum(PedActuation) AS presses FROM ped_in GROUP BY ALL) p USING (ts, DeviceId, Phase)
            LEFT JOIN (SELECT TimeStamp AS ts, DeviceId, Phase, sum(Total) FILTER (PerformanceMeasure IN ('MaxOut', 'ForceOff', 'GapOut')) AS services FROM terminations_in GROUP BY ALL) t USING (ts, DeviceId, Phase)
            WHERE NOT {_clock_change('h.ts')}),
          wd AS (SELECT DeviceId, Phase, sum(presses) AS Presses, sum(walks) AS Walks, sum(services) AS Services FROM pb WHERE ts >= {start} GROUP BY ALL),
          dy AS (SELECT DeviceId, Phase, dy, wk, sum(presses) AS p, sum(walks) AS w FROM pb WHERE ts < {start} GROUP BY ALL HAVING count(*) >= {SILENT_MIN_DAY_BINS}),
          n AS (SELECT DeviceId, Phase, wk, median(p) AS p_day, median(w) AS w_day FROM dy GROUP BY ALL HAVING count(*) >= {MIN_NORMAL_DAYS}),
          wt AS (SELECT DeviceId, Phase, wk, count(*) AS nb FROM pb WHERE ts >= {start} GROUP BY ALL),
          e AS (SELECT wt.DeviceId, wt.Phase, sum(n.p_day * wt.nb / 96.0) AS ExpectedPresses, sum(n.w_day * wt.nb / 96.0) AS ExpectedWalks FROM wt JOIN n USING (DeviceId, Phase, wk) GROUP BY ALL)
          SELECT wd.*, e.ExpectedPresses, e.ExpectedWalks FROM wd JOIN e USING (DeviceId, Phase) WHERE wd.Presses = 0 AND e.ExpectedPresses >= {SILENT_EXPECTED_PRESSES}
          ORDER BY DeviceId, Phase""").df()
    finally:
        con.close()
    tiers, notes = [], []
    for row in found.itertuples():
        if row.Services == 0:
            tiers.append(NOT_MEASURED)
            notes.append('the phase was not served at all')
        elif row.ExpectedWalks >= 1 and row.Walks <= LOST_PED_RATIO * row.ExpectedWalks:
            tiers.append(SERVICE_LOST)
            notes.append(f"{row.Walks:.0f} walks against {row.ExpectedWalks:.0f} expected")
        else:
            tiers.append(DATA_ONLY)
            notes.append(f"{row.Walks:.0f} walks against {row.ExpectedWalks:.0f} expected"
                         + ('; inferred recall, or a button stuck with its ON lost' if row.Walks > 2 * max(row.ExpectedWalks, 1) else ''))
    return found.assign(Tier=tiers, Note=notes)[columns]
