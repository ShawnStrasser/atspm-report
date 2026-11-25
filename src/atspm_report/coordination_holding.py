"""Coordination holding (coordination_holding_enabled): whether each signal holds its own programmed plan, cycle by cycle,
from the raw events (controller_events), every signal in one query. It needs no history: a single day, or a few hours, is
enough. Not the plan check of coordination_check, which sets a signal's plans against its group's.

Judged per cycle from the programmed plan, not from the controller's transition events (150): its add / subtract state
often stays logged for hours while the signal is in step.

* Plan = a pattern change (131 with its cycle 132, offset 133 (else the 318 logged each cycle) and programmed splits
  134-149) up to the next one. The plan before the first 131 is unknown and left out; in connection mode the plan events
  of the day before the report day are read too (PLAN_LEAD_DAYS), so the plan running at midnight is known. Coordinated =
  cycle > 0, pattern not 254 / 255, and an actual cycle logged within a cycle: MaxTime's 316 (not logged while it runs
  free); without 316 (Econolite), the time from one local zero (150 = 5, logged by both once per coordinated cycle) to
  the next ('316' below means this actual cycle). Signals with neither give no rows.
* Master zero = time of day modulo the cycle (plus the fixed shift the 320 master-zero events show; without 320, the one
  the local zeros of in-step cycles show, one per signal); local zero = master zero + offset. The coordinated phase's
  split window ends at local zero + its split when the offset references the begin of its green ('G'), or at local zero
  + its yellow + red clearance for the begin of yellow ('Y'). A cycle is in step when the coordinated phase was green by
  its window start (+2 s) and that green did not end before the window end (-2 s, less the split of a phase that can lag
  it: 1 / 5 for 2 / 6, 3 / 7 for 4 / 8); early return and resting in green count as in step. The reference (G / Y, one
  per signal) and each plan's coordinated phase (a through phase 2 / 4 / 6 / 8 among those 152 names, when logged; the
  longest split among fits within 5 points of the best) are the ones that fit best on cycles whose 316, and its
  neighbours', equal the programmed cycle. The inference is used when it fits 85 %+ of those (30+) cycles; a cycle is
  then out of step when the inference says so or its 316 differs from the programmed cycle by more than 2 s. Otherwise
  the 316 alone decides (method '316', or 'local zero' without 316). With 316, a plan whose inferred out-of-step share
  (10+ steady cycles) is more than 10 points from the 316 share is judged by the 316 alone (method '316').
* (a) Not holding coordination, per pattern: steady cycles (10+ min after a plan change; 36+ over 1 h+) out of step on
  20 %+ (urgent at 33 %+: at least one cycle in three). Cause: a non-coordinated phase served its walk and ran past its
  programmed split (+2 s) in the cycle or the two before (no inference: served its walk in the cycle) on 50 %+ of the
  out-of-step cycles (10+) and 1.5x as often as before in-step cycles ('walk', named phase), else a preempt (102) or TSP
  check-in (112) there on 50 %+ of them.
* (b) Slow transition: after a change into a coordinated plan, 10+ min (urgent at 30+) until the first 3 in-step cycles
  in a row. Not reported for a pattern already reported under (a). StillOutAtEnd: no 3 in-step cycles in a row before
  the plan (or the data) ended; Minutes is then the time to that end.
"""

from typing import Optional

import pandas as pd

from .alarm_processing import sql_time
from .duckdb_utils import query_table

CONFIG_SWITCH = 'coordination_holding_enabled'
CATEGORY = 'Coordination'
NOT_HOLDING, SLOW_TRANSITION = 'not holding coordination', 'slow transition'
URGENT, REVIEW = 'urgent', 'review'
PLAN_EVENT_IDS = tuple(range(131, 150))
EVENT_IDS = (1, 8, 11, 21, 102, 112) + PLAN_EVENT_IDS + (150, 152, 316, 318, 320)
# The plan events read before the report day, so the plan running at midnight is known.
PLAN_LEAD_DAYS = 1
# At most this many rows a signal.
MAX_ROWS = 20

COLUMNS = ['DeviceId', 'Grade', 'Issue', 'Pattern', 'CycleSeconds', 'Start', 'End', 'PctOutOfStep', 'Cycles', 'Episodes',
           'Method', 'Cause', 'WalkPhase', 'PctOutWithCause', 'PctInStepWithCause', 'Minutes', 'StillOutAtEnd', 'Text']

HOLDING_SQL = """
WITH ev AS MATERIALIZED (
  SELECT CAST(DeviceId AS VARCHAR) AS d, ts, EventId AS code, Parameter AS param
  FROM (SELECT DeviceId, CAST(TimeStamp AS TIMESTAMP) AS ts, EventId, Parameter FROM raw WHERE EventId IN ({ids}))
  WHERE (ts >= {start} AND ts < {end}) OR (code BETWEEN 131 AND 149 AND ts >= {lead} AND ts < {start})),
dev AS (SELECT d, MIN(ts) FILTER (WHERE ts >= {start}) AS first_ts, MAX(ts) AS last_ts, bool_or(code = 316) AS has316
        FROM ev GROUP BY d),
p AS (SELECT d, ts, MAX(param) FILTER (WHERE code = 131) AS pattern, MAX(param) FILTER (WHERE code = 132) AS cyc,
             MAX(param) FILTER (WHERE code = 133) AS ofs
      FROM ev WHERE code IN (131, 132, 133) GROUP BY d, ts HAVING COUNT(*) FILTER (WHERE code = 131) > 0),
seg AS (SELECT p.d, p.ts AS seg_ts, p.pattern, COALESCE(p.cyc, 0) AS cyc, p.ofs,
               LAG(p.pattern) OVER (PARTITION BY p.d ORDER BY p.ts) AS prev_pat,
               COALESCE(LEAD(p.ts) OVER (PARTITION BY p.d ORDER BY p.ts), dv.last_ts) AS seg_end
        FROM p JOIN dev dv ON dv.d = p.d),
seg2 AS (SELECT *, SUM(CASE WHEN pattern IS DISTINCT FROM prev_pat THEN 1 ELSE 0 END) OVER (PARTITION BY d ORDER BY seg_ts) AS sid
         FROM seg),
plan0 AS (SELECT d, sid, MIN(seg_ts) AS t0, MAX(seg_end) AS t1, ANY_VALUE(pattern) AS pattern, ARG_MIN(cyc, seg_ts) AS cyc,
                 ARG_MIN(prev_pat, seg_ts) AS prev_pat, MAX(ofs) AS ofs133
          FROM seg2 GROUP BY d, sid),
o318 AS (SELECT pl.d, pl.sid, MODE(e.param) AS o FROM ev e JOIN plan0 pl ON e.d = pl.d AND e.ts > pl.t0 AND e.ts <= pl.t1
         WHERE e.code = 318 GROUP BY 1, 2),
z320 AS (SELECT pl.d, pl.sid, MODE(ROUND(((epoch(e.ts) - epoch(date_trunc('day', e.ts))) % pl.cyc)::DOUBLE, 1)) AS shift
         FROM ev e JOIN plan0 pl ON e.d = pl.d AND e.ts > pl.t0 + INTERVAL 5 MINUTE AND e.ts <= pl.t1
         WHERE e.code = 320 AND pl.cyc > 0 GROUP BY 1, 2),
-- local zero (150 = 5): logged once per coordinated cycle by MaxTime and Econolite alike
lz AS (SELECT DISTINCT d, ts FROM ev WHERE code = 150 AND param = 5),
-- master-zero shift from the local zeros of in-step cycles (the previous local zero one programmed cycle earlier), one
-- per signal
lzs AS (SELECT pl.d, ROUND((((epoch(l.ts) - epoch(date_trunc('day', l.ts)) - COALESCE(pl.ofs133, o.o)) % pl.cyc
                             + pl.cyc) % pl.cyc)::DOUBLE, 1) AS s
        FROM (SELECT d, ts, epoch(ts) - epoch(LAG(ts) OVER (PARTITION BY d ORDER BY ts)) AS iv FROM lz) l
        JOIN plan0 pl ON l.d = pl.d AND l.ts > pl.t0 + INTERVAL 5 MINUTE AND l.ts <= pl.t1
        LEFT JOIN o318 o ON o.d = pl.d AND o.sid = pl.sid
        WHERE pl.cyc > 0 AND COALESCE(pl.ofs133, o.o) IS NOT NULL AND ABS(l.iv - pl.cyc) <= 1),
zlz AS (SELECT d, MODE(s) AS shift FROM lzs GROUP BY d),
plan AS (SELECT pl.*, COALESCE(pl.ofs133, o.o) AS ofs, COALESCE(z.shift, zl.shift, 0) AS shift
         FROM plan0 pl LEFT JOIN o318 o ON o.d = pl.d AND o.sid = pl.sid LEFT JOIN z320 z ON z.d = pl.d AND z.sid = pl.sid
         LEFT JOIN zlz zl ON zl.d = pl.d),
cplan AS (SELECT * FROM plan WHERE cyc > 0 AND pattern NOT IN (254, 255)),
splits AS (SELECT s.d, s.sid, e.code - 133 AS phase, ARG_MAX(e.param, e.ts) AS split
           FROM ev e JOIN seg2 s ON e.d = s.d AND e.ts >= s.seg_ts AND e.ts < s.seg_ts + INTERVAL 1 SECOND
           WHERE e.code BETWEEN 134 AND 149 GROUP BY 1, 2, 3),
-- actual cycles: MaxTime's 316; without it, local zero to local zero (150 = 5)
c316 AS (SELECT d, ts, param::DOUBLE AS actual, LAG(ts) OVER (PARTITION BY d ORDER BY ts) AS cs, NULL::DOUBLE AS span
         FROM ev WHERE code = 316
         UNION ALL
         SELECT d, ts, ROUND(epoch(ts) - epoch(cs)) AS actual, cs, epoch(ts) - epoch(cs) AS span
         FROM (SELECT d, ts, LAG(ts) OVER (PARTITION BY d ORDER BY ts) AS cs FROM lz)
         WHERE d IN (SELECT d FROM dev WHERE NOT has316)),
-- phase services: green (1) -> end of red clearance (11)
svc AS MATERIALIZED (
  SELECT x.d, x.param AS phase, g.ts AS gs, y.ts AS ys, x.ts AS xe
  FROM (SELECT * FROM ev WHERE code = 11 AND param BETWEEN 1 AND 8) x
  ASOF JOIN (SELECT * FROM ev WHERE code = 1) g ON g.d = x.d AND g.param = x.param AND g.ts < x.ts
  ASOF JOIN (SELECT * FROM ev WHERE code = 8) y ON y.d = x.d AND y.param = x.param AND y.ts < x.ts
  WHERE y.ts >= g.ts AND x.ts < g.ts + INTERVAL 2 HOUR),
clr AS (SELECT d, phase, MEDIAN(epoch(xe) - epoch(ys)) AS clr FROM svc GROUP BY 1, 2),
-- lagging phases of the same barrier side in the other ring position (1 / 5 for 2 / 6, 3 / 7 for 4 / 8)
lags AS (SELECT a.d, a.sid, a.phase, LIST(DISTINCT q.split) AS lags FROM splits a JOIN splits q ON q.d = a.d AND q.sid = a.sid
         WHERE a.phase <= 8 AND q.phase <= 8 AND q.split > 0 AND ((q.phase - 1) % 4) // 2 = ((a.phase - 1) % 4) // 2
           AND (q.phase - 1) % 2 <> (a.phase - 1) % 2 GROUP BY 1, 2, 3),
-- the coordinated phases the controller names each cycle (152, MaxTime and Econolite alike)
n152 AS (SELECT d, param AS phase, COUNT(*) AS n FROM ev WHERE code = 152 AND param BETWEEN 1 AND 16 GROUP BY 1, 2),
c152 AS (SELECT d, phase FROM (SELECT *, MAX(n) OVER (PARTITION BY d) AS top FROM n152) WHERE n >= 5 AND n >= 0.2 * top),
cand AS (SELECT pl.d, pl.sid, pl.t0, pl.t1, pl.cyc, pl.ofs, pl.shift, s.phase, s.split, h.h,
                CASE WHEN h.h = 'G' THEN s.split ELSE c.clr END AS aoff
         FROM cplan pl JOIN splits s ON s.d = pl.d AND s.sid = pl.sid JOIN clr c ON c.d = pl.d AND c.phase = s.phase
         CROSS JOIN (VALUES ('G'), ('Y')) h(h)
         WHERE s.split > 0 AND s.phase IN (2, 4, 6, 8) AND pl.ofs IS NOT NULL
           AND (EXISTS (SELECT 1 FROM c152 k WHERE k.d = pl.d AND k.phase = s.phase)
                OR NOT EXISTS (SELECT 1 FROM c152 k WHERE k.d = pl.d))),
days AS (SELECT cand.*, UNNEST(generate_series(date_trunc('day', t0), date_trunc('day', t1), INTERVAL 1 DAY)) AS dd FROM cand),
-- the window ends; a plan running since before the data starts is judged from one cycle after the data's start
anc AS (
  SELECT x.d, x.sid, x.t0, x.t1, x.cyc, x.phase, x.split, x.h, x.dd, x.a FROM (
    SELECT d, sid, t0, t1, cyc, phase, split, h, dd,
           dd + to_microseconds(((shift + ofs + aoff + k * cyc) * 1e6)::BIGINT) AS a
    FROM (SELECT days.*, UNNEST(generate_series(-1, (86400 // cyc)::BIGINT)) AS k FROM days)) x
  JOIN dev dv ON dv.d = x.d
  WHERE x.a > x.t0 AND x.a <= x.t1 AND x.a >= x.dd AND x.a < x.dd + INTERVAL 1 DAY
    AND (x.t0 >= dv.first_ts OR x.a >= dv.first_ts + to_seconds(x.cyc))),
ac AS MATERIALIZED (  -- the coordinated-phase service that started on time (window start + 2 s)
  SELECT n.*, c.ts AS t316, c.actual, c.span,
         COALESCE(epoch(n.xe) >= epoch(n.a) - 2 - COALESCE(list_max(l.lags), 0), false) AS instep
  FROM (SELECT anc.*, s.gs, s.xe FROM (SELECT *, a - to_seconds(split - 2) AS a_start FROM anc) anc
        ASOF LEFT JOIN svc s ON s.d = anc.d AND s.phase = anc.phase AND s.gs <= anc.a_start) n
  LEFT JOIN lags l ON l.d = n.d AND l.sid = n.sid AND l.phase = n.phase
  ASOF LEFT JOIN c316 c ON c.d = n.d AND c.ts >= n.a),
fit AS (SELECT d, sid, phase, h, ANY_VALUE(split) AS split, COUNT(*) FILTER (WHERE ok) AS n_ok,
               AVG(instep::INT) FILTER (WHERE ok) AS in_ok, AVG(instep::INT) FILTER (WHERE a >= t0 + INTERVAL 10 MINUTE) AS in_all
        FROM (SELECT *, a >= t0 + INTERVAL 10 MINUTE AND (epoch(t316) - epoch(a) < cyc OR span <= 3 * cyc) AND abs(actual - cyc) <= 1
                        AND COALESCE(abs(LAG(actual) OVER w - cyc) <= 1 AND abs(LEAD(actual) OVER w - cyc) <= 1, false) AS ok
              FROM ac WINDOW w AS (PARTITION BY d, sid, phase, h ORDER BY a))
        GROUP BY 1, 2, 3, 4),
bestp AS (  -- among the phases that fit within 5 points of the best, the one with the longest split
  SELECT * EXCLUDE (top) FROM (SELECT *, MAX(COALESCE(in_ok, 0)) OVER (PARTITION BY d, sid, h) AS top FROM fit)
  WHERE COALESCE(in_ok, 0) >= top - 0.05
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d, sid, h ORDER BY split DESC, in_all DESC, phase) = 1),
refh AS (SELECT d, h, SUM(in_ok * n_ok) / NULLIF(SUM(n_ok), 0) AS pin FROM bestp WHERE n_ok > 0 GROUP BY d, h
         QUALIFY ROW_NUMBER() OVER (PARTITION BY d ORDER BY pin DESC NULLS LAST) = 1),
ref0 AS (SELECT b.d, b.sid, b.phase, b.h FROM bestp b JOIN refh r ON r.d = b.d AND r.h = b.h
         WHERE r.pin >= 0.85 AND (SELECT SUM(n_ok) FROM bestp x WHERE x.d = r.d AND x.h = r.h) >= 30),
-- with MaxTime's 316, a plan whose inferred out-of-step share of steady cycles (10+) differs from the 316 share by more
-- than 10 points is judged by the 316 alone; its cycles keep the inferred anchors (for the walk / preempt cause windows)
dis316 AS (
  SELECT i.d, i.sid FROM (
    SELECT ac.d, ac.sid, COUNT(*) AS n, AVG((NOT ac.instep OR ABS(ac.actual - ac.cyc) > 2)::INT) AS p
    FROM ac JOIN ref0 r ON r.d = ac.d AND r.sid = ac.sid AND r.phase = ac.phase AND r.h = ac.h
    WHERE (epoch(ac.t316) - epoch(ac.a) < ac.cyc OR ac.span <= 3 * ac.cyc)
      AND ac.a > ac.t0 + INTERVAL 10 MINUTE AND ac.a - to_seconds(ac.cyc) >= ac.t0 GROUP BY 1, 2) i
  JOIN (SELECT pl.d, pl.sid, AVG((ABS(c.actual - pl.cyc) > 2)::INT) AS p
        FROM c316 c JOIN cplan pl ON c.d = pl.d AND c.ts > pl.t0 + INTERVAL 10 MINUTE AND c.ts <= pl.t1
        WHERE c.cs >= pl.t0 GROUP BY 1, 2) t ON t.d = i.d AND t.sid = i.sid
  WHERE i.n >= 10 AND ABS(i.p - t.p) > 0.10 AND i.d IN (SELECT d FROM dev WHERE has316)),
-- cycles: the inferred ones (anchor = end of the coordinated phase's window), else the 316 cycles
cyc AS (
  SELECT ac.d, ac.sid, ac.a AS ts, ac.a - to_seconds(ac.cyc) AS cs,
         (NOT ac.instep AND x.sid IS NULL) OR ABS(ac.actual - ac.cyc) > 2 AS is_out,
         CASE WHEN x.sid IS NOT NULL THEN '316' ELSE 'inferred' END AS method, true AS anch
  FROM ac JOIN ref0 r ON r.d = ac.d AND r.sid = ac.sid AND r.phase = ac.phase AND r.h = ac.h
  LEFT JOIN dis316 x ON x.d = ac.d AND x.sid = ac.sid
  WHERE (epoch(ac.t316) - epoch(ac.a) < ac.cyc OR ac.span <= 3 * ac.cyc)
  UNION ALL
  SELECT pl.d, pl.sid, c.ts, c.cs, ABS(c.actual - pl.cyc) > 2, CASE WHEN dv.has316 THEN '316' ELSE 'local zero' END, false
  FROM c316 c JOIN cplan pl ON c.d = pl.d AND c.ts > pl.t0 AND c.ts <= pl.t1 JOIN dev dv ON dv.d = pl.d
  WHERE c.cs IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ref0 r WHERE r.d = pl.d AND r.sid = pl.sid)),
cyc2 AS (SELECT c.*, pl.t0, pl.t1, pl.pattern, pl.cyc AS cycle_s,
                c.is_out AND NOT COALESCE(LAG(c.is_out) OVER (PARTITION BY c.d, c.sid ORDER BY c.ts), false) AS ep_start
         FROM cyc c JOIN cplan pl ON pl.d = c.d AND pl.sid = c.sid),
steady AS (SELECT * FROM cyc2 WHERE ts > t0 + INTERVAL 10 MINUTE AND cs >= t0),
-- side-phase walks in the cycle or the two before, and whether that phase ran past its programmed split (the service the
-- walk is in: the phase's last green begun by the walk)
coordph AS (SELECT d, sid, phase FROM ref0 UNION SELECT d, sid, CASE WHEN phase <= 4 THEN phase + 4 ELSE phase - 4 END FROM ref0),
walks AS (SELECT w.d, w.ts, w.param AS wph, v.xe >= w.ts AS walk_in, epoch(v.xe) - epoch(v.gs) AS svc_s
          FROM (SELECT * FROM ev WHERE code = 21) w
          ASOF LEFT JOIN svc v ON v.d = w.d AND v.phase = w.param AND v.gs <= w.ts + INTERVAL 200 MILLISECOND),
walk_cyc AS (
  SELECT DISTINCT s.d, s.sid, s.ts, w.wph, COALESCE(w.walk_in AND w.svc_s > sp.split + 2, false) AS overrun
  FROM steady s JOIN walks w ON w.d = s.d AND w.ts <= s.ts
   AND w.ts > CASE WHEN NOT s.anch THEN s.cs ELSE s.cs - to_seconds(2 * s.cycle_s) END
  LEFT JOIN splits sp ON sp.d = s.d AND sp.sid = s.sid AND sp.phase = w.wph
  WHERE NOT EXISTS (SELECT 1 FROM coordph k WHERE k.d = s.d AND k.sid = s.sid AND k.phase = w.wph)),
pre_cyc AS (SELECT DISTINCT s.d, s.sid, s.ts, CASE WHEN e.code = 102 THEN 'preempt' ELSE 'TSP' END AS kind
            FROM steady s JOIN ev e ON e.d = s.d AND e.code IN (102, 112) AND e.ts <= s.ts
              AND e.ts > CASE WHEN NOT s.anch THEN s.cs ELSE s.cs - to_seconds(2 * s.cycle_s) END),
-- (a) per pattern (all the periods it ran)
offp AS (
  SELECT d, pattern, ANY_VALUE(cycle_s) AS cycle_s, MIN(method) AS method,
         COUNT(*) AS cycles, COUNT(*) FILTER (WHERE ep_start) AS episodes, COUNT(*) FILTER (WHERE NOT is_out) AS n_in,
         MIN(ts) FILTER (WHERE is_out) AS example_time, MAX(ts) FILTER (WHERE is_out) AS example_end,
         ROUND(100 * AVG(is_out::INT), 0) AS pct_cycles_out
  FROM steady GROUP BY d, pattern
  HAVING COUNT(*) >= 36 AND SUM(cycle_s) >= 3600 AND AVG(is_out::INT) >= 0.20),
wstat AS (  -- cycles with a walk that ran past the split (without the inference: a walk) in the cycle or the two before
  SELECT s.d, s.pattern, w.wph, ANY_VALUE(o.n_in) AS n_in, ANY_VALUE(o.cycles - o.n_in) AS n_out,
         COUNT(DISTINCT s.ts) FILTER (WHERE s.is_out) AS n_out_walk, COUNT(DISTINCT s.ts) FILTER (WHERE NOT s.is_out) AS n_in_walk
  FROM steady s JOIN walk_cyc w ON w.d = s.d AND w.sid = s.sid AND w.ts = s.ts JOIN offp o ON o.d = s.d AND o.pattern = s.pattern
  WHERE w.overrun OR NOT s.anch GROUP BY 1, 2, 3),
wbest AS (
  SELECT *, n_out_walk / n_out AS share_out FROM wstat
  WHERE n_out_walk >= 10 AND n_out_walk >= 0.5 * n_out AND n_out_walk / n_out >= 1.5 * COALESCE(n_in_walk / NULLIF(n_in, 0), 0)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d, pattern ORDER BY n_out_walk DESC, wph) = 1),
pstat AS (
  SELECT s.d, s.pattern, p.kind, COUNT(DISTINCT s.ts) FILTER (WHERE s.is_out) / ANY_VALUE(o.cycles - o.n_in) AS share_out
  FROM steady s JOIN offp o ON o.d = s.d AND o.pattern = s.pattern JOIN pre_cyc p ON p.d = s.d AND p.sid = s.sid AND p.ts = s.ts
  GROUP BY 1, 2, 3
  HAVING COUNT(DISTINCT s.ts) FILTER (WHERE s.is_out) >= 3
     AND COUNT(DISTINCT s.ts) FILTER (WHERE s.is_out) >= 0.5 * ANY_VALUE(o.cycles - o.n_in)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY s.d, s.pattern ORDER BY share_out DESC) = 1),
a_rows AS (
  SELECT o.d, CASE WHEN o.pct_cycles_out >= 33 THEN 'problem' ELSE 'worth a look' END AS severity,
         'not holding coordination' AS issue, o.pattern, o.cycle_s, o.example_time, o.example_end,
         o.pct_cycles_out AS pct_off_cycles, o.cycles, o.episodes, o.method,
         CASE WHEN w.wph IS NOT NULL THEN 'walk' ELSE p.kind END AS cause, w.wph AS walk_phase,
         ROUND(100 * COALESCE(w.share_out, p.share_out), 0) AS pct_off_with_cause,
         ROUND(100 * w.n_in_walk / NULLIF(w.n_in, 0), 0) AS pct_in_step_with_cause,
         NULL::DOUBLE AS minutes, NULL::BOOLEAN AS still_out_at_end
  FROM offp o LEFT JOIN wbest w ON w.d = o.d AND w.pattern = o.pattern LEFT JOIN pstat p ON p.d = o.d AND p.pattern = o.pattern),
-- (b) slow transition; a change before the data starts is not seen
chg AS (SELECT c.* FROM cplan c JOIN dev dv ON dv.d = c.d
        WHERE c.pattern IS DISTINCT FROM c.prev_pat AND c.t0 >= dv.first_ts
          AND NOT EXISTS (SELECT 1 FROM offp o WHERE o.d = c.d AND o.pattern = c.pattern)),
run3 AS (SELECT d, sid, ts, cs, SUM((NOT is_out)::INT) OVER w3 AS ok3, COUNT(*) OVER w3 AS n3, MIN(cs) OVER w3 AS run_start
         FROM cyc2 WHERE EXISTS (SELECT 1 FROM chg c WHERE c.d = cyc2.d AND c.sid = cyc2.sid)
         WINDOW w3 AS (PARTITION BY d, sid ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)),
settle AS (SELECT r.d, r.sid, MIN(r.run_start) FILTER (WHERE r.ok3 = 3 AND r.n3 = 3) AS settled, COUNT(*) AS ncyc,
                  CASE WHEN bool_or(x.sid IS NOT NULL) THEN '316'
                       WHEN EXISTS (SELECT 1 FROM ref0 f WHERE f.d = r.d AND f.sid = r.sid) THEN 'inferred'
                       WHEN bool_or(dv.has316) THEN '316' ELSE 'local zero' END AS method
           FROM run3 r JOIN dev dv ON dv.d = r.d LEFT JOIN dis316 x ON x.d = r.d AND x.sid = r.sid GROUP BY r.d, r.sid),
b_rows AS (
  SELECT d, CASE WHEN m >= 30 THEN 'problem' ELSE 'worth a look' END AS severity, 'slow transition' AS issue,
         pattern, cycle_s, example_time, example_end, NULL::DOUBLE AS pct_off_cycles, cycles, NULL::BIGINT AS episodes,
         method, NULL::VARCHAR AS cause, NULL::INTEGER AS walk_phase, NULL::DOUBLE AS pct_off_with_cause,
         NULL::DOUBLE AS pct_in_step_with_cause, m AS minutes, still_out_at_end
  FROM (SELECT c.d, c.pattern, c.cyc AS cycle_s, c.t0 AS example_time, COALESCE(s.settled, c.t1) AS example_end, s.ncyc AS cycles,
               s.method, s.settled IS NULL AS still_out_at_end,
               ROUND((epoch(GREATEST(COALESCE(s.settled, c.t1), c.t0)) - epoch(c.t0)) / 60, 1) AS m
        FROM chg c JOIN settle s ON s.d = c.d AND s.sid = c.sid WHERE s.ncyc >= 5)
  WHERE m >= 10)
SELECT * FROM (SELECT * FROM a_rows UNION ALL BY NAME SELECT * FROM b_rows)
QUALIFY ROW_NUMBER() OVER (PARTITION BY d ORDER BY CASE severity WHEN 'problem' THEN 0 ELSE 1 END, example_time) <= {max_rows}
ORDER BY d, CASE severity WHEN 'problem' THEN 0 ELSE 1 END, example_time
"""


def holding_sql(start, end, lead=None) -> str:
    """The query over the table raw for [start, end), with the plan events from lead (default: start)."""
    return HOLDING_SQL.format(ids=', '.join(str(i) for i in EVENT_IDS), start=sql_time(start), end=sql_time(end),
                              lead=sql_time(lead if lead is not None else start), max_rows=MAX_ROWS)


def _at(value) -> str:
    stamp = pd.Timestamp(value)
    return f"{stamp.month}/{stamp.day} {stamp:%H:%M}"


def _num(value) -> str:
    return f"{float(value):,.0f}"


def _minutes(value) -> str:
    return f"{_num(value)} minute{'' if round(float(value)) == 1 else 's'}"


def finding_text(row) -> str:
    """What one finding tells, facts only."""
    if row['Issue'] == SLOW_TRANSITION:
        if row['StillOutAtEnd']:
            return (f"Pattern {int(row['Pattern'])} was not back in step within {_minutes(row['Minutes'])} of the change at "
                    f"{_at(row['Start'])}, before the next plan or the end of the data.")
        return f"Pattern {int(row['Pattern'])} took {_num(row['Minutes'])} minutes to get back in step after the change at {_at(row['Start'])}."
    cycle = f"pattern {int(row['Pattern'])} ({_num(row['CycleSeconds'])} s cycle)"
    out = f"out of step on {_num(row['PctOutOfStep'])}% of cycles from {_at(row['Start'])}"
    if row['Cause'] == 'walk' and pd.notna(row['WalkPhase']):
        return f"The phase {int(row['WalkPhase'])} walk runs past its split and keeps the signal out of step: {cycle} was {out}."
    text = f"{cycle[0].upper()}{cycle[1:]} was {out}"
    if row['Cause'] in ('preempt', 'TSP') and pd.notna(row['PctOutWithCause']):
        kind = 'a preempt' if row['Cause'] == 'preempt' else 'a transit priority check-in'
        return f"{text}, with {kind} in {_num(row['PctOutWithCause'])}% of them."
    if row['Grade'] == URGENT:
        return f"{text}, so the signal spends much of its time in transition."
    return f"{text}."


def holding_findings(controller_events, report_day, plan_lead_days: int = PLAN_LEAD_DAYS) -> pd.DataFrame:
    """Each signal's coordination holding findings of the report day (COLUMNS), every signal in one query.
    controller_events: a DataFrame or DuckDB relation (DeviceId, TimeStamp, EventId, Parameter) with EVENT_IDS over the
    report day, and the plan events (131-149) of the plan_lead_days before it where there are any."""
    if controller_events is None:
        return pd.DataFrame(columns=COLUMNS)
    start = pd.Timestamp(report_day).normalize()
    rows = query_table(controller_events, holding_sql(start, start + pd.Timedelta(days=1), start - pd.Timedelta(days=plan_lead_days)))
    if rows.empty:
        return pd.DataFrame(columns=COLUMNS)
    out = pd.DataFrame({
        'DeviceId': rows['d'].astype(str), 'Grade': rows['severity'].map({'problem': URGENT}).fillna(REVIEW),
        'Issue': rows['issue'], 'Pattern': rows['pattern'], 'CycleSeconds': rows['cycle_s'], 'Start': rows['example_time'],
        'End': rows['example_end'], 'PctOutOfStep': rows['pct_off_cycles'], 'Cycles': rows['cycles'], 'Episodes': rows['episodes'],
        'Method': rows['method'], 'Cause': rows['cause'], 'WalkPhase': rows['walk_phase'],
        'PctOutWithCause': rows['pct_off_with_cause'], 'PctInStepWithCause': rows['pct_in_step_with_cause'],
        'Minutes': rows['minutes'], 'StillOutAtEnd': rows['still_out_at_end'],
    })
    out['Text'] = [finding_text(row) for _, row in out.iterrows()]
    return out[COLUMNS].reset_index(drop=True)
