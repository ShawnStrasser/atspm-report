"""Signal Focus: the few signals worst off across more than one area, each told once.

Every check reports on its own, so a signal in trouble is spread over many tables. This section takes the worst few (top 3 at
most in each report: the top 3 of all regions in the All Regions report, and each region's own top 3 in its report) and lists
each in a two-column table, problem type and problems, followed by its charts in the same order. A signal selected here leaves
every other table and chart of that report. Fewer is good: a signal has to reach MIN_SCORE, which is set for one to three
signals a day over all regions, and many days no signal in a region does.

Its problems must also belong together (see related): a group of them that share a phase, such as a detector fault and max
outs on its phase, has to span more than one area and reach MIN_SCORE by itself. Problems that only happen at the same signal,
such as a detector fault and a preempt input, bring it here only when there are MIN_TYPES or more different kinds of them.

The table holds measured facts only: no causes, no advice. A problem with no chart says so in its row.

The score, from new alerts only: for each area the signal has a problem in, its severity plus AREA_POINTS, plus LINK_POINTS for a
pair of problems that belong together, all times how recent the area's new problems are (RECENCY_WEIGHTS: the most recent day of
data counts in full, older days less). Repeats that suppression held back do not count, but once a signal is selected the table
lists them too, marked as ongoing. Missing data counts for nothing: it is listed last, and never brings a signal here.
A detector or button fault measured to affect operations is a problem in two areas, the detection and how the signal ran, so
it can bring a signal here with nothing else when the harm is wide enough.

Severity is how directly the problem puts road users at risk or delays them, and how much of it there is:
- Clearance: how common it is, not that it happened: CLEARANCE_SCALE times the square root of the share of samples off, so 1% of
  samples scores 0.5 and 25% scores 2.5. Long clearances count half.
- Detectors and ped buttons: by their urgency tier (see detector_triage), plus a point for each further phase hurt, up to 2.
- A point more for a problem covering WIDE_ITEMS or more detectors, phases or movements.

Two problems belong together for the score when they share a phase: a stuck detector and max outs on its phase, a silent
detector and skips on its phase, a detector measured to slow a phase that also alerts, a ped button and max outs or skips on its
phase, a flash alarm and a conflict. The table does not say one caused the other.

Lane closures, timing plan changes and ped set-up issues are information: they count for nothing but are listed, since the
signal leaves their tables too.
"""

import json
import math

import pandas as pd

from .atspm_detector_health import ALERT_TYPE, FAULT_STATUSES, PED, VEHICLE
from .detector_triage import FAULT_KINDS, KIND_WORDS, OTHER_PHASES, PED_KINDS, SERVICE_LOST
from .focus_overview import phase_words
from .health_story import HEADINGS, SILENT_LABELS, _kinds, _when
from .preempt_cascade import DIRECTION_BUS_INTERRUPTED, DIRECTION_FALSE_CALLS, SECTION_RAIL, SECTION_TSP
from .table_generation import ONGOING_SINCE_COLUMN, _format_conflict_pair, _format_clearance_movement

SECTION_TITLE = 'Signal Focus'
SECTION_EXPLANATION = 'Signals with problems in more than one area'

DETECTORS, PED_CATEGORY, SKIPS, MAX_OUTS, CONFLICTS, CLEARANCE, PREEMPT, ALARMS, MISSING = (
    'Detectors', 'Ped', 'Phase skips', 'Max outs', 'Conflicts', 'Clearance', 'Preempt', 'Alarms', 'Missing data')
HARM_ROW, OTHER_DETECTORS_ROW, INFORMATION_ROW = 'Detector problems with worse operations during the fault', 'Other detector problems', 'For information'
HARM_TIERS = (SERVICE_LOST, OTHER_PHASES)

# Severity of an area, and the variants within one.
SEVERITY = {
    CONFLICTS: 5.0,
    'preempt fault': 4.0, 'flash alarm': 4.0, 'service lost': 4.0,
    'harm to other phases': 3.0, SKIPS: 3.0,
    MAX_OUTS: 2.0, 'preempt change': 2.0, ALARMS: 2.0,
    DETECTORS: 1.0, PED_CATEGORY: 1.0, 'skips rare': 1.0,
    MISSING: 0.0,
}
CLEARANCE_SCALE = 5.0
LONG_CLEARANCE_WEIGHT = 0.5
AREA_POINTS = 1.0
LINK_POINTS = 2.0
WIDE_ITEMS = 3
WIDE_BONUS = 1.0
MAX_HURT_PHASE_POINTS = 2
# A fault measured to harm three or more other phases reaches it with nothing else (3 + 2 for the phases + 2 areas); harm to one
# phase (5), or a fault with no measured effect beside an alarm or a few skips (4 to 6), does not. Checked on the mornings of 5 to
# 12 November 2026: one or two signals over all regions on an ordinary day, eight on a morning when whole approaches' detectors
# stuck on at many signals at once (each report still lists its top three).
MIN_SCORE = 7.0
# Problems at one signal that have nothing to do with each other (a detector fault on one phase and a preempt input) bring it
# here only when there are this many different kinds of them; fewer have to belong together (see related).
MIN_TYPES = 3
# A detector or button fault belongs with max outs, skips or another fault on a phase they share.
FAULT_CATEGORIES = frozenset({DETECTORS, PED_CATEGORY})
PHASE_CATEGORIES = frozenset({MAX_OUTS, SKIPS})
# What an area's score is worth by how many days before the most recent day of data its latest new problem was; older still counts the last.
RECENCY_WEIGHTS = (1.0, 0.5, 0.25)
# The table's rows, and so the charts' order, with Missing data always last.
ROW_ORDER = [HARM_ROW, MAX_OUTS, SKIPS, CONFLICTS, CLEARANCE, PREEMPT, ALARMS, PED_CATEGORY, OTHER_DETECTORS_ROW, INFORMATION_ROW, MISSING]
PREEMPT_CHANGES = ('Increase', 'Decrease')
FLASH_ALARMS = ('MMU Flash', 'Flash - MMU', 'Flash - Fault Monitor')
LABEL_WORDS = {'stuck on (together)': 'stuck on', 'count zone held ON': 'stuck on', 'counts dropped': 'counts fall'}
# A max-out alert on a phase with a stuck detector, and skips on a phase with a detector that stopped calling, score as linked.
MAX_OUT_CAUSES = ('Stuck on', 'Ped stuck on', 'Ped erratic')
SKIP_CAUSES = ('Silent', 'Counts low', 'Ped stuck on', 'Ped erratic')
# Exact times stay when there are this few conflicts: someone is going to look for them in the controller log.
EXACT_CONFLICTS = 2
# Every known vehicle detector of a signal down at once (held ON or silent) for this long in all is told as one line and one
# chart: their charts would all show the same thing.
DOWN_LABELS = ('stuck on', 'stuck on (together)', 'count zone held ON', 'silent')
ALL_DOWN_MINUTES = 30
# What backs a line: a chart of this kind, or None for no chart.
CHART_DETECTOR, CHART_MAX_OUT, CHART_WAIT, CHART_GREEN, CHART_SKIPS, CHART_PREEMPT, CHART_PED, CHART_MISSING = (
    'detector', 'max out', 'wait', 'green', 'skips', 'preempt', 'ped', 'missing')
# A measured rise its chart shows, in words without the numbers the chart gives.
RISE_WORDS = {CHART_WAIT: 'Phase wait rose on {}', CHART_MAX_OUT: 'Max-outs rose on {}'}

PROBLEM_COLUMNS = ['DeviceId', 'Category', 'Points', 'Links', 'Items', 'Lines', 'Harm', 'Phases', 'New', 'LastDay', 'OnPhases']
FOCUS_COLUMNS = ['Rank', 'DeviceId', 'Signal', 'Region', 'Score', 'Categories', 'Items', 'Rows', 'HasOngoing', 'Charts']


def _date(value) -> str:
    stamp = pd.Timestamp(value)
    return f"{stamp.month}/{stamp.day}"


def _upper_first(text: str) -> str:
    return text[:1].upper() + text[1:]


def _lower_first(text: str) -> str:
    return text[:1].lower() + text[1:]


def _numbers(values) -> str:
    return ', '.join(str(number) for number in sorted({int(value) for value in values}))


def _ongoing_prefix(rows) -> str:
    if rows is None or '_held' not in rows or not rows['_held'].all():
        return ''
    since = pd.to_datetime(rows[ONGOING_SINCE_COLUMN], errors='coerce').min() if ONGOING_SINCE_COLUMN in rows else pd.NaT
    return f"Ongoing since {_date(since)}: " if pd.notna(since) else 'Ongoing: '


def _line(text: str, chart: str = None, rows=None, detail: str = None) -> dict:
    """One line of the table and the chart that backs it, marked ongoing when all its rows were held back. detail: the line with
    its numbers, told instead when its chart is not drawn."""
    line = {'text': _ongoing_prefix(rows) + text, 'chart': chart}
    if detail:
        line['detail'] = _ongoing_prefix(rows) + detail
    return line


def _last_day(group):
    """The latest day any new row of the group happened on: by its periods when it has them, since a health alert's Date is the report day."""
    rows = group[~group['_held'].astype(bool)] if '_held' in group else group
    days = []
    for _, row in rows.iterrows():
        periods = row.get('Periods')
        ends = [pd.to_datetime(period.get('end'), errors='coerce') for period in periods] if isinstance(periods, (list, tuple)) and len(periods) else []
        ends = [end for end in ends if pd.notna(end)]
        if ends:
            days.append((max(ends) - pd.Timedelta(seconds=1)).normalize())    # a period ending at midnight belongs to the day before
            continue
        date = pd.to_datetime(row.get('Date'), errors='coerce')
        if pd.notna(date):
            days.append(date.tz_localize(None).normalize() if date.tzinfo else date.normalize())
    return max(days) if days else pd.NaT


def _problem(device, category, severity, items, lines, group, links=0.0, harm=None, phases=None, on=None):
    """on: the phases the problem is on (its detectors' and buttons' phases and the phases they hurt, the phases that max out
    or skip), so problems that share one can be told apart from problems that only happen at the same signal."""
    held = group['_held'].astype(bool) if '_held' in group else pd.Series(False, index=group.index)
    return {'DeviceId': device, 'Category': category, 'Points': float(severity), 'Links': float(links), 'Items': int(items), 'Lines': lines,
            'Harm': harm or [], 'Phases': phases or {}, 'New': bool((~held).any()), 'LastDay': _last_day(group),
            'OnPhases': sorted({int(phase) for phase in (() if on is None else on) if pd.notna(phase) and int(phase) > 0})}


def _phases_of(rows, urgent=None, phases: dict = None) -> set:
    """The phases of these fault rows, the phases the urgent ones were measured to hurt, and those their charts name."""
    found = set(pd.to_numeric(rows['Phase'], errors='coerce').dropna().astype(int)) if 'Phase' in rows else set()
    if urgent is not None and not urgent.empty:
        found |= {int(phase) for text in urgent['HurtPhases'].dropna() for phase in str(text).split(',') if phase.strip().lstrip('-').isdigit()}
    for named in (phases or {}).values():
        found |= {int(phase) for phase in named}
    return found


def _faults(frame, info):
    """Fault rows of detectors and buttons with Phase, Kind and Tier, most urgent first."""
    none = pd.DataFrame(columns=['DeviceId', 'DetectorType', 'Detector', 'Status', 'Label', 'Phase', 'Kind', 'Tier', 'HurtPhases', 'ImpactText', 'Group'])
    if frame.empty or 'Status' not in frame:
        return none
    rows = frame[frame['Status'].isin(FAULT_STATUSES)].copy()
    if rows.empty:
        return none
    for column in ('Tier', 'HurtPhases', 'ImpactText', 'TriagePhase', 'Kind', 'Group'):
        if column not in rows:
            rows[column] = None
    rows = rows.drop(columns=[column for column in ('Phase',) if column in rows]).merge(
        info[['DeviceId', 'Detector', 'Phase']].assign(DetectorType=VEHICLE), on=['DeviceId', 'DetectorType', 'Detector'], how='left')
    rows['Phase'] = pd.to_numeric(rows['Phase'], errors='coerce').fillna(pd.to_numeric(rows['TriagePhase'], errors='coerce'))
    vehicle = rows['DetectorType'] == VEHICLE
    kinds = rows['Label'].map(FAULT_KINDS).where(vehicle, rows['Label'].map(PED_KINDS)).fillna(rows['Label'].astype(str).str.capitalize())
    rows['Kind'] = rows['Kind'].where(rows['Kind'].notna(), kinds)
    rows['_rank'] = pd.to_numeric(rows['TriageRank'], errors='coerce') if 'TriageRank' in rows else float('nan')
    return rows.sort_values(['_rank', 'Score', 'Detector'], na_position='last', kind='stable')


def _when_of(row) -> str:
    """How long and when, in the words of the health tables: one period keeps its exact times, repeats are summed."""
    kinds = _kinds(row.get('Periods'), row.get('Evidence'))
    return _when(kinds[0]['spans'], exact=1, short=True, open_end=kinds[0]['open']) if kinds and kinds[0]['spans'] else ''


def _when_of_all(rows) -> str:
    """The same when these rows share it, in brackets: exact times only help when they say one thing."""
    times = {_when_of(row) for _, row in rows.iterrows()}
    return f" ({times.pop()})" if len(times) == 1 and '' not in times else ''


def _fault_words(kind) -> str:
    """A kind of fault in the words of its chart."""
    return KIND_WORDS.get(kind, str(kind).lower()).replace('counts low', 'counts fell')


def _shown_word(row, word: str) -> str:
    """The words for a detector's fault, as atspm-detector found it."""
    return word


def _also_lines(rows, main: str) -> list:
    """The other findings these detectors have in the days shown, which their charts show too: 'Det 33, 35 also counts fall'.

    """
    others = {}
    for _, row in rows.iterrows():
        periods = row.get('Periods')
        for period in periods if isinstance(periods, (list, tuple)) else []:
            word = LABEL_WORDS.get(period.get('what'), period.get('what'))
            if word != main:
                others.setdefault(word, []).append(row['Detector'])
    return [_line(f"Det {_numbers(numbers)} also {word}", CHART_DETECTOR, rows) for word, numbers in others.items()]


def _merged(spans) -> list:
    merged = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return merged


def _intersection(first, second) -> list:
    return [(max(a, c), min(b, d)) for a, b in first for c, d in second if max(a, c) < min(b, d)]


def _without(spans, holes) -> list:
    """spans with the holes cut out."""
    out = []
    for start, end in spans:
        pieces = [(start, end)]
        for hole_start, hole_end in holes:
            pieces = [part for a, b in pieces for part in ((a, min(b, hole_start)), (max(a, hole_end), b)) if part[0] < part[1]]
        out += pieces
    return out


def active_detectors(known, counts=None) -> list:
    """The known detectors that counted anything in `counts` (TimeStamp, Detector, Total of one signal). A channel that counted
    nothing all window is a spare, not a detector that failed. Without counts, all of them."""
    known = sorted({int(number) for number in known})
    if counts is None or counts.empty:
        return known
    counted = set(counts.loc[pd.to_numeric(counts['Total'], errors='coerce') > 0, 'Detector'].astype(int))
    return [number for number in known if number in counted]


def detectors_down_together(faults, known, counts=None) -> list:
    """[(start, end)] when every one of a signal's working vehicle detectors was down at once, from its fault rows (Detector,
    Periods: held ON or silent) and, for a detector without a fault row, from its counts (`counts`: TimeStamp, Detector, Total
    of this signal, 15-minute bins): it is down wherever it counted nothing. Empty when they never all were, for
    ALL_DOWN_MINUTES in all, or fewer than two are working."""
    known = set(active_detectors(known, counts))
    if len(known) < 2 or faults is None or faults.empty:
        return []
    down = {}
    for _, row in faults.iterrows():
        periods = row.get('Periods')
        for period in periods if isinstance(periods, (list, tuple)) else []:
            start, end = pd.to_datetime(period.get('start'), errors='coerce'), pd.to_datetime(period.get('end'), errors='coerce')
            if period.get('what') in DOWN_LABELS and pd.notna(start) and pd.notna(end) and end > start:
                down.setdefault(int(row['Detector']), []).append((start, end))
    faulted = known & set(down)
    if len(faulted) < 2:
        return []
    common = None
    for number in sorted(faulted):
        spans = _merged(down[number])
        common = spans if common is None else _intersection(common, spans)
    for number in sorted(known - faulted):
        if counts is None or counts.empty:
            return []
        mine = counts[(counts['Detector'].astype(int) == number) & (pd.to_numeric(counts['Total'], errors='coerce') > 0)]
        busy = [(stamp, stamp + pd.Timedelta(minutes=15)) for stamp in pd.to_datetime(mine['TimeStamp'])]
        common = _without(common, _merged(busy))
    common = _merged(common)
    total = sum((end - start for start, end in common), pd.Timedelta(0))
    return common if total >= pd.Timedelta(minutes=ALL_DOWN_MINUTES) else []


def known_detectors(info, device) -> list:
    """A signal's vehicle detectors in the detector table."""
    if info is None or info.empty:
        return []
    return info.loc[info['DeviceId'].astype(str) == str(device), 'Detector'].dropna().astype(int).unique().tolist()


def _all_down_line(spans, count: int, rows) -> dict:
    return _line(f"All {count} detectors failed together ({_when(spans, exact=1, short=True)})", CHART_DETECTOR, rows)


def _impact_lines(first, how_many: int, phases: dict, all_phases=None) -> list:
    """The lines of what a fault was measured to do, and the phases each chart must show added to `phases`.

    A group of faults tells its worst one's numbers, naming it; a rise on every phase the signal runs (all_phases) reads 'all
    phases'. Without the measured lines (alerts from before them) it keeps the fault's impact text, without its estimate of
    vehicles."""
    who = f"With {'button' if first['DetectorType'] == PED else 'det'} {int(first['Detector'])}: " if how_many > 1 else ''
    facts = first.get('ImpactLines')
    if not isinstance(facts, str) or not facts:
        return [_line(who + str(first['ImpactText']).split('; about')[0])]
    lines = []
    for fact in json.loads(facts):
        full = who + (_lower_first(fact['text']) if who else fact['text'])
        rise = RISE_WORDS.get(fact['chart'])
        if rise and fact['phases']:
            short = rise.format(phase_words(fact['phases'], all_phases))
            lines.append(_line(who + (_lower_first(short) if who else short), fact['chart'], detail=full))
        else:
            lines.append(_line(full, fact['chart']))
        phases.setdefault(fact['chart'], set()).update(fact['phases'])
    return lines


def _event(row, word: str):
    """What tells one event apart: the triage's group for faults it judged as one (see detector_triage), else the tier and the
    fault's words, as before the groups."""
    group = pd.to_numeric(row.get('Group'), errors='coerce')
    return ('group', float(group)) if pd.notna(group) else (row['Tier'], word)


def _detector_problems(faults, info=None, counts=None, runs=None, told: set = None) -> list:
    """told: the (signal, group) events whose measured effect is already told, so a group of detectors and buttons tells it once."""
    told = told if told is not None else set()
    problems = []
    for device, group in faults[faults['DetectorType'] == VEHICLE].groupby('DeviceId', sort=False):
        all_phases = (runs or {}).get(str(device))
        urgent = group[group['Tier'].isin(HARM_TIERS)]
        others = group.drop(urgent.index)
        harm, lines, phases = [], [], {}
        mine = counts[counts['DeviceId'].astype(str) == str(device)] if counts is not None and not counts.empty else None
        known = active_detectors(known_detectors(info, device), mine)
        together = detectors_down_together(group, known, mine)
        if together:
            # One line for them all, where the first of them would be, and the measured impact of the worst.
            (harm if not urgent.empty else lines).append(_all_down_line(together, len(known), group))
            if not urgent.empty:
                harm += _impact_lines(urgent.iloc[0], 1, phases, all_phases)
        worded = urgent.assign(_word=[_shown_word(row, _fault_words(row['Kind'])) for _, row in urgent.iterrows()])
        worded = worded.assign(_event=[_event(row, row['_word']) for _, row in worded.iterrows()])
        # One event's detectors, by what is wrong with them, then what the event was measured to do, once.
        for event, block in ([] if together else worded.groupby('_event', sort=False)):
            for words, rows in block.groupby('_word', sort=False):
                harm.append(_line(f"Det {_numbers(rows['Detector'])} {words}{_when_of_all(rows)}", CHART_DETECTOR, rows))
                harm += _also_lines(rows, words)
            harm += _impact_lines(block.iloc[0], block['Detector'].nunique(), phases, all_phases)
            if event[0] == 'group':
                told.add((device, event[1]))
        for words, rows in ([] if together else others.assign(
                _word=[_shown_word(row, LABEL_WORDS.get(row['Label'], row['Label'])) for _, row in others.iterrows()]).groupby('_word', sort=False)):
            lines.append(_line(f"Det {_numbers(rows['Detector'])} {words}{_when_of_all(rows)}", CHART_DETECTOR, rows))
            lines += _also_lines(rows, words)
        severity = ('service lost' if (group['Tier'] == SERVICE_LOST).any() else
                    'harm to other phases' if (group['Tier'] == OTHER_PHASES).any() else DETECTORS)
        points = SEVERITY[severity]
        hurt = {phase for text in urgent['HurtPhases'].dropna() for phase in str(text).split(',') if phase}
        points += min(max(len(hurt) - 1, 0), MAX_HURT_PHASE_POINTS)
        problems.append(_problem(device, DETECTORS, points, group['Detector'].nunique(), lines, group, harm=harm, phases=phases,
                                 on=_phases_of(group, urgent, phases)))
    return problems


def _linked(faults, device, phase, kinds) -> bool:
    """Whether a detector or button on this phase has one of these kinds of fault, or was measured to hurt the phase."""
    if faults.empty:
        return False
    mine = faults[faults['DeviceId'] == device]
    if mine.empty:
        return False
    own = (mine['Phase'] == phase) & mine['Kind'].isin(kinds)
    hurting = mine['HurtPhases'].fillna('').astype(str).map(lambda text: str(int(phase)) in text.split(','))
    return bool((own | hurting).any())


def _max_out_problems(frame, faults) -> list:
    if frame.empty or 'Alert' not in frame:
        return []
    problems = []
    for device, group in frame[frame['Alert'] == 1].groupby('DeviceId', sort=False):
        worst = group.sort_values('Percent MaxOut', ascending=False).drop_duplicates('Phase')
        lines, linked = [], False
        for _, row in worst.iterrows():
            rows = group[group['Phase'] == row['Phase']]
            lines.append(_line(f"Ph{int(row['Phase'])} reached {row['Percent MaxOut']:.0%} on {_date(row['Date'])}", CHART_MAX_OUT, rows))
            linked = linked or _linked(faults, device, row['Phase'], MAX_OUT_CAUSES)
        problems.append(_problem(device, MAX_OUTS, SEVERITY[MAX_OUTS], len(worst), lines, group, links=LINK_POINTS if linked else 0.0,
                                 phases={CHART_MAX_OUT: {int(phase) for phase in worst['Phase']}}, on=worst['Phase']))
    return problems


def _skip_problems(frame, faults, retention_days) -> list:
    if frame.empty or 'AggregatedSkips' not in frame:
        return []
    problems = []
    for device, group in frame.groupby('DeviceId', sort=False):
        worst = group.sort_values('AggregatedSkips', ascending=False).drop_duplicates('Phase')
        frequent = worst['AggregatedSkips'].iloc[0] >= retention_days
        lines, linked = [], False
        for _, row in worst.iterrows():
            count = int(row['AggregatedSkips'])
            rows = group[group['Phase'] == row['Phase']]
            lines.append(_line(f"Ph{int(row['Phase'])} skipped {count} {'time' if count == 1 else 'times'} in {retention_days} days", CHART_SKIPS, rows))
            linked = linked or _linked(faults, device, row['Phase'], SKIP_CAUSES)
        problems.append(_problem(device, SKIPS, SEVERITY[SKIPS if frequent else 'skips rare'], len(worst), lines, group,
                                 links=LINK_POINTS if linked else 0.0, on=worst['Phase']))
    return problems


def _ped_problems(faults, runs=None, told: set = None) -> list:
    """told: the (signal, group) events whose measured effect the signal's detectors already told."""
    told = told if told is not None else set()
    problems = []
    for device, pressed in faults[faults['DetectorType'] == PED].groupby('DeviceId', sort=False):
        all_phases = (runs or {}).get(str(device))
        lines, harm, phases = [], [], {}
        for label, rows in pressed.groupby('Label', sort=False):
            first = rows.iloc[0]
            when = _when_of(first) if len(rows) == 1 else ''
            line = _line(f"Button {_numbers(rows['Detector'])}: {_lower_first(HEADINGS.get(label, str(label)))}" + (f" ({when})" if when else ''),
                         CHART_PED, rows)
            if first['Tier'] in HARM_TIERS:
                harm.append(line)
                event = _event(first, label)
                said = _impact_lines(first, rows['Detector'].nunique(), phases, all_phases)
                if (device, event[1]) not in told or event[0] != 'group':
                    harm += said
                if event[0] == 'group':
                    told.add((device, event[1]))
            else:
                lines.append(line)
        severity = ('service lost' if (pressed['Tier'] == SERVICE_LOST).any() else
                    'harm to other phases' if (pressed['Tier'] == OTHER_PHASES).any() else PED_CATEGORY)
        # A point for each further phase hurt, as for the detectors.
        urgent = pressed[pressed['Tier'].isin(HARM_TIERS)]
        hurt = {phase for text in urgent['HurtPhases'].dropna() for phase in str(text).split(',') if phase}
        points = SEVERITY[severity] + min(max(len(hurt) - 1, 0), MAX_HURT_PHASE_POINTS)
        problems.append(_problem(device, PED_CATEGORY, points, pressed['Detector'].nunique(), lines, pressed,
                                 harm=harm, phases=phases, on=_phases_of(pressed, urgent, phases)))
    return problems


def _missing_problems(frame) -> list:
    if frame.empty or 'Alert' not in frame:
        return []
    problems = []
    for device, group in frame[frame['Alert'] == 1].groupby('DeviceId', sort=False):
        worst = group.loc[group['MissingData'].idxmax()]
        problems.append(_problem(device, MISSING, SEVERITY[MISSING], 1, [_line(f"{worst['MissingData']:.0%} of {_date(worst['Date'])}", CHART_MISSING, group)], group))
    return problems


def _conflict_problems(frames: list) -> list:
    """frames: (conflicts) and (dual indications) rows with ConflictStart and DurationSeconds."""
    named = []
    for frame in frames:
        if frame.empty:
            continue
        if 'Movement1Number' in frame:
            labels = frame.apply(lambda row: _format_conflict_pair(row), axis=1)
        else:
            labels = 'Ph ' + frame['Phase'].astype(str) + ' green with overlap ' + frame['OverlapIndication'].astype(str).str.replace('Overlap ', '').str.lower()
        named.append(frame.assign(_pair=labels))
    if not named:
        return []
    problems = []
    for device, group in pd.concat(named).groupby('DeviceId', sort=False):
        lines = []
        summary = group.groupby('_pair', sort=False)['DurationSeconds'].agg(['size', 'max']).sort_values('max', ascending=False)
        for pair, row in summary.iterrows():
            events = group[group['_pair'] == pair].sort_values('ConflictStart')
            if len(events) <= EXACT_CONFLICTS and 'ConflictStart' in events:
                times = ' and '.join(f"{pd.Timestamp(stamp):%H:%M} on {_date(stamp)}" for stamp in events['ConflictStart'])
                lines.append(_line(f"{pair} shown together at {times}, longest {float(row['max']):.1f} s", rows=events))
            else:
                lines.append(_line(f"{pair} shown together {int(row['size'])} times, longest {float(row['max']):.1f} s", rows=events))
        problems.append(_problem(device, CONFLICTS, SEVERITY[CONFLICTS], len(summary), lines, group))
    return problems


def _movement(row) -> str:
    return _format_clearance_movement(row).replace('Yellow', 'yellow').replace('Red', 'red')


def clearance_severity(short_share: float, long_share: float) -> float:
    """How common it is, not that it happened: 1% of samples short scores 0.5, 25% scores 2.5, every sample 5. Long counts half."""
    return CLEARANCE_SCALE * max(math.sqrt(max(short_share, 0.0)), LONG_CLEARANCE_WEIGHT * math.sqrt(max(long_share, 0.0)))


def _clearance_problems(frame) -> list:
    if frame.empty or 'EventClass' not in frame:
        return []
    problems = []
    samples = pd.to_numeric(frame['SampleCount'], errors='coerce').clip(lower=1)
    rated = frame.assign(_short=pd.to_numeric(frame['ShortCount'], errors='coerce').fillna(0) / samples,
                         _long=pd.to_numeric(frame['LongCount'], errors='coerce').fillna(0) / samples)
    for device, group in rated.groupby('DeviceId', sort=False):
        ordered = group.sort_values(['_short', '_long'], ascending=False)
        lines = []
        for position, row in ordered.iterrows():
            short, long = int(row['ShortCount'] or 0), int(row['LongCount'] or 0)
            samples_here = max(int(row['SampleCount']), 1)
            share = lambda count: 'under 1%' if count / samples_here < 0.01 else f"{count / samples_here:.0%}"
            words = [f"{word} in {count} of {samples_here} samples ({share(count)})" for count, word in ((short, 'short'), (long, 'long')) if count]
            lines.append(_line(f"{_upper_first(_movement(row))} {' and '.join(words)}", rows=ordered.loc[[position]]))
        severity = clearance_severity(ordered['_short'].max(), ordered['_long'].max())
        problems.append(_problem(device, CLEARANCE, severity, 1, lines, group))
    return problems


def _preempt_line(row) -> str:
    """One preempt alert in plain words, naming its kind: 'Emergency vehicle preempt 1 increase: 0.5 to 6.0 calls a day',
    'Transit Signal Priority on preempt input 3: bus calls ran as preempts (31 of 34 bus calls in the last 7 days)',
    'Transit Signal Priority on preempt input 2: bus calls pulsed the preempt input (22 in the last 7 days)', 'Rail preempt 5 long hold: ...'."""
    number, section = int(row['Preempt']), row.get('Class')
    if section == SECTION_TSP:
        ran, total = row.get('RanAsPreempt'), row.get('BusCalls')
        known = pd.notna(ran) and pd.notna(total)
        if row['Direction'] == DIRECTION_BUS_INTERRUPTED:
            counts = f" ({int(total)} in the last 7 days)" if known else ''
            return f"Transit Signal Priority on preempt input {number}: bus calls pulsed the preempt input{counts}"
        counts = f" ({int(ran)} of {int(total)} bus calls in the last 7 days)" if known else ''
        return f"Transit Signal Priority on preempt input {number}: bus calls ran as preempts{counts}"
    if row['Direction'] in PREEMPT_CHANGES:
        note = f"{float(row['BaselinePerDay']):.1f} to {float(row['RecentPerDay']):.1f} calls a day"
    else:
        note = str(row.get('Note') or '')
        if row['Direction'] == DIRECTION_FALSE_CALLS:
            note = note.split('. ')[0]
    kind = 'Rail preempt' if section == SECTION_RAIL else 'Emergency vehicle preempt'
    return f"{kind} {number} {str(row['Direction']).lower()}" + (f": {note}" if note else '')


def _preempt_problems(frame) -> list:
    if frame.empty or 'Direction' not in frame:
        return []
    problems = []
    for device, group in frame.groupby('DeviceId', sort=False):
        ordered = group.assign(_change=group['Direction'].isin(PREEMPT_CHANGES)).sort_values(['_change', 'Score'], ascending=[True, False])
        lines = []
        for position, row in ordered.iterrows():
            lines.append(_line(_preempt_line(row), CHART_PREEMPT, ordered.loc[[position]]))
        first = ordered.iloc[0]
        problems.append(_problem(device, PREEMPT, SEVERITY['preempt change' if first['_change'] else 'preempt fault'], group['Preempt'].nunique(), lines, group))
    return problems


def _alarm_problems(frame, conflicts=frozenset()) -> list:
    if frame is None or frame.empty:
        return []
    problems = []
    for device, group in frame.groupby('DeviceId', sort=False):
        # Alarms list every day they recur, so only a first one in the six weeks is news.
        marked = group.assign(_held=group['TotalCount'] > group['DayCount'], _flash=group['AlarmType'].isin(FLASH_ALARMS))
        ordered = marked.sort_values(['_flash', 'TotalCount'], ascending=[False, False])
        lines = [_line(f"{row['AlarmType']}: {int(row['DayCount'])} on {_date(row['Date'])}, {int(row['TotalCount'])} in 6 weeks", rows=ordered.loc[[position]])
                 for position, row in ordered.iterrows()]
        flash = bool(ordered['_flash'].iloc[0])
        problems.append(_problem(device, ALARMS, SEVERITY['flash alarm' if flash else ALARMS], len(group), lines, marked,
                                 links=LINK_POINTS if flash and device in conflicts else 0.0))
    return problems


def _information(health) -> dict:
    """{device: lines} of its lane closures, timing plan changes and ped set-up issues."""
    if health.empty or 'Status' not in health:
        return {}
    words = {'lane_closure': 'Det {} lane closed', 'timing_plan': 'Det {} quiet under a timing plan', 'config_issue': 'Button {} has no walk assigned'}
    rows = health[health['Status'].isin(words)]
    lines = {}
    for device, group in rows.groupby('DeviceId', sort=False):
        lines[device] = [words[status].format(_numbers(items['Detector'])) for status, items in group.groupby('Status', sort=False)]
    return lines


def _combined(new_alerts: dict, held_back: dict, key: str) -> pd.DataFrame:
    """One alert type's rows that are shown and held back, with _held saying which."""
    frames = []
    for source, held in ((new_alerts, False), (held_back, True)):
        frame = (source or {}).get(key)
        if frame is not None and not frame.empty:
            frames.append(frame.assign(DeviceId=frame['DeviceId'].astype(str), _held=held))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=['DeviceId', '_held'])


def _collect(new_alerts: dict, held_back: dict, alarms: pd.DataFrame, info: pd.DataFrame, skip_retention_days: int,
             counts: pd.DataFrame = None, runs: dict = None) -> list:
    health = _combined(new_alerts, held_back, ALERT_TYPE)
    faults = _faults(health, info)
    told = set()
    problems = _detector_problems(faults, info, counts, runs, told)
    problems += _max_out_problems(_combined(new_alerts, held_back, 'maxout'), faults)
    problems += _skip_problems(_combined(new_alerts, held_back, 'phase_skips'), faults, skip_retention_days)
    problems += _ped_problems(faults, runs, told)
    problems += _missing_problems(_combined(new_alerts, held_back, 'missing_data'))
    conflicts = _conflict_problems([_combined(new_alerts, held_back, key) for key in
                                    ('general_phase_conflicts', 'overlap_conflicts', 'same_movement_color_conflicts', 'overlap_dual_indications')])
    problems += conflicts
    problems += _clearance_problems(_combined(new_alerts, held_back, 'clearance_intervals'))
    problems += _preempt_problems(_combined(new_alerts, held_back, 'preempts'))
    if alarms is not None and not alarms.empty:
        problems += _alarm_problems(alarms.assign(DeviceId=alarms['DeviceId'].astype(str)), {problem['DeviceId'] for problem in conflicts})
    return problems


def collect_problems(new_alerts: dict, held_back: dict = None, alarms: pd.DataFrame = None, detector_info: pd.DataFrame = None,
                     skip_retention_days: int = 14, detector_counts: pd.DataFrame = None, signal_phases: dict = None) -> tuple:
    """Each signal's problems, one row per area (PROBLEM_COLUMNS), and {device: its information text}.

    new_alerts and held_back: alert type -> rows, as the report holds them. detector_info: DeviceId, Detector, Phase.
    signal_phases: {device: the phases it runs}, so a rise on every one of them reads 'all phases'.
    Lines and Harm tell everything; Points, Links and LastDay count only what is new, so a problem that is only held back has none.
    """
    info = detector_info if detector_info is not None else pd.DataFrame(columns=['DeviceId', 'Detector', 'Phase'])
    told = pd.DataFrame(_collect(new_alerts, held_back, alarms, info, skip_retention_days, detector_counts, signal_phases),
                        columns=PROBLEM_COLUMNS)
    # An alarm that repeats is not new, however it is held.
    new_alarms = alarms[alarms['TotalCount'] <= alarms['DayCount']] if alarms is not None and not alarms.empty else None
    counted = pd.DataFrame(_collect(new_alerts, None, new_alarms, info, skip_retention_days, detector_counts, signal_phases),
                           columns=PROBLEM_COLUMNS)
    points = counted.set_index(['DeviceId', 'Category'])[['Points', 'Links', 'Items', 'LastDay', 'OnPhases']]
    keys = pd.MultiIndex.from_frame(told[['DeviceId', 'Category']])
    told['Points'] = points['Points'].reindex(keys).fillna(0.0).values
    told['Links'] = points['Links'].reindex(keys).fillna(0.0).values
    told['_items_new'] = points['Items'].reindex(keys).fillna(0).astype(int).values
    told['LastDay'] = pd.to_datetime(points['LastDay'].reindex(keys)).values
    told['OnPhases'] = [phases if isinstance(phases, list) else [] for phases in points['OnPhases'].reindex(keys)]
    health = _combined(new_alerts, held_back, ALERT_TYPE)
    return told, _information(health)


def _severity(problems: pd.DataFrame) -> pd.Series:
    """Severity of each area's new problems, a point more when they cover many items."""
    items = problems['_items_new'] if '_items_new' in problems else problems['Items']
    wide = (items >= WIDE_ITEMS).astype(float) * WIDE_BONUS
    return problems['Points'] + wide.where(problems['Points'] > 0, 0.0)


def _recency(last_days: pd.Series) -> pd.Series:
    """RECENCY_WEIGHTS by days before the most recent day of data; an area with no date counts as recent."""
    days = pd.to_datetime(last_days)
    age = (days.max() - days).dt.days.fillna(0).astype(int).clip(upper=len(RECENCY_WEIGHTS) - 1)
    return age.map(lambda value: RECENCY_WEIGHTS[value])


def related(first, second) -> bool:
    """Whether two of a signal's problems (rows of the problems frame) plausibly belong together: a detector or button fault
    and max outs, skips or another detector or button fault on a phase they share (the fault's own phase or one it was
    measured to hurt), or a flash alarm and a conflict. A preempt, clearance, other alarms and missing data belong with none."""
    pair = {first['Category'], second['Category']}
    if pair == {ALARMS, CONFLICTS}:
        alarm = first if first['Category'] == ALARMS else second
        return float(alarm['Links']) > 0
    faults = [problem for problem in (first, second) if problem['Category'] in FAULT_CATEGORIES]
    if not faults or not pair <= FAULT_CATEGORIES | PHASE_CATEGORIES:
        return False
    on = lambda problem: set(problem.get('OnPhases') or ())
    return bool(on(first) & on(second))


def _qualifies(rows: pd.DataFrame, min_score: float) -> bool:
    """A signal's new problems qualify when a group of them that belong together (see related) spans more than one area and
    reaches min_score by itself, or when they are of MIN_TYPES or more different kinds and reach it together. A fault measured
    to affect operations is two areas that belong together on its own."""
    if rows['Category'].nunique() >= MIN_TYPES and round(rows['_worth'].sum(), 1) >= min_score:
        return True
    records = rows.to_dict('records')
    group = list(range(len(records)))

    def root(index):
        while group[index] != index:
            index = group[index]
        return index

    for one in range(len(records)):
        for other in range(one + 1, len(records)):
            if related(records[one], records[other]):
                group[root(other)] = root(one)
    totals = {}
    for index, record in enumerate(records):
        areas, worth = totals.get(root(index), (0, 0.0))
        totals[root(index)] = (areas + 1 + int(record['_harm']), worth + record['_worth'])
    return any(areas > 1 and round(worth, 1) >= min_score for areas, worth in totals.values())


def rank_signals(problems: pd.DataFrame, min_score: float = MIN_SCORE, related_only: bool = True) -> pd.DataFrame:
    """One row per signal whose new problems qualify (see _qualifies), worst first: DeviceId, Score, Categories, Items. The score
    is that of all its new problems.

    Missing data is never one of the problems. A detector or button fault measured to affect operations (see detector_triage)
    counts as two areas: the detection, and the operations. related_only False: any new problems in more than one area that
    reach min_score together, related or not."""
    columns = ['DeviceId', 'Score', 'Categories', 'Items']
    if problems is None or problems.empty:
        return pd.DataFrame(columns=columns)
    new = problems[problems['New'] & (problems['Category'] != MISSING)]
    if new.empty:
        return pd.DataFrame(columns=columns)
    # A fault measured to affect operations is a problem in two areas: the detection, and how the signal ran.
    harm = new['Harm'].map(lambda lines: len(lines) > 0) if 'Harm' in new else pd.Series(False, index=new.index)
    worth = (_severity(new) + AREA_POINTS * (1 + harm.astype(int)) + new['Links']) * _recency(problems['LastDay']).reindex(new.index)
    scored = new.assign(_worth=worth, _harm=harm)
    if 'OnPhases' not in scored:
        scored['OnPhases'] = [[] for _ in range(len(scored))]
    table = scored.groupby('DeviceId', as_index=False).agg(
        Worth=('_worth', 'sum'), Categories=('Category', 'nunique'), Items=('Items', 'sum'), Harm=('_harm', 'max'))
    table['Categories'] = table['Categories'] + table['Harm'].astype(int)
    table['Score'] = table['Worth'].round(1)
    table = table[(table['Categories'] > 1) & (table['Score'] >= min_score)]
    if related_only and not table.empty:
        keep = {device for device, rows in scored[scored['DeviceId'].isin(table['DeviceId'])].groupby('DeviceId') if _qualifies(rows, min_score)}
        table = table[table['DeviceId'].isin(keep)]
    return table.sort_values(['Score', 'Categories', 'Items', 'DeviceId'], ascending=[False, False, False, True])[columns].reset_index(drop=True)


def _signal_rows(problems: pd.DataFrame, information: list = None) -> tuple:
    """(the table's rows, one {type, lines} per problem type in ROW_ORDER; what the charts must show) of one signal's problems."""
    rows = {}
    charts = {}
    for _, problem in problems.iterrows():
        for chart, phases in problem['Phases'].items():
            charts.setdefault(chart, set()).update(phases)
        if problem['Harm']:
            rows.setdefault(HARM_ROW, []).extend(problem['Harm'])
        if problem['Lines']:
            row = OTHER_DETECTORS_ROW if problem['Category'] == DETECTORS else problem['Category']
            rows.setdefault(row, []).extend(problem['Lines'])
    if information:
        rows[INFORMATION_ROW] = [_line(line) for line in information]
    ordered = [{'type': row, 'lines': rows[row]} for row in ROW_ORDER if row in rows]
    return ordered, charts


def build_signal_focus(problems: pd.DataFrame, signals: pd.DataFrame, information: dict = None, top_signals: int = 3,
                       min_score: float = MIN_SCORE, related_only: bool = True) -> dict:
    """{report: rows}: each report's own worst signals (FOCUS_COLUMNS), per region and 'All Regions'. related_only: see rank_signals."""
    if top_signals <= 0 or problems is None or problems.empty:
        return {}
    table = rank_signals(problems, min_score, related_only)
    names = signals[['DeviceId', 'Name', 'Region']].assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId')
    table = table.merge(names, on='DeviceId', how='inner')
    focus = {}
    for scope, rows in [('All Regions', table)] + list(table.groupby('Region', sort=False)):
        top = rows.head(top_signals).reset_index(drop=True)
        records = []
        for position, row in top.iterrows():
            told, charts = _signal_rows(problems[problems['DeviceId'] == row['DeviceId']], (information or {}).get(row['DeviceId']))
            ongoing = any(line['text'].startswith('Ongoing') for one in told for line in one['lines'])
            records.append({'Rank': position + 1, 'DeviceId': row['DeviceId'], 'Signal': row['Name'], 'Region': row['Region'],
                            'Score': float(row['Score']), 'Categories': int(row['Categories']), 'Items': int(row['Items']),
                            'Rows': told, 'HasOngoing': ongoing, 'Charts': charts})
        focus[scope] = pd.DataFrame(records, columns=FOCUS_COLUMNS)
    return focus


def hidden_signals(focus: dict) -> dict:
    """{report: the signals its Signal Focus lists}, which leave that report's other tables and charts."""
    return {scope: set(rows['DeviceId']) for scope, rows in (focus or {}).items()}
