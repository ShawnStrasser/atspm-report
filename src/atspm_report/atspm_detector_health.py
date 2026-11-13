"""Detector health from the atspm-detector package.

atspm-detector judges every detector and every ped button from the controller's raw event log and
names what it found in a findings table, one row per finding:

    DeviceId, DetectorType (Vehicle or Ped), Detector, Start, End, Label, Confidence, Detail

No row means no finding. This module turns that table into report alerts, and it is all it reads.
Confidence runs from 0 to 1: 0.5 is at the check's suspect limit, 0.8 at its bad limit.

A detector is reported when its minutes add up over the days, not on a single short fault. Each day's
minutes go into two running totals (CUSUM), each forgiving a daily allowance, and the detector is
reported while a total stays over its threshold:

- Bad (Confidence 0.8 or more): a day of 150 bad minutes or more alerts at once, an hour a day does
  within a few days, a single short hold never does.
- Suspect (Confidence 0.5 to under 0.8): slower. One suspect day never alerts, however long it is, and
  several days to a week can.

A finding with no time of its own (no daily pattern, busy at night, misses vehicles at night) that
reaches Confidence 0.8 is reported at once; under that, each day it holds adds to the suspect total.
Lane closures of an hour or more are listed every run; timing plan changes and ped set-up issues are
information too.

Vehicle alerts go in the Detector Health section, ped alerts in the Pedestrian Button Health section.
Their alerts keep one history under the 'atspm_detector_health' key.

The table names no phase or function, so the detector table that health run read (DeviceId, Detector,
phase, function, lanes) can be passed as well: the tables then say 'Ph6 Advance' and the charts group a
phase's detectors. Without it they say 'det 6' and chart a signal's detectors together.

atspm-detector is optional: callers that already hold the findings table can pass it to the report
without installing anything.
"""

import re

import pandas as pd


# Key of this check in the alerts, ongoing_alerts and past_alerts dicts.
ALERT_TYPE = 'atspm_detector_health'

VEHICLE, PED = 'Vehicle', 'Ped'

FINDING_COLUMNS = ['DeviceId', 'DetectorType', 'Detector', 'Start', 'End', 'Label', 'Confidence', 'Detail']

# Statuses of the report's rows. A fault alert says which total went over: bad, or suspect when the weaker
# findings added up. A lane closure, a timing plan change or a ped set-up issue is information: the detector
# works, the traffic it sees or the controller's set-up is what changed.
from .detector_triage import OTHER_PHASES, SERVICE_LOST

FAULT_STATUSES = ('bad', 'suspect')
INFO_STATUSES = ('lane_closure', 'timing_plan', 'config_issue')
# Labels that are information, and the alert status each one gets.
INFO_CONDITIONS = {'lane closure': 'lane_closure', 'timing plan': 'timing_plan', 'never calls a walk': 'config_issue'}
# Findings with no time of their own: their rows (one per day of the window) only say when they were judged.
WHOLE_WINDOW_LABELS = ('busy at night', 'no daily pattern', 'misses vehicles at night', 'pulse mode')
# Lane closures shorter than this are not listed.
MIN_CLOSURE_MINUTES = 60
# A fault within this long of a lane closure or plan switch of its own detector does not count: it is the
# lane reopening, not the detector.
CLOSURE_MARGIN = pd.Timedelta(minutes=30)

# Confidence limits. Weaker findings do not count.
MIN_CONFIDENCE = 0.5
BAD_CONFIDENCE = 0.8

# Running total of the daily bad minutes: each day adds its minutes less the allowance (never below 0, never
# above the cap), and the detector is reported while the total is over the threshold. The cap lets a repaired
# detector drop below the threshold after two clean days.
BAD_ALLOWANCE_MINUTES = 30.0
BAD_THRESHOLD_MINUTES = 120.0
BAD_CAP_MINUTES = 180.0
# The same for the suspect minutes. A day counts for at most SUSPECT_DAY_LIMIT_MINUTES, so one suspect day never
# alerts. A vehicle day adds at most 75, so a single run's 3-day window (at most 225) cannot alert by itself: a heavy
# suspect detector alerts on its fourth day, an hour a day on its sixth, half an hour a day on its 17th. A ped day adds
# at most 105, so a sudden stop alerts on its third day: the next window often loses it, the silence eating its own
# baseline. Two clean days clear it, as for bad.
SUSPECT_ALLOWANCE_MINUTES = 15.0
SUSPECT_THRESHOLD_MINUTES = 240.0
SUSPECT_CAP_MINUTES = 270.0
SUSPECT_DAY_LIMIT_MINUTES = {VEHICLE: 90.0, PED: 120.0}

# Days of daily minutes kept for the running totals.
MINUTES_KEEP_DAYS = 90
MINUTES_COLUMNS = ['DeviceId', 'DetectorType', 'Detector', 'Date', 'BadMinutes', 'SuspectMinutes']
STATUS_LABELS = {
    'bad': 'Bad',
    'suspect': 'Suspect',
    'lane_closure': 'Lane closure',
    'timing_plan': 'Timing plan',
    'config_issue': 'Set-up issue',
}
STATUS_ORDER = {status: order for order, status in enumerate(FAULT_STATUSES + INFO_STATUSES)}

# A fault is one issue whether it is bad or suspect today: the status of a broken detector wobbles between the
# two from one window to the next, and keying on it would report the same detector again each time it did.
FAULT_ISSUE = 'fault'

ALERT_COLUMNS = ['DeviceId', 'DetectorType', 'Detector', 'Date', 'Issue', 'Status', 'Score', 'Label', 'Reason',
                 'Periods', 'Evidence']


def run_atspm_detector_findings(con, events_table: str, detectors_table: str, start=None, end=None, **kwargs):
    """Run atspm-detector on a DuckDB connection -> the findings table, or with summary=True (findings, status).

    events_table holds DeviceId, Timestamp, EventId, Parameter (the controller log, local time), best stored sorted
    by DeviceId then Timestamp; detectors_table holds DeviceId, Detector, phase, function, lanes and optionally the
    class probabilities. start/end bound the window (end exclusive). Extra keyword arguments go to
    atspm_detector.health() unchanged.
    """
    try:
        from atspm_detector import health
    except ImportError as error:
        raise ImportError(
            "The atspm-detector health check needs the atspm-detector package: "
            "pip install 'atspm-report[detector]'"
        ) from error
    return health(con, events_table, detectors_table, start, end, **kwargs)


def findings_frame(findings) -> pd.DataFrame:
    """The findings table with DeviceId as text, Detector as int and times as timestamps."""
    if findings is None or len(findings) == 0:
        return pd.DataFrame(columns=FINDING_COLUMNS)
    frame = findings[FINDING_COLUMNS].copy()
    frame['DeviceId'] = frame['DeviceId'].astype(str)
    frame['Detector'] = pd.to_numeric(frame['Detector'], errors='coerce')
    frame = frame.dropna(subset=['Detector'])
    frame['Detector'] = frame['Detector'].astype(int)
    frame['Start'] = pd.to_datetime(frame['Start'], errors='coerce')
    frame['End'] = pd.to_datetime(frame['End'], errors='coerce')
    frame['Confidence'] = pd.to_numeric(frame['Confidence'], errors='coerce')
    frame['Detail'] = frame['Detail'].fillna('').astype(str)
    # Weaker rows are notes the report does not list: borderline findings, config, signal and ped notes.
    return frame[frame['Label'].isin(INFO_CONDITIONS) | (frame['Confidence'] >= MIN_CONFIDENCE)].reset_index(drop=True)


def _in_closure(frame: pd.DataFrame) -> pd.Series:
    """True for each fault finding inside, or within CLOSURE_MARGIN of, a lane closure or plan switch of its detector."""
    keys = ['DeviceId', 'DetectorType', 'Detector']
    closures = frame[frame['Label'].isin(('lane closure', 'timing plan'))]
    if closures.empty:
        return pd.Series(False, index=frame.index)
    pairs = frame[keys + ['Start', 'End']].reset_index().merge(
        closures[keys + ['Start', 'End']], on=keys, suffixes=('', '_closure'))
    near = pairs[(pairs['Start'] < pairs['End_closure'] + CLOSURE_MARGIN)
                 & (pairs['End'] > pairs['Start_closure'] - CLOSURE_MARGIN)]
    return pd.Series(frame.index.isin(near['index']), index=frame.index)


def _whole_window(frame: pd.DataFrame) -> pd.Series:
    """True for each finding with no time of its own."""
    return frame['Label'].isin(WHOLE_WINDOW_LABELS) | frame['Start'].isna() | frame['End'].isna()


def _adds_minutes(frame: pd.DataFrame) -> pd.Series:
    """True for each fault finding that adds to a running total: one with a time of its own, or a whole-window one
    under the bad level, whose daily rows add to the suspect total. Pulse mode is the detector's set-up, never a total."""
    suspect_whole = (frame['Label'].isin(WHOLE_WINDOW_LABELS) & (frame['Label'] != 'pulse mode')
                     & (frame['Confidence'] < BAD_CONFIDENCE) & frame['Start'].notna() & frame['End'].notna())
    return ~_whole_window(frame) | suspect_whole


def _union_minutes(spans) -> float:
    """Minutes covered by (start, end) spans, overlaps counted once."""
    total, reach = 0.0, None
    for start, end in sorted(spans):
        if reach is not None and start < reach:
            start = reach
        if end > start:
            total += (end - start).total_seconds() / 60
        reach = end if reach is None else max(reach, end)
    return total


def daily_minutes(findings: pd.DataFrame, days, known: pd.DataFrame = None, reported: pd.DataFrame = None) -> pd.DataFrame:
    """Bad and suspect minutes of each detector on each of `days` (MINUTES_COLUMNS).

    Counted: fault findings of at least MIN_CONFIDENCE, outside lane closures and timing plan changes, that have a
    time of their own or are whole-window findings under the bad level (at the bad level they are reported at once).
    `known` adds the detectors of the
    stored history: with no finding they get a day of 0, so a clean day lowers their totals. `reported` (DeviceId,
    Date), when given, names the days each signal reported data: a day it did not gets no row at all, unless it has
    a finding, so an outage leaves the totals as they were.
    """
    days = [pd.Timestamp(day).normalize() for day in days]
    p = findings_frame(findings)
    keys = ['DeviceId', 'DetectorType', 'Detector']
    grid = [p[keys]] + ([known[keys]] if known is not None and not known.empty else [])
    grid = pd.concat(grid, ignore_index=True).drop_duplicates()
    if grid.empty or not days:
        return pd.DataFrame(columns=MINUTES_COLUMNS)

    faults = p[~p['Label'].isin(INFO_CONDITIONS) & ~_in_closure(p)]
    faults = faults[_adds_minutes(faults)]

    minutes = {}
    for day in days:
        day_end = day + pd.Timedelta(days=1)
        on_day = faults[(faults['Start'] < day_end) & (faults['End'] > day)]
        for (device, kind, detector), group in on_day.groupby(keys):
            for column, rows in (('BadMinutes', group[group['Confidence'] >= BAD_CONFIDENCE]),
                                 ('SuspectMinutes', group[group['Confidence'] < BAD_CONFIDENCE])):
                if not rows.empty:
                    spans = [(max(a, day), min(z, day_end)) for a, z in zip(rows['Start'], rows['End'])]
                    minutes[(device, kind, detector, day, column)] = _union_minutes(spans)

    frame = grid.merge(pd.DataFrame({'Date': days}), how='cross')
    for column in ('BadMinutes', 'SuspectMinutes'):
        frame[column] = [minutes.get((d, k, n, day, column), 0.0) for d, k, n, day in
                         zip(frame['DeviceId'], frame['DetectorType'], frame['Detector'], frame['Date'])]
    if reported is not None:
        seen = reported[['DeviceId', 'Date']].astype({'DeviceId': str}).assign(
            Date=lambda t: pd.to_datetime(t['Date']).dt.normalize(), _seen=True).drop_duplicates()
        frame = frame.merge(seen, on=['DeviceId', 'Date'], how='left')
        frame = frame[frame['_seen'].fillna(False).astype(bool) | (frame['BadMinutes'] > 0)
                      | (frame['SuspectMinutes'] > 0)].drop(columns='_seen')
    return frame[MINUTES_COLUMNS].reset_index(drop=True)


def _minutes_frame(history: pd.DataFrame) -> pd.DataFrame:
    """Stored minutes in today's columns."""
    old = history.copy()
    if 'DetectorType' not in old:
        old['DetectorType'] = VEHICLE
    if 'SuspectMinutes' not in old:
        old['SuspectMinutes'] = 0.0
    old = old[MINUTES_COLUMNS]
    old['DeviceId'] = old['DeviceId'].astype(str)
    old['Date'] = pd.to_datetime(old['Date']).dt.normalize()
    return old


def update_minutes_history(history: pd.DataFrame, minutes: pd.DataFrame,
                           keep_days: int = MINUTES_KEEP_DAYS) -> pd.DataFrame:
    """Stored daily minutes with this run's days in place of the stored ones (the latest run judged them with the
    most context), trimmed to the last keep_days days. A detector with nothing but zeros left is dropped: with no
    row its totals are 0 anyway."""
    frames = []
    if history is not None and not history.empty:
        old = _minutes_frame(history)
        if minutes is not None and not minutes.empty:
            old = old[~old['Date'].isin(pd.to_datetime(minutes['Date']).dt.normalize().unique())]
        frames.append(old)
    if minutes is not None and not minutes.empty:
        frames.append(minutes[MINUTES_COLUMNS])
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=MINUTES_COLUMNS)
    merged = pd.concat(frames, ignore_index=True)
    merged['Date'] = pd.to_datetime(merged['Date']).dt.normalize()
    cutoff = merged['Date'].max() - pd.Timedelta(days=keep_days - 1)
    merged = merged[merged['Date'] >= cutoff]
    keys = ['DeviceId', 'DetectorType', 'Detector']
    busy = (merged['BadMinutes'].astype(float) + merged['SuspectMinutes'].astype(float)).groupby(
        [merged[key] for key in keys]).transform('sum') > 0
    return merged[busy].sort_values(keys + ['Date']).reset_index(drop=True)


def running_totals(history: pd.DataFrame, report_date) -> pd.DataFrame:
    """Each detector's running totals on the report day (DeviceId, DetectorType, Detector, BadTotal, SuspectTotal,
    Status); Status is bad or suspect when that total is over its threshold, bad first, else missing.

    A day without a row (no data) leaves the totals as they were: an outage is no sign of a repair.
    """
    columns = ['DeviceId', 'DetectorType', 'Detector', 'BadTotal', 'SuspectTotal', 'Status']
    if history is None or history.empty:
        return pd.DataFrame(columns=columns)
    h = _minutes_frame(history)
    h = h[h['Date'] <= pd.Timestamp(report_date).normalize()]
    rows = []
    for (device, kind, detector), group in h.sort_values('Date').groupby(['DeviceId', 'DetectorType', 'Detector']):
        bad = suspect = 0.0
        for bad_minutes, suspect_minutes in zip(group['BadMinutes'], group['SuspectMinutes']):
            bad = min(BAD_CAP_MINUTES, max(0.0, bad + float(bad_minutes) - BAD_ALLOWANCE_MINUTES))
            suspect = min(SUSPECT_CAP_MINUTES, max(0.0, suspect + min(float(suspect_minutes), SUSPECT_DAY_LIMIT_MINUTES[kind])
                                                   - SUSPECT_ALLOWANCE_MINUTES))
        status = 'bad' if bad > BAD_THRESHOLD_MINUTES else 'suspect' if suspect > SUSPECT_THRESHOLD_MINUTES else None
        rows.append((device, kind, detector, bad, suspect, status))
    return pd.DataFrame(rows, columns=columns)


# Heading of a vehicle finding in the report's text.
LABEL_HEADING = {
    'stuck on': 'Stuck on', 'stuck on (together)': 'Stuck on', 'count zone held ON': 'Stuck on',
    'silent': 'Silent', 'counts dropped': 'Counts fall', 'counts too high': 'Counts too high',
    'silent at night': 'Misses vehicles at night', 'misses vehicles at night': 'Misses vehicles at night',
    'busy at night': 'Busy at night', 'no daily pattern': 'No daily pattern', 'spiky counts': 'Spiky counts',
    'too-fast actuations': 'Too-fast actuations', 'too many for the traffic': 'Too many for the traffic',
    'lane closure': 'Lane closure', 'timing plan': 'Timing plan', 'pulse mode': 'Configuration',
}
# Findings whose check depends on how many lanes the zone covers: only they name the lanes.
LANE_LABELS = {'stuck on', 'stuck on (together)', 'count zone held ON', 'too-fast actuations', 'too many for the traffic',
               'busy at night', 'no daily pattern', 'spiky counts'}
# "counts and occupancy both fell from Mon 04:00", "counts dropped from Mon 04:00", "nearly stopped counting from Mon 04:00".
_DROP_START = re.compile(r'((?:fell|dropped|stopped counting) from )[A-Z][a-z]{2} \d{2}:\d{2}')


def first_judged_hour(rows: pd.DataFrame):
    """The first hour judged low of a detector's count-drop findings, or None without any. Rows of whole days from the
    start of the window name no hour (the check gave no period): None too, so the text keeps the time it has."""
    drops = rows.loc[rows['Label'] == 'counts dropped', ['Start', 'End']].dropna().sort_values('Start')
    if drops.empty:
        return None
    first = drops['Start'].iloc[0]
    later = drops.iloc[1:]
    whole_days = ((drops['End'] == drops['End'].dt.normalize()).all()
                  and (later['End'] - later['Start'] == pd.Timedelta(days=1)).all()
                  and (first == first.normalize() or first != first.floor('h')))
    return None if whole_days else first.floor('h')


def _function_known(detector: int, info: pd.DataFrame = None) -> bool:
    """False when the detector table was given and names no function for this detector."""
    return info is None or (detector in info.index and pd.notna(info.at[detector, 'Function']))


def _lead_in(detector_type: str, detector: int, labels, info: pd.DataFrame = None) -> str:
    """'det 12: P4 Presence (covers 1 lane) - ' for a detector with a known function, else 'det 12: '. A ped
    finding's text says what it needs. The lanes are named only when one of its `labels` depends on them."""
    if detector_type != VEHICLE:
        return ''
    lead = f"det {detector}: "
    if info is None or not _function_known(detector, info):
        return lead
    phase, function, lanes = info.at[detector, 'Phase'], info.at[detector, 'Function'], info.at[detector, 'Lanes']
    covers = ('' if pd.isna(lanes) or not set(labels) & LANE_LABELS
              else f" (covers {int(lanes)} lane{'s' if lanes != 1 else ''})")
    return lead + ('' if pd.isna(phase) else f"P{int(phase)} ") + f"{function.replace('Yellow_Red', 'Yellow-red')}{covers} - "


def findings_reason(rows: pd.DataFrame, info: pd.DataFrame = None) -> str:
    """One sentence from one detector's findings, strongest heading first, each detail said once under its heading.
    `info`: detector_info() rows of its signal, indexed by Detector.

    A count drop's start time reads the same as its first listed period: the first hour judged low.
    """
    ordered = rows.sort_values('Confidence', ascending=False, na_position='last', kind='stable')
    kind, detector = ordered['DetectorType'].iloc[0], int(ordered['Detector'].iloc[0])
    headed = kind == VEHICLE and _function_known(detector, info)
    parts, said = {}, set()
    for label, detail in zip(ordered['Label'], ordered['Detail']):
        if detail in said:
            continue
        said.add(detail)
        heading = LABEL_HEADING.get(label) if headed else None
        parts.setdefault(heading or detail or label, []).append(detail if heading else None)
    text = _lead_in(kind, detector, ordered['Label'], info) + '; '.join(
        f"{key}: {'; '.join(filter(None, details))}" if any(details) else key for key, details in parts.items())
    first = first_judged_hour(rows)
    if first is not None:
        text = _DROP_START.sub(lambda m: f"{m.group(1)}{first:%a %H:%M}", text)
    return phase_label(text)


def _period_dicts(rows: pd.DataFrame) -> list:
    """Periods as the report's tables and charts take them: start, end (text) and what, in time order. The daily
    rows of a whole-window finding read as one period."""
    rows = rows.dropna(subset=['Start', 'End']).sort_values('Start')
    spans = []
    for a, z, what in zip(rows['Start'], rows['End'], rows['Label']):
        same = next((span for span in spans if what in WHOLE_WINDOW_LABELS and span[2] == what and span[1] == a), None)
        if same:
            same[1] = z
        else:
            spans.append([a, z, what])
    return [{'start': f"{a:%Y-%m-%d %H:%M:%S}", 'end': f"{z:%Y-%m-%d %H:%M:%S}", 'what': what} for a, z, what in spans]


def _evidence_dicts(rows: pd.DataFrame) -> list:
    """Each label of a detector's findings, strongest first, with its strongest row's detail (what, detail,
    confidence), for the tables' narrative."""
    ordered = rows.sort_values('Confidence', ascending=False, na_position='last', kind='stable')
    first = ordered.drop_duplicates('Label')
    return [{'what': label, 'detail': detail, 'confidence': None if pd.isna(confidence) else float(confidence)}
            for label, detail, confidence in zip(first['Label'], first['Detail'], first['Confidence'])]


def build_atspm_detector_health_alerts(findings: pd.DataFrame, report_date=None, minutes_history: pd.DataFrame = None,
                                       days: int = 3, reported: pd.DataFrame = None,
                                       info: pd.DataFrame = None) -> tuple:
    """Alerts for the report day, and the daily minutes history updated with this window -> (alerts, history).

    Fault alerts (Issue 'fault'): a detector or ped button with a fault finding in this window whose bad or suspect
    running total is over its threshold on the report day, or that has a whole-window finding of Confidence 0.8 or
    more. Information alerts: each detector with a lane closure of MIN_CLOSURE_MINUTES or more or a timing plan
    change, each ped button with a set-up issue. The `days` days ending on the report day feed the minutes. Date is
    the report day (without one, the latest finding's day, else yesterday). `reported`: see daily_minutes. `info`:
    the detector table (see detector_info), for the phase and function in the alert text.
    """
    p = findings_frame(findings)
    if report_date is None:
        report_date = p['End'].max() if p['End'].notna().any() else pd.Timestamp.today() - pd.Timedelta(days=1)
    report_date = pd.Timestamp(report_date).normalize()

    window_days = [report_date - pd.Timedelta(days=k) for k in range(days - 1, -1, -1)]
    stored = _minutes_frame(minutes_history) if minutes_history is not None and not minutes_history.empty else None
    history = update_minutes_history(minutes_history, daily_minutes(p, window_days, known=stored, reported=reported))
    if p.empty:
        return pd.DataFrame(columns=ALERT_COLUMNS), history

    totals = running_totals(history, report_date)
    over = totals[totals['Status'].notna()]
    alerting = dict(zip(zip(over['DeviceId'], over['DetectorType'], over['Detector']), over['Status']))

    notices = p[p['Label'].isin(INFO_CONDITIONS)]
    faults = p[~p['Label'].isin(INFO_CONDITIONS)]
    notices = notices[(notices['Label'] != 'lane closure') | ((notices['End'] - notices['Start'])
                                                              >= pd.Timedelta(minutes=MIN_CLOSURE_MINUTES))]
    whole_bad = faults[_whole_window(faults) & (faults['Confidence'] >= BAD_CONFIDENCE)]
    at_once = set(zip(whole_bad['DeviceId'], whole_bad['DetectorType'], whole_bad['Detector']))

    rows = []
    keys = ['DeviceId', 'DetectorType', 'Detector']
    for key, group in faults.groupby(keys):
        if key in at_once or key in alerting:
            status = 'bad' if key in at_once else alerting[key]
            rows.append((key, FAULT_ISSUE, status, group))
    for (device, kind, detector, label), group in notices.groupby(keys + ['Label']):
        status = INFO_CONDITIONS[label]
        rows.append(((device, kind, detector), status, status, group))
    if not rows:
        return pd.DataFrame(columns=ALERT_COLUMNS), history

    for_signal = signal_info(info)

    def score(group):
        strongest = group['Confidence'].max()
        return 1.0 if pd.isna(strongest) else 1.0 - float(strongest)

    alerts = pd.DataFrame({
        'DeviceId': [key[0] for key, _, _, _ in rows],
        'DetectorType': [key[1] for key, _, _, _ in rows],
        'Detector': [key[2] for key, _, _, _ in rows],
        'Date': report_date,
        'Issue': [issue for _, issue, _, _ in rows],
        'Status': [status for _, _, status, _ in rows],
        # Lowest health first in the tables, as 1 = healthy.
        'Score': [score(group) for _, _, _, group in rows],
        'Label': [group.sort_values('Confidence', ascending=False, kind='stable')['Label'].iloc[0]
                  for _, _, _, group in rows],
        'Reason': [findings_reason(group, for_signal(key[0])) for key, _, _, group in rows],
        'Periods': [_period_dicts(group) for _, _, _, group in rows],
        'Evidence': [_evidence_dicts(group) for _, _, _, group in rows],
    })
    return alerts[ALERT_COLUMNS].reset_index(drop=True), history


def detector_info(detectors: pd.DataFrame = None) -> pd.DataFrame:
    """The detector table health read (DeviceId, Detector, phase, function, lanes) as (DeviceId, Detector, Phase,
    Function, Lanes), one row per detector. Empty without one."""
    if detectors is None or len(detectors) == 0:
        return pd.DataFrame({'DeviceId': pd.Series(dtype=object), 'Detector': pd.Series(dtype='int64'),
                             'Phase': pd.Series(dtype=float), 'Function': pd.Series(dtype=object),
                             'Lanes': pd.Series(dtype=float)})
    return pd.DataFrame({
        'DeviceId': detectors['DeviceId'].astype(str).values,
        'Detector': pd.to_numeric(detectors['Detector']).astype(int).values,
        'Phase': pd.to_numeric(detectors['phase'], errors='coerce').values,
        'Function': detectors['function'].values,
        'Lanes': pd.to_numeric(detectors['lanes'], errors='coerce').values if 'lanes' in detectors else float('nan'),
    }).drop_duplicates(['DeviceId', 'Detector']).reset_index(drop=True)


def signal_info(detectors: pd.DataFrame = None):
    """A function: signal -> its detector_info() rows indexed by Detector, for findings_reason. A signal the table does
    not hold has no rows (no known functions); without a table at all it gives None."""
    table = detector_info(detectors)
    if table.empty:
        return lambda device: None
    none = table.iloc[:0].set_index('Detector')
    groups = {device: rows.set_index('Detector') for device, rows in table.groupby('DeviceId')}
    return lambda device: groups.get(device, none)


# The check writes a phase as "P2"; the report writes "Ph2".
_PHASE = re.compile(r'\bP(\d{1,2})\b')


def phase_label(text: str) -> str:
    """'det 3: P2 Advance ... moved to det 4 (P2)' -> 'det 3: Ph2 Advance ... moved to det 4 (Ph2)'."""
    return _PHASE.sub(r'Ph\1', text or '')


def report_window(report_date, past_alerts: pd.DataFrame = None, days: int = 3) -> tuple:
    """(start, end) of the days this report covers: the report day, plus any earlier day of the health window
    the last report did not cover (a skipped run), never a day an earlier report already showed. Without a
    history (the first run) it is the whole window of `days` days. The last covered day is the latest Date
    in this check's alert history before the report day."""
    end = pd.Timestamp(report_date).normalize() + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=days)
    if past_alerts is not None and not past_alerts.empty and 'Date' in past_alerts.columns:
        dates = pd.to_datetime(past_alerts['Date'], errors='coerce').dt.tz_localize(None).dt.normalize()
        dates = dates[dates < end - pd.Timedelta(days=1)]
        if not dates.dropna().empty:
            start = max(start, dates.max() + pd.Timedelta(days=1))
    return start, end


def limit_to_window(alerts: pd.DataFrame, start, end) -> pd.DataFrame:
    """Keep each alert's periods that overlap [start, end); drop an alert whose periods all fall before the
    window (an earlier report showed them). An alert without periods is kept."""
    if alerts is None or alerts.empty:
        return alerts
    start, end = pd.Timestamp(start), pd.Timestamp(end)

    def inside(periods):
        kept = []
        for period in periods or []:
            a = pd.to_datetime(period.get('start'), errors='coerce')
            z = pd.to_datetime(period.get('end'), errors='coerce')
            if pd.isna(a) or pd.isna(z) or (z > start and a < end):
                kept.append(period)
        return kept

    limited = alerts.copy()
    had = limited['Periods'].map(lambda p: len(p or []) > 0)
    limited['Periods'] = limited['Periods'].map(inside)
    keep = ~had | limited['Periods'].map(len).gt(0)
    return limited[keep].reset_index(drop=True)


def health_table_order(rows: pd.DataFrame) -> pd.DataFrame:
    """Rows (with Name) in the health tables' order: by TriageRank when they have one, then status, score (lowest health
    first), signal and detector."""
    ordered = rows.assign(
        _triage=pd.to_numeric(rows['TriageRank'], errors='coerce') if 'TriageRank' in rows else float('nan'),
        _status=rows['Status'].map(STATUS_ORDER).fillna(len(STATUS_ORDER)),
        _score=pd.to_numeric(rows['Score'], errors='coerce').fillna(1.0) if 'Score' in rows else 1.0,
    )
    return ordered.sort_values(['_triage', '_status', '_score', 'Name', 'Detector'], na_position='last', kind='stable').drop(
        columns=['_triage', '_status', '_score'])


HARM_TIERS = (SERVICE_LOST, OTHER_PHASES)


def affects_operations(rows: pd.DataFrame) -> pd.Series:
    """Whether each row's fault was measured to cost service or to delay other phases (its urgency tier, see detector_triage).
    These faults get the first health table and the charts; the others are data only. All False without tiers."""
    if 'Tier' not in rows:
        return pd.Series(False, index=rows.index)
    return rows['Tier'].isin(HARM_TIERS)


def signal_wide_rows(rows: pd.DataFrame) -> pd.Series:
    """The table's rows (already in its order) that remain once detectors that tell one thing read as one row, each with the
    numbers of the detectors it stands for besides its own. Those are a signal's detectors (or its buttons) the triage judged
    as one group, with the same tier, and its detectors judged signal-wide: a row each would only repeat the same effect.
    A detector that lost its own phase's service keeps its row."""
    joined = pd.Series([[] for _ in range(len(rows))], index=rows.index, dtype=object)
    if 'Tier' not in rows:
        return joined
    keys = pd.Series([None] * len(rows), index=rows.index, dtype=object)
    if 'SignalWide' in rows:
        wide = rows['SignalWide'].eq(True) & rows['Tier'].eq(OTHER_PHASES) & rows['DetectorType'].eq(VEHICLE)
        for index in rows.index[wide]:
            keys.at[index] = (rows.at[index, 'DeviceId'], 'signal-wide')
    if 'Group' in rows:
        grouped = pd.to_numeric(rows['Group'], errors='coerce').notna() & rows['Tier'].notna() & rows['Tier'].ne(SERVICE_LOST)
        for index in rows.index[grouped]:
            keys.at[index] = (rows.at[index, 'DeviceId'], rows.at[index, 'DetectorType'], rows.at[index, 'Tier'], float(rows.at[index, 'Group']))
    folded = {}
    for index, key in keys.dropna().items():
        folded.setdefault(key, []).append(index)
    for members in folded.values():
        joined.at[members[0]] = rows.loc[members[1:], 'Detector'].astype(int).tolist()
        joined = joined.drop(members[1:])
    return joined


def listed_alerts(alerts: pd.DataFrame, signals: pd.DataFrame, max_rows: int, hidden: dict = None, limits: dict = None) -> pd.DataFrame:
    """The alerts the report's tables actually list: per table (vehicle faults, lane closures, timing plans, ped
    faults, ped set-up issues; with the urgency tiers the faults are two tables, those affecting operations and those that are
    data only, vehicle and ped together) the first max_rows of each region and of All Regions,
    or its own limit from limits, in the tables' order (see health_table_order), whole signals kept together as the tables do.
    hidden, {report: signals}: the signals a report's Signal Focus lists. Their rows leave that report's tables, and every
    alert of theirs counts as listed, since the section names them all.
    Only these go into the alert history, so an alert cut from every table is not suppressed later."""
    if alerts is None or alerts.empty or signals is None or signals.empty or max_rows <= 0:
        return alerts
    from .table_generation import head_by_signal   # imports this module: import on use
    regions = signals[['DeviceId', 'Name', 'Region']].copy()
    regions['DeviceId'] = regions['DeviceId'].astype(str)
    ranked = alerts.assign(DeviceId=alerts['DeviceId'].astype(str)).merge(
        regions.drop_duplicates('DeviceId'), on='DeviceId', how='inner')
    ranked = health_table_order(ranked)
    hidden = hidden or {}
    limits = limits or {}
    together = 'TriageRank' in ranked.columns
    everything = pd.Series(True, index=ranked.index)
    harm = affects_operations(ranked)
    fault_tables = [((VEHICLE, PED), FAULT_STATUSES, 'detector_faults', harm), ((VEHICLE, PED), FAULT_STATUSES, 'detector_faults', ~harm)] if together else [
        ((VEHICLE,), FAULT_STATUSES, 'detector_faults', everything), ((PED,), FAULT_STATUSES, 'ped_faults', everything)]
    tables = fault_tables + [((VEHICLE,), ('lane_closure',), 'lane_closure', everything), ((VEHICLE,), ('timing_plan',), 'timing_plan', everything),
                             ((PED,), ('config_issue',), 'ped_setup', everything)]
    keep = set(ranked.index[ranked['DeviceId'].isin(set().union(*hidden.values()))]) if hidden else set()

    def listed(rows, report, limit):
        shown = rows[~rows['DeviceId'].isin(hidden.get(report, ()))]
        kept = head_by_signal(shown.loc[signal_wide_rows(shown).index], limit, key='Name')
        return shown.index[shown['DeviceId'].isin(kept['DeviceId'])]

    for kinds, statuses, name, part in tables:
        limit = limits.get(name, max_rows)
        table = ranked[ranked['DetectorType'].isin(kinds) & ranked['Status'].isin(statuses) & part]
        keep.update(listed(table, 'All Regions', limit))
        for region, region_rows in table.groupby('Region'):
            keep.update(listed(region_rows, region, limit))
    id_cols = ['DeviceId', 'DetectorType', 'Detector', 'Issue']
    listed = ranked.loc[sorted(keep), id_cols].drop_duplicates()
    return alerts.assign(DeviceId=alerts['DeviceId'].astype(str)).merge(listed, on=id_cols, how='inner')
