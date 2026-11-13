"""One short narrative per detector or ped button, for the report's health tables.

It leads with what is wrong and how bad, then says when. Exact times stay when there are few; repeated periods are
grouped: 'Held ON 19 times, 24 h in total, mostly nights, Oct 30 to Nov 1'. The strongest finding gets its evidence,
the next ones as much as still fits STORY_BUDGET, and whatever does not fit is only named.

The times come from the alert's Periods, which are already limited to the days the report covers, so the text and the
charts agree. The evidence comes from each finding's Detail: known wordings are cut down to the numbers that matter,
and a wording it does not know is kept as it is, without its times and clipped.
"""

import re
from xml.sax.saxutils import escape

import pandas as pd

from .atspm_detector_health import WHOLE_WINDOW_LABELS, phase_label

# Characters a row aims for, without the detector's name. The first finding always shows in full.
STORY_BUDGET = 210
EVIDENCE_LIMIT = 120

# Heading of each finding: findings that say the same thing share one.
HEADINGS = {
    'stuck on': 'Held ON', 'stuck on (together)': 'Held ON', 'count zone held ON': 'Held ON', 'long hold': 'Held ON',
    'silent': 'Silent', 'counts dropped': 'Counts fall',
    'silent at night': 'Misses vehicles at night', 'misses vehicles at night': 'Misses vehicles at night',
    'counts too high': 'Counts too high', 'busy at night': 'Busy at night', 'no daily pattern': 'No daily pattern',
    'spiky counts': 'Spiky counts', 'too-fast actuations': 'Too-fast actuations',
    'too many for the traffic': 'Too many for the traffic', 'pulse mode': 'Looks set to pulse mode',
    'lane closure': 'Lane closed', 'timing plan': 'Quiet under a timing plan',
    'never calls a walk': 'No walk follows presses', 'sudden stop': 'Presses stopped',
    'walks without presses': 'Walks without presses', 'sustained pressing': 'Sustained pressing',
}
# Findings whose periods only say when they were judged, not when anything happened.
NO_TIMES = set(WHOLE_WINDOW_LABELS) | {'never calls a walk'}
HELD_LABELS = ('stuck on', 'stuck on (together)', 'count zone held ON', 'long hold')
SILENT_LABELS = ('silent',)

_LEAD = re.compile(r'^det \d+: (.+?)(?: \(covers (\d+) lanes?\))? - ')
_NUMBER = r'[\d,.]+(?:e\+?\d+)?'
# 'Sat 14:55-Sat 17:55: ' at the start of a detail, or 'Sat 10-31 16:00', or 'Sat 04:40-Sun 02:00' inside one.
_TIMES = re.compile(r'\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?: \d{1,2}-\d{1,2})? \d{2}:\d{2}'
                    r'(?:\s*(?:-|to)\s*(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?: \d{1,2}-\d{1,2})? )?\d{2}:\d{2})?')


def _number(text) -> float:
    return float(str(text).replace(',', ''))


def _times_over(text) -> str:
    """'3.4 x', and 'over 100 x' for the huge ratios a detector gets when its phase mates counted next to nothing."""
    value = _number(text)
    return 'over 100 x' if value > 100 else f"{value:g} x"


def _stuck_evidence(detail):
    bits = []
    if 'still ON' in detail:
        bits.append('still ON at the end of the data')
    walks = re.search(r'through ([\d,]+) walks', detail)
    if walks and _number(walks.group(1)) > 0:
        bits.append(f"through {int(_number(walks.group(1))):,} walks")
    if 'walking every cycle' in detail:
        bits.append('the phase walking every cycle')
    return ', '.join(bits) or None


def _too_fast_evidence(detail):
    match = re.search(r'within ([\d.]+) s \((\d+) per 1,000; limit (\d+)\)', detail)
    if not match:
        return None
    seconds, rate, limit = match.groups()
    return f"{int(rate) / 10:.0f}% of ONs come within {seconds} s of the last (limit {int(limit) / 10:.0f}%)"


def _drop_evidence(detail):
    share = re.search(r'fell to (\d+) ?%', detail)
    rates = re.search(r'~?([\d,]+) an hour where ~?([\d,]+) expected', detail)
    both = re.search(r'([\d,]+) per hour where .*?~([\d,]+)', detail)
    found = rates or both
    if not found:
        return None
    text = f"~{int(_number(found.group(1))):,} an hour where ~{int(_number(found.group(2))):,} expected"
    return f"to {share.group(1)}% of normal, {text}" if share else text


def _silent_evidence(detail):
    whole = re.search(r'whole phase dropped.*?: ([\d,]+) actuations where ~([\d,]+) expected(?:; (\d+ of \d+ detectors) on (Ph?\d+) fell together)?', detail)
    if whole:
        together = f"; {whole.group(3)} on {whole.group(4)} fell together" if whole.group(3) else ''
        return f"whole phase: {whole.group(1)} actuations where ~{whole.group(2)} expected{together}"
    expected = re.search(r'~([\d,]+) expected', detail)
    if not expected:
        return None
    worst = ' in the worst stretch' if '; also' in detail else ''
    return f"no actuations where ~{expected.group(1)} expected{worst}"


def _sudden_stop_evidence(detail):
    # Judged against the button's own weeks (atspm-detector's stop_history): say what makes it more than a quiet day.
    own = re.search(r'no presses for ([\d,]+) logged hours.*?longest quiet spell in the (\d+) days before was ([\d,]+) h', detail)
    if own:
        weeks = f"{int(own.group(2)) // 7} weeks" if int(own.group(2)) % 7 == 0 else f"{own.group(2)} days"
        others = '; the other buttons carried on' if 'other buttons kept' in detail else ''
        return (f"none for {own.group(1)} logged hours; its longest quiet spell in the {weeks} before was "
                f"{own.group(3)} hours{others}")
    match = re.search(r'no presses for ([\d,]+) logged hours.*?expects ~([\d,]+)', detail)
    if not match:
        return None
    return f"none for {match.group(1)} logged hours where ~{match.group(2)} expected; quiet, or a dead button"


def _walks_without_presses_evidence(detail):
    match = re.search(r'([\d,]+) walks in the next ([\d.]+) h where those hours usually bring ~([\d,]+)', detail)
    return f"{match.group(1)} walks in {match.group(2)} h where ~{match.group(3)} usual, with no press" if match else None


def _sustained_pressing_evidence(detail):
    match = re.search(r'([\d,]+) presses on .*?usual ~([\d,]+) a day, with (\d+) 15-minute periods', detail)
    if not match:
        return None
    return f"{match.group(1)} presses in a day where ~{match.group(2)} usual; {match.group(3)} heavy quarter hours"


def _pulse_evidence(detail):
    match = re.search(r'(\d+) ?% of its ([\d,]+) calls last ([\d.]+) s or less', detail)
    return f"{match.group(1)}% of its {match.group(2)} calls last {match.group(3)} s or less" if match else None


EVIDENCE = {
    'stuck on': _stuck_evidence,
    'stuck on (together)': _stuck_evidence,
    'long hold': _stuck_evidence,
    'count zone held ON': lambda d: (lambda m: f"up to {m.group(1)}% ON in light traffic" if m else None)(
        re.search(r'ON up to (\d+) ?%', d)),
    'counts dropped': _drop_evidence,
    'silent': _silent_evidence,
    'silent at night': _silent_evidence,
    'counts too high': lambda d: (
        'far above its phase mates, which gave almost nothing' if 'far above' in d else
        (lambda m: f"{_times_over(m.group(1))} what its phase mates gave" if m else None)(
            re.search(rf'rose to ({_NUMBER}) x', d))),
    'spiky counts': lambda d: (lambda m: f"{m.group(1)} bursts a day at {m.group(2)} x its usual level" if m else None)(
        re.search(r'(\d+) bursts a day.*?at (\d+) x', d)),
    'too-fast actuations': _too_fast_evidence,
    'too many for the traffic': lambda d: (lambda m: f"busiest 5 min {m.group(1)} veh/h/lane, limit {m.group(2)}" if m else None)(
        re.search(r'busiest 5 min ([\d,]+) veh/h/lane vs limit ([\d,]+)', d)),
    'busy at night': lambda d: re.sub(r' %', '%', d),
    'no daily pattern': lambda d: re.sub(r'^counts do not follow the day: | \(noise, not traffic\)$', '', d),
    'misses vehicles at night': lambda d: None if d == 'misses vehicles at night' else re.sub(r' \(GEH [\d.]+\)', '', d),
    'pulse mode': _pulse_evidence,
    'never calls a walk': lambda d: (
        lambda m: f"pressed {m.group(1)} times on {m.group(2)} with no ped call or walk; no ped movement seems assigned"
        if m else None)(re.search(r'pressed ([\d,]+) times on (\d+ days?)', d)),
    'sudden stop': _sudden_stop_evidence,
    'walks without presses': _walks_without_presses_evidence,
    'sustained pressing': _sustained_pressing_evidence,
    'timing plan': lambda d: (lambda m: f"coordination pattern {m.group(1)} in effect" if m else None)(
        re.search(r'coordination pattern (\w+)', d)),
}


def _clip(text: str, limit: int) -> str:
    """Whole clauses up to `limit` characters, or else whole words."""
    if len(text) <= limit:
        return text
    kept = ''
    for clause in text.split('; '):
        if len(kept) + len(clause) + 2 > limit:
            break
        kept = f"{kept}; {clause}" if kept else clause
    return kept or text[:limit].rsplit(' ', 1)[0].rstrip(',:;')


def evidence_text(label: str, detail: str) -> str:
    """The numbers that matter from a finding's Detail, without what the times already say."""
    detail = phase_label(str(detail or '')).strip()
    if not detail or detail == label:
        return ''
    try:
        text = EVIDENCE[label](detail) if label in EVIDENCE else None
    except (ValueError, AttributeError, IndexError):
        text = None
    if text is None:
        if label in EVIDENCE and label in ('stuck on', 'stuck on (together)', 'long hold', 'misses vehicles at night'):
            return ''   # its detail only repeats the times
        text = _TIMES.sub('', detail)
        # GEH is the check's working measure, and hours in brackets repeat the times.
        text = re.sub(r'\(\d+ h(?:, GEH [\d.]+)?\)|\(GEH [\d.]+\)|,? GEH [\d.]+', '', text).strip(' :;,')
        text = re.sub(r'\s+', ' ', text)
    return _clip(text, EVIDENCE_LIMIT)


def _duration(minutes: float) -> str:
    if minutes < 1:
        return 'under 1 min'
    minutes = round(minutes)
    hours, rest = divmod(minutes, 60)
    if not hours:
        return f"{rest} min"
    return f"{round(minutes / 60)} h" if hours >= 10 or not rest else f"{hours} h {rest} min"


def _span(start, end) -> str:
    # One date is enough when the end time cannot be misread: a long period over midnight names its last day.
    if end - start < pd.Timedelta(hours=12) or end.normalize() == start.normalize():
        return f"{start:%a} {start.month}/{start.day} {start:%H:%M}-{end:%H:%M}"
    return f"{start:%a} {start.month}/{start.day} {start:%H:%M} to {end:%a} {end.month}/{end.day} {end:%H:%M}"


def _time_of_day(spans) -> str:
    """'mostly nights' and the like when 60% or more of the time falls in one part of the day, else nothing."""
    parts = {'nights': [(0, 6), (22, 24)], 'mornings': [(6, 10)], 'midday': [(10, 15)], 'afternoons and evenings': [(15, 22)]}
    totals = dict.fromkeys(parts, 0.0)
    for start, end in spans:
        day = start.normalize()
        while day < end:
            for name, ranges in parts.items():
                for low, high in ranges:
                    overlap = min(end, day + pd.Timedelta(hours=high)) - max(start, day + pd.Timedelta(hours=low))
                    totals[name] += max(overlap.total_seconds(), 0.0)
            day += pd.Timedelta(days=1)
    whole = sum(totals.values())
    if not whole:
        return ''
    name = max(totals, key=totals.get)
    return f"mostly {name}" if totals[name] / whole >= 0.6 else ''


def _dates(spans) -> str:
    first, last = spans[0][0], max(end for _, end in spans)
    # A span ending at midnight belongs to the day before.
    last = last - pd.Timedelta(seconds=1)
    if first.normalize() == last.normalize():
        return f"{first:%b} {first.day}"
    return f"{first:%b} {first.day} to {last:%b} {last.day}"


def _open_span(start) -> str:
    return f"{start:%a} {start.month}/{start.day} {start:%H:%M} until the data stopped"


def _when(spans, exact: int, short: bool = False, open_end: bool = False) -> str:
    """The period text: exact times for up to `exact` periods, else grouped. open_end: the last one runs into the end of the
    network's data, so its end says nothing about the fault."""
    if not spans:
        return ''
    total = sum((end - start).total_seconds() for start, end in spans) / 60
    count = len(spans)
    if count == 1 and exact >= 1:
        return f"{_duration(total)}, {_open_span(spans[0][0]) if open_end else _span(*spans[0])}"
    if count <= exact:
        told = [_span(*span) for span in spans]
        if open_end:
            told[-1] = _open_span(spans[-1][0])
        return f"{_duration(total)} in total ({'; '.join(told)})"
    times = 'time' if count == 1 else 'times'
    text = [f"{count} {times}, {_duration(total)} in total" if count > 1 else _duration(total)]
    if not short:
        text.append(_time_of_day(spans))
    text.append(_dates(spans))
    longest = max((end - start).total_seconds() for start, end in spans) / 60
    if not short and count > 1 and longest >= total * 0.5:
        text.append(f"longest {_duration(longest)}")
    if open_end:
        text.append('the last until the data stopped')
    return ', '.join(filter(None, text))


def _kinds(periods, evidence) -> list:
    """The findings of a row by heading, strongest first: heading, spans, evidence, confidence."""
    kinds = {}

    def kind(label):
        heading = HEADINGS.get(label) or (str(label)[:1].upper() + str(label)[1:])
        return kinds.setdefault(heading, {'heading': heading, 'spans': [], 'evidence': '', 'confidence': 0.0, 'timed': False, 'open': False})

    for item in evidence or []:
        entry = kind(item.get('what'))
        confidence = item.get('confidence')
        if confidence is not None and not pd.isna(confidence):
            entry['confidence'] = max(entry['confidence'], float(confidence))
        entry['timed'] = entry['timed'] or item.get('what') not in NO_TIMES
        if not entry['evidence']:
            entry['evidence'] = evidence_text(item.get('what'), item.get('detail'))
    for period in periods if isinstance(periods, (list, tuple)) else []:
        label = period.get('what')
        entry = kind(label)
        start, end = pd.to_datetime(period.get('start'), errors='coerce'), pd.to_datetime(period.get('end'), errors='coerce')
        if label not in NO_TIMES and pd.notna(start) and pd.notna(end):
            entry['spans'].append((start, end))
            entry['open'] = entry['open'] or bool(period.get('open'))
    for entry in kinds.values():
        merged = []
        for start, end in sorted(entry['spans']):
            # Under a minute apart is one period: a hold the check split, not two.
            if merged and start <= merged[-1][1] + pd.Timedelta(minutes=1):
                merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
            else:
                merged.append((start, end))
        entry['spans'] = merged
        entry['minutes'] = sum((end - start).total_seconds() for start, end in merged) / 60
    return sorted(kinds.values(), key=lambda k: (-k['confidence'], -k['minutes']))


def _sentence(kind, exact: int, with_evidence: bool = True, short: bool = False, bold: bool = True) -> str:
    when = _when(kind['spans'], exact, short, open_end=kind['open'])
    heading = f"<b>{escape(kind['heading'])}</b>" if bold else escape(kind['heading'])
    text = f"{heading} {escape(when)}" if when else heading
    if with_evidence and kind['evidence']:
        evidence = kind['evidence']
        if len(kind['spans']) > 1:   # the walks are the worst hold's, not all of them
            evidence = re.sub(r'\bthrough ([\d,]+) walks', r'up to \1 walks in one hold', evidence)
        text += f": {escape(evidence)}"
    return text + '.'


def _visible_length(text: str) -> int:
    return len(re.sub(r'</?b>', '', text))


def detector_text(reason) -> str:
    """'Ph8 Advance, 1 lane' from the alert's reason, or nothing when it names no function."""
    match = _LEAD.match(str(reason or ''))
    if not match:
        return ''
    lanes = match.group(2)
    return match.group(1) + (f", {lanes} lane{'' if lanes == '1' else 's'}" if lanes else '')


def detector_label(number, reason, item: str = 'Det') -> str:
    """'Det 22, Ph8 Count, 1 lane': the number and what the reason says of it. The function Other tells nothing."""
    text = re.sub(r'(^| )Other(?=,|$)', '', detector_text(reason)).lstrip(', ')
    return f"{item} {number}, {text}" if text else f"{item} {number}"


_HEADING = re.compile(r'^(<b>.*?</b>)\s*(.*)$')


def _story_sentences(text: str) -> list:
    """[(heading or the whole sentence, its clauses)] of a story, without the sentences' full stops."""
    sentences = []
    for sentence in re.split(r'(?<=\.)\s+', str(text).strip()):
        sentence = sentence.rstrip('.')
        heading = _HEADING.match(sentence)
        if heading:
            sentences.append((heading.group(1), [clause for clause in re.split(r', |: |; ', heading.group(2)) if clause]))
        elif sentence:
            sentences.append((sentence, []))
    return sentences


def story_difference(previous: str, current: str) -> str:
    """What a story says that the one before it does not, for the next detector of a signal: a finding the one before had keeps only
    its own times and numbers, a new finding stays whole. Empty when it says nothing more."""
    before = dict(_story_sentences(previous))
    parts = []
    for heading, clauses in _story_sentences(current):
        if heading not in before:
            parts.append(f"{heading} {', '.join(clauses)}".strip())
            continue
        own = [clause for clause in clauses if clause not in before[heading]]
        if own:
            parts.append(', '.join(own))
    return ' '.join(f"{part}." for part in parts)


def health_story(reason, periods=None, evidence=None, bold: bool = True, name_detector: bool = True) -> str:
    """The row's narrative as reportlab markup, or plain text with bold=False.

    name_detector=False leaves out the detector's name, for a table that has it in a column of its own.
    """
    kinds = _kinds(periods, evidence)
    if not kinds:
        text = str(reason or '').strip()
        return escape(text.split(': ', 1)[1] if text.lower().startswith('det ') and ': ' in text else text)
    parts = [_sentence(kinds[0], exact=3 if len(kinds) == 1 else 2, bold=bold)]
    used = _visible_length(parts[0])
    named = []
    for kind in kinds[1:]:
        for text in (_sentence(kind, 1, bold=bold), _sentence(kind, 0, with_evidence=False, short=True, bold=bold)):
            if used + _visible_length(text) <= STORY_BUDGET:
                parts.append(text)
                used += _visible_length(text) + 1
                break
        else:
            named.append(kind['heading'][:1].lower() + kind['heading'][1:])
    if named:
        parts.append(f"Also {', '.join(escape(name) for name in named)}.")
    detector = detector_text(reason) if name_detector else ''
    if detector:
        parts.append(f"{escape(detector)}.")
    return ' '.join(parts)
