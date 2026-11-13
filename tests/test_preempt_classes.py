"""Emergency vehicle preempt, TSP and rail: call classes, per-class alerts, report sections and history."""
import re
from unittest.mock import patch

import pandas as pd
import pytest

from atspm_report import ReportGenerator
from atspm_report.preempt_cascade import (
    CLASS_BUS_PREEMPT,
    CLASS_EV,
    CLASS_RAIL,
    CLASS_TSP,
    CLASS_UNCLASSIFIED,
    CONSTANT_MIN_SECONDS,
    DIRECTION_BUS_INTERRUPTED,
    DIRECTION_BUS_PREEMPT,
    DIRECTION_CHATTER,
    EPISODE_GAP_SECONDS,
    PULSE_MAX_SECONDS,
    SECTION_EV,
    SECTION_RAIL,
    SECTION_TSP,
    TSP_CLASSES,
    build_bus_preempt_alerts,
    build_preempt_duration_alerts,
    classify_preempt_calls,
)
from atspm_report.preempt_processing import build_section_preempt_alerts, rebuild_history_classes

DAY = pd.Timestamp('2026-11-08')
RAIL = [5, 6]


def test_constants_are_the_documented_values():
    assert (PULSE_MAX_SECONDS, CONSTANT_MIN_SECONDS, EPISODE_GAP_SECONDS) == (1.5, 5.0, 30.0)


def _burst(device, preempt, start, on=0.6, off=2.0, pulses=4, entry=False, check_in=True):
    """A burst of pulses as calls, plus its raw events (102/104 per pulse, 105 entry, 112 check-in: a bus)."""
    calls, events = [], []
    t = pd.Timestamp(start)
    for _ in range(pulses):
        calls.append({'DeviceId': device, 'Preempt': preempt, 'StartTime': t, 'Duration': on, 'IsValid': True})
        events += [(device, t, 102, preempt), (device, t + pd.Timedelta(seconds=on), 104, preempt)]
        t += pd.Timedelta(seconds=on + off)
    if entry:
        events.append((device, pd.Timestamp(start) + pd.Timedelta(seconds=0.1), 105, preempt))
    if check_in:
        events.append((device, pd.Timestamp(start) - pd.Timedelta(seconds=1), 112, 1))
    return calls, events


def _call(device, preempt, start, duration, valid=True):
    return {'DeviceId': device, 'Preempt': preempt, 'StartTime': pd.Timestamp(start), 'Duration': duration, 'IsValid': valid}


def _frames(calls, events):
    return pd.DataFrame(calls), pd.DataFrame(events, columns=['DeviceId', 'TimeStamp', 'EventId', 'Parameter'])


def _classes(calls, events=None):
    out = classify_preempt_calls(calls, events, RAIL)
    return out.groupby(['DeviceId', 'Preempt'])['Class'].agg(lambda s: sorted(set(s))).to_dict()


# ---- classification ---------------------------------------------------------

def test_pulses_without_entry_are_tsp_and_with_entry_ran_as_preempt():
    tsp, tsp_events = _burst('s', 2, '2026-11-08 08:00')
    ran, ran_events = _burst('s', 1, '2026-11-08 08:00', entry=True)
    calls, events = _frames(tsp + ran, tsp_events + ran_events)
    assert _classes(calls, events) == {('s', 1): [CLASS_BUS_PREEMPT], ('s', 2): [CLASS_TSP]}


def test_pulses_beside_a_constant_call_are_one_emergency_vehicle_episode():
    pulses, events = _burst('s', 1, '2026-11-08 08:00', entry=True)
    # 20 s after the last pulse: the same episode; the constant call makes it a preempt.
    constant = _call('s', 1, pulses[-1]['StartTime'] + pd.Timedelta(seconds=20), 40.0)
    calls, events = _frames(pulses + [constant], events)
    assert _classes(calls, events) == {('s', 1): [CLASS_EV]}


def test_episodes_split_on_a_gap_longer_than_thirty_seconds():
    pulses, events = _burst('s', 1, '2026-11-08 08:00')
    constant = _call('s', 1, pulses[-1]['StartTime'] + pd.Timedelta(seconds=40), 40.0)
    out = classify_preempt_calls(*_frames(pulses + [constant], events), RAIL)
    assert out['Class'].tolist() == [CLASS_TSP] * 4 + [CLASS_EV]


def test_mid_length_and_unclosed_calls_are_unclassified():
    calls = pd.DataFrame([_call('s', 1, '2026-11-08 08:00', 3.0), _call('s', 1, '2026-11-08 09:00', 1.0, valid=False)])
    assert classify_preempt_calls(calls, None, RAIL)['Class'].tolist() == [CLASS_UNCLASSIFIED] * 2


def test_a_bus_burst_with_one_longer_pulse_is_still_a_bus():
    pulses, events = _burst('s', 2, '2026-11-08 08:00', pulses=5, entry=True)
    pulses[0]['Duration'] = 1.6
    assert _classes(*_frames(pulses, events)) == {('s', 2): [CLASS_BUS_PREEMPT]}


def test_a_burst_with_no_tsp_check_in_is_chatter_not_a_bus():
    # The same pulses as a bus burst, each with an entry, at a signal that logs no TSP check-ins.
    pulses, events = _burst('s', 2, '2026-11-08 08:00', entry=True, check_in=False)
    assert _classes(*_frames(pulses, events)) == {('s', 2): [CLASS_EV]}


def test_bursts_from_before_check_ins_were_recorded_follow_their_input():
    # Check-ins on record for the last two days only: the input's earlier bursts are bus calls too.
    calls, events = [], []
    for day, known in ((5, False), (6, False), (7, True), (8, True)):
        c, e = _burst('s', 1, f'2026-11-0{day} 08:00', entry=True, check_in=known)
        calls += c
        events += e
    assert _classes(*_frames(calls, events)) == {('s', 1): [CLASS_BUS_PREEMPT]}


def test_tsp_check_in_makes_a_pulse_episode_tsp():
    pulses, events = _burst('s', 3, '2026-11-08 08:00', on=0.1, off=0.15, pulses=6, check_in=True)
    assert _classes(*_frames(pulses, events)) == {('s', 3): [CLASS_TSP]}


def test_rail_is_by_input_number():
    calls = pd.DataFrame([_call('s', 5, '2026-11-08 08:00', 0.5), _call('s', 6, '2026-11-08 08:00', 90.0)])
    assert set(classify_preempt_calls(calls, None, RAIL)['Class']) == {CLASS_RAIL}
    assert set(classify_preempt_calls(calls, None, None)['Class']) == {CLASS_EV}


def test_without_raw_events_every_burst_is_chatter():
    pulses, _ = _burst('s', 1, '2026-11-08 08:00', entry=True)
    flicker, _ = _burst('s', 2, '2026-11-08 08:00', on=0.1, off=0.15, pulses=20)
    calls = pd.DataFrame(pulses + flicker + [_call('s', 3, '2026-11-08 08:00', 30.0), _call('s', 4, '2026-11-08 08:00', 2.0)])
    assert _classes(calls) == {('s', 1): [CLASS_EV], ('s', 2): [CLASS_EV], ('s', 3): [CLASS_EV],
                               ('s', 4): [CLASS_UNCLASSIFIED]}


# ---- per-class alerts --------------------------------------------------------

def _week(maker, days=7, per_day=2, end=None, **kwargs):
    calls, events = [], []
    for day in pd.date_range(end=DAY if end is None else end, periods=days):
        for hour in range(8, 8 + per_day):
            c, e = maker(day + pd.Timedelta(hours=hour), **kwargs)
            calls += c
            events += e
    return _frames(calls, events)


def _alerts(calls, events):
    classed = classify_preempt_calls(calls, events, RAIL)
    checked = classed[~classed['Class'].isin(TSP_CLASSES)]
    duration = build_preempt_duration_alerts(checked, None, DAY, rail_preempts=RAIL)
    bus = build_bus_preempt_alerts(classed, DAY)
    return duration, bus


def test_bus_pulses_that_ran_no_preempt_raise_priority_interrupted():
    # Lowry P2-like: bus bursts of 0.6 s pulses on a preempt input, no entry. Normal TSP logs no preempt calls at all.
    calls, events = _week(lambda t: _burst('lowry', 2, t))
    duration, bus = _alerts(calls, events)
    assert duration.empty
    assert len(bus) == 1
    row = bus.iloc[0]
    assert (row['Class'], row['Direction'], row['RanAsPreempt'], row['BusCalls']) == (SECTION_TSP, DIRECTION_BUS_INTERRUPTED, 0, 14)
    assert row['Note'].startswith('Bus priority calls pulsed this preempt input: 14 in the last 7 days, 0 ran as full preempts.')


def test_bursts_with_no_check_ins_raise_chatter_whether_or_not_they_ran_preempts():
    for entry in (False, True):
        calls, events = _week(lambda t: _burst('s', 2, t, entry=entry, check_in=False))
        duration, bus = _alerts(calls, events)
        assert bus.empty
        assert duration['Direction'].tolist() == [DIRECTION_CHATTER]


def test_bus_calls_that_ran_as_preempts_raise_the_tsp_alert_instead_of_chatter():
    # Lowry P1-like: the same bursts, each with a preempt entry.
    calls, events = _week(lambda t: _burst('lowry', 1, t, entry=True))
    duration, bus = _alerts(calls, events)
    assert duration.empty
    assert len(bus) == 1
    row = bus.iloc[0]
    assert (row['Class'], row['Direction'], row['RanAsPreempt'], row['BusCalls']) == (SECTION_TSP, DIRECTION_BUS_PREEMPT, 14, 14)
    assert row['Note'].startswith('Bus priority calls ran as full preempts: 14 of 14 bus calls in the last 7 days.')
    assert 'must not skip phases' in row['Note']


def test_bus_alert_needs_five_bus_calls_that_ran_as_preempts():
    calls, events = _week(lambda t: _burst('s', 1, t, entry=True), days=4, per_day=1)
    assert build_bus_preempt_alerts(classify_preempt_calls(calls, events, RAIL), DAY).empty
    calls, events = _week(lambda t: _burst('s', 1, t, entry=True), days=5, per_day=1)
    assert len(build_bus_preempt_alerts(classify_preempt_calls(calls, events, RAIL), DAY)) == 1


def test_flicker_on_an_emergency_vehicle_input_still_raises_chatter():
    calls, events = _week(lambda t: _burst('us199', 2, t, on=0.1, off=0.15, pulses=25, entry=True, check_in=False),
                          days=1, per_day=1)
    duration, bus = _alerts(calls, events)
    assert bus.empty
    assert duration['Direction'].tolist() == [DIRECTION_CHATTER]


def _history(device, preempt, counts, cls, end=DAY):
    days = pd.date_range(end=end, periods=len(counts))
    rows = [{'DeviceId': device, 'Preempt': 0, 'Date': d, 'Count': 0} for d in days]
    rows += [{'DeviceId': device, 'Preempt': preempt, 'Date': d, 'Count': c, 'Class': cls} for d, c in zip(days, counts) if c]
    return pd.DataFrame(rows)


def test_frequency_is_tested_per_section_and_never_on_tsp():
    # A busy TSP input that goes quiet, and an emergency vehicle input whose calls stop, on one signal.
    tsp = _history('s', 2, [12] * 28 + [0] * 7, CLASS_TSP)
    ev = _history('s', 1, [4] * 28 + [0] * 7, CLASS_EV).query('Preempt > 0')
    rail = _history('s', 5, [3] * 28 + [30] * 7, CLASS_RAIL).query('Preempt > 0')
    alerts = build_section_preempt_alerts(pd.concat([tsp, ev, rail]), DAY, RAIL)
    assert set(zip(alerts['Preempt'], alerts['Class'], alerts['Direction'])) == {
        (1, SECTION_EV, 'Decrease'), (5, SECTION_RAIL, 'Increase')}


def test_tsp_pulses_do_not_fill_an_emergency_vehicle_baseline():
    # The input used to carry TSP and now carries a few emergency vehicle calls a week: no change for emergency vehicles.
    tsp = _history('s', 1, [12] * 28 + [0] * 7, CLASS_TSP)
    ev = _history('s', 1, [0, 0, 1, 0, 0, 0, 0] * 5, CLASS_EV).query('Preempt > 0')
    assert build_section_preempt_alerts(pd.concat([tsp, ev]), DAY, RAIL).empty


# ---- history ----------------------------------------------------------------

def test_old_history_rows_without_a_class_count_as_before():
    old = _history('s', 1, [4] * 28 + [0] * 7, None).drop(columns='Class')
    alerts = build_section_preempt_alerts(old, DAY, RAIL)
    assert alerts[['Preempt', 'Class', 'Direction']].values.tolist() == [[1, SECTION_EV, 'Decrease']]


def test_rebuild_recounts_covered_days_by_class_and_keeps_the_rest():
    old = _history('s', 1, [9, 9], None).drop(columns='Class')
    pulses, events = _burst('s', 1, DAY + pd.Timedelta(hours=8))
    calls = classify_preempt_calls(*_frames(pulses + [_call('s', 1, DAY + pd.Timedelta(hours=9), 30.0)], events), RAIL)
    rebuilt = rebuild_history_classes(old, calls)
    today = rebuilt[(rebuilt['Date'] == DAY) & (rebuilt['Preempt'] == 1)].set_index('Class')['Count'].to_dict()
    assert today == {CLASS_TSP: 4, CLASS_EV: 1}
    before = rebuilt[(rebuilt['Date'] == DAY - pd.Timedelta(days=1)) & (rebuilt['Preempt'] == 1)]
    assert before['Count'].tolist() == [9] and before['Class'].isna().all()
    assert (rebuilt['Preempt'] == 0).sum() == 2


# ---- generate(): wiring, suppression keys, report sections --------------------

SIGNALS = pd.DataFrame([{'DeviceId': 'dev-a', 'Name': '01001-Main at 1st', 'Region': 'Region 1'}])


def _timeline(calls):
    calls = calls.copy()
    return pd.DataFrame({
        'DeviceId': calls['DeviceId'], 'StartTime': calls['StartTime'],
        'EndTime': calls['StartTime'] + pd.to_timedelta(calls['Duration'], unit='s'),
        'Duration': calls['Duration'], 'IsValid': calls['IsValid'], 'EventClass': 'Preempt', 'EventValue': calls['Preempt'],
    })


def _mixed(end):
    """Bus calls that ran as preempts on P1, a flickering P2, and a rail P5 held 2 h while P6 kept working."""
    bus_calls, bus_events = _week(lambda t: _burst('dev-a', 1, t, entry=True), end=end)
    # Hours after the bus calls: a check-in is matched by signal, so a burst beside one would read as a bus.
    flick_calls, flick_events = _week(lambda t: _burst('dev-a', 2, t + pd.Timedelta(hours=4), on=0.1, off=0.15, pulses=25,
                                                       entry=True, check_in=False), days=1, per_day=1, end=end)
    rail = pd.DataFrame([_call('dev-a', 5, end + pd.Timedelta(hours=1), 7200.0),
                         _call('dev-a', 6, end + pd.Timedelta(hours=1, minutes=30), 60.0)])
    calls = pd.concat([bus_calls, flick_calls, rail], ignore_index=True)
    events = pd.concat([bus_events, flick_events], ignore_index=True)
    return calls, events


def _generate(calls, events, past_alerts=None, pdf=True):
    config = {'verbosity': 0, 'preempt_rail_numbers': RAIL, 'signal_focus_signals': 0}
    kwargs = dict(signals=SIGNALS, timeline=_timeline(calls), preempt_events=events, past_alerts=past_alerts)
    if pdf:
        return ReportGenerator(config).generate(**kwargs)
    with patch('atspm_report.generator.generate_pdf_report', return_value={}):
        return ReportGenerator(config).generate(**kwargs)


def test_generate_sorts_alerts_into_sections_and_classes_the_stored_calls():
    end = pd.Timestamp.today().normalize()
    calls, events = _mixed(end)
    result = _generate(calls, events, pdf=False)
    alerts = result['alerts']['preempts']
    got = set(zip(alerts['Preempt'], alerts['Class'], alerts['Direction']))
    assert (1, SECTION_TSP, DIRECTION_BUS_PREEMPT) in got
    assert (2, SECTION_EV, DIRECTION_CHATTER) in got
    assert any(p == 5 and c == SECTION_RAIL for p, c, _ in got)
    assert not any(p == 1 and d == DIRECTION_CHATTER for p, _, d in got)
    stored = result['updated_preempt_calls']
    assert set(stored.loc[stored['Preempt'] == 1, 'Class']) == {CLASS_BUS_PREEMPT}
    assert 'Class' in result['updated_preempt_history'].columns
    assert set(result['updated_past_alerts']['preempts'].columns) == {'DeviceId', 'Preempt', 'Class', 'Direction', 'Date'}


def test_old_chatter_history_does_not_hold_back_the_new_bus_alert():
    end = pd.Timestamp.today().normalize()
    calls, events = _mixed(end)
    yesterday = end - pd.Timedelta(days=1)
    # History from before the split: no Class column.
    old = pd.DataFrame([{'DeviceId': 'dev-a', 'Preempt': 1, 'Direction': 'Chatter', 'Date': yesterday},
                        {'DeviceId': 'dev-a', 'Preempt': 2, 'Direction': 'Chatter', 'Date': yesterday}])
    alerts = _generate(calls, events, {'preempts': old}, pdf=False)['alerts']['preempts']
    got = set(zip(alerts['Preempt'], alerts['Direction']))
    assert (1, DIRECTION_BUS_PREEMPT) in got        # new kind of alert: shown
    assert (2, DIRECTION_CHATTER) not in got        # the same emergency vehicle chatter: held back as before
    # Once filed with its class, the bus alert is a repeat.
    filed = pd.concat([old, pd.DataFrame([{'DeviceId': 'dev-a', 'Preempt': 1, 'Class': SECTION_TSP,
                                           'Direction': DIRECTION_BUS_PREEMPT, 'Date': yesterday}])])
    alerts = _generate(calls, events, {'preempts': filed}, pdf=False)['alerts']['preempts']
    assert (1, DIRECTION_BUS_PREEMPT) not in set(zip(alerts['Preempt'], alerts['Direction']))


def _pdf_text(buffer):
    pypdf = pytest.importorskip('pypdf')
    return ' '.join(page.extract_text() for page in pypdf.PdfReader(buffer).pages)


def test_report_has_three_preempt_sections_in_order_at_the_end():
    end = pd.Timestamp.today().normalize()
    calls, events = _mixed(end)
    text = ' '.join(_pdf_text(_generate(calls, events)['reports']['Region 1']).split())
    # Each heading straight over its table, no explanation, in this order.
    ev = text.find('Emergency Vehicle Preempt Showing')
    tsp = text.find('Transit Signal Priority Showing')
    rail = text.find('Rail Showing')
    assert 0 <= ev < tsp < rail, (ev, tsp, rail)
    assert 'Preempt Monitoring' not in text and not re.search(r'TSP', text)
    assert '14 of 14 bus calls in the last 7 days ran as full preempts' in text
    assert 'must not skip phases' not in text and 'Emergency vehicle preempt inputs' not in text


def test_preempt_sections_come_after_detector_health_and_before_missing_data():
    import inspect
    from atspm_report import report_generation
    source = inspect.getsource(report_generation.generate_pdf_report)
    order = [source.find(marker) for marker in (
        '# Section: Detector Health', '# Section: Pedestrian Button Health', 'PREEMPT_SECTION_TEXT',
        '# Section: Missing Data')]
    assert all(p >= 0 for p in order) and order == sorted(order)
    assert [s[0] for s in report_generation.PREEMPT_SECTION_TEXT] == [SECTION_EV, SECTION_TSP, SECTION_RAIL]


def test_signal_focus_lines_name_the_kind_of_preempt():
    from atspm_report.signal_focus import _preempt_line
    ev = pd.Series({'Preempt': 1, 'Class': SECTION_EV, 'Direction': 'Increase', 'BaselinePerDay': 0.5, 'RecentPerDay': 6.0})
    tsp = pd.Series({'Preempt': 3, 'Class': SECTION_TSP, 'Direction': DIRECTION_BUS_PREEMPT, 'RanAsPreempt': 31, 'BusCalls': 34})
    rail = pd.Series({'Preempt': 5, 'Class': SECTION_RAIL, 'Direction': 'Long Hold', 'Note': '1 call over 60 min'})
    pulsed = pd.Series({'Preempt': 2, 'Class': SECTION_TSP, 'Direction': DIRECTION_BUS_INTERRUPTED, 'RanAsPreempt': 0, 'BusCalls': 22})
    assert _preempt_line(pulsed) == 'Transit Signal Priority on preempt input 2: bus calls pulsed the preempt input (22 in the last 7 days)'
    assert _preempt_line(ev) == 'Emergency vehicle preempt 1 increase: 0.5 to 6.0 calls a day'
    assert _preempt_line(tsp) == 'Transit Signal Priority on preempt input 3: bus calls ran as preempts (31 of 34 bus calls in the last 7 days)'
    assert _preempt_line(rail) == 'Rail preempt 5 long hold: 1 call over 60 min'


def test_false_calls_option_adds_the_alert_and_draws_its_chart():
    end = pd.Timestamp.today().normalize()
    days = pd.date_range(end=end, periods=7)
    # A signal half a mile away has vehicle calls of its own, none of them when dev-a calls.
    calls = pd.DataFrame([_call('dev-a', 4, day + pd.Timedelta(hours=hour), 5.2) for day in days for hour in (1, 5, 9)]
                         + [_call('dev-b', 1, day + pd.Timedelta(hours=13), 12.0 + i) for i, day in enumerate(days[:4])])
    config = {'verbosity': 0, 'preempt_rail_numbers': RAIL, 'signal_focus_signals': 0}
    signals = pd.concat([SIGNALS, SIGNALS.assign(DeviceId='dev-b', Name='01002-Main at 9th')]).assign(
        Latitude=[45.0, 45.00724], Longitude=-122.0)
    off = ReportGenerator({**config, 'preempt_false_calls_enabled': False}).generate(signals=signals, timeline=_timeline(calls))
    assert off['alerts']['preempts'].empty
    on = ReportGenerator(config).generate(signals=signals, timeline=_timeline(calls))
    alerts = on['alerts']['preempts']
    assert alerts['Direction'].tolist() == ['False Calls']
    text = ' '.join(_pdf_text(on['reports']['Region 1']).split())
    assert '21 of 21 calls in the last 7 days had the same duration, lasting about 5.2 s' in text
