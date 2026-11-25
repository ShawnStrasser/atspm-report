"""Flash verification and coordination holding, from the raw controller events (both off by default)."""
import json
import os
import sys

import duckdb
import matplotlib
import numpy as np
import pandas as pd

matplotlib.use('Agg')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atspm_report import ReportGenerator, connection  # noqa: E402
from atspm_report.alarm_processing import flash_verdicts, verify_flash_alarms  # noqa: E402
from atspm_report.coordination_holding import holding_findings  # noqa: E402

DAY = pd.Timestamp('2026-11-23')
SPLITS = {1: 15, 2: 35, 3: 15, 4: 35}   # 100 s cycle, coordinated phases 2 / 6 leading at local zero


def _events(device, rows):
    rows = sorted(rows)
    return pd.DataFrame({'DeviceId': device, 'TimeStamp': DAY + pd.to_timedelta([r[0] for r in rows], unit='s'),
                         'EventId': [r[1] for r in rows], 'Parameter': [r[2] for r in rows]})


def _coordinated(device, start_h=6.0, end_h=10.0, late=None, walk_over=None, preempt=False):
    """A signal running pattern 1 (100 s cycle, offset 20) from start_h; late(k): seconds cycle k starts after its local
    zero; walk_over(k): phase 4 serves its walk and runs 15 s past its split in cycle k (the next one starts 10 s late)."""
    t0, cycle, offset = start_h * 3600 + 0.1, 100, 20
    rows = [(t0, 131, 1), (t0, 132, cycle), (t0, 133, offset)] + [(t0, 133 + p, SPLITS[(p - 1) % 4 + 1]) for p in range(1, 9)]
    grid = offset + np.arange(int(np.ceil((t0 - offset) / cycle)), int((end_h * 3600 - offset) // cycle)) * cycle
    over = [bool(walk_over and walk_over(k)) for k in range(len(grid))]
    starts = [g + (late(k) if late else 0) + (10 if k and over[k - 1] else 0) for k, g in enumerate(grid)]
    for k in range(len(grid) - 1):
        s, e = starts[k], starts[k + 1]
        durations = np.array([SPLITS[2], SPLITS[3], SPLITS[4] + (15 if over[k] else 0), SPLITS[1] - (5 if over[k] else 0)], float)
        t = s
        for d, phase in zip(durations * (e - s) / durations.sum(), (2, 3, 4, 1)):
            for p in (phase, phase + 4):
                rows += [(t, 1, p), (t + d - 4.5, 8, p), (t + d, 11, p)]
            if phase == 4 and over[k]:
                rows.append((t + 0.1, 102, 1) if preempt else (t + 0.1, 21, 4))
            t += d
        rows += [(s, 150, 5), (s, 152, 2), (s, 152, 6)] + ([(s, 316, round(s - starts[k - 1]))] if k else [])
    return rows


def _flash_rows():
    """1: MMU flash flipping 40 times while the signal cycles; 2: an MMU flash that is over; 3: one still on at as_of."""
    greens = lambda a, b: [(t, 1, 2) for t in np.arange(a, b, 100.0)]
    one = greens(0, 6 * 3600) + [(t, 173, 6) for t in np.arange(600, 4600, 100.0) + 3] + \
        [(t, 173, 2) for t in np.arange(600, 4600, 100.0) + 5] + [(t, 174, 16) for t in np.arange(600, 4600, 100.0) + 3] + \
        [(t, 174, 0) for t in np.arange(600, 4600, 100.0) + 5]
    two = greens(0, 3600) + greens(7200, 6 * 3600) + [(3605, 173, 6), (3605, 174, 16), (7190, 173, 2), (7190, 174, 0)]
    three = greens(0, 3600) + [(3605, 173, 6), (3605, 174, 16), (86400 + 3600, 173, 6)]   # still in MMU flash after midnight
    return pd.concat([_events('1', one), _events('2', two), _events('3', three)], ignore_index=True)


def _timeline(events):
    onsets = events[(events['EventId'] == 173) & (events['Parameter'] == 6) & (events['TimeStamp'] < DAY + pd.Timedelta(days=1))]
    flash = pd.DataFrame({'DeviceId': onsets['DeviceId'], 'StartTime': onsets['TimeStamp'], 'EndTime': onsets['TimeStamp'],
                          'Duration': 0.0, 'IsValid': True, 'EventClass': 'Flash - MMU', 'EventValue': 6})
    return pd.concat([flash, flash.assign(EventClass='MMU Flash', EventValue=4)], ignore_index=True)


SIGNALS = pd.DataFrame({'DeviceId': ['1', '2', '3'], 'Name': ['One', 'Two', 'Three'], 'Region': 'R'})


def test_flash_verdicts_tell_a_flipping_input_from_a_flash_over_or_still_on():
    events = _flash_rows()
    verdicts = flash_verdicts(events, DAY, as_of=DAY + pd.Timedelta(days=1, hours=2)).set_index(['DeviceId', 'AlarmType'])
    assert verdicts.loc[('1', 'Flash - MMU'), 'Onsets'] == 40 and verdicts.loc[('1', 'Flash - MMU'), 'KeptCycling']
    assert verdicts.loc[('1', 'MMU Flash'), 'KeptCycling']
    assert not verdicts.loc[('2', 'Flash - MMU'), 'KeptCycling'] and not verdicts.loc[('2', 'Flash - MMU'), 'Ongoing']
    assert verdicts.loc[('3', 'Flash - MMU'), 'Ongoing'] and verdicts.loc[('3', 'Flash - MMU'), 'OngoingNow']
    assert verdicts.loc[('3', 'MMU Flash'), 'Ongoing'] and verdicts.loc[('3', 'MMU Flash'), 'OngoingNow']
    alarms = pd.DataFrame({'DeviceId': ['1', '1', '2'], 'AlarmType': ['Flash - MMU', 'MMU Flash', 'Cycle Fault']})
    verified = verify_flash_alarms(alarms, verdicts.reset_index())
    assert list(verified['AlarmType']) == ['Flash - MMU', 'Cycle Fault']     # the same flash told once
    assert verified.loc[1, 'KeptCycling'] is None


def test_flash_verification_grades_signal_problems_and_today():
    events = _flash_rows()
    timeline = _timeline(events)
    as_of = DAY + pd.Timedelta(days=1, hours=2)
    off = ReportGenerator({'verbosity': 0}).generate(signals=SIGNALS, timeline=timeline, controller_events=events,
                                                     report_date=DAY.date(), as_of=as_of, analytics_only=True)
    assert 'KeptCycling' not in off['alarms'].columns and 'coordination_holding' not in off
    assert set(off['signal_problems']['Points']) == {4.0}      # without the switch, every MMU flash is urgent
    on = ReportGenerator({'verbosity': 0, 'flash_verification_enabled': True}).generate(
        signals=SIGNALS, timeline=timeline, controller_events=events, report_date=DAY.date(), as_of=as_of, analytics_only=True)
    problems = on['signal_problems'].set_index('DeviceId')
    lines = {device: [(line['text'], line['grade']) for line in problems.loc[device, 'Lines']] for device in problems.index}
    assert lines['1'] == [('MMU flash logged 40 times while the signal kept cycling', 'review')]
    assert lines['2'] == [('Flash - MMU: 1 on 11/23, 1 in 6 weeks; not in flash at the end of the data', 'review')]
    assert lines['3'] == [('Flash - MMU: 1 on 11/23, 1 in 6 weeks; still in flash at the end of the data', 'urgent')]
    assert problems['Points'].to_dict() == {'1': 2.0, '2': 2.0, '3': 4.0}
    assert problems['Today'].to_dict() == {'1': None, '2': False, '3': True}


def test_coordination_holding_finds_walk_overruns_and_a_slow_transition_on_one_morning():
    walk = _events('10', _coordinated('10', walk_over=lambda k: k >= 36 and k % 3 == 0))
    slow = _events('11', _coordinated('11', late=lambda k: max(0.0, 90 - 6 * k)))
    # the plan of the evening before (read as the lead-in) neither makes cycles nor a change before the data starts
    steady = _events('12', _coordinated('12') + [(-7200.0, 131, 9), (-7200.0, 132, 100), (-7200.0, 133, 0)])
    found = holding_findings(pd.concat([walk, slow, steady], ignore_index=True), DAY).set_index('DeviceId')
    assert set(found.index) == {'10', '11'} and (pd.to_datetime(found['Start']) >= DAY).all()
    assert found.loc['10', 'Issue'] == 'not holding coordination' and found.loc['10', 'Cause'] == 'walk'
    assert found.loc['10', 'Text'].startswith('The phase 4 walk runs past its split and keeps the signal out of step: '
                                              'pattern 1 (100 s cycle) was out of step on ')
    assert found.loc['11', 'Issue'] == 'slow transition' and found.loc['11', 'Grade'] == 'review'
    assert found.loc['11', 'Text'] == 'Pattern 1 took 24 minutes to get back in step after the change at 11/23 06:00.'


def test_connection_mode_reads_the_raw_events_only_when_switched_on():
    events = pd.concat([_events('10', _coordinated('10', walk_over=lambda k: k >= 36 and k % 3 == 0)), _flash_rows()])
    con = duckdb.connect()
    con.register('f', events)
    con.execute('CREATE TABLE HiResEvents AS SELECT * FROM f')
    con.register('t', _timeline(events))
    con.execute('CREATE TABLE timeline AS SELECT * FROM t')
    signals = pd.concat([SIGNALS, pd.DataFrame({'DeviceId': ['10'], 'Name': ['Ten'], 'Region': 'R'})])
    assert 'controller_events' not in connection.load_inputs(con, DAY.date(), signals=signals, config={'verbosity': 0})
    config = {'verbosity': 0, 'flash_verification_enabled': True, 'coordination_holding_enabled': True}
    result = connection.run(con, DAY.date(), signals=signals, config=config, as_of=DAY + pd.Timedelta(days=1, hours=2),
                            analytics_only=True)
    assert list(result['coordination_holding']['DeviceId']) == ['10']
    table = con.execute('SELECT DeviceId, Category, Today, Lines FROM atspm_report.signal_problems ORDER BY 1').df()
    assert dict(zip(table['DeviceId'], table['Category'])) == {'1': 'Alarms', '10': 'Coordination', '2': 'Alarms', '3': 'Alarms'}
    assert json.loads(table.set_index('DeviceId').loc['10', 'Lines'])[0]['grade'] == 'urgent'
