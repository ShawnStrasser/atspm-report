"""Report length: signal limits on the Data Only, phase skip and alarm tables, and the rotation of the ongoing alerts.

The rule behind them: an alert a table leaves out is not filed in the alert history, so it comes up on a later run."""

from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd

from atspm_report import ReportGenerator
from atspm_report.atspm_detector_health import listed_alerts
from atspm_report.detector_triage import DATA_ONLY, OTHER_PHASES, attach_triage
from atspm_report.table_generation import (
    head_signals, prepare_alarms_table, prepare_atspm_detector_health_table, prepare_phase_skip_alerts_table,
)
from atspm_report import table_limits
from atspm_report.table_limits import alert_keys, listed_phase_skips, normalize_state, rotate_ongoing

CLEARANCE_IDS = {'clearance_intervals': ['DeviceId', 'EventClass', 'EventValue']}


# Detector health rows (as in test_detector_triage).
def _alert(device, detector, score=0.1):
    return {'DeviceId': device, 'DetectorType': 'Vehicle', 'Detector': detector, 'Date': pd.Timestamp('2026-10-28'), 'Issue': 'fault',
            'Status': 'bad', 'Score': score, 'Label': 'stuck on', 'Reason': f'det {detector}: Ph2 Advance - Stuck on: x',
            'Periods': [], 'Evidence': []}


def _ranked(*rows):
    columns = ['DeviceId', 'DetectorType', 'Detector', 'Phase', 'Kind', 'KindOrder', 'FaultHours', 'Tier', 'SignalWide', 'ImpactText', 'HurtPhases',
               'ExtraVehHoursPerDay', 'MaxOutVehPerDay', 'OtherWaitUp', 'OtherMaxOutUp', 'OwnRatio', 'OtherRatio', 'Lost', 'Coordinated']
    return pd.DataFrame([row + (0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, False) for row in rows], columns=columns)


def _below(device, detector):
    return (device, 'Vehicle', detector, 2, 'Counts high', 4, 5.0, DATA_ONLY, False, 'No effect on other phases over 5 h', '')


def _harm(device, detector):
    return (device, 'Vehicle', detector, 2, 'Stuck on', 1, 5.0, OTHER_PHASES, False, 'Other phases wait 27 s longer over 5 h', '4')


HEALTH_SIGNALS = pd.DataFrame({'DeviceId': list('ABCD'), 'Name': ['Alpha', 'Bravo', 'Charlie', 'Delta'], 'Region': 'Region 1'})


def test_signal_limit_counts_signals_not_rows():
    rows = pd.DataFrame({'Signal': ['A', 'A', 'A', 'B', 'C', 'C'], 'Value': range(6)})
    assert head_signals(rows, 2)['Signal'].tolist() == ['A', 'A', 'A', 'B']
    assert head_signals(rows, 0).equals(rows)


def test_data_only_table_stops_at_its_signal_limit_and_only_those_are_filed():
    # Bravo's detector hurts traffic; Alpha (two detectors), Charlie and Delta are data only, in that order of health score.
    alerts = pd.DataFrame([_alert('A', 1, 0.01), _alert('A', 2, 0.02), _alert('B', 3, 0.5), _alert('C', 1, 0.1), _alert('D', 1, 0.2)])
    ranked = attach_triage(alerts, _ranked(_below('A', 1), _below('A', 2), _harm('B', 3), _below('C', 1), _below('D', 1)))
    data, total = prepare_atspm_detector_health_table(ranked, HEALTH_SIGNALS, ('bad',), max_rows=50, impact=False, max_signals=2)
    assert data['Signal'].tolist() == ['Alpha', 'Alpha', 'Charlie'] and total == 4    # two signals, three rows; the total is all
    listed = listed_alerts(ranked, HEALTH_SIGNALS, 50, data_only_signals=2)
    assert sorted(set(listed['DeviceId'])) == ['A', 'B', 'C']     # Delta was in no table: not filed, so told later
    assert len(listed_alerts(ranked, HEALTH_SIGNALS, 50)) == 5


def _skip_rows(devices, day):
    return pd.DataFrame([{'DeviceId': device, 'Phase': phase, 'Date': day, 'TotalSkips': 20 - index}
                         for index, device in enumerate(devices) for phase in (2, 6)])


def test_phase_skip_table_lists_five_signals_however_many_rows_they_have():
    devices = [f'S{n}' for n in range(7)]
    signals = pd.DataFrame({'DeviceId': devices, 'Name': [f'Signal {n}' for n in range(7)],
                            'Region': ['Region 1'] * 4 + ['Region 2'] * 3})
    rows = pd.concat([_skip_rows(devices, pd.Timestamp('2026-11-15')), _skip_rows(devices, pd.Timestamp('2026-11-16'))])
    table, total = prepare_phase_skip_alerts_table(rows, signals, region='All Regions', max_rows=50, max_signals=5)
    assert table['Signal'].nunique() == 5 and len(table) == 10 and total == 14
    # Filed: each region's first five and All Regions' first five. With four signals in Region 1, all of them are listed
    # there, and Region 2's three: all seven. With a limit of two, two in each region.
    alerts = rows[['DeviceId', 'Phase', 'Date', 'TotalSkips']]
    assert set(listed_phase_skips(alerts, rows, signals, 50, 5)['DeviceId']) == set(devices)
    assert set(listed_phase_skips(alerts, rows, signals, 50, 2)['DeviceId']) == {'S0', 'S1', 'S4', 'S5'}
    # A Signal Focus signal leaves its report's table, so the next one moves up; its own alerts count as told.
    hidden = {'All Regions': {'S0'}, 'Region 1': {'S0'}, 'Region 2': set()}
    assert set(listed_phase_skips(alerts, rows, signals, 50, 2, hidden=hidden)['DeviceId']) == {'S0', 'S1', 'S2', 'S4', 'S5'}


def test_new_phase_skips_left_out_of_the_tables_are_not_filed():
    now = datetime.now()
    day = pd.Timestamp(now.date()) - pd.Timedelta(days=1)
    devices = [f'{n}' for n in range(1, 8)]
    signals = pd.DataFrame({'DeviceId': devices, 'Name': [f'Signal {n}' for n in devices], 'Region': 'Region 1'})
    phase_wait = pd.DataFrame([{'TimeStamp': day + pd.Timedelta(hours=8), 'DeviceId': device, 'Phase': 2, 'AvgPhaseWait': 30.0,
                                'MaxPhaseWait': 60.0, 'TotalSkips': 10 + index} for index, device in enumerate(devices)])
    with patch('atspm_report.generator.create_phase_skip_plots', return_value=[]), \
            patch('atspm_report.generator.generate_pdf_report', return_value={}) as report:
        result = ReportGenerator({'verbosity': 0}).generate(signals=signals, phase_wait=phase_wait, past_alerts={})
    assert result['alerts']['phase_skips']['DeviceId'].nunique() == 7
    filed = result['updated_past_alerts']['phase_skips']
    assert sorted(filed['DeviceId'].unique()) == ['3', '4', '5', '6', '7']     # the five with the most skips
    assert report.call_args.kwargs['table_signal_limits'] == {'data_only': 10, 'phase_skips': 5, 'alarms': 10}
    # The next run: the two left out are new again; the five filed ones are repeats.
    with patch('atspm_report.generator.create_phase_skip_plots', return_value=[]), \
            patch('atspm_report.generator.generate_pdf_report', return_value={}):
        again = ReportGenerator({'verbosity': 0}).generate(signals=signals, phase_wait=phase_wait,
                                                            past_alerts=result['updated_past_alerts'])
    assert sorted(again['alerts']['phase_skips']['DeviceId'].unique()) == ['1', '2']


def test_alarm_table_ranks_signals_by_their_six_week_total():
    signals = pd.DataFrame({'DeviceId': [str(n) for n in range(12)], 'Name': [f'Signal {n:02d}' for n in range(12)], 'Region': 'Region 1'})
    alarms = pd.DataFrame([{'DeviceId': str(n), 'AlarmType': 'Cycle Fault', 'Date': pd.Timestamp('2026-11-16'), 'DayCount': 1,
                            'TotalCount': n + 1, 'LatestAlarm': pd.Timestamp('2026-11-16 08:00')} for n in range(12)])
    # Signal 00 has the fewest of one kind, but two kinds together give it the most.
    alarms = pd.concat([alarms, pd.DataFrame([{'DeviceId': '0', 'AlarmType': 'MMU Flash', 'Date': pd.Timestamp('2026-11-16'),
                                               'DayCount': 1, 'TotalCount': 30, 'LatestAlarm': pd.Timestamp('2026-11-16 09:00')}])])
    rows, total = prepare_alarms_table(alarms, signals, region='Region 1', max_rows=50, max_signals=10)
    assert total == 13 and rows['Signal'].nunique() == 10
    assert rows['Signal'].tolist()[:3] == ['Signal 00', 'Signal 00', 'Signal 11']
    assert rows['Alarm'].tolist()[:2] == ['MMU Flash', 'Cycle Fault']
    assert 'Signal 01' not in rows['Signal'].tolist() and 'Signal 02' not in rows['Signal'].tolist()


def _ongoing_clearance(devices, since, deltas=None):
    deltas = deltas or {}
    return pd.DataFrame([{'DeviceId': device, 'EventClass': 'Yellow', 'EventValue': 4.0, 'MaxAbsDelta': deltas.get(device, 1.0),
                          'OngoingSince': since} for device in devices])


def _run(ongoing, signals, state, limit, today):
    return rotate_ongoing({'clearance_intervals': ongoing}, signals, state, {'clearance_intervals': limit}, CLEARANCE_IDS, today)


ROTATION_SIGNALS = pd.DataFrame({'DeviceId': list('ABCD'), 'Name': list('ABCD'), 'Region': 'Region 1'})


def test_rotation_cycles_through_the_ongoing_alerts_over_successive_runs():
    start = pd.Timestamp('2026-10-05')
    ongoing = _ongoing_clearance(list('ABCD'), start)
    state, shown = None, []
    for week in range(1, 5):
        cut, state = _run(ongoing, ROTATION_SIGNALS, state, 2, start + pd.Timedelta(weeks=week))
        shown.append(sorted(cut['clearance_intervals']['DeviceId']))
    assert shown == [['A', 'B'], ['C', 'D'], ['A', 'B'], ['C', 'D']]
    assert len(state) == 4 and set(state['Key']) == {f'{d}|Yellow|4' for d in 'ABCD'}


def test_rotation_shows_worse_problems_more_often_and_every_one_in_time():
    start = pd.Timestamp('2026-10-05')
    ongoing = _ongoing_clearance(list('ABCD'), start, deltas={'A': 2.0, 'B': 0.5, 'C': 0.5, 'D': 0.5})
    state, counts = None, dict.fromkeys('ABCD', 0)
    for week in range(1, 13):
        cut, state = _run(ongoing, ROTATION_SIGNALS, state, 1, start + pd.Timedelta(weeks=week))
        for device in cut['clearance_intervals']['DeviceId']:
            counts[device] += 1
    assert sum(counts.values()) == 12
    assert counts['A'] > max(counts['B'], counts['C'], counts['D'])
    assert min(counts['B'], counts['C'], counts['D']) >= 1


def test_rotation_limit_is_per_region_and_leaves_out_signal_focus_signals():
    signals = ROTATION_SIGNALS.assign(Region=['Region 1', 'Region 1', 'Region 2', 'Region 2'])
    ongoing = _ongoing_clearance(list('ABCD'), pd.Timestamp('2026-10-05'))
    cut, state = rotate_ongoing({'clearance_intervals': ongoing}, signals, None, {'clearance_intervals': 1}, CLEARANCE_IDS,
                                pd.Timestamp('2026-11-16'), hidden={'Region 1': {'A'}})
    assert sorted(cut['clearance_intervals']['DeviceId']) == ['B', 'C']
    assert sorted(state['Key']) == ['B|Yellow|4', 'C|Yellow|4']


def test_last_shown_state_round_trips_through_parquet(tmp_path):
    start = pd.Timestamp('2026-10-05')
    ongoing = _ongoing_clearance(list('ABCD'), start)
    _, state = _run(ongoing, ROTATION_SIGNALS, None, 2, start + pd.Timedelta(weeks=1))
    path = tmp_path / 'ongoing_last_shown.parquet'
    state.to_parquet(path, index=False)
    loaded = pd.read_parquet(path)
    pd.testing.assert_frame_equal(normalize_state(loaded), state)
    in_memory, _ = _run(ongoing, ROTATION_SIGNALS, state, 2, start + pd.Timedelta(weeks=2))
    from_file, _ = _run(ongoing, ROTATION_SIGNALS, loaded, 2, start + pd.Timedelta(weeks=2))
    assert sorted(in_memory['clearance_intervals']['DeviceId']) == sorted(from_file['clearance_intervals']['DeviceId']) == ['C', 'D']
    # Keys are the same whether an id came as 4 or 4.0.
    assert alert_keys(pd.DataFrame({'DeviceId': ['A'], 'Phase': [4.0]}), ['DeviceId', 'Phase']).tolist() == ['A|4']


HEALTH_IDS = {'atspm_detector_health': ['DeviceId', 'DetectorType', 'Detector', 'Issue']}


def test_rotation_keeps_the_two_detector_health_tables_apart():
    signals = pd.DataFrame({'DeviceId': list('ABCDEF'), 'Name': list('ABCDEF'), 'Region': 'Region 1'})
    alerts = [_alert(device, 1, score) for device, score in zip('ABCDEF', (0.4, 0.05, 0.3, 0.2, 0.1, 0.1))]
    plan = {**_alert('A', 9), 'Status': 'timing_plan', 'Issue': 'timing_plan'}
    ranked = attach_triage(pd.DataFrame(alerts + [plan]),
                           _ranked(*[_below(device, 1) for device in 'ABCD'], _harm('E', 1), _harm('F', 1)))
    ranked['OngoingSince'] = pd.Timestamp('2026-10-05')
    limits = {'atspm_detector_health': 2, ('atspm_detector_health', table_limits.DATA_ONLY_TABLE): 1}
    cut, state = rotate_ongoing({'atspm_detector_health': ranked}, signals, None, limits, HEALTH_IDS, pd.Timestamp('2026-11-16'))
    shown = cut['atspm_detector_health']
    # Affecting Operations: both; Data Only: one, Bravo, the lowest health score. The timing plan row stays, untold.
    assert sorted(shown.loc[shown['Status'] == 'bad', 'DeviceId']) == ['B', 'E', 'F']
    assert ((shown['DeviceId'] == 'A') & (shown['Status'] == 'timing_plan')).sum() == 1
    assert sorted(state['Key']) == ['B|Vehicle|1|fault', 'E|Vehicle|1|fault', 'F|Vehicle|1|fault']


def test_rotation_judges_severity_on_the_alert_days_and_keeps_every_row_of_a_signal():
    signals = pd.DataFrame({'DeviceId': ['X', 'Y'], 'Name': ['X', 'Y'], 'Region': 'Region 1'})
    rows = pd.DataFrame([
        ('X', 2, '2026-11-15', 0.9, 1), ('X', 2, '2026-11-14', 0.1, 0),
        ('Y', 4, '2026-11-15', 0.3, 1), ('Y', 4, '2026-11-14', 0.95, 0),    # a high day that did not alert
    ], columns=['DeviceId', 'Phase', 'Date', 'Percent MaxOut', 'Alert']).assign(OngoingSince=pd.Timestamp('2026-11-01'))
    cut, _ = rotate_ongoing({'maxout': rows}, signals, None, {'maxout': 1}, {'maxout': ['DeviceId', 'Phase']},
                            pd.Timestamp('2026-11-16'))
    assert cut['maxout']['DeviceId'].tolist() == ['X', 'X']


def _yellow_timeline(devices, now):
    rows = []
    for device in devices:
        for index in range(3):
            start = now - timedelta(minutes=10 * (index + 1))
            rows.append({'DeviceId': device, 'StartTime': start, 'EndTime': start + timedelta(seconds=3), 'Duration': 3.0,
                         'IsValid': True, 'EventClass': 'Yellow', 'EventValue': 8})
    return pd.DataFrame(rows)


def test_ongoing_alerts_not_shown_are_still_filed_and_the_next_run_shows_others():
    now = datetime.now().replace(microsecond=0)
    devices = ['s1', 's2', 's3']
    signals = pd.DataFrame({'DeviceId': devices, 'Name': ['One', 'Two', 'Three'], 'Region': 'Region 1'})
    past = {'clearance_intervals': pd.DataFrame([{'DeviceId': device, 'EventClass': 'Yellow', 'EventValue': 8,
                                                  'Date': now - timedelta(days=3)} for device in devices])}
    config = {'verbosity': 0, 'include_ongoing_issues': True, 'table_row_limits': {'ongoing': 1}}

    def generate(state, ongoing=True):
        with patch('atspm_report.generator.generate_pdf_report', return_value={}) as report:
            result = ReportGenerator({**config, 'include_ongoing_issues': ongoing}).generate(
                signals=signals, timeline=_yellow_timeline(devices, now), past_alerts=dict(past), ongoing_last_shown=state)
        return result, report.call_args.kwargs

    result, kwargs = generate(None)
    shown = result['ongoing_alerts']['clearance_intervals']['DeviceId'].unique().tolist()
    assert len(shown) == 1 and kwargs['ongoing_rotated'] is True
    filed = result['updated_past_alerts']['clearance_intervals']
    today = pd.Timestamp(now.date())
    assert sorted(filed.loc[pd.to_datetime(filed['Date']).dt.normalize() == today, 'DeviceId'].unique()) == devices
    state = result['updated_ongoing_last_shown']
    assert state['Key'].tolist() == [f'{shown[0]}|Yellow|8'] and state['LastShown'].tolist() == [today]

    again, _ = generate(state)
    assert again['ongoing_alerts']['clearance_intervals']['DeviceId'].unique().tolist() != shown
    # A run without the Ongoing Issues leaves the state as it was.
    quiet, kwargs = generate(state, ongoing=False)
    pd.testing.assert_frame_equal(quiet['updated_ongoing_last_shown'], state)
    assert kwargs['ongoing_rotated'] is False


def test_a_rotated_ongoing_table_counts_the_alerts_waiting_their_turn_in_its_total():
    from atspm_report import report_generation
    now = datetime.now().replace(microsecond=0)
    devices = ['s1', 's2', 's3']
    signals = pd.DataFrame({'DeviceId': devices, 'Name': ['One', 'Two', 'Three'], 'Region': 'Region 1'})
    past = {'clearance_intervals': pd.DataFrame([{'DeviceId': device, 'EventClass': 'Yellow', 'EventValue': 8,
                                                  'Date': now - timedelta(days=3)} for device in devices])}
    tables = []
    real = report_generation.create_reportlab_table

    def spy(df, title, *args, **kwargs):
        tables.append((title, len(df), kwargs.get('total_count')))
        return real(df, title, *args, **kwargs)
    with patch.object(report_generation, 'create_reportlab_table', side_effect=spy):
        ReportGenerator({'verbosity': 0, 'include_ongoing_issues': True, 'table_row_limits': {'ongoing': 1}}).generate(
            signals=signals, timeline=_yellow_timeline(devices, now), past_alerts=past)
    ongoing = [table for table in tables if table[0] == 'Ongoing Clearance Interval Alerts']
    assert ongoing == [('Ongoing Clearance Interval Alerts', 1, 3)] * 2     # All Regions and Region 1: 1 shown of 3


def test_ongoing_tables_keep_their_row_limit_after_the_rotation():
    from atspm_report import report_generation
    signals = pd.DataFrame({'DeviceId': ['s1', 's2'], 'Name': ['One', 'Two'], 'Region': 'Region 1'})
    ongoing = _ongoing_clearance(['s1', 's2'], pd.Timestamp('2026-10-05')).assign(
        MedianDuration=3.0, MinDuration=3.0, MaxDuration=3.0, Count=3)
    calls = []
    real = report_generation.prepare_clearance_interval_alerts_table

    def spy(frame, signals_df, region=None, max_rows=10, **kwargs):
        calls.append((region, max_rows, frame is ongoing))
        return pd.DataFrame(), 0
    with patch.object(report_generation, 'prepare_clearance_interval_alerts_table', side_effect=spy):
        report_generation.generate_pdf_report(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), [], [], signals_df=signals,
                                              max_table_rows=50, verbosity=0, table_row_limits={'ongoing': 1},
                                              ongoing_alerts={'clearance_intervals': ongoing}, ongoing_rotated=True)
    assert real is not spy
    assert sorted(call[:2] for call in calls if call[2]) == [('All Regions', 1), ('Region 1', 1)]


def test_ongoing_limits_follow_the_table_limits():
    limits = ReportGenerator({'max_table_rows': 50, 'table_row_limits': {'detector_faults': 25, 'ongoing': 15}})._ongoing_limits()
    assert limits['maxout'] == 15 and limits['phase_skips'] == 5
    assert limits['atspm_detector_health'] == 15 and limits[('atspm_detector_health', table_limits.DATA_ONLY_TABLE)] == 10
    assert 'coordination' not in limits and 'system_outages' not in limits


def test_a_rotated_table_stays_within_its_row_limit_keeping_each_signal_whole():
    """15 signals with all their rows ran an ongoing table to 61 rows (replay of 2026-11-16): the limit counts rows."""
    start = pd.Timestamp('2026-10-05')
    many = pd.DataFrame([{'DeviceId': 'A', 'EventClass': 'Yellow', 'EventValue': float(v), 'MaxAbsDelta': 5.0,
                          'OngoingSince': start} for v in range(1, 5)])                      # A: four rows, the worst
    ongoing = pd.concat([many, _ongoing_clearance(list('BCD'), start)], ignore_index=True)
    cut, _ = _run(ongoing, ROTATION_SIGNALS, None, 5, start + pd.Timedelta(weeks=1))
    shown = cut['clearance_intervals']
    assert len(shown) <= 5 and (shown['DeviceId'] == 'A').sum() == 4                    # A whole, then one more signal
    cut, _ = _run(ongoing, ROTATION_SIGNALS, None, 2, start + pd.Timedelta(weeks=1))
    assert set(cut['clearance_intervals']['DeviceId']) == {'A'}                              # the first always fits
