"""Detector urgency tiers: service lost first, then harm to other phases, vehicle detectors and ped buttons in one order."""

import pandas as pd
import pytest

from atspm_report.atspm_detector_health import listed_alerts, signal_wide_rows
from atspm_report.detector_triage import (
    DATA_ONLY, HARM_PROBABILITY, NOT_MEASURED, OTHER_PHASES, SERVICE_LOST, attach_triage, effect_probability, impact_label, rank_detectors,
    silent_ped_phases, usual_detector_counts,
)
from atspm_report.table_generation import (
    MAX_MERGED_HEIGHT, TABLE_AVAILABLE_HEIGHT, TABLE_AVAILABLE_WIDTH, _blocks_that_fit, create_reportlab_table, prepare_atspm_detector_health_table,
)
from reportlab.lib.styles import getSampleStyleSheet

REPORT_DAY = '2026-10-28'            # a Wednesday: the window is Monday to Wednesday
WEEKDAYS = [d for d in pd.date_range('2026-10-08', REPORT_DAY) if d.weekday() < 5]
FAULT = ('A', 5, '2026-10-27 08:00', '2026-10-27 12:00', 'stuck on', 0.8)
DETECTORS = [('A', 5, 2, 'Presence'), ('A', 6, 4, 'Presence')]


def in_fault(ts):
    return pd.Timestamp('2026-10-27 08:00') <= ts < pd.Timestamp('2026-10-27 12:00')


def rank(faults, phase_rows, detectors=None, traffic=100.0, walks=None, ped_inputs=None, cycle=None, kind='Vehicle', **kwargs):
    """phase_rows(ts, phase) -> (max outs, gap outs, wait) of a phase at signal A in one 15-minute bin; walks(ts, phase) -> (walks, presses)."""
    terminations, waits, data, ped, coordination = [], [], [], [], []
    for day in WEEKDAYS:
        for ts in pd.date_range(day, periods=96, freq='15min'):
            data.append((ts, 'A'))
            if cycle is not None:
                coordination.append((ts, 'A', cycle))
            for phase in (2, 4):
                maxouts, gapouts, wait = phase_rows(ts, phase)
                if maxouts:
                    terminations.append((ts, 'A', phase, 'MaxOut', maxouts))
                if gapouts:
                    terminations.append((ts, 'A', phase, 'GapOut', gapouts))
                if wait is not None:
                    waits.append((ts, 'A', phase, wait))
                if walks is not None:
                    walked, pressed = walks(ts, phase)
                    ped.append((ts, 'A', phase, walked, pressed))
    findings = pd.DataFrame(faults, columns=['DeviceId', 'Detector', 'Start', 'End', 'Label', 'Confidence']).assign(DetectorType=kind)
    findings = findings.astype({'Start': 'datetime64[us]', 'End': 'datetime64[us]'})
    table = pd.DataFrame(detectors or DETECTORS, columns=['DeviceId', 'Detector', 'Phase', 'Function'])
    counts = pd.DataFrame([(ts, 'A', detector, traffic) for ts in pd.date_range('2026-10-08', periods=96, freq='15min') for _, detector, _, _ in (detectors or DETECTORS)],
                          columns=['TimeStamp', 'DeviceId', 'Detector', 'Total'])
    return rank_detectors(
        findings, table,
        pd.DataFrame(terminations, columns=['TimeStamp', 'DeviceId', 'Phase', 'PerformanceMeasure', 'Total']),
        pd.DataFrame(waits, columns=['TimeStamp', 'DeviceId', 'Phase', 'AvgPhaseWait']),
        pd.DataFrame(data, columns=['TimeStamp', 'DeviceId']), REPORT_DAY,
        pedestrian=pd.DataFrame(ped, columns=['TimeStamp', 'DeviceId', 'Phase', 'PedServices', 'PedActuation']) if walks is not None else None,
        ped_inputs=ped_inputs, usual_counts=usual_detector_counts(counts),
        coordination=pd.DataFrame(coordination, columns=['TimeStamp', 'DeviceId', 'CycleLength']) if cycle is not None else None, **kwargs)


def steady(ts, phase):
    return 1, 4, 20.0


def test_nothing_changed_is_data_only():
    row = rank([FAULT], steady).iloc[0]
    assert row.Tier == DATA_ONLY and row.ImpactText == 'No effect on other phases over 4 h'


def test_max_outs_on_the_detector_s_own_phase_do_not_rank():
    row = rank([FAULT], lambda ts, phase: (5, 0, 90.0) if in_fault(ts) and phase == 2 else (1, 4, 20.0)).iloc[0]
    assert row.Tier == DATA_ONLY


def test_other_phases_waiting_longer_is_harm_and_names_them():
    row = rank([FAULT], lambda ts, phase: (1, 4, 40.0) if in_fault(ts) and phase == 4 else (1, 4, 20.0)).iloc[0]
    assert row.Tier == OTHER_PHASES and row.HurtPhases == '4'
    assert row.OtherWaitUp == pytest.approx(20) and row.ExtraVehHoursPerDay == pytest.approx(1600 * 20 / 3600 / 3)
    assert row.ImpactText == 'Other phases wait longer over 4 h, 20 s a vehicle on average: Ph4 20 to 40 s'
    assert row.HarmProbability > 0.95
    assert impact_label(row).startswith('<b>Harm to other phases.</b> Other phases wait')


def test_other_phases_maxing_out_more_is_harm():
    row = rank([FAULT], lambda ts, phase: (5, 0, 20.0) if in_fault(ts) and phase == 4 else (1, 4, 20.0)).iloc[0]
    assert row.Tier == OTHER_PHASES and row.OtherMaxOutUp == pytest.approx(80)
    assert row.ImpactText == 'Other phases max out more over 4 h: Ph4 20% to 100%'


def test_little_traffic_on_the_other_phases_counts_for_little():
    row = rank([FAULT], lambda ts, phase: (1, 4, 40.0) if in_fault(ts) and phase == 4 else (1, 4, 20.0), traffic=1.0).iloc[0]
    assert row.Tier == DATA_ONLY and row.OtherWaitUp == pytest.approx(20)


def test_own_phase_almost_gone_is_service_lost():
    row = rank([FAULT], lambda ts, phase: (0, 0, 20.0) if in_fault(ts) and phase == 2 else (1, 4, 20.0)).iloc[0]
    assert row.Tier == SERVICE_LOST and row.Lost == pytest.approx(80 / 3)
    assert row.ImpactText == 'Ph2 got 0 greens against 80 normal over 4 h'


def test_a_phase_running_longer_and_less_often_has_not_lost_service():
    row = rank([FAULT], lambda ts, phase: (1, 0, 20.0) if in_fault(ts) and phase == 2 else (1, 4, 20.0)).iloc[0]
    assert row.OwnRatio == pytest.approx(0.2) and row.Tier != SERVICE_LOST


def test_the_whole_signal_stopping_is_not_measured():
    row = rank([FAULT], lambda ts, phase: (0, 0, None) if in_fault(ts) else (1, 4, 20.0)).iloc[0]
    assert row.Tier == NOT_MEASURED and 'whole signal' in row.ImpactText


def test_no_known_phase_is_not_measured():
    row = rank([('A', 9, '2026-10-27 08:00', '2026-10-27 12:00', 'stuck on', 0.8)], steady).iloc[0]
    assert row.Tier == NOT_MEASURED and row.ImpactText == 'Not measured: phase unknown'


def test_a_phase_with_a_fault_of_its_own_is_left_out_and_all_at_fault_is_signal_wide():
    other = ('A', 6, '2026-10-27 08:00', '2026-10-27 12:00', 'stuck on', 0.8)
    ranked = rank([FAULT, other], lambda ts, phase: (1, 4, 40.0) if in_fault(ts) else (1, 4, 20.0))
    assert ranked['SignalWide'].tolist() == [True, True] and ranked['Tier'].tolist() == [OTHER_PHASES, OTHER_PHASES]
    assert ranked.iloc[0].ImpactText.startswith('Its phases wait longer over 4 h, 20 s a vehicle on average:')
    assert ranked.iloc[0].ExtraVehHoursPerDay == pytest.approx(2 * 1600 * 20 / 3600 / 3)
    assert ranked['Group'].notna().all() and ranked['Group'].nunique() == 1     # one event, told once


def test_a_rise_the_normal_days_show_too_is_not_harm():
    # Phase 4 waits 40 s in these hours on the fault day, and on every fourth normal day as well.
    busy = lambda ts: 8 <= ts.hour < 12 and (ts.day % 4 == 3)
    row = rank([FAULT], lambda ts, phase: (1, 4, 40.0) if busy(ts) and phase == 4 else (1, 4, 20.0)).iloc[0]
    assert in_fault(pd.Timestamp('2026-10-27 09:00')) and busy(pd.Timestamp('2026-10-27 09:00'))
    assert row.OtherWaitUp == pytest.approx(20) and row.HarmProbability < HARM_PROBABILITY and row.Tier == DATA_ONLY


def test_a_few_seconds_count_when_the_normal_days_agree():
    row = rank([FAULT], lambda ts, phase: (1, 4, 24.0) if in_fault(ts) and phase == 4 else (1, 4, 20.0), traffic=400.0).iloc[0]
    assert row.OtherWaitUp == pytest.approx(4) and row.Tier == OTHER_PHASES


def test_a_few_seconds_do_not_count_when_the_normal_days_scatter():
    # The same 4 s, on a phase whose wait in these hours moves 8 s either way from one day to the next.
    scatter = lambda ts: 8.0 * ((ts.day * 7) % 5 - 2) / 2.0
    wait = lambda ts, phase: 24.0 if in_fault(ts) else 20.0 + (scatter(ts) if 8 <= ts.hour < 12 else 0.0)
    row = rank([FAULT], lambda ts, phase: (1, 4, wait(ts, phase)) if phase == 4 else (1, 4, 20.0), traffic=400.0).iloc[0]
    assert row.Tier == DATA_ONLY and row.HarmProbability < HARM_PROBABILITY


def test_a_fault_is_judged_on_the_hours_it_had_to_itself():
    # Detector 5 is at fault all day, detector 6 from 08:00 to 12:00, and only then do the phases wait longer.
    long = ('A', 5, '2026-10-27 00:00', '2026-10-28 00:00', 'stuck on', 0.8)
    short = ('A', 6, '2026-10-27 08:00', '2026-10-27 12:00', 'stuck on', 0.8)
    ranked = rank([long, short], lambda ts, phase: (1, 4, 40.0) if in_fault(ts) else (1, 4, 20.0)).set_index('Detector')
    assert ranked.loc[6, 'Tier'] == OTHER_PHASES and ranked.loc[5, 'Tier'] == DATA_ONLY
    assert ranked.loc[5, 'FaultHours'] == 20 and ranked['Group'].isna().all()


def test_effect_probability_weighs_the_rise_against_the_normal_days():
    steady = [0.0, 0.5, -0.5, 0.2, -0.3, 0.1]
    sure = effect_probability([(6.0, steady, 1.0)], 3.0, 0.3, 10.0)
    assert sure > 0.95
    # The same rise is worth less when the normal days scatter, when one of them rose as much, and when they are few.
    assert effect_probability([(6.0, [-9.0, 7.0, -4.0, 8.0, -6.0, 5.0], 1.0)], 3.0, 0.3, 10.0) < 0.3
    assert effect_probability([(6.0, steady + [6.5], 1.0)], 3.0, 0.3, 10.0) < sure - 0.2
    assert effect_probability([(6.0, [0.0], 3.0)], 3.0, 0.3, 10.0) < sure - 0.2
    # No rise is no effect, a fault less likely to matter needs more, and fault days that agree add up.
    assert effect_probability([(0.0, steady, 1.0)], 3.0, 0.3, 10.0) < 0.02
    assert effect_probability([(6.0, steady, 1.0)], 3.0, 0.03, 10.0) < sure
    two_days = effect_probability([(4.0, [0.0, 2.5, -2.5], 2.0)] * 2, 3.0, 0.3, 10.0)
    assert two_days > effect_probability([(4.0, [0.0, 2.5, -2.5], 2.0)], 3.0, 0.3, 10.0)
    # Something else that moved all the fault days explains a small rise as well as the fault does.
    assert effect_probability([(6.0, steady, 1.0)], 3.0, 0.3, 10.0, other=0.1) < sure
    assert effect_probability([], 3.0, 0.3, 10.0) == 0.0


def test_a_detector_s_own_fault_days_are_not_its_normal():
    faults = [('A', 5, f'2026-10-{day} 08:00', f'2026-10-{day} 12:00', 'stuck on', 0.8) for day in (26, 27, 28)]
    window = lambda ts: ts >= pd.Timestamp('2026-10-26') and 8 <= ts.hour < 12
    row = rank(faults, lambda ts, phase: (1, 4, 40.0) if window(ts) and phase == 4 else (1, 4, 20.0)).iloc[0]
    assert row.OtherWaitUp == pytest.approx(20) and row.FaultHours == 12


def test_low_confidence_and_notes_are_not_faults():
    faults = [('A', 5, '2026-10-27 08:00', '2026-10-27 12:00', 'stuck on', 0.4), ('A', 5, '2026-10-27 08:00', '2026-10-27 12:00', 'lane closure', 1.0)]
    assert rank(faults, steady).empty


def test_each_detector_shows_once_at_its_most_urgent():
    faults = [FAULT, ('A', 5, '2026-10-27 14:00', '2026-10-27 18:00', 'spiky counts', 0.8)]
    ranked = rank(faults, lambda ts, phase: (1, 4, 40.0) if 14 <= ts.hour < 18 and ts.date() == pd.Timestamp('2026-10-27').date() and phase == 4 else (1, 4, 20.0))
    assert ranked['Detector'].tolist() == [5] and ranked['Kind'].tolist() == ['Counts high'] and ranked['Tier'].tolist() == [OTHER_PHASES]


def test_rank_without_a_detector_table_or_inputs_is_empty():
    findings = pd.DataFrame([FAULT], columns=['DeviceId', 'Detector', 'Start', 'End', 'Label', 'Confidence'])
    assert rank_detectors(findings, pd.DataFrame(), None, None, None, REPORT_DAY).empty


INPUTS = pd.DataFrame({'DeviceId': ['A'], 'Input': [3], 'Phase': [2.0]})
PED_FAULT = ('A', 3, '2026-10-26 00:00', '2026-10-27 00:00', 'sudden stop', 0.7)


def test_silent_button_that_lost_its_walks_is_service_lost():
    first_day = pd.Timestamp('2026-10-26')
    walks = lambda ts, phase: (0, 0) if first_day <= ts < first_day + pd.Timedelta(days=1) or phase != 2 else (1, 1)
    row = rank([PED_FAULT], steady, walks=walks, ped_inputs=INPUTS, kind='Ped').iloc[0]
    assert row.Tier == SERVICE_LOST and row.ImpactText == 'Ph2: 0 walks against 96 normal over 24 h'
    assert row.DetectorType == 'Ped' and row.Phase == 2 and row.Kind == 'Ped silent'


def test_stuck_button_that_hurts_nothing_is_data_only():
    stuck = ('A', 3, '2026-10-27 08:00', '2026-10-27 12:00', 'stuck on', 0.9)
    row = rank([stuck], steady, walks=lambda ts, phase: (1, 1), ped_inputs=INPUTS, kind='Ped').iloc[0]
    assert row.Tier == DATA_ONLY and row.Kind == 'Ped stuck on'


def test_button_with_no_known_phase_is_not_measured():
    row = rank([PED_FAULT], steady, walks=lambda ts, phase: (1, 1), kind='Ped').iloc[0]
    assert row.Tier == NOT_MEASURED


def test_phase_served_every_cycle_is_labelled_coordinated_and_does_not_change_the_tier():
    # Six services a bin at a 150 s cycle: served every cycle, never gapping out.
    row = rank([FAULT], lambda ts, phase: (6, 0, 20.0), cycle=150.0).iloc[0]
    assert row.Tier == DATA_ONLY and row.Coordinated
    assert row.ImpactText == 'No effect on other phases over 4 h, phase behaves as coordinated in these hours'
    assert not rank([FAULT], steady, cycle=150.0).iloc[0].Coordinated


def test_silent_ped_phases_finds_a_phase_with_no_press_and_says_what_its_walks_did():
    terminations, data, ped = [], [], []
    for day in pd.date_range('2026-10-08', REPORT_DAY):
        if day.weekday() >= 5:
            continue
        for ts in pd.date_range(day, periods=96, freq='15min'):
            data.append((ts, 'A'))
            terminations.append((ts, 'A', 2, 'GapOut', 4))
            terminations.append((ts, 'A', 4, 'GapOut', 4))
            before = ts < pd.Timestamp('2026-10-26')
            ped.append((ts, 'A', 2, 1 if before else 0, 1 if before else 0))     # lost its walks too
            ped.append((ts, 'A', 4, 5, 1 if before else 0))                      # walks go on without presses
    found = silent_ped_phases(pd.DataFrame(ped, columns=['TimeStamp', 'DeviceId', 'Phase', 'PedServices', 'PedActuation']),
                              pd.DataFrame(terminations, columns=['TimeStamp', 'DeviceId', 'Phase', 'PerformanceMeasure', 'Total']),
                              pd.DataFrame(data, columns=['TimeStamp', 'DeviceId']), REPORT_DAY)
    assert found['Phase'].tolist() == [2, 4]
    assert found['Tier'].tolist() == [SERVICE_LOST, DATA_ONLY]
    assert found['Note'].iloc[0].startswith('0 walks against')


SIGNALS = pd.DataFrame({'DeviceId': ['A', 'B', 'C'], 'Name': ['Alpha', 'Bravo', 'Charlie'], 'Region': ['Region 1', 'Region 1', 'Region 2']})


def _alert(device, detector, status='bad', score=0.1, kind='Vehicle', label='stuck on', issue='fault'):
    return {'DeviceId': device, 'DetectorType': kind, 'Detector': detector, 'Date': pd.Timestamp(REPORT_DAY), 'Issue': issue, 'Status': status,
            'Score': score, 'Label': label, 'Reason': f'det {detector}: Ph2 Advance - Stuck on: x' if kind == 'Vehicle' else 'Presses stopped: x',
            'Periods': [], 'Evidence': []}


def _ranked(*rows):
    columns = ['DeviceId', 'DetectorType', 'Detector', 'Phase', 'Kind', 'KindOrder', 'FaultHours', 'Tier', 'SignalWide', 'ImpactText', 'HurtPhases',
               'ExtraVehHoursPerDay', 'MaxOutVehPerDay', 'OtherWaitUp', 'OtherMaxOutUp', 'OwnRatio', 'OtherRatio', 'Lost', 'Coordinated']
    return pd.DataFrame([row + (0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, False) for row in rows], columns=columns)


def _lost(device, detector, kind='Vehicle'):
    return (device, kind, detector, 2, 'Silent', 2, 5.0, SERVICE_LOST, False, 'Ph2 got 1 green against 14 normal over 5 h', '')


def _harm(device, detector, wide=False):
    return (device, 'Vehicle', detector, 2, 'Stuck on', 1, 5.0, OTHER_PHASES, wide, 'Other phases wait 27 s longer over 5 h: Ph4 20 to 73 s; about 18 extra vehicle-hours a day', '4')


def _below(device, detector, kind='Counts high', order=4, hours=5.0):
    return (device, 'Vehicle', detector, 2, kind, order, hours, DATA_ONLY, False, 'No effect on other phases over 5 h', '')


def test_table_lists_service_lost_then_harm_then_the_rest():
    # Alpha's detector has the lowest health score, Charlie's is the one hurting traffic.
    alerts = pd.DataFrame([_alert('A', 1, score=0.01), _alert('B', 3, score=0.3), _alert('C', 2, score=0.5)])
    ranked = attach_triage(alerts, _ranked(_lost('B', 3), _harm('C', 2), _below('A', 1)))
    rows, total = prepare_atspm_detector_health_table(ranked, SIGNALS, ('bad',))
    assert rows['Signal'].tolist() == ['Bravo', 'Charlie', 'Alpha'] and total == 3
    assert rows.columns.tolist() == ['Signal', 'Det', 'Impact', 'What Is Wrong']
    assert rows['Impact'].iloc[0] == '<b>Service lost.</b> Ph2 got 1 green against 14 normal over 5 h'
    assert 'Ph4 20 to 73 s' in rows['Impact'].iloc[1]
    assert rows['Impact'].iloc[2] == '<b>Data only.</b> No effect on other phases over 5 h'


def test_ped_buttons_rank_in_one_order_with_the_detectors():
    alerts = pd.DataFrame([_alert('A', 1), _alert('B', 3, kind='Ped', label='sudden stop')])
    ranked = attach_triage(alerts, _ranked(_lost('B', 3, kind='Ped'), _below('A', 1)))
    rows, total = prepare_atspm_detector_health_table(ranked, SIGNALS, ('bad',), detector_type=None,
                                                      hourly_counts={('A', 1): [1.0, 2.0]}, ped_hourly_counts={('B', 3): [3.0, 4.0]})
    assert rows['Signal'].tolist() == ['Bravo', 'Alpha'] and total == 2
    assert rows['Det'].iloc[0] == 'Button 3, Ph2'
    assert rows['Sparkline_Data'].tolist() == [[3.0, 4.0], [1.0, 2.0]]    # the same number means a different thing on each side
    assert ranked.sort_values('TriageRank')['DetectorType'].tolist() == ['Ped', 'Vehicle']


def test_remaining_detectors_go_bad_first_then_by_kind_then_longest():
    alerts = pd.DataFrame([_alert('A', 1, status='suspect'), _alert('A', 2), _alert('A', 3), _alert('A', 4)])
    ranked = attach_triage(alerts, _ranked(_below('A', 1, 'Stuck on', 1), _below('A', 2), _below('A', 3, 'Stuck on', 1, 2.0), _below('A', 4, 'Stuck on', 1, 9.0)))
    assert ranked.sort_values('TriageRank')['Detector'].tolist() == [4, 3, 2, 1]


def test_detector_with_no_ranking_is_labelled_and_comes_last():
    ranked = attach_triage(pd.DataFrame([_alert('A', 1), _alert('A', 2)]), _ranked(_below('A', 2)))
    rows = ranked.sort_values('TriageRank')
    assert rows['Detector'].tolist() == [2, 1] and rows['ImpactLabel'].tolist() == ['<b>Data only.</b> No effect on other phases over 5 h', 'Not measured']


def test_information_rows_stay_out_of_the_ranking():
    alerts = pd.DataFrame([_alert('A', 1), _alert('A', 1, status='lane_closure', label='lane closure', issue='lane_closure')])
    ranked = attach_triage(alerts, _ranked(_harm('A', 1)))
    assert ranked['TriageRank'].notna().tolist() == [True, False]
    assert ranked['Tier'].isna().tolist() == [False, True]


def test_without_a_ranking_alerts_and_tables_are_as_before():
    alerts = pd.DataFrame([_alert('A', 1)])
    assert attach_triage(alerts, None) is alerts and attach_triage(alerts, _ranked()) is alerts
    rows, _ = prepare_atspm_detector_health_table(alerts, SIGNALS, ('bad',))
    assert rows.columns.tolist() == ['Signal', 'Det', 'What Is Wrong']


def test_a_signal_s_signal_wide_rows_read_as_one():
    alerts = pd.DataFrame([_alert('A', 2), _alert('A', 1), _alert('A', 7), _alert('B', 1)])
    ranked = attach_triage(alerts, _ranked(_harm('A', 2, wide=True), _harm('A', 1, wide=True), _below('A', 7), _harm('B', 1)))
    rows, total = prepare_atspm_detector_health_table(ranked, SIGNALS, ('bad',))
    assert total == 3
    assert [text.split(',')[0] for text in rows[rows['Signal'] == 'Bravo']['Det']] == ['Det 1']
    alpha = rows[rows['Signal'] == 'Alpha']
    assert alpha['Det'].iloc[0] == 'Det 1, 2' and alpha['What Is Wrong'].iloc[0].endswith('Det 2 shown; 1 more at fault with it.')
    assert signal_wide_rows(alerts).map(len).sum() == 0     # without the tiers nothing folds


def test_faults_split_into_affecting_operations_and_data_only():
    alerts = pd.DataFrame([_alert('A', 1, score=0.01), _alert('B', 3, score=0.3), _alert('C', 2, score=0.5)])
    ranked = attach_triage(alerts, _ranked(_lost('B', 3), _harm('C', 2), _below('A', 1)))
    harm, harm_total = prepare_atspm_detector_health_table(ranked, SIGNALS, ('bad',), impact=True)
    data, data_total = prepare_atspm_detector_health_table(ranked, SIGNALS, ('bad',), impact=False)
    assert harm['Signal'].tolist() == ['Bravo', 'Charlie'] and harm_total == 2 and 'Impact' in harm.columns
    assert data['Signal'].tolist() == ['Alpha'] and data_total == 1
    assert data.columns.tolist() == ['Signal', 'Det', 'What Is Wrong']     # nothing to say of its impact
    # Without the tiers every fault is data only, and nothing affects operations.
    assert prepare_atspm_detector_health_table(alerts, SIGNALS, ('bad',), impact=True)[1] == 0
    assert prepare_atspm_detector_health_table(alerts, SIGNALS, ('bad',), impact=False)[1] == 3


def test_detectors_judged_as_one_group_read_as_one_row():
    alerts = pd.DataFrame([_alert('A', 2), _alert('A', 1), _alert('A', 7), _alert('A', 4, kind='Ped', label='stuck on'),
                           _alert('A', 6, kind='Ped', label='stuck on')])
    ranked = _ranked(_below('A', 2, 'Stuck on', 1), _below('A', 1, 'Stuck on', 1), _below('A', 7),
                     ('A', 'Ped', 4, 2, 'Ped stuck on', 1, 5.0, OTHER_PHASES, False, 'Other phases wait longer over 5 h', '4'),
                     ('A', 'Ped', 6, 2, 'Ped stuck on', 1, 5.0, OTHER_PHASES, False, 'Other phases wait longer over 5 h', '4'))
    ranked['Group'] = [1.0, 1.0, float('nan'), 2.0, 2.0]
    told = attach_triage(alerts, ranked)
    harm, harm_total = prepare_atspm_detector_health_table(told, SIGNALS, ('bad',), detector_type=None, impact=True)
    data, data_total = prepare_atspm_detector_health_table(told, SIGNALS, ('bad',), detector_type=None, impact=False)
    assert harm['Det'].tolist() == ['Button 4, 6'] and harm_total == 1
    assert harm['What Is Wrong'].iloc[0].endswith('Button 4 shown; 1 more at fault with it.')
    assert data['Det'].tolist()[0] == 'Det 1, 2' and data_total == 2


def test_history_records_what_the_two_fault_tables_listed():
    # A limit of one row a table: the detector hurting traffic, and the first of the data-only ones, not the lowest health score.
    alerts = pd.DataFrame([_alert('A', 1, score=0.01), _alert('B', 1, score=0.2), _alert('C', 2, score=0.5)])
    ranked = attach_triage(alerts, _ranked(_below('A', 1), _below('B', 1, 'Stuck on', 1), _harm('C', 2)))
    regions_one = SIGNALS.assign(Region='Region 1')
    assert listed_alerts(ranked, regions_one, 1)['DeviceId'].tolist() == ['B', 'C']


def test_history_follows_each_table_s_own_limit_and_ranks_ped_with_the_detectors():
    alerts = pd.DataFrame([_alert('A', 1), _alert('B', 1), _alert('B', 3, kind='Ped', label='sudden stop'),
                           _alert('A', 8, status='lane_closure', label='lane closure', issue='lane_closure'),
                           _alert('B', 8, status='lane_closure', label='lane closure', issue='lane_closure')])
    ranked = attach_triage(alerts, _ranked(_lost('B', 3, kind='Ped'), _harm('A', 1), _below('B', 1)))
    regions_one = SIGNALS.assign(Region='Region 1')
    listed = listed_alerts(ranked, regions_one, 10, limits={'detector_faults': 1, 'lane_closure': 1})
    assert sorted(zip(listed['DeviceId'], listed['DetectorType'], listed['Detector'])) == [('A', 'Vehicle', 8), ('B', 'Ped', 3), ('B', 'Vehicle', 1)]


def test_merged_signal_cell_never_takes_a_whole_page():
    blocks = _blocks_that_fit(1, 20, [0.0] + [50.0] * 20, MAX_MERGED_HEIGHT)
    assert len(blocks) > 1 and sum(length for _, length in blocks) == 20
    assert max(length for _, length in blocks) * 50.0 <= MAX_MERGED_HEIGHT


def test_ranked_table_tells_how_much_room_its_first_signal_needs():
    styles = getSampleStyleSheet()
    alerts = pd.DataFrame([_alert('A', n) for n in range(1, 21)] + [_alert('B', 1)])
    ranked = attach_triage(alerts, _ranked(*[_harm('A', n) for n in range(1, 21)], _harm('B', 1)))
    rows, total = prepare_atspm_detector_health_table(ranked, SIGNALS, ('bad',), max_rows=50)
    table = create_reportlab_table(rows, 'Detector Health', styles, total_count=total, max_rows=50, include_trend=False)[-1]
    assert 0 < table.start_height <= MAX_MERGED_HEIGHT + table._rowHeights[0]
    width, height = table.wrap(TABLE_AVAILABLE_WIDTH, TABLE_AVAILABLE_HEIGHT)
    assert width <= TABLE_AVAILABLE_WIDTH + 0.5 and table.split(TABLE_AVAILABLE_WIDTH, 300.0)    # it can start in a part of a page
