"""Signal Focus: the few signals worst off across more than one area, told once, leaving every other table and chart."""

import pandas as pd
import pytest

from atspm_report import ReportGenerator
from atspm_report.atspm_detector_health import listed_alerts
from atspm_report.detector_triage import DATA_ONLY, OTHER_PHASES, SERVICE_LOST
from atspm_report.signal_focus import (
    HARM_ROW, MISSING, ROW_ORDER, SECTION_EXPLANATION, build_signal_focus, clearance_severity, collect_problems, hidden_signals, rank_signals,
)
from atspm_report.data_processing import detect_feed_stop, mark_open_periods, process_missing_data, trim_findings_to_stop
from atspm_report.generator import _usual_by_hour
from atspm_report.report_generation import generate_pdf_report
from atspm_report.table_generation import create_reportlab_table, create_sparkline, prepare_atspm_detector_health_table
from reportlab.lib.styles import getSampleStyleSheet
from atspm_report.visualization import create_atspm_detector_health_plots, create_device_plots

DAY = pd.Timestamp('2026-11-03')
SIGNALS = pd.DataFrame({'DeviceId': ['A', 'B', 'C', 'D'], 'Name': ['Alpha', 'Bravo', 'Charlie', 'Delta'],
                        'Region': ['Region 1', 'Region 1', 'Region 2', 'Region 2']})
INFO = pd.DataFrame({'DeviceId': ['A', 'A', 'B', 'C'], 'Detector': [5, 6, 1, 7], 'Phase': [2.0, 4.0, 2.0, 2.0]})
HARM_TEXT = 'Other phases wait 27 s longer over 5 h: Ph4 20 to 73 s; about 18 extra vehicle-hours a day'


def health(device, detector, kind='Vehicle', status='bad', label='stuck on', tier=DATA_ONLY, rank=None, **extra):
    return {'DeviceId': device, 'DetectorType': kind, 'Detector': detector, 'Date': DAY, 'Issue': 'fault', 'Status': status, 'Score': 0.1,
            'Label': label, 'Reason': f'det {detector}: x', 'Periods': [], 'Evidence': [], 'Tier': tier, 'TriageRank': rank,
            'Kind': extra.pop('fault_kind', 'Stuck on' if kind == 'Vehicle' else 'Ped stuck on'),
            'ImpactText': extra.pop('impact', HARM_TEXT if tier == OTHER_PHASES else 'Ph2 got 1 green against 14 normal over 5 h' if tier == SERVICE_LOST else 'No effect on other phases over 5 h'),
            'HurtPhases': extra.pop('hurt', '4' if tier == OTHER_PHASES else ''), **extra}


def maxout(device, phase, pct=0.64):
    return {'Date': DAY, 'DeviceId': device, 'Phase': phase, 'Percent MaxOut': pct, 'Alert': 1}


def skips(device, phase, count):
    return {'DeviceId': device, 'Phase': phase, 'Date': DAY, 'AggregatedSkips': count}


def clearance(device, value=4, event_class='Yellow', short=30, long=0, samples=300):
    return {'DeviceId': device, 'EventClass': event_class, 'EventValue': value, 'Date': DAY, 'MedianDuration': 3.5, 'SampleCount': samples,
            'MaxAbsDelta': 1.0, 'ShortCount': short, 'LongCount': long}


def conflict(device, m1=2, m2=5, seconds=1.5, at=None):
    return {'DeviceId': device, 'Movement1Type': 'Phase', 'Movement1Number': m1, 'Movement2Type': 'Phase', 'Movement2Number': m2,
            'ConflictStart': at if at is not None else DAY + pd.Timedelta(hours=14, minutes=3), 'DurationSeconds': seconds, 'Date': DAY}


def preempt(device, number=4, direction='False Calls'):
    return {'DeviceId': device, 'Preempt': number, 'Class': 'Emergency Vehicle', 'Direction': direction, 'Score': 1.0, 'Date': DAY,
            'Note': '8 of 10 calls in the last 7 days had the same duration, lasting about 5.2 s. This does not represent normal EVP calls.'}


def missing(device, share=0.42):
    return {'DeviceId': device, 'Date': DAY, 'MissingData': share, 'Alert': 1}


def frames(**kinds):
    return {key: pd.DataFrame(value) for key, value in kinds.items()}


def problems_of(held=None, alarms=None, **kinds):
    return collect_problems(frames(**kinds), frames(**(held or {})), alarms, INFO)


def scores(problems):
    return rank_signals(problems, min_score=0.0, related_only=False).set_index('DeviceId')['Score'].to_dict()


def lines_of(problem) -> list:
    return [line['text'] for line in problem['Harm']] + [line['text'] for line in problem['Lines']]


def table_of(row) -> dict:
    return {told['type']: [line['text'] for line in told['lines']] for told in row['Rows']}


def test_only_signals_with_more_than_one_area_qualify():
    problems, _ = problems_of(atspm_detector_health=[health('A', 5), health('B', 1)], maxout=[maxout('A', 2)])
    assert list(scores(problems)) == ['A']


def test_a_fault_measured_to_affect_operations_is_two_areas():
    # The detection and how the signal ran: a fault with no measured effect (B) is one area and does not count at all.
    problems, _ = problems_of(atspm_detector_health=[
        health('A', 5, tier=OTHER_PHASES, hurt='1,3,4,8'), health('B', 1), health('C', 3, kind='Ped', tier=SERVICE_LOST, TriagePhase=2.0),
        health('D', 3, kind='Ped', tier=OTHER_PHASES, TriagePhase=2.0, hurt='4,6,8'), health('D', 9, tier=OTHER_PHASES)])
    assert scores(problems) == {'D': (3 + 2 + 2) + (3 + 2), 'A': 3 + 2 + 2, 'C': 4 + 2}
    # Harm to three or more phases brings a signal here with nothing else; one phase's lost service does not.
    listed = rank_signals(problems)
    assert listed['DeviceId'].tolist() == ['D', 'A'] and listed['Categories'].tolist() == [3, 2]


def test_faults_judged_as_one_event_tell_its_effect_once():
    def effects(**group):
        rows = [health('A', 5, tier=OTHER_PHASES, **group), health('A', 6, tier=OTHER_PHASES, label='silent', fault_kind='Silent', **group),
                health('A', 3, kind='Ped', tier=OTHER_PHASES, TriagePhase=2.0, **group)]
        problems, _ = problems_of(atspm_detector_health=rows)
        return [line for _, problem in problems.iterrows() for line in lines_of(problem) if 'wait 27 s longer' in line]
    assert len(effects()) == 3                      # told with each kind of fault, and with the button
    assert effects(Group=1.0) == ['With det 5: Other phases wait 27 s longer over 5 h: Ph4 20 to 73 s']


def test_three_conflict_checks_are_one_area():
    problems, _ = problems_of(atspm_detector_health=[health('A', 1, kind='Ped', label='sudden stop')],
                              general_phase_conflicts=[conflict('B')], overlap_conflicts=[conflict('B', 1, 3)])
    assert scores(problems) == {}
    assert set(problems['Category']) == {'Ped', 'Conflicts'}


def test_score_is_severity_plus_a_point_for_each_area():
    problems, _ = problems_of(general_phase_conflicts=[conflict('D')], maxout=[maxout('D', 2), maxout('B', 2)],
                              atspm_detector_health=[health('B', 1, tier=DATA_ONLY)])
    assert scores(problems) == {'D': 5 + 1 + 2 + 1, 'B': 1 + 1 + 2 + 1 + 2}      # B's stuck detector is on its Ph2


def test_missing_data_never_counts_and_comes_last():
    problems, _ = problems_of(general_phase_conflicts=[conflict('A')], missing_data=[missing('A')])
    assert scores(problems) == {}                     # one area, and missing data is none
    both, _ = problems_of(general_phase_conflicts=[conflict('A')], maxout=[maxout('A', 2)], missing_data=[missing('A')])
    without, _ = problems_of(general_phase_conflicts=[conflict('A')], maxout=[maxout('A', 2)])
    assert scores(both) == scores(without)
    row = build_signal_focus(both, SIGNALS, related_only=False)['All Regions'].iloc[0]
    assert [told['type'] for told in row['Rows']][-1] == MISSING and ROW_ORDER[-1] == MISSING


def test_older_new_problems_count_less():
    period = lambda day: [{'start': f'{day} 08:00:00', 'end': f'{day} 12:00:00', 'what': 'stuck on'}]
    today, _ = problems_of(general_phase_conflicts=[conflict('A')], atspm_detector_health=[{**health('A', 5), 'Periods': period('2026-11-03')}])
    older, _ = problems_of(general_phase_conflicts=[conflict('A')], atspm_detector_health=[{**health('A', 5), 'Periods': period('2026-11-02')}])
    oldest, _ = problems_of(general_phase_conflicts=[conflict('A')], atspm_detector_health=[{**health('A', 5), 'Periods': period('2026-10-30')}])
    assert scores(today)['A'] == 6 + 2 and scores(older)['A'] == 6 + 1 and scores(oldest)['A'] == 6.5


def test_ranking_prefers_what_happened_on_the_latest_day():
    yesterday = [{'start': '2026-11-03 08:00:00', 'end': '2026-11-03 12:00:00', 'what': 'stuck on'}]
    before = [{'start': '2026-11-01 08:00:00', 'end': '2026-11-01 12:00:00', 'what': 'stuck on'}]
    problems, _ = problems_of(atspm_detector_health=[{**health('A', 5, tier=OTHER_PHASES), 'Periods': before}, {**health('B', 1), 'Periods': yesterday}],
                              maxout=[{**maxout('A', 2), 'Date': pd.Timestamp('2026-11-01')}, maxout('B', 2)])
    assert list(scores(problems)) == ['B', 'A']


def test_fewer_is_good_a_weak_pair_does_not_qualify():
    problems, _ = problems_of(atspm_detector_health=[health('B', 1)], phase_skips=[skips('B', 2, 2)])
    assert scores(problems) == {'B': 4.0} and rank_signals(problems).empty


def test_interrelated_problems_score_more():
    together, _ = problems_of(atspm_detector_health=[health('A', 5)], maxout=[maxout('A', 2)])         # stuck det 5 is on Ph2
    apart, _ = problems_of(atspm_detector_health=[health('A', 6)], maxout=[maxout('A', 2)])            # det 6 is on Ph4
    assert scores(together)['A'] == scores(apart)['A'] + 2
    silent, _ = problems_of(atspm_detector_health=[health('A', 5, label='silent', fault_kind='Silent')], phase_skips=[skips('A', 2, 2)])
    assert silent.set_index('Category').loc['Phase skips', 'Links'] == 2
    hurt, _ = problems_of(atspm_detector_health=[health('A', 5, tier=OTHER_PHASES)], maxout=[maxout('A', 4)])
    assert hurt.set_index('Category').loc['Max outs', 'Links'] == 2
    ped, _ = collect_problems(frames(atspm_detector_health=[health('A', 3, kind='Ped', TriagePhase=2.0)], maxout=[maxout('A', 2)]), None, None, INFO)
    assert ped.set_index('Category').loc['Max outs', 'Links'] == 2
    alarm = pd.DataFrame([{'DeviceId': 'A', 'AlarmType': 'MMU Flash', 'DayCount': 1, 'TotalCount': 1, 'Date': DAY}])
    flash, _ = problems_of(alarms=alarm, general_phase_conflicts=[conflict('A')])
    assert flash.set_index('Category').loc['Alarms', 'Links'] == 2


def focus_of(problems) -> list:
    return rank_signals(problems)['DeviceId'].tolist()


def test_a_detector_fault_and_an_unrelated_preempt_are_not_enough():
    # 07048 on 16 Nov 2026: det 5 (Ph2) with no daily pattern, and false calls on emergency vehicle preempt 4. Score 7, no link.
    fault = health('A', 5, label='no daily pattern', fault_kind='No daily pattern')
    problems, _ = problems_of(atspm_detector_health=[fault], preempts=[preempt('A')])
    assert scores(problems) == {'A': 7.0} and focus_of(problems) == []
    assert rank_signals(problems, related_only=False)['DeviceId'].tolist() == ['A']      # as before
    # Three different kinds of problem are enough, related or not.
    third, _ = problems_of(atspm_detector_health=[fault], preempts=[preempt('A')], clearance_intervals=[clearance('A', short=1, samples=900)])
    assert focus_of(third) == ['A']


def test_problems_on_a_shared_phase_belong_together():
    # Det 5 is on Ph2, det 6 on Ph4 (INFO).
    together, _ = problems_of(atspm_detector_health=[health('A', 5)], maxout=[maxout('A', 2)])
    assert scores(together) == {'A': 7.0} and focus_of(together) == ['A']
    apart, _ = problems_of(atspm_detector_health=[health('A', 6)], general_phase_conflicts=[conflict('A')])
    assert scores(apart) == {'A': 8.0} and focus_of(apart) == []
    # A phase the fault was measured to hurt counts as its phase: det 5 hurt Ph4, which skips.
    hurt, _ = problems_of(atspm_detector_health=[health('A', 5, tier=OTHER_PHASES, hurt='4')], phase_skips=[skips('A', 4, 30)])
    assert focus_of(hurt) == ['A']
    # A ped button and skips of its phase.
    ped, _ = problems_of(atspm_detector_health=[health('A', 3, kind='Ped', label='erratic', fault_kind='Ped erratic', TriagePhase=4.0)],
                         phase_skips=[skips('A', 4, 30)])
    assert focus_of(ped) == ['A']
    # A flash alarm and a conflict; another alarm and a conflict do not.
    flash = pd.DataFrame([{'DeviceId': 'A', 'AlarmType': 'MMU Flash', 'DayCount': 1, 'TotalCount': 1, 'Date': DAY}])
    linked, _ = problems_of(alarms=flash, general_phase_conflicts=[conflict('A')])
    unlinked, _ = problems_of(alarms=flash.assign(AlarmType='Power Failure'), general_phase_conflicts=[conflict('A')])
    assert focus_of(linked) == ['A'] and scores(unlinked) == {'A': 9.0} and focus_of(unlinked) == []


def test_the_related_problems_have_to_reach_the_score_themselves():
    # Harm to one phase (5) with an unrelated preempt fault (5): 10 in all, but nothing that belongs together reaches 7.
    problems, _ = problems_of(atspm_detector_health=[health('A', 5, tier=OTHER_PHASES)], preempts=[preempt('A', direction='Stuck On')])
    assert scores(problems) == {'A': 10.0} and focus_of(problems) == []
    # Harm to three or more phases still qualifies by itself, and an unrelated problem beside it does not take that away.
    wide, _ = problems_of(atspm_detector_health=[health('A', 5, tier=OTHER_PHASES, hurt='1,3,4,8')], preempts=[preempt('A')])
    assert focus_of(wide) == ['A'] and scores(wide) == {'A': 12.0}


def test_relatedness_reads_only_what_the_problems_share():
    from atspm_report.signal_focus import related
    fault = {'Category': 'Detectors', 'OnPhases': [2], 'Links': 0.0}
    assert related(fault, {'Category': 'Max outs', 'OnPhases': [2], 'Links': 0.0})
    assert not related(fault, {'Category': 'Max outs', 'OnPhases': [6], 'Links': 0.0})
    assert not related(fault, {'Category': 'Preempt', 'OnPhases': [2], 'Links': 0.0})
    assert not related({'Category': 'Max outs', 'OnPhases': [2], 'Links': 0.0}, {'Category': 'Phase skips', 'OnPhases': [2], 'Links': 0.0})
    assert not related({'Category': 'Clearance', 'OnPhases': [], 'Links': 0.0}, {'Category': 'Conflicts', 'OnPhases': [], 'Links': 0.0})


def test_detector_severity_follows_the_urgency_tiers():
    def points(tier):
        problems, _ = problems_of(atspm_detector_health=[health('A', 5, tier=tier)])
        return problems['Points'].iloc[0]
    assert (points(SERVICE_LOST), points(OTHER_PHASES), points(DATA_ONLY)) == (4, 3, 1)
    wider, _ = problems_of(atspm_detector_health=[health('A', 5, tier=OTHER_PHASES, hurt='1,3,4,8')])
    assert wider['Points'].iloc[0] == 5


def test_clearance_is_weighed_by_how_common_it_is():
    assert clearance_severity(0.01, 0) == pytest.approx(0.5) and clearance_severity(0.25, 0) == pytest.approx(2.5)
    assert clearance_severity(0, 0.25) == pytest.approx(1.25)     # long counts half
    rare, _ = problems_of(clearance_intervals=[clearance('A', short=1, samples=900)])
    common, _ = problems_of(clearance_intervals=[clearance('A', short=90, samples=900)])
    assert rare['Points'].iloc[0] < 0.2 < 1.5 < common['Points'].iloc[0] < 1.6
    assert 'short in 1 of 900 samples (under 1%)' in rare['Lines'].iloc[0][0]['text'] and '(10%)' in common['Lines'].iloc[0][0]['text']


def test_skips_under_once_a_day_score_low():
    rare, _ = problems_of(phase_skips=[skips('A', 2, 3)])
    common, _ = problems_of(phase_skips=[skips('A', 2, 30)])
    assert rare['Points'].iloc[0] == 1 and common['Points'].iloc[0] == 3


def test_ongoing_does_not_count_but_is_told_once_selected():
    held_only, _ = problems_of(held=dict(maxout=[maxout('A', 2)], phase_skips=[skips('A', 2, 30)]))
    assert scores(held_only) == {}
    since = pd.Timestamp('2026-10-12')
    problems, _ = problems_of(held=dict(maxout=[{**maxout('A', 2), 'OngoingSince': since}]),
                              general_phase_conflicts=[conflict('A')], phase_skips=[skips('A', 4, 30)])
    assert scores(problems) == {'A': 5 + 1 + 3 + 1}      # the ongoing max outs add nothing
    row = build_signal_focus(problems, SIGNALS, related_only=False)['All Regions'].iloc[0]
    assert table_of(row)['Max outs'] == ['Ongoing since 10/12: Ph2 reached 64% on 11/3'] and row['HasOngoing']


def test_a_held_fault_is_listed_but_scores_no_link():
    problems, _ = problems_of(held=dict(atspm_detector_health=[health('A', 5)]), maxout=[maxout('A', 2)], general_phase_conflicts=[conflict('A')])
    row = problems.set_index('Category').loc['Max outs']
    assert row['Links'] == 0 and row['Lines'][0]['text'] == 'Ph2 reached 64% on 11/3'


def test_table_lists_facts_one_a_line_with_harm_first_and_no_causes():
    problems, information = problems_of(
        atspm_detector_health=[health('A', 5, tier=OTHER_PHASES, rank=0), health('A', 6, tier=DATA_ONLY, rank=1, fault_kind='Silent', label='silent'),
                               health('A', 9, status='lane_closure', label='lane closure', Tier=None)],
        maxout=[maxout('A', 2)], phase_skips=[skips('A', 4, 30)])
    row = build_signal_focus(problems, SIGNALS, information)['All Regions'].iloc[0]
    told = table_of(row)
    assert row['Signal'] == 'Alpha'
    assert [one['type'] for one in row['Rows']] == [HARM_ROW, 'Max outs', 'Phase skips', 'Other detector problems', 'For information']
    assert told[HARM_ROW] == ['Det 5 stuck on', 'Other phases wait 27 s longer over 5 h: Ph4 20 to 73 s']     # no estimate of vehicles
    assert told['Max outs'] == ['Ph2 reached 64% on 11/3']
    assert told['Phase skips'] == ['Ph4 skipped 30 times in 14 days']
    assert told['Other detector problems'] == ['Det 6 silent']
    assert told['For information'] == ['Det 9 lane closed']
    words = ' '.join(line for lines in told.values() for line in lines).lower()
    assert not any(word in words for word in ('start with', 'likely', 'possibly', 'because', 'look at', 'about 18'))


def test_harm_lines_name_every_phase_and_ask_its_chart_to_show_them():
    facts = '[{"text": "Max outs, normal to during the fault (9 h): Ph8 20% to 50%, Ph4 16% to 47%, Ph2 5% to 47%, Ph6 5% to 30%", "chart": "max out", "phases": [8, 4, 2, 6]}]'
    problems, _ = problems_of(atspm_detector_health=[health('A', 5, tier=OTHER_PHASES, ImpactLines=facts)], maxout=[maxout('A', 3)], general_phase_conflicts=[conflict('A')])
    row = build_signal_focus(problems, SIGNALS)['All Regions'].iloc[0]
    # The chart gives the numbers: the line only names the phases.
    assert table_of(row)[HARM_ROW][1] == 'Max-outs rose on phases 2, 4, 6 & 8'
    assert 'Ph6 5% to 30%' in row['Rows'][0]['lines'][1]['detail']     # told instead when the chart is left out
    assert row['Charts']['max out'] == {8, 4, 2, 6, 3}


def test_detectors_are_listed_by_number():
    info = pd.DataFrame({'DeviceId': ['A'] * 5, 'Detector': [1, 2, 3, 4, 5], 'Phase': [2.0, 4.0, 6.0, 8.0, 2.0]})
    problems, _ = collect_problems(frames(atspm_detector_health=[health('A', n, tier=OTHER_PHASES, rank=n, fault_kind='Silent') for n in range(1, 6)]), None, None, info)
    assert problems.set_index('Category').loc['Detectors', 'Harm'][0]['text'] == 'Det 1, 2, 3, 4, 5 silent'


def test_one_period_keeps_its_exact_times():
    period = [{'start': '2026-11-03 08:00:00', 'end': '2026-11-03 12:30:00', 'what': 'stuck on'}]
    problems, _ = problems_of(atspm_detector_health=[{**health('A', 5, tier=OTHER_PHASES), 'Periods': period}])
    assert problems['Harm'].iloc[0][0]['text'] == 'Det 5 stuck on (4 h 30 min, Tue 11/3 08:00-12:30)'
    still = [{**period[0], 'open': True}]
    problems, _ = problems_of(atspm_detector_health=[{**health('A', 5, tier=OTHER_PHASES), 'Periods': still}])
    assert problems['Harm'].iloc[0][0]['text'] == 'Det 5 stuck on (4 h 30 min, Tue 11/3 08:00 until the data stopped)'


def test_a_conflict_or_two_keep_their_exact_times_more_are_counted():
    few, _ = problems_of(general_phase_conflicts=[conflict('A')])
    assert few['Lines'].iloc[0][0]['text'] == 'Ph 2 / Ph 5 shown together at 14:03 on 11/3, longest 1.5 s'
    many, _ = problems_of(general_phase_conflicts=[conflict('A') for _ in range(3)])
    assert many['Lines'].iloc[0][0]['text'] == 'Ph 2 / Ph 5 shown together 3 times, longest 1.5 s'


def test_each_report_takes_at_most_its_own_top_three():
    rows = [conflict(device) for device in 'ABCD']
    problems, _ = problems_of(general_phase_conflicts=rows, phase_skips=[skips(device, 2, 30) for device in 'ABCD'])
    focus = build_signal_focus(problems, SIGNALS, related_only=False)
    assert sorted(focus) == ['All Regions', 'Region 1', 'Region 2']
    assert len(focus['All Regions']) == 3 and focus['Region 2']['DeviceId'].tolist() == ['C', 'D']
    one = build_signal_focus(problems, SIGNALS, top_signals=1, related_only=False)
    assert hidden_signals(one) == {'All Regions': {'A'}, 'Region 1': {'A'}, 'Region 2': {'C'}}
    assert build_signal_focus(problems, SIGNALS, top_signals=0) == {}


def test_listed_history_counts_a_focus_signal_as_shown_in_every_report():
    alerts = pd.DataFrame([health(device, n) for device in 'AB' for n in (1, 2)]).drop(columns=['TriageRank', 'Tier'])
    listed = listed_alerts(alerts, SIGNALS, 1, hidden={'All Regions': {'A'}, 'Region 1': {'A'}, 'Region 2': set()})
    assert sorted(listed['DeviceId'] + listed['Detector'].astype(str)) == ['A1', 'A2', 'B1', 'B2']


def test_charts_leave_out_a_report_s_focus_signals():
    daily = pd.DataFrame([{**maxout(device, 2), 'CUSUM_Percent MaxOut': 1.0} for device in 'AB'])
    hourly = pd.DataFrame([{'TimeStamp': DAY + pd.Timedelta(hours=h), 'DeviceId': device, 'Phase': 2, 'Percent MaxOut': 0.5} for device in 'AB' for h in range(3)])
    figures = create_device_plots(daily, SIGNALS, 3, hourly, exclude={'Region 1': {'A'}})
    by_report = pd.Series([region for _, region in figures]).value_counts()
    assert by_report['Region 1'] == 1 and by_report['All Regions'] == 2

    alerts = pd.DataFrame([{**health('A', 5), 'Periods': [{'start': '2026-11-03 01:00:00', 'end': '2026-11-03 03:00:00', 'what': 'stuck on'}]}])
    counts = pd.DataFrame({'TimeStamp': pd.date_range(DAY, periods=8, freq='15min'), 'DeviceId': 'A', 'Detector': 5, 'Total': 3})
    window = (DAY, DAY + pd.Timedelta(days=1))
    drawn = create_atspm_detector_health_plots(alerts, SIGNALS, 3, counts, INFO, window=window)
    assert sorted(region for _, region in drawn) == ['All Regions', 'Region 1']
    kept = create_atspm_detector_health_plots(alerts, SIGNALS, 3, counts, INFO, window=window, exclude={'All Regions': {'A'}})
    assert [region for _, region in kept] == ['Region 1']


def _pdf_text(buffer) -> str:
    pymupdf = pytest.importorskip('pymupdf')
    return '\n'.join(page.get_text() for page in pymupdf.open(stream=buffer.getvalue(), filetype='pdf'))


def test_report_tells_a_focus_signal_only_in_the_section():
    alerts = pd.DataFrame([health('A', 5, tier=OTHER_PHASES), health('B', 1)]).drop(columns=['TriageRank', 'Tier'])
    alerts = alerts.assign(Tier=[OTHER_PHASES, DATA_ONLY])
    problems, information = collect_problems(frames(atspm_detector_health=alerts.to_dict('records'), general_phase_conflicts=[conflict('A')]), None, None, INFO)
    focus = build_signal_focus(problems, SIGNALS, information, related_only=False)
    empty = pd.DataFrame()
    reports = generate_pdf_report(
        empty, empty, empty, [], [], signals_df=SIGNALS, verbosity=0,
        clearance_alerts_df=pd.DataFrame([clearance('A'), clearance('B')]), atspm_detector_health_df=alerts, signal_focus=focus)
    text = _pdf_text(reports['Region 1'])
    assert text.count('Alpha') == 1 and 'Signal Focus' in text and SECTION_EXPLANATION not in text
    assert 'Problem type' in text and 'Start with' not in text and 'What Is Wrong\n' not in text.split('Bravo')[0]
    assert 'No chart' in text      # conflicts have none, and nothing here drew the rest
    assert text.count('Bravo') == 2      # its clearance row and its detector row
    assert text.index('Signal Focus') < text.index('Clearance Interval')
    assert reports.keys() >= {'All Regions', 'Region 1'}


def test_the_report_has_no_intro_text():
    alerts = pd.DataFrame([health('A', 5), health('B', 1)]).drop(columns=['TriageRank', 'Tier'])
    empty = pd.DataFrame()

    def intro(**kwargs):
        reports = generate_pdf_report(empty, empty, empty, [], [], signals_df=SIGNALS, verbosity=0,
                                      atspm_detector_health_df=alerts, **kwargs)
        return ' '.join(_pdf_text(reports['Region 1']).split())

    assert 'recurring issues' not in intro()
    ongoing = {'atspm_detector_health': alerts.assign(OngoingSince=pd.Timestamp('2026-10-12'))}
    text = intro(ongoing_alerts=ongoing)
    assert 'Ongoing Issues' in text and 'at the end of the report' not in text and 'still occurring' not in text
    since = pd.Timestamp('2026-10-12')
    problems, _ = problems_of(held=dict(maxout=[{**maxout('A', 2), 'OngoingSince': since}]), general_phase_conflicts=[conflict('A')], phase_skips=[skips('A', 4, 30)])
    told = intro(signal_focus=build_signal_focus(problems, SIGNALS))
    assert 'except for the signals in Signal Focus' not in told


def test_no_section_without_a_qualifying_signal():
    alerts = pd.DataFrame([health('A', 5)]).drop(columns=['TriageRank', 'Tier'])
    empty = pd.DataFrame()
    reports = generate_pdf_report(empty, empty, empty, [], [], signals_df=SIGNALS, verbosity=0,
                                  atspm_detector_health_df=alerts, signal_focus={})
    assert 'Signal Focus' not in _pdf_text(reports['Region 1'])


def test_generate_builds_the_section_and_leaves_the_alerts_whole():
    timeline = pd.DataFrame([{'DeviceId': 'A', 'StartTime': DAY.replace(hour=9), 'EndTime': DAY.replace(hour=9, minute=5), 'Duration': 300.0,
                              'IsValid': True, 'EventClass': 'MMU Flash', 'EventValue': 1}])
    findings = pd.DataFrame([{'DeviceId': device, 'DetectorType': 'Vehicle', 'Detector': 5, 'Start': DAY.replace(hour=1), 'End': DAY.replace(hour=8),
                              'Label': 'stuck on', 'Confidence': 0.9, 'Detail': 'x'} for device in 'AB'])
    # A stuck detector and a flash alarm do not belong together: by default two such problems are not enough.
    unrelated = ReportGenerator({'verbosity': 0, 'alert_flagging_days': 3650}).generate(signals=SIGNALS, timeline=timeline, atspm_detector_findings=findings)
    assert unrelated['signal_focus']['All Regions'].empty
    result = ReportGenerator({'verbosity': 0, 'alert_flagging_days': 3650, 'signal_focus_related_only': False}).generate(
        signals=SIGNALS, timeline=timeline, atspm_detector_findings=findings)
    rows = result['signal_focus']['All Regions']
    assert rows['DeviceId'].tolist() == ['A'] and rows['Signal'].tolist() == ['Alpha']
    assert 'MMU Flash' in table_of(rows.iloc[0])['Alarms'][0]
    assert set(result['alerts']['atspm_detector_health']['DeviceId']) == {'A', 'B'}    # the alert tables keep it
    text = _pdf_text(result['reports']['Region 1'])
    assert 'Signal Focus' in text and text.count('Alpha') == 1 and 'Bravo' in text
    off = ReportGenerator({'verbosity': 0, 'signal_focus_signals': 0, 'alert_flagging_days': 3650}).generate(signals=SIGNALS, timeline=timeline, atspm_detector_findings=findings)
    assert off['signal_focus'] == {}
    text = _pdf_text(off['reports']['Region 1'])
    assert 'Signal Focus' not in text and text.count('Alpha') == 2      # back in its detector and alarm tables


def _feed(devices=8, day=DAY, last_bin='23:45', overrides=None):
    """has_data: full days before `day`, and on it each device up to last_bin (overrides: device -> its own)."""
    rows = []
    for number in range(devices):
        device = f"S{number}"
        end = (overrides or {}).get(device, last_bin)
        stamps = list(pd.date_range(day - pd.Timedelta(days=2), day, freq='15min', inclusive='left'))
        stamps += list(pd.date_range(day, f"{day.date()} {end}", freq='15min'))
        rows.append(pd.DataFrame({'TimeStamp': stamps, 'DeviceId': device}))
    return pd.concat(rows, ignore_index=True)


FEED_SIGNALS = pd.DataFrame({'DeviceId': [f"S{n}" for n in range(8)], 'Name': [f"Sig {n}" for n in range(8)], 'Region': 'Region 1'})


def test_a_full_day_is_not_a_stopped_feed_and_alerts_stay_as_they_were():
    feed = _feed()
    assert detect_feed_stop(feed) is None and detect_feed_stop(feed, DAY + pd.Timedelta(days=1)) is None
    assert process_missing_data(feed).equals(process_missing_data(feed, None))
    result = ReportGenerator({'verbosity': 0}).generate(signals=FEED_SIGNALS, has_data=feed)
    assert result['data_stopped_at'] is None and 'stopped updating' not in _pdf_text(next(iter(result['reports'].values()))) if result['reports'] else True


def test_a_stopped_feed_is_not_missing_data():
    feed = _feed(last_bin='18:00')
    stop = detect_feed_stop(feed)
    assert stop == DAY + pd.Timedelta(hours=18, minutes=15)
    assert process_missing_data(feed)['MissingData'].max() > 0.2       # what it used to report
    assert process_missing_data(feed, stop)['MissingData'].max() == 0
    assert detect_feed_stop(_feed(last_bin='23:45'), DAY + pd.Timedelta(days=2)) == DAY + pd.Timedelta(days=1)      # a whole day never came


def test_a_real_loss_before_the_stop_stays_missing():
    feed = _feed(last_bin='18:00', overrides={'S3': '09:00'})
    stop = detect_feed_stop(feed)
    assert stop == DAY + pd.Timedelta(hours=18, minutes=15)
    missing_day = process_missing_data(feed, stop).assign(Date=lambda t: pd.to_datetime(t['Date'])).query('Date == @DAY').set_index('DeviceId')['MissingData']
    assert missing_day['S0'] == 0 and missing_day['S3'] == pytest.approx(1 - 37 / 73)


def test_signals_stopping_apart_are_not_a_stopped_feed():
    spread = {f"S{n}": f"{9 + n}:00" for n in range(8)}
    assert detect_feed_stop(_feed(overrides=spread)) is None
    assert detect_feed_stop(_feed(devices=3, last_bin='10:00')) is None       # too few signals to say


def test_faults_are_cut_at_the_stop_and_their_end_is_not_told_as_theirs():
    stop = DAY + pd.Timedelta(hours=18, minutes=15)
    findings = pd.DataFrame({'Start': [DAY.replace(hour=9), DAY.replace(hour=20), DAY.replace(hour=2)],
                             'End': [DAY + pd.Timedelta(hours=23), DAY.replace(hour=22), DAY.replace(hour=3)]})
    trimmed = trim_findings_to_stop(findings, stop)
    assert len(trimmed) == 2 and trimmed['End'].iloc[0] == stop
    alerts = pd.DataFrame({'Periods': [[{'start': '2026-11-03 09:00:00', 'end': '2026-11-03 18:13:00', 'what': 'stuck on'},
                                        {'start': '2026-11-03 02:00:00', 'end': '2026-11-03 03:00:00', 'what': 'stuck on'}]]})
    marked = mark_open_periods(alerts, stop)['Periods'].iloc[0]
    assert marked[0]['open'] and not marked[1]['open']
    assert mark_open_periods(alerts, None) is alerts


def test_report_says_when_the_data_stopped_at_its_top():
    empty = pd.DataFrame()
    alerts = pd.DataFrame([health('A', 5)]).drop(columns=['TriageRank', 'Tier'])
    reports = generate_pdf_report(empty, empty, empty, [], [], signals_df=SIGNALS, verbosity=0, atspm_detector_health_df=alerts,
                                  data_stopped_at=DAY + pd.Timedelta(hours=18, minutes=15))
    text = ' '.join(_pdf_text(reports['Region 1']).split())
    assert text.index('Data stopped updating at Tue 11/3/2026 18:15.') < text.index('Detector Health')


def test_only_the_signal_name_is_merged_in_a_health_table():
    """A detector's details are its own: equal text on the next detector's row stays on that row, never one cell over both."""
    first = '<b>Silent</b> 5 h 35 min, Wed 11/4 09:00-14:35.'
    styles = getSampleStyleSheet()
    impact = '<b>Other phases</b> wait 27 s longer'
    rows = pd.DataFrame({'Signal': ['Alpha'] * 3 + ['Bravo'], 'Det': ['Det 22', 'Det 23', 'Det 24', 'Det 1'],
                         'Impact': [impact] * 3 + ['None'],
                         'What Is Wrong': [first, first, first.replace('5 h 35 min', '2 h'), first]})
    table = create_reportlab_table(rows, 'Health', styles, include_trend=False)[-1]
    spans = {(cmd[1], cmd[2]) for cmd in table._spanCmds}
    assert spans == {((0, 1), (0, 3))}                     # Alpha's name, and nothing else
    stories = [table._cellvalues[row][3][0].text.strip() for row in range(1, 5)]
    assert stories[0] == stories[1] == first
    assert stories[2] == '<b>Silent</b> 2 h, Wed 11/4 09:00-14:35.'
    assert all('Other phases' in table._cellvalues[row][2][0].text for row in range(1, 4))


def test_health_rows_that_continue_on_a_new_block_start_whole():
    first = '<b>Counts fell</b> 3 h, Wed 11/4 09:00-12:00: to 5% of normal.'
    styles = getSampleStyleSheet()
    rows = pd.DataFrame({'Signal': ['Alpha'] * 60, 'Det': [f"Det {n}" for n in range(60)], 'Impact': ['Same'] * 60, 'What Is Wrong': [first] * 60})
    table = create_reportlab_table(rows, 'Health', styles, include_trend=False)[-1]
    labels = [row for row in range(1, 61) if table._cellvalues[row][0] == 'Alpha']
    assert len(labels) >= 2                                # the signal needed more than one block
    assert all('Counts fell' in table._cellvalues[row][3][0].text for row in labels)      # each repeats its label and the whole story
    assert table.split(528, 600)                           # and the table still breaks between blocks


def test_sparkline_draws_the_usual_counts_behind_and_leaves_them_out_without_history():
    usual = pd.DataFrame({'DeviceId': 'A', 'Detector': 5, 'weekend': False, 'clock': [pd.Timestamp('2026-11-03 09:00').time(), pd.Timestamp('2026-11-03 09:15').time()],
                          'Total': [2.0, 3.0], 'Samples': [15, 15]})
    start, end = DAY, DAY + pd.Timedelta(days=1)
    expected = _usual_by_hour(usual, ['A'], start, end)
    assert expected[('A', 5)][9] == 5.0 and pd.isna(expected[('A', 5)][10]) and len(expected[('A', 5)]) == 24
    assert _usual_by_hour(usual.assign(Samples=2), ['A'], start, end) == {}
    stopped = _usual_by_hour(usual, ['A'], start, end, stop=DAY.replace(hour=9))
    assert pd.isna(stopped[('A', 5)][9])
    plain, drawn = create_sparkline([1.0, 2.0, 3.0]), create_sparkline([1.0, 2.0, 3.0], expected=[3.0, 3.0, 3.0])
    assert plain is not drawn
    rows, _ = prepare_atspm_detector_health_table(pd.DataFrame([health('A', 5)]), SIGNALS, ('bad',), hourly_counts={('A', 5): [1.0, 2.0]}, expected_counts={('A', 5): [2.0, 2.0]})
    assert rows['Sparkline_Expected'].iloc[0] == [2.0, 2.0]
    notice = create_reportlab_table(rows, 'Health', getSampleStyleSheet())[0].text
    assert 'usual count' in notice


def test_a_row_past_the_chart_limit_says_no_chart():
    from reportlab.lib.styles import getSampleStyleSheet
    from atspm_report.report_generation import _focus_table
    told = [{'type': 'Other detector problems', 'no_chart': True, 'lines': [{'text': 'Det 3 silent', 'chart': 'detector'}]},
            {'type': 'Kept', 'lines': [{'text': 'Det 4 silent', 'chart': 'detector'}]}]
    cells = _focus_table(told, {'detector'}, getSampleStyleSheet())._cellvalues
    assert 'No chart' in cells[1][0].text and 'No chart' not in cells[2][0].text


def _period(what, start, end):
    return {'start': f'2026-11-03 {start}:00', 'end': f'2026-11-03 {end}:00', 'what': what}


def test_all_detectors_down_together_is_one_line_and_one_chart():
    from atspm_report.signal_focus import detectors_down_together
    # Alpha has two detectors (INFO): both down from 11:00, one held ON and one silent.
    rows = [{**health('A', 5, tier=OTHER_PHASES, rank=0), 'Periods': [_period('stuck on', '10:30', '14:40')]},
            {**health('A', 6, label='silent', fault_kind='Silent', rank=1), 'Periods': [_period('silent', '11:00', '14:00')]}]
    together = detectors_down_together(pd.DataFrame(rows), [5, 6])
    assert together == [(pd.Timestamp('2026-11-03 11:00'), pd.Timestamp('2026-11-03 14:00'))]
    problems, _ = problems_of(atspm_detector_health=rows, maxout=[maxout('A', 2)])
    detectors = problems.set_index('Category').loc['Detectors']
    assert [line['text'] for line in detectors['Harm']][0] == 'All 2 detectors failed together (3 h, Tue 11/3 11:00-14:00)'
    assert not any(line['text'].startswith('Det ') for line in list(detectors['Harm']) + list(detectors['Lines']))
    # Not all of them, or not at once: the detectors are told one by one.
    assert detectors_down_together(pd.DataFrame(rows[:1]), [5, 6]) == []
    apart = [rows[0], {**rows[1], 'Periods': [_period('silent', '18:00', '20:00')]}]
    assert detectors_down_together(pd.DataFrame(apart), [5, 6]) == []


def test_wait_and_max_out_rises_name_the_phases_only():
    facts = ('[{"text": "Phase wait, normal to during the fault (3 h): Ph3 20 to 45 s, Ph2 30 to 41 s, Ph5 9 to 30 s", "chart": "wait", "phases": [3, 2, 5]},'
             ' {"text": "Max outs, normal to during the fault (3 h): Ph4 10% to 40%", "chart": "max out", "phases": [4]}]')
    problems, _ = problems_of(atspm_detector_health=[health('A', 5, tier=OTHER_PHASES, ImpactLines=facts)])
    harm = [line['text'] for line in problems.iloc[0]['Harm']]
    assert harm[1:] == ['Phase wait rose on phases 2, 3 & 5', 'Max-outs rose on phase 4']


def test_a_rise_on_every_phase_the_signal_runs_reads_all_phases():
    facts = '[{"text": "Phase wait, normal to during the fault (3 h): Ph2 20 to 45 s, Ph4 30 to 41 s, Ph6 9 to 30 s", "chart": "wait", "phases": [2, 4, 6]}]'
    alerts = frames(atspm_detector_health=[health('A', 5, tier=OTHER_PHASES, ImpactLines=facts)])
    every, _ = collect_problems(alerts, None, None, INFO, signal_phases={'A': [2, 4, 6]})
    some, _ = collect_problems(alerts, None, None, INFO, signal_phases={'A': [2, 4, 6, 8]})
    assert every.iloc[0]['Harm'][1]['text'] == 'Phase wait rose on all phases'
    assert some.iloc[0]['Harm'][1]['text'] == 'Phase wait rose on phases 2, 4 & 6'
