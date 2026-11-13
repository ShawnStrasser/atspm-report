import sys
from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd
import pytest

from atspm_report import ReportGenerator, run_atspm_detector_findings
from atspm_report.atspm_detector_health import (
    FINDING_COLUMNS,
    ALERT_TYPE,
    MINUTES_COLUMNS,
    build_atspm_detector_health_alerts,
    daily_minutes,
    detector_info,
    findings_reason,
    running_totals,
    update_minutes_history,
)
from atspm_report.table_generation import prepare_atspm_detector_health_table


SIGNALS = pd.DataFrame([
    {'DeviceId': 'dev-a', 'Name': '01001-Main at 1st', 'Region': 'Region 1'},
    {'DeviceId': 'dev-b', 'Name': '02002-Oak at 2nd', 'Region': 'Region 2'},
])
CONFIG = pd.DataFrame([
    {'DeviceId': 'dev-a', 'Detector': 1, 'phase': 2, 'function': 'Advance', 'lanes': 1.0},
    {'DeviceId': 'dev-a', 'Detector': 2, 'phase': 2, 'function': 'Advance', 'lanes': 1.0},
    {'DeviceId': 'dev-a', 'Detector': 3, 'phase': 6, 'function': 'Advance', 'lanes': float('nan')},
    {'DeviceId': 'dev-b', 'Detector': 5, 'phase': 2, 'function': 'Mid', 'lanes': float('nan')},
    {'DeviceId': 'dev-b', 'Detector': 6, 'phase': 2, 'function': 'Mid', 'lanes': float('nan')},
])


def _finding(device, detector, start, end, label, confidence=0.9, detail='', kind='Vehicle'):
    return {'DeviceId': device, 'DetectorType': kind, 'Detector': detector, 'Start': pd.Timestamp(start),
            'End': pd.Timestamp(end), 'Label': label, 'Confidence': confidence, 'Detail': detail}


def _findings():
    return pd.DataFrame([
        # a suspect 42-minute hold: a one-off
        _finding('dev-a', 2, '2026-10-19 18:10:22', '2026-10-19 18:52:15', 'stuck on', 0.6, 'held ON 42 min from Mon 18:10'),
        # 131 min on Monday, then 60 + 120 min of silence: the bad total passes 120 on Tuesday
        _finding('dev-a', 3, '2026-10-19 11:45:24', '2026-10-19 13:56:38', 'stuck on', 0.8, 'held ON 2 times'),
        _finding('dev-a', 3, '2026-10-20 23:00:00', '2026-10-21 02:00:00', 'silent', 0.8, 'went silent'),
        _finding('dev-b', 5, '2026-10-20 09:00:00', '2026-10-20 12:00:00', 'lane closure', None,
                 'Tue 09:00-Tue 12:00: ~300 of its vehicles moved to det 6 (P2)'),
        # notes the report does not list
        _finding('dev-b', 6, '2026-10-20 00:00', '2026-10-21 00:00', 'borderline counts fall', 0.4),
        _finding('dev-b', 7, '2026-10-20 00:00', '2026-10-21 00:00', 'not_enough_data', 0.0),
    ])


def test_alerts_report_what_adds_up_and_information():
    alerts, _ = build_atspm_detector_health_alerts(_findings(), report_date='2026-10-21 00:00', info=CONFIG)

    # det 3's minutes add up; det 2's single 42-minute hold does not; notes are not listed
    issues = dict(zip(alerts['Detector'], alerts['Issue']))
    assert issues == {3: 'fault', 5: 'lane_closure'}
    assert sorted(alerts['Status']) == ['bad', 'lane_closure']
    # The report writes a phase as Ph.
    closure = alerts.loc[alerts['Detector'] == 5, 'Reason'].iloc[0]
    assert closure == 'det 5: Ph2 Mid - Lane closure: Tue 09:00-Tue 12:00: ~300 of its vehicles moved to det 6 (Ph2)'
    assert (alerts['Date'] == pd.Timestamp('2026-10-21')).all()
    assert len(alerts.loc[alerts['Detector'] == 3, 'Periods'].iloc[0]) == 2
    assert set(alerts['DetectorType']) == {'Vehicle'}


def test_suspect_findings_never_alert_on_one_day_however_long():
    day = pd.DataFrame([_finding('dev-a', 2, '2026-10-20 00:00', '2026-10-21 00:00', 'counts dropped', 0.6)])
    alerts, history = build_atspm_detector_health_alerts(day, report_date='2026-10-20')
    assert alerts.empty
    assert history['SuspectMinutes'].max() == pytest.approx(1440.0)
    assert running_totals(history, '2026-10-20').iloc[0]['SuspectTotal'] == pytest.approx(75.0)


def test_suspect_findings_alert_after_several_days():
    days = pd.date_range('2026-10-01', periods=8)
    def history(minutes):
        return pd.DataFrame({'DeviceId': 'dev-a', 'DetectorType': 'Vehicle', 'Detector': 3, 'Date': days,
                             'BadMinutes': 0.0, 'SuspectMinutes': minutes})

    def first_alert(minutes):
        h = history(minutes)
        for k, day in enumerate(days, start=1):
            if running_totals(h, day).iloc[0]['Status'] == 'suspect':
                return k
        return None
    assert first_alert(1440.0) == 4    # heavy: not on day 1, 2 or 3
    assert first_alert(60.0) == 6      # an hour a day: within a week
    assert first_alert(20.0) is None   # short ones stay under it for the week


def test_a_ped_sudden_stop_at_the_suspect_limit_alerts_on_its_third_day():
    stop = pd.DataFrame([_finding('dev-a', 2, '2026-10-18 09:00', '2026-10-21 00:00', 'sudden stop', 0.5,
                                  'no presses for 63 logged hours', kind='Ped')])
    alerts, _ = build_atspm_detector_health_alerts(stop, report_date='2026-10-19')
    assert alerts.empty
    alerts, _ = build_atspm_detector_health_alerts(stop, report_date='2026-10-20')
    assert list(alerts['Status']) == ['suspect'] and list(alerts['Label']) == ['sudden stop']


def test_ped_walks_without_presses_alert_at_once_when_bad_and_never_on_one_suspect_day():
    def walks(confidence):
        return pd.DataFrame([_finding('dev-a', 6, '2026-09-27 10:30', '2026-09-27 14:00', 'walks without presses', confidence,
                                      'walks jumped with no presses from Sun 09-27 10:30', kind='Ped')])
    alerts, _ = build_atspm_detector_health_alerts(walks(0.8), report_date='2026-09-27')
    assert list(alerts['Status']) == ['bad'] and list(alerts['Label']) == ['walks without presses']
    alerts, _ = build_atspm_detector_health_alerts(walks(0.79), report_date='2026-09-27')
    assert alerts.empty


def test_bad_total_keeps_todays_settings():
    days = pd.date_range('2026-10-01', periods=6)
    history = pd.DataFrame({'DeviceId': 'dev-a', 'DetectorType': 'Vehicle', 'Detector': 3, 'Date': days,
                            'BadMinutes': [100.0, 100.0, 0.0, 0.0, 0.0, 0.0], 'SuspectMinutes': 0.0})
    total = running_totals(history, days[1]).iloc[0]
    assert total['BadTotal'] == pytest.approx(140.0) and total['Status'] == 'bad'
    assert pd.isna(running_totals(history, days[2]).iloc[0]['Status'])   # 110 after a clean day
    capped = history.assign(BadMinutes=[600.0, 600.0, 0.0, 0.0, 0.0, 0.0])
    assert running_totals(capped, days[2]).iloc[0]['BadTotal'] == pytest.approx(150.0)   # cap 180 - 30
    assert pd.isna(running_totals(capped, days[3]).iloc[0]['Status'])    # clear after 2 clean days


def test_bad_and_suspect_minutes_are_kept_apart():
    findings = pd.DataFrame([
        _finding('dev-a', 3, '2026-10-20 06:00', '2026-10-20 07:00', 'silent', 0.85),
        _finding('dev-a', 3, '2026-10-20 12:00', '2026-10-20 14:00', 'silent', 0.79),
    ])
    row = daily_minutes(findings, ['2026-10-20']).iloc[0]
    assert row['BadMinutes'] == pytest.approx(60.0) and row['SuspectMinutes'] == pytest.approx(120.0)


def test_status_says_which_total_went_over():
    days = pd.date_range('2026-10-01', periods=4)
    stored = pd.DataFrame({'DeviceId': 'dev-a', 'DetectorType': 'Vehicle', 'Detector': 3, 'Date': days,
                           'BadMinutes': 0.0, 'SuspectMinutes': 120.0})
    latest = pd.DataFrame([_finding('dev-a', 3, '2026-10-04 06:00', '2026-10-04 08:00', 'silent', 0.6)])
    alerts, _ = build_atspm_detector_health_alerts(latest, report_date='2026-10-04', minutes_history=stored, days=1)
    assert list(alerts['Status']) == ['suspect'] and list(alerts['Issue']) == ['fault']
    stronger = latest.assign(Confidence=0.9, End=pd.Timestamp('2026-10-04 10:00'))
    alerts, _ = build_atspm_detector_health_alerts(stronger, report_date='2026-10-04', minutes_history=stored, days=1)
    assert list(alerts['Status']) == ['bad']


def test_weak_and_closure_findings_do_not_count():
    findings = pd.DataFrame([
        _finding('dev-a', 2, '2026-10-20 06:00', '2026-10-20 12:00', 'counts dropped', 0.4),   # under 0.5
        _finding('dev-a', 1, '2026-10-20 06:00', '2026-10-20 12:00', 'silent', 0.8),
        _finding('dev-a', 1, '2026-10-20 05:00', '2026-10-20 05:45', 'lane closure', None),
    ])
    minutes = daily_minutes(findings, ['2026-10-20'])
    assert minutes['BadMinutes'].sum() == 0 and minutes['SuspectMinutes'].sum() == 0


def test_every_fault_counts_its_real_minutes():
    def bad_minutes(row):
        return daily_minutes(pd.DataFrame([row]), ['2026-10-20'])['BadMinutes'].iloc[0]

    def suspect_minutes(row):
        return daily_minutes(pd.DataFrame([row]), ['2026-10-20'])['SuspectMinutes'].iloc[0]

    assert bad_minutes(_finding('dev-a', 3, '2026-10-20 10:00', '2026-10-20 10:30', 'stuck on', 0.95)) == pytest.approx(30.0)
    assert suspect_minutes(_finding('dev-a', 3, '2026-10-20 10:00', '2026-10-20 10:10', 'spiky counts', 0.6)) == pytest.approx(10.0)
    assert suspect_minutes(_finding('dev-a', 3, '2026-10-20 10:00:00', '2026-10-20 10:02:30', 'long hold', 0.5, kind='Ped')) == pytest.approx(2.5)
    # a ped button held 47 minutes (05028 ped 4 on 11-10) counts 47, under the one-day limit of 150, so it does not alert
    held = _finding('dev-a', 4, '2026-10-20 10:00', '2026-10-20 10:47', 'stuck on', 0.99, kind='Ped')
    assert bad_minutes(held) == pytest.approx(47.0)
    alerts, _ = build_atspm_detector_health_alerts(pd.DataFrame([held]), report_date='2026-10-20')
    assert alerts.empty


def test_whole_window_finding_at_bad_confidence_is_reported_at_once():
    days = ['2026-10-19', '2026-10-20', '2026-10-21']
    findings = pd.DataFrame([_finding('dev-a', 3, day, pd.Timestamp(day) + pd.Timedelta(days=1), 'no daily pattern', 0.9,
                                      'counts do not follow the day') for day in days])
    alerts, history = build_atspm_detector_health_alerts(findings, report_date='2026-10-21')
    assert list(alerts['Detector']) == [3] and list(alerts['Status']) == ['bad']
    assert history.empty                         # it is not counted as bad minutes as well
    # the daily rows of one such finding read as one period
    assert alerts.iloc[0]['Periods'] == [{'start': '2026-10-19 00:00:00', 'end': '2026-10-22 00:00:00',
                                          'what': 'no daily pattern'}]
    # weaker, it waits for the suspect total to add up
    weaker, _ = build_atspm_detector_health_alerts(findings.iloc[:2].assign(Confidence=0.7), report_date='2026-10-20')
    assert weaker.empty
    weaker, stored = build_atspm_detector_health_alerts(findings.assign(Confidence=0.7), report_date='2026-10-21')
    assert weaker.empty                          # one run's 3-day window does not alert a detector by itself
    next_day = findings.assign(Confidence=0.7, Start=findings['Start'] + timedelta(days=1), End=findings['End'] + timedelta(days=1))
    weaker, _ = build_atspm_detector_health_alerts(next_day, report_date='2026-10-22', minutes_history=stored)
    assert list(weaker['Status']) == ['suspect']


def test_whole_window_finding_under_bad_adds_up_on_the_suspect_total():
    rows = []
    for day in ('2026-10-04', '2026-10-05', '2026-10-06'):
        a, z = pd.Timestamp(day), pd.Timestamp(day) + timedelta(days=1)
        rows.append(['s', 'Vehicle', 1, a, z, 'busy at night', 0.66, 'night level 38 %'])
        rows.append(['s', 'Vehicle', 2, a, z, 'pulse mode', 0.5, 'looks set to pulse mode'])
    findings = pd.DataFrame(rows, columns=FINDING_COLUMNS)
    one_day, _ = build_atspm_detector_health_alerts(findings[findings['Start'] == pd.Timestamp('2026-10-06')],
                                                    report_date=datetime(2026, 10, 6))
    assert one_day.empty                        # one suspect day never alerts
    alerts, history = build_atspm_detector_health_alerts(findings, report_date=datetime(2026, 10, 6))
    assert alerts.empty                         # nor does one run's 3-day window
    assert set(history['Detector']) == {1}      # pulse mode is set-up
    next_day = findings.assign(Start=findings['Start'] + timedelta(days=1), End=findings['End'] + timedelta(days=1))
    alerts, _ = build_atspm_detector_health_alerts(next_day, report_date=datetime(2026, 10, 7), minutes_history=history)
    assert alerts[['Detector', 'Status']].values.tolist() == [[1, 'suspect']]   # the fourth day does


def test_short_lane_closures_are_left_out():
    findings = pd.DataFrame([_finding('dev-b', 5, '2026-10-20 09:00', '2026-10-20 09:45', 'lane closure', None)])
    alerts, _ = build_atspm_detector_health_alerts(findings, report_date='2026-10-21')
    assert alerts.empty


def test_no_finding_means_a_clean_day_for_known_detectors():
    stored = pd.DataFrame({'DeviceId': 'dev-a', 'DetectorType': 'Vehicle', 'Detector': 3,
                           'Date': pd.to_datetime(['2026-10-19']), 'BadMinutes': [200.0], 'SuspectMinutes': [0.0]})
    empty = pd.DataFrame(columns=_findings().columns)
    alerts, history = build_atspm_detector_health_alerts(empty, report_date='2026-10-20', minutes_history=stored, days=1)
    assert alerts.empty
    assert running_totals(history, '2026-10-20').iloc[0]['BadTotal'] == pytest.approx(140.0)


def test_a_day_without_data_leaves_the_total():
    history = pd.DataFrame({'DeviceId': 'dev-a', 'DetectorType': 'Vehicle', 'Detector': 3, 'SuspectMinutes': 0.0,
                            'Date': pd.to_datetime(['2026-10-01', '2026-10-05']), 'BadMinutes': [200.0, 0.0]})
    # 10-02..10-04 have no row (the signal was dark): the total waits for data, then a clean day lowers it
    assert running_totals(history, '2026-10-04').iloc[0]['BadTotal'] == pytest.approx(170.0)
    assert running_totals(history, '2026-10-05').iloc[0]['BadTotal'] == pytest.approx(140.0)


def test_a_dark_signal_gets_no_clean_day_when_reported_days_are_given():
    known = pd.DataFrame({'DeviceId': ['dev-a', 'dev-b'], 'DetectorType': 'Vehicle', 'Detector': [3, 5]})
    reported = pd.DataFrame({'DeviceId': ['dev-b'], 'Date': pd.to_datetime(['2026-10-20'])})
    minutes = daily_minutes(pd.DataFrame(columns=_findings().columns), ['2026-10-20'], known=known, reported=reported)
    assert list(minutes['DeviceId']) == ['dev-b']


def test_this_runs_days_replace_the_stored_ones():
    old = pd.DataFrame({'DeviceId': 'dev-a', 'DetectorType': 'Vehicle', 'Detector': 3, 'SuspectMinutes': 0.0,
                        'Date': pd.to_datetime(['2026-10-19', '2026-10-20']), 'BadMinutes': [500.0, 500.0]})
    new = pd.DataFrame({'DeviceId': 'dev-a', 'DetectorType': 'Vehicle', 'Detector': 3, 'SuspectMinutes': 0.0,
                        'Date': pd.to_datetime(['2026-10-20', '2026-10-21']), 'BadMinutes': [0.0, 10.0]})
    merged = update_minutes_history(old, new)
    assert list(merged['BadMinutes']) == [500.0, 0.0, 10.0]
    assert len(update_minutes_history(merged, new, keep_days=2)) == 2


def test_history_keeps_only_detectors_with_minutes():
    new = pd.DataFrame({'DeviceId': ['dev-a', 'dev-b'], 'DetectorType': 'Vehicle', 'Detector': [3, 5],
                        'Date': pd.Timestamp('2026-10-21'), 'BadMinutes': [10.0, 0.0], 'SuspectMinutes': 0.0})
    assert list(update_minutes_history(None, new)['Detector']) == [3]


def test_history_from_before_ped_health_is_read():
    old = pd.DataFrame({'DeviceId': 'dev-a', 'Detector': 3, 'Date': pd.to_datetime(['2026-10-19']), 'BadMinutes': [200.0]})
    merged = update_minutes_history(old, None)
    assert list(merged.columns) == MINUTES_COLUMNS
    assert merged.iloc[0]['DetectorType'] == 'Vehicle' and merged.iloc[0]['SuspectMinutes'] == 0.0


def test_alert_date_falls_back_to_last_finding():
    alerts, _ = build_atspm_detector_health_alerts(_findings())
    assert (alerts['Date'] == pd.Timestamp('2026-10-21')).all()


def test_empty_findings_give_no_alerts():
    assert build_atspm_detector_health_alerts(pd.DataFrame())[0].empty
    assert build_atspm_detector_health_alerts(_findings().tail(2))[0].empty


def test_ped_and_vehicle_numbers_do_not_mix():
    findings = pd.DataFrame([
        _finding('dev-a', 3, '2026-10-20 06:00', '2026-10-20 10:00', 'silent', 0.9, 'went silent'),
        _finding('dev-a', 3, '2026-10-20 06:00', '2026-10-20 10:00', 'stuck on', 0.9,
                 'stuck ON: held 4 h through 80 walks (limit 5 min, 3 walks, or 3 min through 2 walks)', kind='Ped'),
    ])
    alerts, history = build_atspm_detector_health_alerts(findings, report_date='2026-10-20')
    assert sorted(alerts['DetectorType']) == ['Ped', 'Vehicle']
    assert len(history) == 6     # two detectors, three days
    ped = alerts[alerts['DetectorType'] == 'Ped'].iloc[0]
    assert ped['Reason'].startswith('stuck ON: held 4 h')     # a ped button has no "det 3:" lead-in
    rows, total = prepare_atspm_detector_health_table(alerts, SIGNALS, ('bad',), detector_type='Ped')
    assert total == 1 and list(rows.columns) == ['Signal', 'Button', 'What Is Wrong']
    rows, total = prepare_atspm_detector_health_table(alerts, SIGNALS, ('bad',))
    assert total == 1 and 'Det' in rows.columns


def test_ped_notes_are_not_listed_and_setup_issues_are_information():
    findings = pd.DataFrame([
        _finding('dev-a', 4, '2026-10-20 06:00', '2026-10-20 10:00', 'call dropped', 0.0, kind='Ped'),
        _finding('dev-a', 4, '2026-10-20 06:00', '2026-10-20 06:10', 'omitted during preemption', 0.0, kind='Ped'),
        _finding('dev-a', 5, '2026-10-20 06:00', '2026-10-20 10:00', 'never calls a walk', 0.8,
                 'pressed 40 times on 1 day but no ped call or walk ever followed', kind='Ped'),
    ])
    alerts, history = build_atspm_detector_health_alerts(findings, report_date='2026-10-20')
    assert list(alerts['Detector']) == [5] and list(alerts['Status']) == ['config_issue']
    assert history.empty     # information does not add up to a fault


def test_count_drop_text_uses_the_first_judged_hour():
    detail = 'counts and occupancy both fell from Mon 04:00: 52 per hour where its phase mates give ~153 (GEH 11.3)'
    rows = pd.DataFrame([
        _finding('dev-a', 3, '2026-10-20 15:00', '2026-10-21 00:00', 'counts dropped', 0.7, detail),
        _finding('dev-a', 3, '2026-10-20 04:10', '2026-10-20 14:00', 'counts dropped', 0.7, detail),
    ])
    assert findings_reason(rows).startswith(
        'det 3: Counts fall: counts and occupancy both fell from Tue 04:00: 52 per hour')
    rows['Detail'] = 'nearly stopped counting from Tue 08:00: 6 per hour'
    assert findings_reason(rows) == 'det 3: Counts fall: nearly stopped counting from Tue 04:00: 6 per hour'


def test_count_drop_over_whole_days_keeps_the_time_in_its_text():
    detail = 'counts and occupancy both fell from Mon 09:00 to Mon 21:00: 656 per hour'
    rows = pd.DataFrame([
        _finding('dev-a', 3, '2026-10-04 16:15', '2026-10-05 00:00', 'counts dropped', 0.7, detail),
        _finding('dev-a', 3, '2026-10-05 00:00', '2026-10-06 00:00', 'counts dropped', 0.7, detail),
    ])
    assert findings_reason(rows) == f'det 3: Counts fall: {detail}'
    # a period that runs on over midnight has a first hour
    rows.loc[0, 'Start'] = pd.Timestamp('2026-10-04 22:00')
    assert 'both fell from Sun 22:00 to Mon 21:00' in findings_reason(rows)


def test_reason_names_each_detail_once_strongest_first():
    rows = pd.DataFrame([
        _finding('dev-a', 3, '2026-10-20 10:00', '2026-10-20 10:30', 'spiky counts', 0.6, '5 bursts a day'),
        _finding('dev-a', 3, '2026-10-20 11:00', '2026-10-20 11:30', 'stuck on', 0.9, 'held ON 2 times'),
        _finding('dev-a', 3, '2026-10-20 12:00', '2026-10-20 12:30', 'stuck on', 0.7, 'held ON 2 times'),
    ])
    info = detector_info(CONFIG).query("DeviceId == 'dev-a'").set_index('Detector')
    assert findings_reason(rows, info) == 'det 3: Ph6 Advance - Stuck on: held ON 2 times; Spiky counts: 5 bursts a day'


def test_reason_reads_like_the_checks_text():
    def row(label, confidence, detail, detector=3):
        return _finding('dev-a', detector, '2026-10-20 10:00', '2026-10-20 10:30', label, confidence, detail)
    table = pd.DataFrame({'DeviceId': ['dev-a', 'dev-a', 'dev-a'], 'Detector': [3, 4, 5], 'phase': [6, 4, None],
                          'function': ['Count', 'Yellow_Red', None], 'lanes': [1.0, 2.0, 1.0]})
    info = detector_info(table).set_index('Detector')
    # one heading holds its details; lanes only where the check depends on them; a detail is said once
    stuck = pd.DataFrame([row('stuck on', 0.9, 'held ON 1 h'), row('count zone held ON', 0.85, 'zone ON 135 min'),
                          row('silent', 0.6, 'went silent')])
    assert findings_reason(stuck, info) == ('det 3: Ph6 Count (covers 1 lane) - Stuck on: held ON 1 h; zone ON 135 min; '
                                            'Silent: went silent')
    assert findings_reason(pd.DataFrame([row('silent', 0.9, 'went silent')]), info) == 'det 3: Ph6 Count - Silent: went silent'
    twice = pd.DataFrame([row('misses vehicles at night', 0.9, 'night 139 actuations'), row('silent', 0.9, 'night 139 actuations')])
    assert findings_reason(twice, info) == 'det 3: Ph6 Count - Misses vehicles at night: night 139 actuations'
    assert findings_reason(pd.DataFrame([row('spiky counts', 0.9, '5 bursts', 4)]), info) == (
        'det 4: Ph4 Yellow-red (covers 2 lanes) - Spiky counts: 5 bursts')
    # no known function: the plain text, as the check gives it
    plain = pd.DataFrame([row('silent', 0.9, 'went silent', 5), row('misses vehicles at night', 0.8, 'misses vehicles at night', 5)])
    assert findings_reason(plain, info) == 'det 5: went silent; misses vehicles at night'


def test_fault_table_orders_bad_first_and_formats_periods():
    alerts, _ = build_atspm_detector_health_alerts(_findings(), report_date='2026-10-21', info=CONFIG)
    rows, total = prepare_atspm_detector_health_table(alerts, SIGNALS, ('bad',))

    assert total == 1
    assert list(rows.columns) == ['Signal', 'Det', 'What Is Wrong']
    # One narrative: what is wrong first, with its times. The detector's name has a column of its own.
    assert rows.iloc[0]['What Is Wrong'] == (
        '<b>Silent</b> 3 h, Tue 10/20 23:00-02:00: went silent. '
        '<b>Held ON</b> 2 h 11 min, Mon 10/19 11:45-13:56.'
    )


def test_info_table_and_region_filter():
    alerts, _ = build_atspm_detector_health_alerts(_findings(), report_date='2026-10-21')

    info, total = prepare_atspm_detector_health_table(
        alerts, SIGNALS, ('lane_closure', 'timing_plan'), story_header='What Happened')
    assert total == 1
    assert '<b>Lane closed</b> 3 h, Tue 10/20 09:00-12:00' in info.iloc[0]['What Happened']

    region_two, total = prepare_atspm_detector_health_table(
        alerts, SIGNALS, ('bad', 'suspect'), region='Region 2')
    assert region_two.empty and total == 0


def test_repeated_periods_are_grouped_not_listed():
    findings = pd.DataFrame([_finding('dev-a', 9, f'2026-10-20 0{h}:00:00', f'2026-10-20 0{h}:30:00', 'silent', 0.9)
                             for h in range(6)])
    alerts, _ = build_atspm_detector_health_alerts(findings, report_date='2026-10-20')
    rows, _ = prepare_atspm_detector_health_table(alerts, SIGNALS, ('bad',))
    assert rows.iloc[0]['What Is Wrong'].startswith('<b>Silent</b> 6 times, 3 h in total, mostly nights, Oct 20')


def _recent_findings():
    """_findings() moved so the last fault ends today, inside the flagging window."""
    shift = pd.Timestamp.today().normalize() - pd.Timestamp('2026-10-21')
    findings = _findings()
    findings['Start'] += shift
    findings['End'] += shift
    return findings


def _generate(past_alerts, **kwargs):
    with patch('atspm_report.generator.generate_pdf_report', return_value={}) as pdf:
        result = ReportGenerator({'verbosity': 0}).generate(
            signals=SIGNALS,
            atspm_detector_findings=_recent_findings(),
            atspm_detector_config=CONFIG,
            past_alerts=past_alerts,
            **kwargs,
        )
    return result, pdf


def test_suppression_is_independent_of_other_alert_types():
    recent = datetime.now() - timedelta(days=2)
    # Another check already reported dev-a. That history must not hide the
    # detector health alerts for the same signal.
    result, pdf = _generate({
        'maxout': pd.DataFrame([{'DeviceId': 'dev-a', 'Phase': 2, 'Date': recent}]),
    })

    alerts = result['alerts'][ALERT_TYPE]
    assert sorted(alerts['Detector']) == [3, 5]
    assert pdf.call_args.kwargs['atspm_detector_health_df'] is alerts
    history = result['updated_past_alerts'][ALERT_TYPE]
    assert sorted(history['Detector']) == [3, 5]
    assert list(history.columns) == ['DeviceId', 'DetectorType', 'Detector', 'Issue', 'Date']
    # ... and the other type's history is left exactly as it was.
    assert list(result['updated_past_alerts']['maxout']['Phase']) == [2]
    # the retired CUSUM detector type is gone
    assert 'actuations' not in result['updated_past_alerts']
    # the daily minutes come back for the next run
    minutes = result['updated_atspm_detector_minutes']
    assert list(minutes.columns) == MINUTES_COLUMNS
    assert minutes['Date'].nunique() == 3


def test_repeat_fault_is_suppressed_but_a_closure_is_listed_every_run():
    recent = datetime.now() - timedelta(days=2)
    result, _ = _generate({
        ALERT_TYPE: pd.DataFrame([
            {'DeviceId': 'dev-a', 'DetectorType': 'Vehicle', 'Detector': 3, 'Issue': 'fault', 'Date': recent},
            {'DeviceId': 'dev-b', 'DetectorType': 'Vehicle', 'Detector': 5, 'Issue': 'lane_closure', 'Date': recent},
        ]),
    })

    alerts = result['alerts'][ALERT_TYPE]
    assert sorted(alerts['Detector']) == [5]
    # An earlier detector health alert does not touch another type's history either.
    assert result['updated_past_alerts']['maxout'].empty


def test_alert_history_from_before_ped_health_still_suppresses_vehicle_faults():
    recent = datetime.now() - timedelta(days=2)
    result, _ = _generate({
        ALERT_TYPE: pd.DataFrame([{'DeviceId': 'dev-a', 'Detector': 3, 'Issue': 'fault', 'Date': recent}]),
    })
    assert sorted(result['alerts'][ALERT_TYPE]['Detector']) == [5]
    assert 'DetectorType' in result['updated_past_alerts'][ALERT_TYPE].columns


def test_a_ped_fault_is_not_suppressed_by_a_vehicle_detector_of_the_same_number():
    recent = datetime.now() - timedelta(days=2)
    today = pd.Timestamp.today().normalize()
    ped = pd.DataFrame([_finding('dev-a', 3, today - pd.Timedelta(hours=8), today - pd.Timedelta(hours=2), 'stuck on', 0.95,
                                 'stuck ON: held 6 h through 90 walks', kind='Ped')])
    with patch('atspm_report.generator.generate_pdf_report', return_value={}):
        result = ReportGenerator({'verbosity': 0}).generate(
            signals=SIGNALS, atspm_detector_findings=pd.concat([_recent_findings(), ped], ignore_index=True),
            past_alerts={ALERT_TYPE: pd.DataFrame([
                {'DeviceId': 'dev-a', 'DetectorType': 'Vehicle', 'Detector': 3, 'Issue': 'fault', 'Date': recent}])})
    alerts = result['alerts'][ALERT_TYPE]
    assert set(zip(alerts['DetectorType'], alerts['Detector'])) == {('Ped', 3), ('Vehicle', 5)}


def test_stored_minutes_make_a_detector_alert_sooner():
    today = pd.Timestamp.today().normalize()
    stored = pd.DataFrame({'DeviceId': 'dev-a', 'DetectorType': 'Vehicle', 'Detector': 2, 'BadMinutes': 150.0,
                           'SuspectMinutes': 0.0, 'Date': [today - pd.Timedelta(days=4), today - pd.Timedelta(days=3)]})
    hour = pd.DataFrame([_finding('dev-a', 2, today - pd.Timedelta(hours=10), today - pd.Timedelta(hours=9),
                                  'silent', 0.8)])
    findings = pd.concat([_recent_findings(), hour], ignore_index=True)
    with patch('atspm_report.generator.generate_pdf_report', return_value={}):
        alone = ReportGenerator({'verbosity': 0}).generate(signals=SIGNALS, atspm_detector_findings=findings)
        after = ReportGenerator({'verbosity': 0}).generate(
            signals=SIGNALS, atspm_detector_findings=findings, atspm_detector_minutes=stored)
    # det 2's hour alone is nothing, but on top of two bad days it keeps the total over 120
    assert 2 not in set(alone['alerts'][ALERT_TYPE]['Detector'])
    assert 2 in set(after['alerts'][ALERT_TYPE]['Detector'])


def test_report_includes_the_new_section():
    result = ReportGenerator({'verbosity': 0}).generate(signals=SIGNALS, atspm_detector_findings=_recent_findings())
    # Region 1 has faults; Region 2 only a lane closure, which is no reason for a report.
    assert set(result['reports']) == {'Region 1', 'All Regions'}
    assert result['reports']['Region 1'].getvalue().startswith(b'%PDF')


def _pdf_text(buffer):
    pypdf = pytest.importorskip('pypdf')
    return ' '.join(page.extract_text() for page in pypdf.PdfReader(buffer).pages)


def test_report_has_a_ped_section_in_plain_words():
    today = pd.Timestamp.today().normalize()
    findings = pd.DataFrame([
        _finding('dev-a', 4, today - pd.Timedelta(hours=8), today - pd.Timedelta(hours=2), 'stuck on', 0.95,
                 'stuck ON: held 6 h through 90 walks (limit 5 min, 3 walks, or 3 min through 2 walks)', kind='Ped'),
        _finding('dev-a', 6, today - pd.Timedelta(hours=8), today - pd.Timedelta(hours=2), 'never calls a walk', 0.8,
                 'pressed 40 times on 1 day but no ped call or walk ever followed', kind='Ped'),
    ])
    presses = pd.DataFrame([{'TimeStamp': t, 'DeviceId': 'dev-a', 'Detector': d, 'Total': 3}
                            for t in pd.date_range(today - pd.Timedelta(days=2), today, freq='15min') for d in (4, 5)])
    with patch('atspm_report.generator.create_atspm_detector_health_plots',
               wraps=__import__('atspm_report.visualization', fromlist=['x']).create_atspm_detector_health_plots) as plots:
        result = ReportGenerator({'verbosity': 0}).generate(
            signals=SIGNALS, atspm_detector_findings=findings, ped_press_counts=presses)
    assert plots.call_count == 1 and plots.call_args.kwargs['kind'] == 'ped'
    text = _pdf_text(result['reports']['Region 1'])
    assert 'Pedestrian Button Health' in text and 'Button Set-Up Issues' in text
    assert 'Held ON 6 h' in text and 'through 90 walks' in text


def test_ped_chart_shows_presses_and_periods():
    from atspm_report.visualization import create_atspm_detector_health_plots
    today = pd.Timestamp('2026-10-20')
    findings = pd.DataFrame([_finding('dev-a', 4, today + pd.Timedelta(hours=8), today + pd.Timedelta(hours=14), 'stuck on',
                                      0.95, 'stuck ON: held 6 h', kind='Ped')])
    alerts, _ = build_atspm_detector_health_alerts(findings, report_date=today)
    presses = pd.DataFrame([{'TimeStamp': t, 'DeviceId': 'dev-a', 'Detector': d, 'Total': 3}
                            for t in pd.date_range(today, periods=20, freq='15min') for d in (4, 5)])
    figures = create_atspm_detector_health_plots(alerts, SIGNALS, 3, presses, kind='ped',
                                                 window=(today, today + pd.Timedelta(days=1)))
    assert [region for _, region in figures] == ['Region 1', 'All Regions']
    fig = figures[0][0]
    assert 'ped buttons' in fig.texts[0].get_text() and 'Stuck on: ped 4' in fig.texts[1].get_text()
    labels = [line.get_label() for line in fig.axes[0].get_lines()]
    assert 'Ped 4' in labels and 'Other ped buttons' in labels
    assert fig.axes[0].get_ylabel() == 'Presses / 15 min'
    # a quarter hour nobody pressed is a zero, not a gap
    assert fig.axes[0].get_lines()[-1].get_ydata()[-1] == 0


def test_ped_chart_keeps_brief_presses_inside_a_stuck_period():
    from atspm_report.visualization import create_atspm_detector_health_plots
    today = pd.Timestamp('2026-10-20')
    findings = pd.DataFrame([_finding('dev-a', 4, today + pd.Timedelta(hours=8), today + pd.Timedelta(hours=14), 'stuck on',
                                      0.95, 'stuck ON: held 6 h', kind='Ped')])
    alerts, _ = build_atspm_detector_health_alerts(findings, report_date=today)
    presses = pd.DataFrame([{'TimeStamp': t, 'DeviceId': 'dev-a', 'Detector': d, 'Total': 3}
                            for t in pd.date_range(today, periods=96, freq='15min') for d in (4, 5)])
    fig = create_atspm_detector_health_plots(alerts, SIGNALS, 3, presses, kind='ped',
                                             window=(today, today + pd.Timedelta(days=1)))[0][0]
    lines = {line.get_label(): line.get_ydata() for line in fig.axes[0].get_lines()}
    assert lines['Ped 4'].tolist() == [3] * 96    # a stuck period with blips is what happened
    assert lines['Other ped buttons'].tolist() == [3] * 96


def test_ped_chart_draws_walks_in_a_panel_of_their_own():
    from atspm_report.visualization import create_atspm_detector_health_plots
    today = pd.Timestamp('2026-10-20')
    findings = pd.DataFrame([_finding('dev-a', 4, today + pd.Timedelta(hours=8), today + pd.Timedelta(hours=14), 'stuck on',
                                      0.95, 'stuck ON: held 6 h', kind='Ped')])
    alerts, _ = build_atspm_detector_health_alerts(findings, report_date=today)
    presses = pd.DataFrame([{'TimeStamp': t, 'DeviceId': 'dev-a', 'Detector': 4, 'Total': 3} for t in pd.date_range(today, periods=96, freq='15min')])
    walks = pd.DataFrame([{'TimeStamp': t, 'DeviceId': 'dev-a', 'Phase': phase, 'PedServices': 2}
                          for t in pd.date_range(today, periods=8, freq='15min') for phase in (2, 8)])
    inputs = pd.DataFrame({'DeviceId': ['dev-a'], 'Input': [4], 'Phase': [8.0]})
    fig = create_atspm_detector_health_plots(alerts, SIGNALS, 3, presses, kind='ped', window=(today, today + pd.Timedelta(days=1)),
                                             ped_services=walks, ped_inputs=inputs)[0][0]
    panels = {axis.get_ylabel(): axis for axis in fig.axes}
    assert {'Presses / 15 min', 'Walks / 15 min'} <= set(panels)
    walk_lines = {line.get_label(): line for line in panels['Walks / 15 min'].get_lines()}
    assert walk_lines['Ph8 walks'].get_ydata().tolist() == [2] * 8 + [0] * 88    # no row is no walk
    assert 'Other phases' in walk_lines
    press_color = next(line.get_color() for line in panels['Presses / 15 min'].get_lines() if line.get_label() == 'Ped 4')
    assert walk_lines['Ph8 walks'].get_color() == press_color
    # Without walks the chart is as before.
    plain = create_atspm_detector_health_plots(alerts, SIGNALS, 3, presses, kind='ped', window=(today, today + pd.Timedelta(days=1)))[0][0]
    assert len(plain.axes) == 2


def test_run_needs_atspm_detector(monkeypatch):
    monkeypatch.setitem(sys.modules, 'atspm_detector', None)
    with pytest.raises(ImportError, match='atspm-report\\[detector\\]'):
        run_atspm_detector_findings(None, 'events', 'detectors')


def test_run_gives_the_findings_table_of_the_package():
    pytest.importorskip('atspm_detector')
    duckdb = pytest.importorskip('duckdb')
    con = duckdb.connect()
    con.execute("""CREATE TABLE events AS SELECT '1' AS DeviceId, TIMESTAMP '2026-10-05 08:00:00' + (i * INTERVAL 40 SECOND) AS Timestamp,
                   CASE WHEN i % 2 = 0 THEN 82 ELSE 81 END AS EventId, 3 AS Parameter FROM range(4000) t(i)""")
    con.execute("""CREATE TABLE detectors AS SELECT '1' AS DeviceId, 3 AS Detector, 2.0 AS phase, 'Advance' AS function, 1.0 AS lanes""")
    findings, status = run_atspm_detector_findings(con, 'events', 'detectors', pd.Timestamp('2026-10-05'),
                                                   pd.Timestamp('2026-10-07'), summary=True)
    assert list(findings.columns) == FINDING_COLUMNS
    assert not status.duplicated(['DeviceId', 'DetectorType', 'Detector', 'Date']).any()


def test_fault_charts_one_per_phase_with_its_other_detectors_in_grey():
    from atspm_report.visualization import create_atspm_detector_health_plots
    findings = _findings()
    findings.loc[findings['Detector'] == 2, ['End', 'Confidence']] = [pd.Timestamp('2026-10-19 21:52:15'), 0.9]
    alerts, _ = build_atspm_detector_health_alerts(findings, report_date='2026-10-20', info=CONFIG)
    times = pd.date_range('2026-10-18', '2026-10-21', freq='15min', inclusive='left')
    counts = pd.DataFrame([{'TimeStamp': t, 'DeviceId': 'dev-a', 'Detector': d, 'Total': 10}
                           for t in times for d in (1, 2, 3)])
    info = detector_info(CONFIG)
    assert info.set_index(['DeviceId', 'Detector']).loc[('dev-a', 2), ['Phase', 'Function']].tolist() == [2, 'Advance']
    window = (pd.Timestamp('2026-10-20'), pd.Timestamp('2026-10-21'))
    faults = alerts[alerts['Status'] == 'bad']
    figures = create_atspm_detector_health_plots(faults, SIGNALS, 3, counts, info, window=window)
    # one chart per phase (Ph2, Ph6) in Region 1 and again in All Regions
    assert [region for _, region in figures] == ['Region 1'] * 2 + ['All Regions'] * 2
    texts = [[t.get_text() for t in fig.texts] for fig, _ in figures[:2]]
    assert any('Ph2' in t[0] for t in texts) and any('Ph6' in t[0] for t in texts)
    ph2 = next(fig for fig, _ in figures[:2] if 'Ph2' in fig.texts[0].get_text())
    labels = [line.get_label() for line in ph2.axes[0].get_lines()]
    assert 'Det 2' in labels and 'Other Ph2 detectors' in labels
    assert ph2.axes[0].get_xlim()[1] - ph2.axes[0].get_xlim()[0] == pytest.approx(1.0)   # the report day only
    assert create_atspm_detector_health_plots(faults, SIGNALS, 3, pd.DataFrame(), info) == []


def test_lane_closure_chart_draws_the_lane_that_took_the_traffic():
    from atspm_report.visualization import create_atspm_detector_health_plots
    alerts, _ = build_atspm_detector_health_alerts(_findings(), report_date='2026-10-20', info=CONFIG)
    times = pd.date_range('2026-10-20', '2026-10-21', freq='15min', inclusive='left')
    counts = pd.DataFrame([{'TimeStamp': t, 'DeviceId': 'dev-b', 'Detector': d, 'Total': 10}
                           for t in times for d in (5, 6)])
    closures = alerts[alerts['Status'] == 'lane_closure']
    figures = create_atspm_detector_health_plots(closures, SIGNALS, 3, counts, detector_info(CONFIG),
                                                 window=(pd.Timestamp('2026-10-20'), pd.Timestamp('2026-10-21')),
                                                 kind='closure')
    labels = [line.get_label() for line in figures[0][0].axes[0].get_lines()]
    assert 'Det 5' in labels and 'Det 6 (took its traffic)' in labels


def test_report_window_covers_only_days_not_yet_reported():
    from atspm_report.atspm_detector_health import report_window
    day = pd.Timestamp('2026-10-21')
    first = report_window(day)
    assert first == (pd.Timestamp('2026-10-19'), pd.Timestamp('2026-10-22'))         # first run: all 3 days
    daily = report_window(day, pd.DataFrame({'Date': ['2026-10-20']}))
    assert daily == (pd.Timestamp('2026-10-21'), pd.Timestamp('2026-10-22'))         # yesterday reported
    skipped = report_window(day, pd.DataFrame({'Date': ['2026-10-19']}))
    assert skipped == (pd.Timestamp('2026-10-20'), pd.Timestamp('2026-10-22'))       # a skipped day comes back


def test_alerts_outside_the_window_are_dropped():
    from atspm_report.atspm_detector_health import limit_to_window
    alerts, _ = build_atspm_detector_health_alerts(_findings(), report_date='2026-10-21')
    limited = limit_to_window(alerts, pd.Timestamp('2026-10-21'), pd.Timestamp('2026-10-22'))
    # det 3 keeps only its period reaching into 10-21; the 10-20 lane closure was already reported
    assert list(limited['Detector']) == [3]
    assert len(limited.iloc[0]['Periods']) == 1


def test_history_keeps_only_alerts_the_tables_listed():
    today = pd.Timestamp.today().normalize()
    findings = pd.DataFrame([
        _finding('dev-a' if d < 4 else 'dev-c', d, today - pd.Timedelta(hours=5), today - pd.Timedelta(hours=2),
                 'silent', 1 - d / 100, 'went silent')
        for d in range(1, 6)])     # dets 4, 5 on a second signal, same region
    signals = pd.concat([SIGNALS, pd.DataFrame([{'DeviceId': 'dev-c', 'Name': '01003-Elm at 3rd',
                                                 'Region': 'Region 1'}])])
    with patch('atspm_report.generator.generate_pdf_report', return_value={}):
        result = ReportGenerator({'verbosity': 0, 'max_table_rows': 2}).generate(
            signals=signals, atspm_detector_findings=findings)
    assert len(result['alerts'][ALERT_TYPE]) == 5
    # the worst signal (dets 1-3) fills the 2-row table and stays whole; the other signal can alert again tomorrow
    assert sorted(result['updated_past_alerts'][ALERT_TYPE]['Detector']) == [1, 2, 3]


def test_tables_keep_a_signals_rows_together():
    from atspm_report.table_generation import head_by_signal
    rows = pd.DataFrame({'Signal': ['A', 'B', 'A', 'C', 'A', 'B'], 'v': [1, 2, 3, 4, 5, 6]})
    top = head_by_signal(rows, 4)
    # A ranked first, then B: all of A and all of B, past 4 rows; C left out
    assert list(top['Signal']) == ['A', 'A', 'A', 'B', 'B']
    assert list(top['v']) == [1, 3, 5, 2, 6]
    assert list(head_by_signal(rows, 2)['Signal']) == ['A', 'A', 'A']   # A alone reaches 2: stop there
    assert len(head_by_signal(pd.DataFrame({'Region': ['x', 'y', 'z']}), 2)) == 2   # not per signal: plain cut


def test_old_pedestrian_check_is_retired():
    # Pedestrian Button Health (atspm-detector) replaced the daily walks-up, presses-per-walk-down check: no alerts, no history.
    assert 'pedestrian' not in ReportGenerator.ALERT_TYPES
    assert 'ped_erratic_enabled' not in ReportGenerator({'verbosity': 0}).config
