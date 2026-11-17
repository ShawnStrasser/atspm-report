from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from atspm_report import ReportGenerator
from atspm_report.coordination_check import (
    ALERT_TYPE,
    attached_to,
    BY_POSITION,
    CAUSE_CLOCK,
    CAUSE_SCHEDULE,
    CONFIDENCE_CHECK,
    CONFIDENCE_HIGH,
    LEARNED,
    PART_TIME,
    build_coordination_alerts,
    create_coordination_plots,
    group_of,
    listed_alerts,
    plan_findings,
    preempt_clock_offsets,
    prepare_coordination_table,
    us_clock_changes,
)

REPORT_DAY = pd.Timestamp('2026-11-15')   # a Sunday
DAYS = pd.date_range(REPORT_DAY - pd.Timedelta(days=41), REPORT_DAY)


def _signals(ids, spacing=0.004):
    """Signals on one north-south line, about a quarter mile apart, plus a far one."""
    rows = [{'DeviceId': d, 'Name': f"{d}-Main at {n}", 'Region': 'R', 'Latitude': 45.0 + spacing * n, 'Longitude': -122.0}
            for n, d in enumerate(ids)]
    rows.append({'DeviceId': 'FAR', 'Name': 'FAR-Far away', 'Region': 'R', 'Latitude': 46.0, 'Longitude': -122.0})
    return pd.DataFrame(rows)


GROUP = ['A', 'B', 'C', 'D']
SIGNALS = _signals(GROUP + ['E'])


def _plan(day, minute, start=6 * 60, end=20 * 60, cycle=75, pattern=1):
    """(Pattern, CycleLength): the cycle from start to end, free (pattern 21, cycle 0) otherwise."""
    return (pattern, cycle) if start <= minute < end else (21, 0)


def _coordination(rule, devices=GROUP + ['E', 'FAR'], days=DAYS, skip=None):
    """coordination_agg rows from rule(device, day, minute) -> (Pattern, CycleLength); skip(device, ts) drops a row."""
    rows = []
    for day in days:
        for minute in range(0, 24 * 60, 15):
            ts = day + pd.Timedelta(minutes=minute)
            for device in devices:
                if skip is not None and skip(device, ts):
                    continue
                pattern, cycle = rule(device, day, minute)
                rows.append((ts, device, pattern, cycle))
    return pd.DataFrame(rows, columns=['TimeStamp', 'DeviceId', 'Pattern', 'CycleLength'])


def _all_together(device, day, minute):
    return (21, 0) if device == 'FAR' else _plan(day, minute)


def _findings(rule, **kwargs):
    return plan_findings(_coordination(rule, **kwargs), SIGNALS, REPORT_DAY)['findings']


# ---- Groups -------------------------------------------------------------------------------------------------------------

def test_groups_join_nearby_signals_that_run_matching_cycles():
    def rule(device, day, minute):
        if device == 'E':          # always free: not coordinated, so in no group
            return (21, 0)
        if device == 'C':          # double the others' cycle: still coordinated with them
            return _plan(day, minute, cycle=150)
        return _all_together(device, day, minute)
    pairs = plan_findings(_coordination(rule), SIGNALS, REPORT_DAY)['pairs']
    joined = set(map(frozenset, zip(pairs['DeviceId'], pairs['NextDeviceId'])))
    assert frozenset({'A', 'B'}) in joined and frozenset({'B', 'C'}) in joined
    assert not any('E' in pair or 'FAR' in pair for pair in joined)


def test_too_little_history_learns_no_group():
    pairs = plan_findings(_coordination(_all_together, days=DAYS[-3:]), SIGNALS, REPORT_DAY)['pairs']
    assert pairs.empty


# ---- Plan check: what it finds ------------------------------------------------------------------------------------------

def test_weekend_schedule_set_wrong_on_a_correct_clock():
    """08043: free all weekend while its group runs 75 s, with them on weekdays."""
    def rule(device, day, minute):
        if device == 'B' and day.dayofweek >= 5:
            return (21, 0)
        return _all_together(device, day, minute)
    findings = _findings(rule)
    assert set(findings['DeviceId']) == {'B'}
    assert set(findings['DayType']) == {'Saturdays', 'Sundays'}
    alerts = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(rule), clock_check=False)['alerts']
    assert alerts['Cause'].tolist() == [CAUSE_SCHEDULE]
    assert 'Weekends 6:00-20:00 it runs free while the other four run 75 s' in alerts['Text'].iloc[0]


def test_a_steady_late_change_on_weekdays():
    def rule(device, day, minute):
        if device == 'B' and day.dayofweek < 5:
            return _plan(day, minute, start=7 * 60)
        return _all_together(device, day, minute)
    findings = _findings(rule)
    row = findings.iloc[0]
    assert (row['DeviceId'], row['DayType'], row['Edge'], row['Start'], row['End']) == ('B', 'Weekdays', 'late', 24, 28)
    text = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(rule), clock_check=False)['alerts']['Text'][0]
    assert 'changes from free to 75 s at 7:00, an hour after the other four (6:00)' in text


def test_a_steady_early_change_of_thirty_minutes():
    def rule(device, day, minute):
        if device == 'B':
            return _plan(day, minute, end=19 * 60 + 30)
        return _all_together(device, day, minute)
    findings = _findings(rule)
    assert len(findings) and set(findings['Edge']) == {'early'}
    assert set(findings['DayType']) == {'Weekdays', 'Saturdays', 'Sundays'}


def test_a_different_plan_on_saturdays():
    def rule(device, day, minute):
        if device == 'B' and day.dayofweek == 5:
            return _plan(day, minute, start=10 * 60, end=16 * 60, cycle=120, pattern=3) if 10 * 60 <= minute < 16 * 60 \
                else _plan(day, minute)
        return _all_together(device, day, minute)
    findings = _findings(rule)
    assert findings[['DeviceId', 'DayType', 'Edge', 'Own', 'Cons']].values.tolist() == [['B', 'Saturdays', 'whole', 120.0, 75.0]]


def test_a_coordinated_pattern_that_logs_cycle_zero_is_not_free():
    """Pattern 1 logs CycleLength 0 in some bins while it runs its usual 75 s."""
    def rule(device, day, minute):
        pattern, cycle = _all_together(device, day, minute)
        return (pattern, 0) if device == 'B' and pattern == 1 and minute % 60 == 0 else (pattern, cycle)
    assert _findings(rule).empty


# ---- Plan check: what it must not find ----------------------------------------------------------------------------------

def test_a_plan_change_lost_in_a_gap_is_not_a_difference():
    """Every weekday B's data stops 5:00-6:15, so its change to 75 s at 6:00 is lost: the record carries free on
    until it next changes (here 8:00, to a new pattern). Those bins are not trusted."""
    def rule(device, day, minute):
        if device == 'B' and day.dayofweek < 5 and minute < 8 * 60:
            return (21, 0)
        if device == 'B' and day.dayofweek < 5:
            return _plan(day, minute, pattern=2)
        return _all_together(device, day, minute)

    def gap(device, ts):
        return device == 'B' and ts.dayofweek < 5 and 5 * 60 <= ts.hour * 60 + ts.minute < 6 * 60 + 15
    assert _findings(rule, skip=gap).empty


def test_a_sub_system_at_the_edge_of_a_group_is_not_alone():
    """C and D both run free at weekends: each has a neighbour running what it runs."""
    signals = _signals(['A', 'B', 'C', 'D', 'E'])

    def rule(device, day, minute):
        if device in ('C', 'D') and day.dayofweek >= 5:
            return (21, 0)
        return _all_together(device, day, minute)
    found = plan_findings(_coordination(rule), signals, REPORT_DAY)['findings']
    assert found.empty


def test_a_weekday_plan_of_its_own_is_flagged_for_review():
    """Another cycle every weekday 9:00-15:00 may be set up that way or set wrong: it is told, for the review to decide."""
    def rule(device, day, minute):
        if device == 'B' and day.dayofweek < 5 and 9 * 60 <= minute < 15 * 60:
            return (4, 100)
        return _all_together(device, day, minute)
    findings = _findings(rule)
    assert findings[['DeviceId', 'DayType', 'Edge', 'Start', 'End', 'Own', 'Cons']].values.tolist() == [
        ['B', 'Weekdays', 'whole', 36, 60, 100.0, 75.0]]


def test_a_difference_every_day_is_flagged_for_review():
    """Free every night 20:00-6:00 while the group runs 75 s: told on all three kinds of day, once as Every day."""
    def rule(device, day, minute):
        if device == 'B':
            return _plan(day, minute)
        return (21, 0) if device == 'FAR' else (1, 75)
    alerts = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(rule), clock_check=False)['alerts']
    assert alerts['DeviceId'].tolist() == ['B'] and alerts['Text'][0].startswith('Every day')


def test_one_odd_day_does_not_repeat():
    def rule(device, day, minute):
        if device == 'B' and day == REPORT_DAY - pd.Timedelta(days=1):
            return (21, 0)
        return _all_together(device, day, minute)
    assert _findings(rule).empty


def test_a_difference_that_stopped_is_not_told():
    def rule(device, day, minute):
        if device == 'B' and day.dayofweek >= 5 and day < REPORT_DAY - pd.Timedelta(days=14):
            return (21, 0)
        return _all_together(device, day, minute)
    assert _findings(rule).empty


# ---- Clock check --------------------------------------------------------------------------------------------------------

CLOCK_SIGNALS = _signals(['X', 'N1', 'N2', 'N3'])
FALL_BACK = pd.Timestamp('2026-11-01 02:00')


def _vehicle_calls(fast_since=None, devices=('X', 'N1', 'N2', 'N3'), rail=False):
    """Two emergency vehicles a day drive the line, calling each signal 20 s after the one before. X's clock is an
    hour fast from fast_since on."""
    rows = []
    for day in pd.date_range('2026-10-05', REPORT_DAY):
        for hour in (9.25, 17.5):
            start = day + pd.Timedelta(hours=hour)
            for n, device in enumerate(devices):
                t = start + pd.Timedelta(seconds=20 * n)
                if device == 'X' and fast_since is not None and t >= fast_since:
                    t += pd.Timedelta(hours=1)
                rows.append({'DeviceId': device, 'Preempt': 5 if rail else 1, 'StartTime': t, 'Duration': 30.0,
                             'IsValid': True, 'Class': 'Rail' if rail else 'Emergency Vehicle'})
    return pd.DataFrame(rows)


def test_us_clock_changes_follow_the_rule_each_year():
    changes = us_clock_changes('2026-01-01', '2027-12-31')
    assert changes == [(pd.Timestamp('2026-03-08 02:00'), 1), (pd.Timestamp('2026-11-01 02:00'), -1),
                       (pd.Timestamp('2027-03-14 02:00'), 1), (pd.Timestamp('2027-11-07 02:00'), -1)]


def test_a_signal_that_did_not_fall_back():
    found = preempt_clock_offsets(_vehicle_calls(FALL_BACK + pd.Timedelta(hours=1)), CLOCK_SIGNALS, REPORT_DAY)
    offsets = found['offsets']
    assert offsets['DeviceId'].tolist() == ['X']
    row = offsets.iloc[0]
    assert abs(row['OffsetSeconds'] - 3600) <= 60
    assert row['Since'] == FALL_BACK and row['ClockChange'] == -1
    assert set(found['judged']) >= {'X', 'N1', 'N2', 'N3'}
    alerts = build_coordination_alerts(CLOCK_SIGNALS, REPORT_DAY, preempt_calls=_vehicle_calls(FALL_BACK + pd.Timedelta(hours=1)))['alerts']
    assert alerts['Cause'].tolist() == [CAUSE_CLOCK]
    assert alerts['Text'][0].startswith('Its clock is an hour fast since the clock change on 11/1 (it did not fall back)')


def test_clocks_that_agree_raise_nothing():
    assert preempt_clock_offsets(_vehicle_calls(), CLOCK_SIGNALS, REPORT_DAY)['offsets'].empty


def test_a_clock_off_against_a_single_neighbour_is_not_judged():
    calls = _vehicle_calls(FALL_BACK, devices=('X', 'N1'))
    assert preempt_clock_offsets(calls, CLOCK_SIGNALS, REPORT_DAY)['offsets'].empty


def test_rail_calls_are_left_out():
    calls = _vehicle_calls(FALL_BACK, rail=True)
    assert preempt_clock_offsets(calls, CLOCK_SIGNALS, REPORT_DAY)['offsets'].empty


def test_a_clock_set_right_again_is_not_told():
    calls = _vehicle_calls(FALL_BACK)
    fixed = REPORT_DAY - pd.Timedelta(days=5)
    back = (calls['DeviceId'] == 'X') & (calls['StartTime'] >= fixed + pd.Timedelta(hours=1))
    calls.loc[back, 'StartTime'] -= pd.Timedelta(hours=1)
    assert preempt_clock_offsets(calls, CLOCK_SIGNALS, REPORT_DAY)['offsets'].empty


# ---- Report wiring ------------------------------------------------------------------------------------------------------

def _timeline(calls):
    return pd.DataFrame({
        'DeviceId': calls['DeviceId'], 'StartTime': calls['StartTime'],
        'EndTime': calls['StartTime'] + pd.to_timedelta(calls['Duration'], unit='s'), 'Duration': calls['Duration'],
        'IsValid': calls['IsValid'], 'EventClass': 'Preempt', 'EventValue': calls['Preempt'],
    })


def _generate(config=None, past_alerts=None, coordination=None):
    end = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
    shift = end - REPORT_DAY
    calls = _vehicle_calls(FALL_BACK + pd.Timedelta(hours=1))
    calls['StartTime'] += shift
    settings = {'verbosity': 0, 'signal_focus_signals': 0, **(config or {})}
    with patch('atspm_report.generator.generate_pdf_report', return_value={}) as pdf:
        result = ReportGenerator(settings).generate(signals=CLOCK_SIGNALS, timeline=_timeline(calls),
                                                    past_alerts=past_alerts, coordination_agg=coordination)
    return result, pdf.call_args.kwargs


def test_the_clock_check_runs_by_default_and_files_its_alert():
    result, pdf = _generate()
    alerts = result['alerts'][ALERT_TYPE]
    assert alerts['Cause'].tolist() == [CAUSE_CLOCK]
    assert len(pdf['coordination_figures']) == 2          # its region and All Regions
    assert set(result['updated_past_alerts'][ALERT_TYPE].columns) == {'DeviceId', 'Cause', 'Date'}


def test_a_repeat_clock_alert_is_ongoing():
    first, _ = _generate()
    past = {ALERT_TYPE: first['updated_past_alerts'][ALERT_TYPE].assign(Date=pd.Timestamp.today().normalize() - pd.Timedelta(days=2))}
    result, _ = _generate({'include_ongoing_issues': True}, past_alerts=past)
    assert result['alerts'][ALERT_TYPE].empty
    assert result['ongoing_alerts'][ALERT_TYPE]['Cause'].tolist() == [CAUSE_CLOCK]


def test_both_switches_off_leave_the_report_as_it_was():
    result, pdf = _generate({'coordination_clock_check_enabled': False})
    assert result['alerts'][ALERT_TYPE].empty and pdf['coordination_figures'] == []
    assert result['updated_past_alerts'][ALERT_TYPE].empty


def test_the_plan_check_is_off_by_default():
    rows = _coordination(lambda d, day, m: (21, 0) if d == 'X' and day.dayofweek >= 5 else _plan(day, m),
                         devices=['X', 'N1', 'N2', 'N3'], days=DAYS[-3:])
    result, _ = _generate({'coordination_clock_check_enabled': False}, coordination=rows)
    assert result['alerts'][ALERT_TYPE].empty


def test_table_and_chart_of_a_plan_alert():
    def rule(device, day, minute):
        if device == 'B' and day.dayofweek >= 5:
            return (21, 0)
        return _all_together(device, day, minute)
    alerts = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(rule), clock_check=False)['alerts']
    rows, total = prepare_coordination_table(alerts, SIGNALS, 'R')
    assert total == 1 and rows.columns.tolist() == ['Signal', 'What Is Wrong']
    figures = create_coordination_plots(alerts, SIGNALS)
    assert [region for _, region in figures] == ['All Regions', 'R']
    alert = alerts.iloc[0]
    assert pd.Timestamp(alert['ChartDay']).dayofweek >= 5 and set(alert['ChartGroup']) == {'A', 'C', 'D', 'E'}
    assert np.nanmax(alert['ChartOwn']) == 0


# ---- Groups by position ---------------------------------------------------------------------------------------------

def _free_in_the_middle(device, day, minute):
    """C runs free all day between B and D; B runs free on weekends."""
    if device == 'C' or (device == 'B' and day.dayofweek >= 5):
        return (21, 0)
    return _all_together(device, day, minute)


def test_a_signal_running_free_in_the_middle_of_a_group_is_in_it():
    """2B418: free all day between signals that run plans together."""
    result = plan_findings(_coordination(_free_in_the_middle), SIGNALS, REPORT_DAY, by_position=True)
    pairs, findings = result['pairs'], result['findings']
    placed = pairs[pairs['Kind'] == BY_POSITION]
    assert set(placed['DeviceId']) == {'C'} and {'B', 'D'} <= set(placed['NextDeviceId'])
    assert {'B', 'D'} <= set(group_of(pairs, 'C')) and 'C' not in group_of(pairs, 'B')
    assert attached_to(pairs, 'B') == ['C']
    mine = findings[findings['DeviceId'] == 'C']
    assert set(mine['DayType']) == {'Weekdays'} and set(mine['Own']) == {0.0} and set(mine['Cons']) == {75.0}


def test_a_signal_placed_by_position_does_not_cover_for_its_neighbours():
    """B runs free alone on weekends: C, free all day beside it, is not a neighbour sharing what B runs."""
    findings = plan_findings(_coordination(_free_in_the_middle), SIGNALS, REPORT_DAY, by_position=True)['findings']
    mine = findings[findings['DeviceId'] == 'B']
    assert set(mine['DayType']) == {'Saturdays', 'Sundays'} and set(mine['Own']) == {0.0}


def test_a_part_time_signal_at_the_end_of_a_group_is_in_it():
    """03021: coordinated with the end signal for a few hours a day, free while the rest of the line runs its plan."""
    def rule(device, day, minute):
        if device == 'E':
            return _plan(day, minute, end=9 * 60)
        return _all_together(device, day, minute)
    result = plan_findings(_coordination(rule), SIGNALS, REPORT_DAY)
    pairs, findings = result['pairs'], result['findings']
    part_time = pairs[pairs['Kind'] == PART_TIME]
    # with D, the end signal it runs with, and the signals along the line past D
    assert set(part_time['DeviceId']) == {'E'} and set(part_time['NextDeviceId']) == {'A', 'B', 'C', 'D'}
    assert pairs.loc[pairs['Kind'] == LEARNED, ['DeviceId', 'NextDeviceId']].isin(['E']).sum().sum() == 0
    assert {'C', 'D'} <= set(group_of(pairs, 'E'))
    assert set(findings['DeviceId']) == {'E'}
    row = findings[findings['DayType'] == 'Weekdays'].iloc[0]
    assert (row['Start'], row['End'], row['Own'], row['Cons']) == (36, 80, 0.0, 75.0)


def test_signals_in_the_middle_are_left_alone_until_the_position_rule_is_switched_on():
    result = plan_findings(_coordination(_free_in_the_middle), SIGNALS, REPORT_DAY)
    assert (result['pairs']['Kind'] == BY_POSITION).sum() == 0
    assert set(result['findings']['DeviceId']) == {'B'}


# ---- Confidence -----------------------------------------------------------------------------------------------------

def _inside_and_end(device, day, minute):
    """B, inside the group, runs free on weekends; A, at its end, starts an hour late on weekdays."""
    if device == 'B' and day.dayofweek >= 5:
        return (21, 0)
    if device == 'A' and day.dayofweek < 5:
        return _plan(day, minute, start=7 * 60)
    return _all_together(device, day, minute)


def test_a_signal_inside_its_group_is_high_confidence_and_one_at_an_end_is_not():
    """The 2026-11-16 review: every signal inside its group was called a real problem, every end one 'probably'."""
    alerts = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(_inside_and_end), clock_check=False)['alerts']
    assert alerts.set_index('DeviceId')['Confidence'].to_dict() == {'A': CONFIDENCE_CHECK, 'B': CONFIDENCE_HIGH}
    rows, _ = prepare_coordination_table(alerts, SIGNALS, 'R')
    assert rows['Signal'].str[0].tolist() == ['B', 'A']                     # high confidence first
    figures = create_coordination_plots(alerts, SIGNALS, 1)                # one chart each report: the high one
    assert all(figure.axes[0].get_title().startswith('B ') for figure, _ in figures)


def test_a_nearby_signal_outside_the_group_running_the_same_lowers_confidence():
    """Where corridors cross, a signal may run with the other corridor for part of the day."""
    signals = pd.concat([SIGNALS, pd.DataFrame([{'DeviceId': 'X', 'Name': 'X-Cross at Main', 'Region': 'R',
                                                 'Latitude': 45.004, 'Longitude': -121.998}])], ignore_index=True)

    def rule(device, day, minute):
        if device == 'X':
            return _plan(day, minute, cycle=120, pattern=2)
        if device == 'B' and day.dayofweek == 5 and 10 * 60 <= minute < 16 * 60:
            return (3, 120)
        return _all_together(device, day, minute)
    alerts = build_coordination_alerts(signals, REPORT_DAY, clock_check=False,
                                       coordination=_coordination(rule, devices=GROUP + ['E', 'FAR', 'X']))['alerts']
    assert alerts['DeviceId'].tolist() == ['B'] and alerts['Confidence'].tolist() == [CONFIDENCE_CHECK]
    assert alerts['Text'].iloc[0].endswith('X nearby runs the same then, so it may be set to run with it.')


def test_a_clock_alert_tells_the_signal_once():
    """2B095: its plans run at the wrong hours because its clock is off; the clock alert is the one told."""
    rows = _coordination(lambda d, day, m: (21, 0) if d == 'N1' and day.dayofweek >= 5 else _plan(day, m),
                         devices=['X', 'N1', 'N2', 'N3'])
    calls = _vehicle_calls(FALL_BACK + pd.Timedelta(hours=1), devices=('N1', 'X', 'N2', 'N3')).replace({'DeviceId': {'X': 'N1', 'N1': 'X'}})
    plan_only = build_coordination_alerts(CLOCK_SIGNALS, REPORT_DAY, coordination=rows, clock_check=False)['alerts']
    assert plan_only[['DeviceId', 'Cause']].values.tolist() == [['N1', CAUSE_SCHEDULE]]
    both = build_coordination_alerts(CLOCK_SIGNALS, REPORT_DAY, coordination=rows, preempt_calls=calls)['alerts']
    assert both[['DeviceId', 'Cause', 'Confidence']].values.tolist() == [['N1', CAUSE_CLOCK, CONFIDENCE_HIGH]]


def test_only_high_confidence_plan_alerts_stay_on_the_ongoing_list():
    end = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
    names = {'A': 'X', 'B': 'N1', 'C': 'N2', 'D': 'N3'}
    rows = _coordination(lambda d, day, m: _inside_and_end({v: k for k, v in names.items()}[d], day, m),
                         devices=list(names.values()), days=pd.date_range(end - pd.Timedelta(days=41), end))
    config = {'coordination_clock_check_enabled': False, 'coordination_plan_check_enabled': True}
    first, _ = _generate(config, coordination=rows)
    assert set(first['alerts'][ALERT_TYPE]['DeviceId']) == {'X', 'N1'}
    past = {ALERT_TYPE: first['updated_past_alerts'][ALERT_TYPE].assign(Date=pd.Timestamp.today().normalize() - pd.Timedelta(days=2))}
    again, _ = _generate({**config, 'include_ongoing_issues': True}, past_alerts=past, coordination=rows)
    assert again['alerts'][ALERT_TYPE].empty                                # neither is told a second time
    assert again['ongoing_alerts'][ALERT_TYPE]['DeviceId'].tolist() == ['N1']
    filed = again['updated_past_alerts'][ALERT_TYPE]
    assert set(filed.loc[filed['Date'] == filed['Date'].max(), 'DeviceId']) == {'X', 'N1'}   # both stay held back


def test_an_alert_the_report_had_no_room_for_is_told_on_the_next_run():
    """One alert a region each run, with its chart: the one left over is not filed, so it is new again next time."""
    end = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
    names = {'A': 'X', 'B': 'N1', 'C': 'N2', 'D': 'N3'}
    rows = _coordination(lambda d, day, m: _inside_and_end({v: k for k, v in names.items()}[d], day, m),
                         devices=list(names.values()), days=pd.date_range(end - pd.Timedelta(days=41), end))
    config = {'coordination_clock_check_enabled': False, 'coordination_plan_check_enabled': True, 'coordination_figures': 1}
    first, pdf = _generate(config, coordination=rows)
    assert first['alerts'][ALERT_TYPE]['DeviceId'].tolist() == ['N1']                     # the high confidence one
    assert len(pdf['coordination_figures']) == 2                                          # its chart, in both reports
    assert first['updated_past_alerts'][ALERT_TYPE]['DeviceId'].tolist() == ['N1']        # X is not held back yet
    past = {ALERT_TYPE: first['updated_past_alerts'][ALERT_TYPE].assign(Date=pd.Timestamp.today().normalize() - pd.Timedelta(days=1))}
    second, pdf = _generate(config, past_alerts=past, coordination=rows)
    assert second['alerts'][ALERT_TYPE]['DeviceId'].tolist() == ['X'] and len(pdf['coordination_figures']) == 2


def test_the_chart_shades_the_hours_the_alert_is_about_and_the_table_counts_the_alerts_waiting():
    alerts = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(_inside_and_end), clock_check=False)['alerts']
    listed = listed_alerts(alerts, SIGNALS, 1)
    assert listed['DeviceId'].tolist() == ['B'] and listed['RegionTotal'].tolist() == [2]
    rows, total = prepare_coordination_table(listed, SIGNALS, 'R')
    assert len(rows) == 1 and total == 2                                  # "1 of 2": the other waits for a later run
    figure = create_coordination_plots(listed, SIGNALS)[0][0]
    ax = figure.axes[0]
    assert 'Hours it is out of step' in ax.get_legend_handles_labels()[1]
    spans = [p.get_extents() for p in ax.patches]
    assert len(spans) == 1                                                # B is free 6:00-20:00 that weekend day
    shaded = ax.transData.inverted().transform(spans[0])[:, 0]
    assert shaded.round(2).tolist() == [6.0, 20.0]
