from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from atspm_report import ReportGenerator
from atspm_report import coordination_check as cc
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
    group_rows,
    listed_alerts,
    plan_findings,
    preempt_clock_offsets,
    us_clock_changes,
)


@pytest.fixture(autouse=True)
def _no_base_map(monkeypatch):
    """Maps are drawn without the base map in tests: no network."""
    monkeypatch.setattr(cc, 'MAP_TILES', None)

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
        if device == 'B' and day.day_of_week >= 5:
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
        if device == 'B' and day.day_of_week < 5:
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
        if device == 'B' and day.day_of_week == 5:
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
        if device == 'B' and day.day_of_week < 5 and minute < 8 * 60:
            return (21, 0)
        if device == 'B' and day.day_of_week < 5:
            return _plan(day, minute, pattern=2)
        return _all_together(device, day, minute)

    def gap(device, ts):
        return device == 'B' and ts.day_of_week < 5 and 5 * 60 <= ts.hour * 60 + ts.minute < 6 * 60 + 15
    assert _findings(rule, skip=gap).empty


def test_a_sub_system_at_the_edge_of_a_group_is_not_alone():
    """C and D both run free at weekends: each has a neighbour running what it runs."""
    signals = _signals(['A', 'B', 'C', 'D', 'E'])

    def rule(device, day, minute):
        if device in ('C', 'D') and day.day_of_week >= 5:
            return (21, 0)
        return _all_together(device, day, minute)
    found = plan_findings(_coordination(rule), signals, REPORT_DAY)['findings']
    assert found.empty


def test_a_weekday_plan_of_its_own_is_flagged_for_review():
    """Another cycle every weekday 9:00-15:00 may be set up that way or set wrong: it is told, for the review to decide."""
    def rule(device, day, minute):
        if device == 'B' and day.day_of_week < 5 and 9 * 60 <= minute < 15 * 60:
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
        if device == 'B' and day.day_of_week >= 5 and day < REPORT_DAY - pd.Timedelta(days=14):
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


def _generate(config=None, past_alerts=None, coordination=None, signals=None):
    end = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
    shift = end - REPORT_DAY
    calls = _vehicle_calls(FALL_BACK + pd.Timedelta(hours=1))
    calls['StartTime'] += shift
    settings = {'verbosity': 0, 'signal_focus_signals': 0, **(config or {})}
    with patch('atspm_report.generator.generate_pdf_report', return_value={}) as pdf:
        result = ReportGenerator(settings).generate(signals=CLOCK_SIGNALS if signals is None else signals,
                                                    timeline=_timeline(calls),
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
    rows = _coordination(lambda d, day, m: (21, 0) if d == 'X' and day.day_of_week >= 5 else _plan(day, m),
                         devices=['X', 'N1', 'N2', 'N3'], days=DAYS[-3:])
    result, _ = _generate({'coordination_clock_check_enabled': False}, coordination=rows)
    assert result['alerts'][ALERT_TYPE].empty


def test_a_plan_alert_is_shown_as_its_group_map_and_a_chart_of_each_day_that_differs():
    def rule(device, day, minute):
        if device == 'B' and day.day_of_week >= 5:
            return (21, 0)
        return _all_together(device, day, minute)
    alerts = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(rule), clock_check=False)['alerts']
    alert = alerts.iloc[0]
    assert set(alert['ChartMembers']) == {'A', 'C', 'D', 'E'} and len(alert['ChartWeek']) == 7
    figures = create_coordination_plots(alerts, SIGNALS)
    assert [region for _, region in figures] == ['All Regions', 'R']
    group = figures[0][0]
    assert group.map is not None and group.title == 'B-Main at 1'
    # the weekend shows B free against its group; Saturday is the same as Sunday and the weekdays show nothing
    assert [chart._suptitle.get_text() for chart in group.charts] == ['Cycle length by time of day, Sun 11/15']
    assert not group.charts[0].axes[0].patches                              # nothing shaded: the lines tell it


# ---- Groups by position ---------------------------------------------------------------------------------------------

def _free_in_the_middle(device, day, minute):
    """C runs free all day between B and D; B runs free on weekends."""
    if device == 'C' or (device == 'B' and day.day_of_week >= 5):
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
    # every kind of day: B, free on weekends beside it, is out of step itself and does not excuse it
    assert set(mine['DayType']) == {'Weekdays', 'Saturdays', 'Sundays'} and set(mine['Own']) == {0.0}
    assert set(mine['Cons']) == {75.0}


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
    if device == 'B' and day.day_of_week >= 5:
        return (21, 0)
    if device == 'A' and day.day_of_week < 5:
        return _plan(day, minute, start=7 * 60)
    return _all_together(device, day, minute)


def test_a_signal_inside_its_group_is_high_confidence_and_one_at_an_end_is_not():
    """The 2026-11-16 review: every signal inside its group was called a real problem, every end one 'probably'."""
    alerts = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(_inside_and_end), clock_check=False)['alerts']
    assert alerts.set_index('DeviceId')['Confidence'].to_dict() == {'A': CONFIDENCE_CHECK, 'B': CONFIDENCE_HIGH}
    figures = create_coordination_plots(alerts, SIGNALS, 1)                # one group, both alerts in it
    assert [group.devices for group, _ in figures] == [['A', 'B'], ['A', 'B']]


def test_a_nearby_signal_outside_the_group_running_the_same_lowers_confidence():
    """Where corridors cross, a signal may run with the other corridor for part of the day."""
    signals = pd.concat([SIGNALS, pd.DataFrame([{'DeviceId': 'X', 'Name': 'X-Cross at Main', 'Region': 'R',
                                                 'Latitude': 45.004, 'Longitude': -121.998}])], ignore_index=True)

    def rule(device, day, minute):
        if device == 'X':
            return _plan(day, minute, cycle=120, pattern=2)
        if device == 'B' and day.day_of_week == 5 and 10 * 60 <= minute < 16 * 60:
            return (3, 120)
        return _all_together(device, day, minute)
    alerts = build_coordination_alerts(signals, REPORT_DAY, clock_check=False,
                                       coordination=_coordination(rule, devices=GROUP + ['E', 'FAR', 'X']))['alerts']
    assert alerts['DeviceId'].tolist() == ['B'] and alerts['Confidence'].tolist() == [CONFIDENCE_CHECK]
    assert alerts['Text'].iloc[0].endswith('X nearby runs the same then, so it may be set to run with it.')


def test_a_clock_alert_tells_the_signal_once():
    """2B095: its plans run at the wrong hours because its clock is off; the clock alert is the one told."""
    rows = _coordination(lambda d, day, m: (21, 0) if d == 'N1' and day.day_of_week >= 5 else _plan(day, m),
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


def test_a_group_the_report_had_no_room_for_is_told_on_the_next_run():
    """One group a report each run, with its map and charts: the one left over is not filed, so it is new again."""
    end = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
    other = CLOCK_SIGNALS.assign(DeviceId=['P0', 'P1', 'P2', 'P3', 'FAR2'], Longitude=-121.5)
    other['Name'] = other['DeviceId'] + '-Second St'
    signals = pd.concat([CLOCK_SIGNALS, other], ignore_index=True)
    lines = {'X': 'A', 'N1': 'B', 'N2': 'C', 'N3': 'D', 'P0': 'A', 'P1': 'B', 'P2': 'C', 'P3': 'D'}

    def rule(device, day, minute):
        if device == 'P1':
            return (21, 0) if day.day_of_week >= 5 else _plan(day, minute)
        return _inside_and_end(lines[device], day, minute) if device in ('X', 'N1', 'N2', 'N3') else _plan(day, minute)
    rows = _coordination(rule, devices=list(lines), days=pd.date_range(end - pd.Timedelta(days=41), end))
    config = {'coordination_clock_check_enabled': False, 'coordination_plan_check_enabled': True, 'coordination_figures': 1}
    first, pdf = _generate(config, coordination=rows, signals=signals)
    assert sorted(first['alerts'][ALERT_TYPE]['DeviceId']) == ['N1', 'X']                # one group: both its alerts
    assert len(pdf['coordination_figures']) == 2                                          # in both reports
    assert sorted(first['updated_past_alerts'][ALERT_TYPE]['DeviceId']) == ['N1', 'X']    # P1 is not held back yet
    past = {ALERT_TYPE: first['updated_past_alerts'][ALERT_TYPE].assign(Date=pd.Timestamp.today().normalize() - pd.Timedelta(days=1))}
    second, pdf = _generate(config, past_alerts=past, coordination=rows, signals=signals)
    assert second['alerts'][ALERT_TYPE]['DeviceId'].tolist() == ['P1'] and len(pdf['coordination_figures']) == 2


def test_a_group_gets_a_chart_for_each_distinct_day_that_shows_a_difference():
    """B free on weekends, A late on weekdays: Sunday (Saturday was the same) and Friday (the weekdays were the same)."""
    alerts = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(_inside_and_end), clock_check=False)['alerts']
    listed = listed_alerts(alerts, SIGNALS, 1)
    assert sorted(listed['DeviceId']) == ['A', 'B'] and set(listed['RegionTotal']) == {1} and set(listed['Row']) == {'A'}
    row = group_rows(listed, SIGNALS)[0]
    assert [f'{day:%a}' for day in row['days']] == ['Sun', 'Fri'] and row['own'] == []
    assert sorted(row['parts'][0]) == ['C', 'D', 'E']


def test_a_day_with_every_line_together_gets_no_chart():
    """user, 2026-11-17: a chart where the signal runs with its group shows nothing."""
    def rule(device, day, minute):
        if device == 'B' and day.day_of_week >= 5:
            return (21, 0)
        return _all_together(device, day, minute)
    alerts = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(rule), clock_check=False)['alerts']
    together = {day: dict(lines) for day, lines in alerts.iloc[0]['ChartWeek'].items()}
    for lines in together.values():
        lines['B'] = lines['A']                                           # B with its group all week
    assert group_rows(alerts.assign(ChartWeek=[together]), SIGNALS) == []


# ---- Corridors given in the signals table ------------------------------------------------------------------------------

def _free_signal_between_two_corridors(device, day, minute):
    """C free all day between A-B and D-E; B free on weekends."""
    if device == 'C' or (device == 'B' and day.day_of_week >= 5):
        return (21, 0)
    return _all_together(device, day, minute)


def test_corridors_given_in_the_signals_table_are_used_as_they_are():
    """08108: free on its own between two groups, which nothing in the data could show (user, 2026-11-17)."""
    rows = _coordination(_free_signal_between_two_corridors)
    learned = plan_findings(rows, SIGNALS, REPORT_DAY, by_position=True)
    assert 'C' in set(learned['findings']['DeviceId'])                       # attached by position and told
    given = SIGNALS.assign(corridor=['West', 'West', 'Solo', 'East', 'East', None])
    result = plan_findings(rows, given, REPORT_DAY, by_position=True)
    pairs = result['pairs']
    assert (pairs['Kind'] != LEARNED).sum() == 0 and group_of(pairs, 'C') == []   # on its own: never judged
    assert group_of(pairs, 'B') == ['A'] and group_of(pairs, 'D') == ['E']
    assert set(result['findings']['DeviceId']) == set()                      # two to a group: no consensus to break


def test_a_corridor_signal_is_judged_against_its_corridor_whatever_it_runs():
    """A signal free all day, put in the corridor by the table, is told like any member."""
    given = SIGNALS.assign(corridor=['Main', 'Main', 'Main', 'Main', ' ', None])
    result = plan_findings(_coordination(_free_signal_between_two_corridors), given, REPORT_DAY)
    assert set(group_of(result['pairs'], 'C')) == {'A', 'B', 'D'}
    mine = result['findings'][result['findings']['DeviceId'] == 'C']
    assert set(mine['DayType']) == {'Weekdays', 'Saturdays', 'Sundays'} and set(mine['Own']) == {0.0}


def test_blank_corridors_are_grouped_from_what_they_run():
    blank = SIGNALS.assign(corridor=['', ' ', None, '', '', None])
    rows = _coordination(_free_in_the_middle)
    plain = plan_findings(rows, SIGNALS, REPORT_DAY, by_position=True)
    given = plan_findings(rows, blank, REPORT_DAY, by_position=True)
    assert given['findings'][['DeviceId', 'DayType', 'Start', 'End']].values.tolist() == \
        plain['findings'][['DeviceId', 'DayType', 'Start', 'End']].values.tolist()


# ---- Half a mile ------------------------------------------------------------------------------------------------------

def test_signals_more_than_half_a_mile_from_the_group_are_not_in_it():
    """user, 2026-11-17: signals over half a mile apart were drawn as one group."""
    far = _signals(GROUP + ['E'], spacing=0.0087)                           # about 0.6 mi apart
    pairs = plan_findings(_coordination(_all_together), far, REPORT_DAY)['pairs']
    assert pairs.empty or (pairs['Kind'] == LEARNED).sum() == 0


def test_a_signal_in_the_middle_links_the_group_across_a_longer_gap():
    """A and C 0.6 mi apart, both within half a mile of B between them, whatever B runs."""
    spaced = pd.DataFrame([{'DeviceId': d, 'Name': f'{d}-Main', 'Region': 'R', 'Latitude': 45.0 + lat, 'Longitude': -122.0}
                           for d, lat in [('A', 0.0), ('B', 0.0045), ('C', 0.0087), ('D', 0.0130), ('E', 0.0174)]])

    def rule(device, day, minute):
        return (21, 0) if device in ('B', 'D') else _plan(day, minute)
    pairs = plan_findings(_coordination(rule, devices=list('ABCDE')), spaced, REPORT_DAY)['pairs']
    learned = pairs[pairs['Kind'] == LEARNED]
    assert sorted(map(sorted, learned[['DeviceId', 'NextDeviceId']].values.tolist())) == [['A', 'C'], ['A', 'E'], ['C', 'E']]


# ---- Maps -------------------------------------------------------------------------------------------------------------

def test_a_map_without_its_base_map_is_still_drawn(monkeypatch):
    monkeypatch.setattr(cc, 'MAP_TILES', 'http://127.0.0.1:9/{z}/{x}/{y}.png')       # nothing answers there
    monkeypatch.setitem(cc._basemap, 'on', True)
    monkeypatch.setattr(cc, '_tiles', {})
    alerts = build_coordination_alerts(SIGNALS, REPORT_DAY, coordination=_coordination(_inside_and_end), clock_check=False)['alerts']
    group = create_coordination_plots(alerts, SIGNALS)[0][0]
    assert group.map is not None and cc._basemap['on'] is False and not group.map.axes[0].images


def test_without_coordinates_groups_come_from_the_corridors_and_have_no_map():
    plain = SIGNALS.drop(columns=['Latitude', 'Longitude'])
    rows = _coordination(_inside_and_end)
    assert build_coordination_alerts(plain, REPORT_DAY, coordination=rows, clock_check=False)['alerts'].empty
    given = plain.assign(corridor=['Main'] * 5 + [None])
    alerts = build_coordination_alerts(given, REPORT_DAY, coordination=rows, clock_check=False)['alerts']
    assert set(alerts['DeviceId']) == {'A', 'B'}
    group = create_coordination_plots(alerts, given)[0][0]
    assert group.map is None and len(group.charts) == 2


# ---- Groups have no holes -------------------------------------------------------------------------------------------

LINE = _signals(GROUP + ['E', 'F'], spacing=0.003)        # about a fifth of a mile apart


def _part_time_past_a_free_signal(device, day, minute):
    """E runs free all day; F, past it, runs the group's plan until 9:00 only."""
    if device == 'E':
        return (21, 0)
    if device == 'F':
        return _plan(day, minute, end=9 * 60)
    return _all_together(device, day, minute)


def test_a_signal_is_not_attached_past_a_signal_outside_the_group():
    """2B013: attached past 2B012, which ran free and was in no group, left a hole in the group."""
    rows = _coordination(_part_time_past_a_free_signal, devices=GROUP + ['E', 'F', 'FAR'])
    pairs = plan_findings(rows, LINE, REPORT_DAY)['pairs']
    assert (pairs['Kind'] != LEARNED).sum() == 0 and group_of(pairs, 'F') == []


def test_with_the_signal_between_them_in_the_group_both_are_attached():
    rows = _coordination(_part_time_past_a_free_signal, devices=GROUP + ['E', 'F', 'FAR'])
    result = plan_findings(rows, LINE, REPORT_DAY, by_position=True)
    pairs = result['pairs']
    assert set(pairs.loc[pairs['Kind'] == BY_POSITION, 'DeviceId']) == {'E'}
    assert set(pairs.loc[pairs['Kind'] == PART_TIME, 'DeviceId']) == {'F'}
    assert {'C', 'D'} <= set(group_of(pairs, 'E')) and {'C', 'D'} <= set(group_of(pairs, 'F'))
    assert set(result['findings']['DeviceId']) == {'E', 'F'}


def test_a_signal_beside_the_road_is_not_in_the_middle_of_the_group():
    """12051: on a parallel street 0.13 mi off the highway its 'group' was on."""
    signals = pd.concat([SIGNALS[SIGNALS['DeviceId'] != 'C'], pd.DataFrame([
        {'DeviceId': 'C', 'Name': 'C-Side St at 2', 'Region': 'R', 'Latitude': 45.008, 'Longitude': -121.9975}])], ignore_index=True)

    def rule(device, day, minute):
        return (21, 0) if device == 'C' else _all_together(device, day, minute)
    pairs = plan_findings(_coordination(rule), signals, REPORT_DAY, by_position=True)['pairs']
    assert 'C' not in set(pairs.loc[pairs['Kind'] != LEARNED, 'DeviceId']) and group_of(pairs, 'C') == []


def test_a_signal_of_another_system_is_not_put_in_the_group():
    """12051, 2B048, 2B080: named by the agency as another system than the signals they sat among."""
    named = SIGNALS.assign(group_name=['Main', 'Main', 'Cross St', 'Main', 'Main', 'Elsewhere'])
    rows = _coordination(_free_in_the_middle)
    assert 'C' in set(plan_findings(rows, SIGNALS, REPORT_DAY, by_position=True)['findings']['DeviceId'])
    result = plan_findings(rows, named, REPORT_DAY, by_position=True)
    assert (result['pairs']['Kind'] != LEARNED).sum() == 0 and 'C' not in set(result['findings']['DeviceId'])


def test_a_signal_is_not_attached_part_time_past_a_nearer_signal_with_no_data():
    """2B147: 2B148, right beside it and with no data, may be the one it runs with."""
    rows = _coordination(_part_time_past_a_free_signal, devices=GROUP + ['F', 'FAR'])       # nothing known of E
    pairs = plan_findings(rows, LINE, REPORT_DAY, by_position=True)['pairs']
    assert (pairs['Kind'] != LEARNED).sum() == 0


def test_a_neighbour_that_is_itself_out_of_step_does_not_excuse_a_signal():
    """2B050 beside 2B052: free at hours when 2B052, free all weekend, was the only one sharing it."""
    def rule(device, day, minute):
        weekend = day.day_of_week >= 5
        if device == 'B' and weekend:
            return (21, 0)                                              # free all weekend
        if device == 'C' and weekend and not 9 * 60 <= minute < 18 * 60:
            return (21, 0)                                              # free in the morning and evening, with B
        return _all_together(device, day, minute)
    findings = _findings(rule)
    assert set(findings['DeviceId']) == {'B', 'C'}
    mine = findings[(findings['DeviceId'] == 'C') & (findings['DayType'] == 'Saturdays')]
    assert sorted(zip(mine['Start'], mine['End'])) == [(24, 36), (72, 80)] and set(mine['Own']) == {0.0}
    whole = findings[(findings['DeviceId'] == 'B') & (findings['DayType'] == 'Saturdays')]
    assert (int(whole['Start'].min()), int(whole['End'].max())) == (24, 80)   # B's hours beside C count again too
