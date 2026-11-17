"""Phase timing check: split failures without max outs (maxing out without a queue is not checked)."""

import duckdb
import numpy as np
import pandas as pd
import pytest

from atspm_report import timing_checks as tc
from atspm_report.measure_inputs import SPAN_COLUMNS

DEVICE = 'dev-1'
REPORT_DAY = pd.Timestamp('2026-11-12')            # a Thursday; 11-11 is Veterans Day
DAYS = [pd.Timestamp('2026-11-09'), pd.Timestamp('2026-11-10'), REPORT_DAY]
ALL = {tc.CHECK_SPLIT_FAIL}


def ordinary(day, phase, hour):
    """A phase gapping out on every cycle, no split failures, waits of 30 s, free."""
    return dict(gap=6, maxout=0, forceoff=0, sf=0, cycles=6, wait=30.0, cl=0)


def tables(rule=ordinary, phases=(2, 4), days=DAYS, device=DEVICE):
    """The four 15-minute tables of one signal: rule(day, phase, hour) gives each bin of that hour (counts per bin)."""
    sf, term, wait, coord = [], [], [], []
    for day in days:
        for hour in range(24):
            for quarter in range(4):
                ts = day + pd.Timedelta(hours=hour, minutes=15 * quarter)
                cl = None
                for phase in phases:
                    b = rule(day, phase, hour)
                    cl = b['cl']
                    for measure, key in (('GapOut', 'gap'), ('MaxOut', 'maxout'), ('ForceOff', 'forceoff')):
                        if b[key]:
                            term.append((ts, device, phase, measure, b[key]))
                    if b['cycles']:
                        sf.append((ts, device, phase, 0.0, 0.5, 0.5, b['sf'], b['cycles']))
                    wait.append((ts, device, phase, b['wait'], b['wait'], 0.0, 6))
                coord.append((ts, device, 1 if cl else 21, cl, cl, 0))
    return (pd.DataFrame(sf, columns=['TimeStamp', 'DeviceId', 'Phase', 'Green_Time', 'Green_Occupancy', 'Red_Occupancy',
                                      'Split_Failure', 'Cycles']),
            pd.DataFrame(term, columns=['TimeStamp', 'DeviceId', 'Phase', 'PerformanceMeasure', 'Total']),
            pd.DataFrame(wait, columns=['TimeStamp', 'DeviceId', 'Phase', 'AvgPhaseWait', 'MaxPhaseWait', 'TotalSkips', 'Samples']),
            pd.DataFrame(coord, columns=['TimeStamp', 'DeviceId', 'Pattern', 'CycleLength', 'ActualCycleLength', 'ActualOffset']))


def run(rule=ordinary, faults=None, checks=ALL, **kw):
    sf, term, wait, coord = tables(rule, **{k: kw.pop(k) for k in list(kw) if k in ('phases', 'days')})
    return tc.build_timing_alerts(sf, term, wait, coord, faults, REPORT_DAY, checks, **kw)


def queue_left(day, phase, hour):
    """Phase 4 fails 2 of 6 cycles at 10-13 h while gapping out on all of them."""
    b = ordinary(day, phase, hour)
    if phase == 4 and 10 <= hour < 13:
        b.update(sf=2)
    return b


def maxing(day, phase, hour):
    """Phase 2 maxes out on 5 of 6 cycles at 8-11 h with no split failures."""
    b = ordinary(day, phase, hour)
    if phase == 2 and 8 <= hour < 11:
        b.update(gap=1, maxout=5)
    return b




def test_split_failures_without_max_outs():
    alerts = run(queue_left)
    assert list(alerts['Check']) == [tc.CHECK_SPLIT_FAIL]
    row = alerts.iloc[0]
    assert (row['DeviceId'], row['Phase'], row['FromHour'], row['ToHour']) == (DEVICE, 4, 10, 13)
    assert row['Date'] == REPORT_DAY
    assert 'gaps out and leaves a queue on 33 % of greens between 10:00 and 13:00' in row['Text']
    assert 'gap time is too short, or a stop-bar zone is not extending the green' in row['Text']
    assert 'never runs to its limit' in row['Text'] and 'Seen on 3 of the last 3 weekdays' in row['Text']
    assert row['ChartFlagged'][10:13] == [True] * 3 and not any(row['ChartFlagged'][:10])
    assert len(row['ChartSplitFail']) == 24 and row['ChartSplitFail'][11] == pytest.approx(100 / 3)


def test_failures_matched_by_max_outs_are_not_early_gap_outs():
    def rule(day, phase, hour):
        b = queue_left(day, phase, hour)
        if phase == 4 and 10 <= hour < 13:
            b.update(gap=4, maxout=2)
        return b
    assert run(rule).empty


def test_maxing_out_without_a_queue_is_not_a_problem():
    assert run(maxing).empty
    assert set(tc.CONFIG_SWITCHES) == {tc.CHECK_SPLIT_FAIL}


def test_checks_run_only_when_chosen():
    assert run(queue_left, checks=set()).empty


def test_max_recall_all_day_is_left_out():
    def rule(day, phase, hour):
        b = ordinary(day, phase, hour)
        if phase == 2:
            b.update(gap=0, maxout=6)
        return b
    assert run(rule).empty


def test_a_single_hour_is_not_a_span():
    def rule(day, phase, hour):
        b = ordinary(day, phase, hour)
        if phase == 4 and hour == 9:
            b.update(sf=2)
        return b
    assert run(rule).empty


def test_fault_on_the_phase_masks_its_hours():
    faults = pd.DataFrame([(DEVICE, 4, 'stop bar', d + pd.Timedelta(hours=9), d + pd.Timedelta(hours=14)) for d in DAYS],
                          columns=SPAN_COLUMNS)
    assert run(queue_left, faults=faults).empty
    other = faults.assign(Phase=2)
    assert len(run(queue_left, faults=other)) == 1


def test_hour_needs_its_cycles_measured():
    def rule(day, phase, hour):
        b = queue_left(day, phase, hour)
        if phase == 4 and 10 <= hour < 13:
            b.update(cycles=3, sf=2)       # half the cycles measured
        return b
    assert run(rule).empty


def test_too_few_cycles():
    def rule(day, phase, hour):
        b = queue_left(day, phase, hour)
        if phase == 4 and 10 <= hour < 13:
            b.update(gap=4, cycles=4, sf=2)    # 16 cycles an hour
        return b
    assert run(rule).empty


def test_phase_resting_in_green():
    def rule(day, phase, hour):
        b = queue_left(day, phase, hour)
        if phase == 4:
            b.update(wait=8.0)
        return b
    assert run(rule).empty


def test_saturated_signal():
    def rule(day, phase, hour):
        b = ordinary(day, phase, hour)
        if 10 <= hour < 13:
            b.update(sf=2)
        return b
    assert len(run(rule, phases=(2, 4))) == 2
    assert run(rule, phases=(2, 4, 6)).empty


def test_zones_failing_overnight():
    def rule(day, phase, hour):
        b = queue_left(day, phase, hour)
        if phase == 4 and hour < 5:
            b.update(sf=3)
        return b
    assert run(rule).empty


def test_coordinated_phase_and_force_offs():
    def rule(day, phase, hour):
        b = ordinary(day, phase, hour)
        b.update(cl=150)        # coordinated, six cycles an hour fit 150 s
        if phase == 2:
            b.update(gap=0, maxout=0, forceoff=6, sf=0)
        if phase == 4 and 10 <= hour < 13:
            b.update(gap=0, forceoff=6, sf=0)
        return b
    # Under coordination nothing maxes out (force offs are by design) and the coordinated phase is left out.
    assert run(rule).empty


def test_repeat_rule():
    def rule(day, phase, hour):
        return queue_left(day, phase, hour) if day == REPORT_DAY else ordinary(day, phase, hour)
    assert run(rule).empty
    alerts = run(rule, repeat_days=1)
    assert len(alerts) == 1 and 'Seen on 1 of the last 3 weekdays' in alerts.iloc[0]['Text']


def test_holiday():
    assert tc.judged_days('2026-11-11') == []
    assert tc.judged_days(REPORT_DAY) == [REPORT_DAY, pd.Timestamp('2026-11-10'), pd.Timestamp('2026-11-09')]
    assert tc.judged_days('2026-11-14') == [pd.Timestamp('2026-11-14'), pd.Timestamp('2026-11-07'), pd.Timestamp('2026-10-31')]


def test_missing_input_gives_nothing():
    sf, term, wait, coord = tables(queue_left)
    for args in ((None, term, wait, coord), (sf, None, wait, coord), (sf, term, None, coord), (sf, term, wait, None),
                 (sf.head(0), term, wait, coord)):
        result = tc.build_timing_alerts(*args, None, REPORT_DAY, ALL)
        assert result.empty and list(result.columns) == tc.ALERT_COLUMNS
    assert tc.build_timing_alerts(sf, term, wait, coord, None, None, ALL).empty


def test_relations_are_read_by_day():
    sf, term, wait, coord = tables(queue_left)
    con = duckdb.connect()
    try:
        rels = []
        for name, frame in (('sf', sf), ('term', term), ('wait', wait), ('coord', coord)):
            con.register(name + '_df', frame)
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM {name}_df")
            rels.append(con.table(name))
        alerts = tc.build_timing_alerts(*rels, None, REPORT_DAY, ALL)
    finally:
        con.close()
    assert list(alerts['Check']) == [tc.CHECK_SPLIT_FAIL]


def test_signals_limit_the_devices():
    sf, term, wait, coord = tables(queue_left)
    signals = pd.DataFrame({'DeviceId': ['other'], 'Name': ['00001-X'], 'Region': ['Region 1']})
    assert tc.build_timing_alerts(sf, term, wait, coord, None, REPORT_DAY, ALL, signals=signals).empty


SIGNALS = pd.DataFrame({'DeviceId': [DEVICE], 'Name': ['04057-US20 at I-5'], 'Region': ['Region 3']})


def test_table_and_charts():
    alerts = run(queue_left)
    rows, total = tc.prepare_timing_table(alerts, SIGNALS, 'Region 3', 10, tc.CHECK_SPLIT_FAIL)
    assert total == 1 and list(rows.columns) == ['Signal', 'Phase', 'What Is Wrong']
    rows, total = tc.prepare_timing_table(alerts, SIGNALS, 'Region 1', 10, tc.CHECK_SPLIT_FAIL)
    assert total == 0 and rows.empty
    ongoing = alerts.assign(OngoingSince=pd.Timestamp('2026-11-02'))
    rows, _ = tc.prepare_timing_table(ongoing, SIGNALS, None, 10, tc.CHECK_SPLIT_FAIL)
    assert len(rows.columns) == 4
    figures = tc.create_timing_plots(alerts, SIGNALS, 10)
    assert len(figures) == 2 and {scope for _, scope in figures} == {'All Regions', 'Region 3'}
    split = tc.section_figures(figures, tc.CHECK_SPLIT_FAIL)
    assert len(split) == 2
    ax = split[0][0].axes[0]
    assert ax.get_title() .startswith('04057 Ph4 by hour, Thu 11/12')
    assert ax.get_legend() is not None
    assert [t.get_text() for t in ax.get_legend().get_texts()] == ['Split failures', 'Ran to its limit', 'Flagged hours']
    assert len(tc.create_timing_plots(alerts, SIGNALS, 0)) == 0
    assert tc.create_timing_plots(None, SIGNALS) == [] and tc.create_timing_plots(alerts.head(0), SIGNALS) == []
