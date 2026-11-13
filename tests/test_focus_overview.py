"""Signal Focus overview: faults lined up with what they did, on one time axis, against the normal range."""

import numpy as np
import pandas as pd

from atspm_report.focus_overview import (
    GREEN, HELD, MAX_OUT, NO_WAIT, OTHER, SILENT, STATE_COLOR, WAIT, WHOLE, create_focus_overview, fault_rows, hourly_cycles,
    measure, no_data_hours, normal_range, phase_words, smooth, walk_share,
)
from atspm_report.generator import _no_calls_note

DAY = pd.Timestamp('2026-11-06')   # a Friday
END = DAY + pd.Timedelta(days=1)


def at(clock, day=DAY):
    return pd.Timestamp(f"{day.date()} {clock}")


def fault(detector, *periods, kind='Vehicle'):
    return {'DetectorType': kind, 'Detector': detector,
            'Periods': [{'what': what, 'start': at(a), 'end': at(b) if b != '24:00' else END} for what, a, b in periods]}


def days_of(values_by_day: dict) -> pd.Series:
    """An hourly series: each day at one level."""
    parts = [pd.Series(value, index=pd.date_range(day, day + pd.Timedelta(days=1), freq='h', inclusive='left'))
             for day, value in values_by_day.items()]
    return pd.concat(parts).sort_index()


def test_normal_range_leaves_out_a_failed_week_even_when_it_is_half_the_weekends():
    before = pd.date_range(DAY - pd.Timedelta(days=21), DAY, freq='D', inclusive='left')
    # Max outs at 10% to 20% on normal days; every green to max on the days the detection was dead, two of the four weekend days.
    dead = set(pd.date_range('2026-10-28', '2026-11-02', freq='D'))
    values = {day: (1.0 if day in dead else 0.1 + 0.01 * (k % 10)) for k, day in enumerate(before)}
    band = normal_range(days_of(values), DAY, DAY + pd.Timedelta(days=3))
    assert band['hi'].max() < 0.25 and band['lo'].min() >= 0.1
    assert band.loc[at('12:00', pd.Timestamp('2026-11-07')), 'hi'] < 0.25    # Saturday


def test_normal_range_is_empty_without_enough_days():
    values = {DAY - pd.Timedelta(days=1): 0.2, DAY - pd.Timedelta(days=2): 0.3}
    assert normal_range(days_of(values), DAY, END)['hi'].isna().all()


def test_fault_rows_group_detectors_with_much_the_same_times():
    faults = pd.DataFrame([
        fault(2, ('stuck on', '11:30', '22:02')),
        fault(3, ('stuck on', '11:34', '22:02')),
        fault(9, ('silent', '03:00', '05:00')),
        fault(4, ('counts dropped', '06:00', '09:00'), kind='Ped'),
    ])
    rows = fault_rows(faults, DAY, END)
    assert [row['label'] for row in rows] == ['Det 2, 3', 'Det 9', 'Button 4']
    assert [state for _, _, state in rows[0]['spans']] == [HELD]
    assert rows[1]['spans'][0][2] == SILENT and rows[2]['spans'][0][2] == OTHER


def test_all_down_is_one_row_and_the_others_keep_only_what_is_outside_it():
    faults = pd.DataFrame([
        fault(2, ('stuck on', '11:30', '22:02'), ('silent', '22:05', '24:00')),
        fault(3, ('stuck on', '11:30', '22:02'), ('silent', '22:05', '24:00')),
        fault(8, ('silent', '22:05', '24:00')),
    ])
    rows = fault_rows(faults, DAY, END, together=[(at('22:15'), END)], active=3)
    assert rows[0]['label'] == 'All 3 detectors' and rows[0]['spans'] == [(at('22:15'), END, SILENT)]
    # Det 8's ten minutes before everything was down is a sliver of the same fault, not a row.
    assert [row['label'] for row in rows] == ['All 3 detectors', 'Det 2, 3']
    assert rows[1]['spans'] == [(at('11:30'), at('22:02'), HELD)]


def test_many_groups_share_a_last_row():
    faults = pd.DataFrame([fault(number, ('silent', f"{number:02d}:00", f"{number:02d}:30")) for number in range(1, 10)])
    rows = fault_rows(faults, DAY, END)
    assert len(rows) == 6 and rows[-1]['label'] == '4 more'


def test_measures_by_hour():
    stamps = [at('08:00'), at('08:00'), at('09:00')]
    term = pd.DataFrame({'TimeStamp': stamps, 'Phase': [2, 4, 2], 'MaxOut': [5.0, 0.0, 10.0], 'Total': [10.0, 10.0, 10.0]})
    assert measure(MAX_OUT, term, {2, 4}).tolist() == [0.25, 1.0]
    assert measure(GREEN, term, {2}).tolist() == [10.0, 10.0]
    waits = pd.DataFrame({'TimeStamp': [at('08:00'), at('08:15'), at('08:00')], 'Phase': [2, 2, 3], 'AvgPhaseWait': [10.0, 20.0, 90.0]})
    assert measure(WAIT, waits, {2}).tolist() == [15.0]


def test_cycles_come_from_the_phase_served_most():
    term = pd.DataFrame({'DeviceId': 'A', 'TimeStamp': [at('08:00'), at('08:00'), at('09:00'), at('09:00')], 'Phase': [2, 4, 2, 4],
                         'MaxOut': 0.0, 'Total': [30.0, 12.0, 28.0, 3.0]})
    cycles = hourly_cycles(term, DAY, END)
    assert cycles['Cycles'].tolist() == [30.0, 28.0]


def test_walk_share_is_walks_over_cycles_and_blank_with_too_few_cycles():
    walks = pd.DataFrame({'TimeStamp': [at('08:00'), at('08:15'), at('09:00'), at('10:00')], 'Phase': 4, 'PedServices': [5, 10, 40, 1]})
    cycles = pd.DataFrame({'TimeStamp': [at('08:00'), at('09:00'), at('10:00')], 'Cycles': [30.0, 28.0, 2.0]})
    share = walk_share(walks, cycles, DAY, END)[4]
    assert share[at('08:00')] == 0.5 and share[at('09:00')] == 1.0 and np.isnan(share[at('10:00')])


def test_no_data_hours_merge():
    missing = pd.DataFrame({'DeviceId': 'A', 'TimeStamp': [at('01:00'), at('02:00'), at('05:00'), at('06:00')], 'MissingData': [1.0, 0.6, 0.2, 1.0]})
    assert no_data_hours(missing, 'A', DAY, END) == [(at('01:00'), at('03:00')), (at('06:00'), at('07:00'))]


def test_phase_words():
    assert phase_words({4}, [2, 4, 6]) == 'phase 4'
    assert phase_words({2, 6}, [2, 4, 6]) == 'phases 2 & 6'
    assert phase_words({2, 4, 6}, [2, 4, 6]) == 'all phases'
    assert phase_words({2, 4}) == 'phases 2 & 4'


def test_overview_draws_only_its_parts_on_one_axis():
    series = pd.Series(0.2, index=pd.date_range(DAY - pd.Timedelta(days=7), END, freq='h', inclusive='left'))
    panels = [{'kind': MAX_OUT, 'title': 'Percent Max Out (all phases)', 'series': series, 'band': normal_range(series, DAY, END)}]
    rows = fault_rows(pd.DataFrame([fault(2, ('stuck on', '11:30', '22:00'))]), DAY, END)
    ticks = [{'title': 'Preempt Calls', 'rows': [('Preempt 1', [])], 'empty': 'no calls in these days'}]
    figure = create_focus_overview('Alpha', (DAY, END), rows, panels, ticks, [(at('01:00'), at('02:00'))])
    assert [ax.get_title(loc='left') for ax in figure.axes] == ['Detector Faults', 'Percent Max Out (all phases)', 'Preempt Calls']
    assert {ax.get_xlim() for ax in figure.axes} == {figure.axes[0].get_xlim()}
    # Every part inside the figure, the last one too.
    assert all(0 <= ax.get_position().y0 and ax.get_position().y1 <= 1 for ax in figure.axes)
    legend = [text.get_text() for text in figure.legends[0].get_texts()]
    assert legend == ['Normal range', 'Measured', 'Stuck on', 'No data']
    assert create_focus_overview('Alpha', (DAY, END)) is None


def texts(ax):
    return [text.get_text() for text in ax.texts]


def test_a_fault_with_no_time_of_its_own_is_another_fault_not_shaded_behind_the_measures():
    whole = {'DetectorType': 'Vehicle', 'Detector': 2, 'Periods': [{'what': 'no daily pattern', 'start': None, 'end': None}]}
    rows = fault_rows(pd.DataFrame([whole, fault(9, ('counts dropped', '06:00', '09:00'))]), DAY, END)
    assert rows[0]['spans'] == [(DAY, END, WHOLE)] and STATE_COLOR[WHOLE] == STATE_COLOR[OTHER]
    series = pd.Series(0.2, index=pd.date_range(DAY - pd.Timedelta(days=7), END, freq='h', inclusive='left'))
    panels = [{'kind': MAX_OUT, 'title': 'Percent Max Out', 'series': series, 'band': normal_range(series, DAY, END)}]
    figure = create_focus_overview('Alpha', (DAY, END), rows, panels)
    legend = [text.get_text() for text in figure.legends[0].get_texts()]
    assert legend == ['Normal range', 'Measured', 'Other fault']
    # Shaded behind the measures: only det 9's three hours, not the whole day.
    shaded = [patch for patch in figure.axes[1].patches if patch.get_facecolor()[:3] != (1.0, 1.0, 1.0)]
    assert len(shaded) == 1


def test_smoothing_keeps_blank_hours_blank():
    values = pd.Series([0.0, 3.0, 0.0, np.nan, 6.0], index=pd.date_range(DAY, periods=5, freq='h'))
    smoothed = smooth(values)
    assert smoothed.tolist()[:3] == [1.5, 1.0, 1.5] and np.isnan(smoothed.iloc[3]) and smoothed.iloc[4] == 6.0


def test_measures_are_lines_through_the_middle_of_each_hour_and_max_out_tops_out_at_100():
    hours = pd.date_range(DAY - pd.Timedelta(days=7), END, freq='h', inclusive='left')
    series = pd.Series(1.0, index=hours)
    series[at('05:00')] = np.nan
    series[at('07:00')] = np.nan    # 06:00 alone between two blank hours
    panels = [{'kind': MAX_OUT, 'title': 'Percent Max Out', 'series': series, 'band': normal_range(series, DAY, END)}]
    ax = create_focus_overview('Alpha', (DAY, END), panels=panels).axes[0]
    line = ax.lines[0]
    x = [pd.Timestamp(value) for value in line.get_xdata()]
    assert x[0] == DAY and x[1] == DAY + pd.Timedelta(minutes=30) and x[-1] == END
    assert line.get_drawstyle() == 'default' and not line.get_clip_on()
    assert ax.lines[1].get_marker() == 'o' and len(ax.lines[1].get_xdata()) == 1
    assert ax.get_ylim()[1] == 1.0 and ax.get_ylim()[0] < 0
    assert [label.get_text() for label in ax.get_yticklabels()] == ['0%', '50%', '100%']


def test_a_wait_panel_says_when_nothing_was_measured_outside_the_no_data_hours():
    hours = pd.date_range(DAY - pd.Timedelta(days=7), END + pd.Timedelta(days=1), freq='h', inclusive='left')
    series = pd.Series(30.0, index=hours)
    series[(series.index >= at('12:00'))] = np.nan     # no calls from noon on
    panels = [{'kind': WAIT, 'title': 'Average Wait', 'series': series, 'band': normal_range(series, DAY, END), 'blank': NO_WAIT}]
    ax = create_focus_overview('Alpha', (DAY, END + pd.Timedelta(days=1)), panels=panels).axes[0]
    assert texts(ax) == [NO_WAIT]
    # The same hours with no data at all are the hatching's to tell.
    gaps = [(at('12:00'), END + pd.Timedelta(days=1))]
    ax = create_focus_overview('Alpha', (DAY, END + pd.Timedelta(days=1)), panels=panels, gaps=gaps).axes[0]
    assert texts(ax) == []


def test_a_preempt_row_with_no_calls_says_so_with_its_last_call():
    history = pd.DataFrame({'Preempt': [1, 1, 2], 'Date': [pd.Timestamp('2026-10-28'), DAY, pd.Timestamp('2026-10-30')], 'Count': [1, 0, 2]})
    assert _no_calls_note(1, [], history, DAY) == 'no calls in these days (last call Wed 10/28)'
    assert _no_calls_note(1, [pd.Timestamp('2026-11-01 07:00')], history, DAY) == 'no calls in these days (last call Sun 11/1)'
    assert _no_calls_note(3, [], history, DAY) == 'no calls in these days'
    ticks = [{'title': 'Preempt Calls', 'empty': 'no calls in these days',
              'rows': [('Preempt 1', [], 'no calls in these days (last call Wed 10/28)'), ('Preempt 2', [at('08:00')])]}]
    ax = create_focus_overview('Alpha', (DAY, END), ticks=ticks).axes[0]
    assert texts(ax) == ['no calls in these days (last call Wed 10/28)'] and len(ax.collections) == 1
