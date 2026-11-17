"""The coordination impact chart, and the report with and without atspm's per-phase measures."""

from pathlib import Path

import numpy as np
import pandas as pd

from atspm_report import ReportGenerator
from atspm_report import measure_inputs as mi
from atspm_report.coordination_check import create_coordination_impact_chart, create_coordination_plots
from atspm_report.impact_chart import impact_data

SIGNALS = pd.DataFrame({'DeviceId': ['A', 'B', 'C', 'D'], 'Name': ['08043-Odd', '08050-Next', '08046-Far', '08055-Far'],
                        'Region': 'Region 1', 'Latitude': [42.1973, 42.1967, 42.1990, 42.1962],
                        'Longitude': [-122.7135, -122.7139, -122.7163, -122.7107]})
DAYS = pd.date_range('2026-10-26', '2026-11-15')
WEEKENDS = [d for d in DAYS if d.dayofweek >= 5]
WEEKDAYS = [d for d in DAYS if d.dayofweek < 5]


def hourly(worse=True, devices=('A', 'B', 'C', 'D'), vehicles=60.0):
    """Hourly arrivals and delay: A (and B, the next signal) worse at weekends when `worse`."""
    rows = []
    for device in devices:
        for day in DAYS:
            for h in range(24):
                weekend = day.dayofweek >= 5
                bad = worse and weekend and device in ('A', 'B')
                aog, delay = (0.30, 20.0) if bad else (0.45, 10.0)
                rows.append((device, 8, day + pd.Timedelta(hours=h), vehicles, vehicles * aog, vehicles * delay))
    return pd.DataFrame(rows, columns=['DeviceId', 'Phase', 'hr', 'veh', 'gre', 'dsum'])


def schedule_alert(matched=None):
    runs = [{'DayType': kind, 'Start': '0:00', 'End': '0:00', 'Days': [d for d in WEEKENDS if mi.day_kind(d) == kind]}
            for kind in ('Saturdays', 'Sundays')]
    return pd.Series({'DeviceId': 'A', 'Cause': 'Schedule', 'Group': ['B', 'C', 'D'], 'Runs': runs,
                      'Matched': matched if matched is not None else [{'DayType': 'Saturdays', 'Days': WEEKDAYS}],
                      'ClockOffsetSeconds': np.nan, 'ClockSince': pd.NaT})


def test_weekend_schedule_compares_with_its_weekdays_when_no_weekend_matched():
    data = impact_data(schedule_alert(), ['B', 'C', 'D'], schedule_alert()['Runs'], schedule_alert()['Matched'], hourly(), SIGNALS)
    assert data['bad_label'] == 'weekends' and data['good_label'] == 'weekdays'
    assert data['next'] == 'B'                       # the nearest member of the group
    assert data['shade'] == []                       # all day: nothing to shade
    lines = data['lines']
    assert lines['own_bad']['dpv'].iloc[12] == 20.0 and lines['own_good']['dpv'].iloc[12] == 10.0
    assert lines['group']['aog'].iloc[12] == 45.0    # median of B (30), C and D (45) on the weekends


def test_chart_is_drawn_with_two_panels_and_its_legend_above():
    figure = create_coordination_impact_chart(schedule_alert(), ['B', 'C', 'D'], schedule_alert()['Runs'],
                                              schedule_alert()['Matched'], {'hourly': hourly(), 'signals': SIGNALS})
    assert figure is not None and len(figure.axes) == 2
    labels = [t.get_text() for t in figure.legends[0].get_texts()]
    assert labels[:2] == ['08043, weekends', '08043, weekdays'] and '08050 (nearest), weekends' in labels
    top = figure.axes[0].get_position().y1
    assert figure.legends[0].get_window_extent(figure.canvas.get_renderer()).y0 >= top * figure.bbox.height - 1
    assert figure.report_own_proportions


def test_no_chart_without_a_difference_too_little_data_or_measures():
    measures = {'hourly': hourly(worse=False), 'signals': SIGNALS}
    alert = schedule_alert()
    assert create_coordination_impact_chart(alert, ['B'], alert['Runs'], alert['Matched'], measures) is None
    few = hourly()[lambda t: t['hr'] >= '2026-11-14']          # one weekend, no weekdays
    assert create_coordination_impact_chart(alert, ['B'], alert['Runs'], alert['Matched'], {'hourly': few, 'signals': SIGNALS}) is None
    quiet = {'hourly': hourly(vehicles=10.0), 'signals': SIGNALS}  # under 20 arrivals an hour
    assert create_coordination_impact_chart(alert, ['B'], alert['Runs'], alert['Matched'], quiet) is None
    assert create_coordination_impact_chart(alert, ['B'], alert['Runs'], alert['Matched'], None) is None


def test_faulting_hours_are_gaps():
    hours = hourly()
    hours = hours[~((hours['DeviceId'] == 'A') & (hours['hr'].dt.hour == 12))]   # left out by measure_inputs.unmasked
    data = impact_data(schedule_alert(), ['B', 'C', 'D'], schedule_alert()['Runs'], schedule_alert()['Matched'], hours, SIGNALS)
    assert np.isnan(data['lines']['own_bad']['dpv'].iloc[12]) and not np.isnan(data['lines']['own_bad']['dpv'].iloc[13])


def test_clock_alert_without_days_before_the_offset_has_no_chart():
    clock = pd.Series({'DeviceId': 'A', 'Cause': 'Clock', 'Group': [], 'Runs': [], 'Matched': [],
                       'ClockOffsetSeconds': 3600.0, 'ClockSince': pd.Timestamp('2026-10-01')})
    assert create_coordination_impact_chart(clock, ['B', 'C'], [], [], {'hourly': hourly(), 'signals': SIGNALS}) is None
    # With days before it, the signal's own stamps are moved back an hour and compared.
    since = clock.copy()
    since['ClockSince'] = pd.Timestamp('2026-11-07')
    worse_since = hourly(worse=False)
    late = (worse_since['DeviceId'] == 'A') & (worse_since['hr'] >= '2026-11-07')
    worse_since.loc[late, 'dsum'] *= 2
    data = impact_data(since, ['B', 'C'], [], [], worse_since, SIGNALS)
    assert data['bad_label'] == 'since 11/7' and data['good_label'] == 'before'


# ---- the report with and without the measures --------------------------------------------------------------------------

DATA = Path(__file__).parent / 'data'


def _inputs():
    signals = pd.read_parquet(DATA / 'signals.parquet')
    return {'signals': signals, 'terminations': pd.read_parquet(DATA / 'terminations.parquet'),
            'has_data': pd.read_parquet(DATA / 'has_data.parquet')}


def _measures(device):
    stamps = pd.date_range(pd.Timestamp.now().normalize() - pd.Timedelta(days=7), periods=96 * 7, freq='15min')
    base = pd.DataFrame({'TimeStamp': stamps, 'DeviceId': device, 'Phase': 2})
    return {'split_failures': base.assign(Split_Failure=1, Cycles=4, Green_Occupancy=0.5, Red_Occupancy=0.2),
            'arrival_on_green': base.assign(Total_Actuations=20, Percent_AOG=0.5),
            'platoon_ratio': base.assign(Total_Actuations=20, Green_Actuations=10, Green_Seconds=300.0),
            'approach_delay': base.assign(Delay_Seconds=100.0, Queued_Delay_Seconds=0.0)}


def test_measures_given_with_their_checks_off_leave_the_report_as_it_was():
    inputs = _inputs()
    config = {'figures_per_device': 1, 'verbosity': 0}
    before = ReportGenerator(config).generate(**inputs)
    after = ReportGenerator(config).generate(**inputs, **_measures(str(inputs['signals']['DeviceId'].iloc[0])))
    assert sorted(before['reports']) == sorted(after['reports'])
    for alert_type, frame in before['alerts'].items():
        pd.testing.assert_frame_equal(frame.reset_index(drop=True), after['alerts'][alert_type].reset_index(drop=True))
    for alert_type in ('delay_rise', 'split_failure_trend', 'platoon_ratio_trend', 'phase_timing'):
        assert after['alerts'][alert_type].empty and after['updated_past_alerts'][alert_type].empty


def test_checks_switched_on_without_their_tables_do_nothing():
    inputs = _inputs()
    config = {'figures_per_device': 1, 'verbosity': 0, 'delay_rise_check_enabled': True, 'split_failure_trend_enabled': True,
              'platoon_ratio_trend_enabled': True, 'split_failures_no_maxout_check_enabled': True,
              }
    result = ReportGenerator(config).generate(**inputs)
    assert all(result['alerts'][t].empty for t in ('delay_rise', 'split_failure_trend', 'platoon_ratio_trend', 'phase_timing'))


def test_signal_focus_does_not_read_the_new_alert_types():
    from atspm_report.signal_focus import collect_problems
    row = pd.DataFrame({'DeviceId': ['A'], 'Date': [pd.Timestamp('2026-11-13')], 'Phase': [4], 'Text': ['x']})
    told, _ = collect_problems({'delay_rise': row, 'split_failure_trend': row, 'platoon_ratio_trend': row,
                                'phase_timing': row.assign(Check='Split failures without max outs')})
    assert told.empty


def test_coordination_charts_are_as_before_without_an_impact_chart():
    alert = schedule_alert().to_dict()
    alert.update({'Date': pd.Timestamp('2026-11-15'), 'Text': 'x', 'ChartDay': pd.Timestamp('2026-11-15'),
                  'ChartOwn': [75.0] * 96, 'ChartGroup': {'08050': [75.0] * 96}, 'ChartCalls': None})
    alerts = pd.DataFrame([alert])
    generator = ReportGenerator({'verbosity': 0})
    plain = create_coordination_plots(alerts, SIGNALS, 10)
    flat = generator._coordination_figures(alerts, SIGNALS, hourly(worse=False))
    assert [scope for _, scope in flat] == [scope for _, scope in plain]
    with_impact = generator._coordination_figures(alerts, SIGNALS, hourly())
    assert [scope for _, scope in with_impact] == ['All Regions', 'All Regions', 'Region 1', 'Region 1']
    assert len(with_impact[1][0].axes) == 2
