"""A run on today so far (report_date the day of as_of, analytics only): today's hours up to as_of judged against the same
hours of the earlier days, so it finds what started today; only what no report told before is returned."""
import os
import sys
from pathlib import Path

import matplotlib
import pandas as pd
import pytest

matplotlib.use('Agg')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atspm_report import ReportGenerator  # noqa: E402
from atspm_report.data_processing import process_missing_data  # noqa: E402
from atspm_report.generator import is_today_so_far  # noqa: E402
from atspm_report.statistical_analysis import maxout_end_of_day_totals  # noqa: E402

DATA = Path(__file__).parent / 'data'
CONFIG = {'historical_window_days': 21, 'alert_flagging_days': 7, 'figures_per_device': 1, 'verbosity': 0}
DAY_CATEGORIES = ('Missing data', 'Max outs')


@pytest.fixture(scope='module')
def inputs():
    has_data = pd.read_parquet(DATA / 'has_data.parquet')
    # the day before the last: every signal reported all of it (the last day has gaps of its own in the morning)
    day = pd.to_datetime(has_data['TimeStamp']).max().normalize() - pd.Timedelta(days=1)
    return {
        'signals': pd.read_parquet(DATA / 'signals.parquet'),
        'terminations': pd.read_parquet(DATA / 'terminations.parquet'),
        'detector_counts': pd.read_parquet(DATA / 'detector_counts.parquet'),
        'has_data': has_data,
        'pedestrian': pd.read_parquet(DATA / 'full_ped.parquet'),
    }, day


def _upto(frames, end):
    return {name: frame[pd.to_datetime(frame['TimeStamp']) < end] if isinstance(frame, pd.DataFrame)
            and 'TimeStamp' in frame.columns else frame for name, frame in frames.items()}


def _today(frames, latest, hour):
    as_of = latest + pd.Timedelta(hours=hour)
    return ReportGenerator(CONFIG).generate(**_upto(frames, as_of), report_date=latest.date(), as_of=as_of,
                                            analytics_only=True)


def _found(result, category):
    problems = result['signal_problems']
    return set() if problems is None or problems.empty else set(problems.loc[problems['Category'] == category, 'DeviceId'])


def test_it_is_today_so_far_when_the_report_day_is_the_day_of_as_of():
    assert is_today_so_far('2026-01-07', pd.Timestamp('2026-01-07 10:00'))
    assert not is_today_so_far('2026-01-06', pd.Timestamp('2026-01-07 10:00'))
    assert not is_today_so_far('2026-01-07', None)


def test_part_of_a_day_is_never_a_report(inputs):
    frames, latest = inputs
    with pytest.raises(ValueError):
        ReportGenerator(CONFIG).generate(**frames, report_date=latest.date(), as_of=latest + pd.Timedelta(hours=10))


def test_missing_data_sets_each_day_against_the_same_hours():
    stamps = pd.date_range('2026-01-01', '2026-01-03 09:45', freq='15min')
    has_data = pd.DataFrame({'TimeStamp': stamps, 'DeviceId': 'a'})
    # b reported every day until 05:00 today: half of today's ten hours are missing, none of the earlier days' first ten
    b = stamps[(stamps < '2026-01-03 05:00') & (stamps.hour < 10)]
    has_data = pd.concat([has_data, pd.DataFrame({'TimeStamp': b, 'DeviceId': 'b'})])
    missing = process_missing_data(has_data, day_end=pd.Timedelta(hours=10)).set_index(['DeviceId', 'Date'])['MissingData']
    assert missing[('a', pd.Timestamp('2026-01-03'))] == 0
    assert missing[('b', pd.Timestamp('2026-01-02'))] == 0
    assert missing[('b', pd.Timestamp('2026-01-03'))] == pytest.approx(0.5)


def test_the_max_out_drain_stops_at_the_hours_there_are():
    hours = pd.date_range('2026-01-01', '2026-01-03 09:45', freq='15min')
    rows = [{'TimeStamp': t, 'DeviceId': 'a', 'Phase': 2, 'Total': 10,
             'PerformanceMeasure': 'MaxOut' if t >= pd.Timestamp('2026-01-03') else 'GapOut'} for t in hours]
    days = pd.DataFrame({'DeviceId': ['a'], 'Phase': [2], 'Date': [pd.Timestamp('2026-01-03')]})
    # ten hours fully maxed out against a usual of none, less the drain: still well over the threshold of 1.0
    assert maxout_end_of_day_totals(days, pd.DataFrame(rows), 0.1, hours=10).iloc[0] == pytest.approx(9.0)
    # run through all 24 hours, the hours not there yet drain it away
    assert maxout_end_of_day_totals(days, pd.DataFrame(rows), 0.1).iloc[0] == pytest.approx(7.6)


def test_a_signal_that_went_quiet_this_morning_is_found_at_ten(inputs):
    frames, latest = inputs
    device = sorted(frames['signals']['DeviceId'].astype(str))[0]
    has_data = frames['has_data']
    stamps = pd.to_datetime(has_data['TimeStamp'])
    quiet = (has_data['DeviceId'].astype(str) == device) & (stamps >= latest + pd.Timedelta(hours=6))
    result = _today({**frames, 'has_data': has_data[~quiet]}, latest, 10)
    assert device in _found(result, 'Missing data')
    assert _found(_today(frames, latest, 10), 'Missing data') == set()


def test_a_phase_maxing_out_since_this_morning_is_found_at_ten(inputs):
    frames, latest = inputs
    terminations = frames['terminations'].copy()
    stamps = pd.to_datetime(terminations['TimeStamp'])
    # the busiest phase of those that seldom max out
    maxed = terminations['PerformanceMeasure'].isin(['MaxOut', 'ForceOff']) * terminations['Total']
    by_phase = terminations.assign(Maxed=maxed).groupby([terminations['DeviceId'].astype(str), 'Phase'])[['Maxed', 'Total']].sum()
    quiet = by_phase[(by_phase['Maxed'] / by_phase['Total'] < 0.05)]
    device, phase = quiet['Total'].idxmax()
    mine = ((terminations['DeviceId'].astype(str) == device) & (terminations['Phase'] == phase)
            & (stamps >= latest + pd.Timedelta(hours=6)) & (stamps < latest + pd.Timedelta(hours=10)))
    terminations.loc[mine, 'PerformanceMeasure'] = 'MaxOut'
    result = _today({**frames, 'terminations': terminations}, latest, 10)
    assert device in _found(result, 'Max outs')


@pytest.mark.parametrize('hour', [10, 14, 18])
def test_today_so_far_finds_only_what_the_whole_day_finds_on_clean_data(inputs, hour):
    """The backtest: on a day with nothing wrong in its first hours, no run during it flags what the day as a whole does not."""
    frames, latest = inputs
    whole = ReportGenerator(CONFIG).generate(**_upto(frames, latest + pd.Timedelta(days=1)), report_date=latest.date(),
                                             analytics_only=True)
    part = _today(frames, latest, hour)
    problems = part['signal_problems']
    assert problems is None or problems.empty or set(problems['LastDay']) == {latest}
    for category in DAY_CATEGORIES:
        assert _found(part, category) <= _found(whole, category), category
