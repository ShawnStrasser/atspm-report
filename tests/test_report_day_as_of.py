"""report_date, as_of and analytics_only: rows after the report day never change the report, an analytics-only run
leaves every state as it was, and today's hours tell which problems still show (today.today_flags)."""
import os
import sys
from pathlib import Path

import matplotlib
import pandas as pd
import pytest

matplotlib.use('Agg')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atspm_report import ReportGenerator  # noqa: E402
from atspm_report import today as today_check  # noqa: E402

DATA = Path(__file__).parent / 'data'
CONFIG = {'historical_window_days': 21, 'alert_flagging_days': 7, 'figures_per_device': 1, 'verbosity': 0}


@pytest.fixture(scope='module')
def inputs():
    has_data = pd.read_parquet(DATA / 'has_data.parquet')
    latest = pd.to_datetime(has_data['TimeStamp']).max().normalize()
    return {
        'signals': pd.read_parquet(DATA / 'signals.parquet'),
        'terminations': pd.read_parquet(DATA / 'terminations.parquet'),
        'detector_counts': pd.read_parquet(DATA / 'detector_counts.parquet'),
        'has_data': has_data,
        'pedestrian': pd.read_parquet(DATA / 'full_ped.parquet'),
    }, latest


def _cut(frames, end):
    out = {}
    for name, frame in frames.items():
        if isinstance(frame, pd.DataFrame) and 'TimeStamp' in frame.columns:
            frame = frame[pd.to_datetime(frame['TimeStamp']) < end]
        out[name] = frame
    return out


def _same(a, b):
    a = a.reset_index(drop=True) if isinstance(a, pd.DataFrame) else a
    b = b.reset_index(drop=True) if isinstance(b, pd.DataFrame) else b
    if isinstance(a, pd.DataFrame):
        pd.testing.assert_frame_equal(a, b, check_dtype=False)
    else:
        assert a == b


def test_rows_after_the_report_day_never_change_the_report(inputs):
    frames, latest = inputs
    report_day = latest - pd.Timedelta(days=1)     # the latest day of data is "today"
    with_today = ReportGenerator(CONFIG).generate(**frames, report_date=report_day.date(), analytics_only=True)
    without = ReportGenerator(CONFIG).generate(**_cut(frames, latest), report_date=report_day.date(), analytics_only=True)
    for alert_type in without['alerts']:
        _same(with_today['alerts'][alert_type], without['alerts'][alert_type])
    _same(with_today['signal_problems'], without['signal_problems'])
    # a full report run: the same state for the next run too
    full_with = ReportGenerator(CONFIG).generate(**frames, report_date=report_day.date())
    full_without = ReportGenerator(CONFIG).generate(**_cut(frames, latest), report_date=report_day.date())
    for alert_type in full_without['updated_past_alerts']:
        _same(full_with['updated_past_alerts'][alert_type], full_without['updated_past_alerts'][alert_type])
    _same(full_with['updated_atspm_detector_minutes'], full_without['updated_atspm_detector_minutes'])


def test_an_analytics_only_run_draws_nothing_and_changes_no_state(inputs):
    frames, latest = inputs
    past = {'maxout': pd.DataFrame({'DeviceId': ['x'], 'Phase': [1], 'Date': [latest - pd.Timedelta(days=30)]})}
    minutes = pd.DataFrame({'DeviceId': ['1'], 'DetectorType': ['Vehicle'], 'Detector': [1], 'Date': [latest],
                            'BadMinutes': [10.0], 'SuspectMinutes': [0.0]})
    shown = pd.DataFrame({'AlertType': ['maxout'], 'Key': ['1|2'], 'LastShown': [latest]})
    result = ReportGenerator(CONFIG).generate(**frames, past_alerts=past, atspm_detector_minutes=minutes,
                                              ongoing_last_shown=shown, analytics_only=True)
    assert result['reports'] == {}
    assert set(result['updated_past_alerts']) == {'maxout'}
    _same(result['updated_past_alerts']['maxout'], past['maxout'])
    assert result['updated_atspm_detector_minutes'] is minutes
    assert result['updated_ongoing_last_shown'] is shown
    # what a report run on the same inputs finds
    full = ReportGenerator(CONFIG).generate(**frames, past_alerts=past, atspm_detector_minutes=minutes,
                                            ongoing_last_shown=shown)
    for alert_type in full['alerts']:
        _same(result['alerts'][alert_type], full['alerts'][alert_type])


def test_as_of_adds_today_to_the_problems(inputs):
    frames, latest = inputs
    report_day = latest - pd.Timedelta(days=1)
    result = ReportGenerator(CONFIG).generate(**frames, report_date=report_day.date(),
                                              as_of=latest + pd.Timedelta(hours=6), analytics_only=True)
    problems = result['signal_problems']
    assert today_check.TODAY_COLUMN in problems.columns
    assert set(problems[today_check.TODAY_COLUMN].dropna().unique()) <= {True, False}
    # without as_of: no Today column (the output is as before)
    plain = ReportGenerator(CONFIG).generate(**frames, report_date=report_day.date(), analytics_only=True)
    assert today_check.TODAY_COLUMN not in plain['signal_problems'].columns


def _terminations(rows):
    return pd.DataFrame(rows, columns=['TimeStamp', 'DeviceId', 'Phase', 'PerformanceMeasure', 'Total'])


def test_today_sets_the_same_clock_hours_against_each_other():
    end = pd.Timestamp('2026-11-24')        # midnight after the report day
    as_of = end + pd.Timedelta(hours=6)
    problems = pd.DataFrame([
        {'DeviceId': '1', 'Category': 'Max outs', 'OnPhases': [2]},
        {'DeviceId': '1', 'Category': 'Max outs', 'OnPhases': [4]},
        {'DeviceId': '1', 'Category': 'Max outs', 'OnPhases': [6]},     # no max outs those hours yesterday: can't tell
        {'DeviceId': '1', 'Category': 'Phase skips', 'OnPhases': [3]},
        {'DeviceId': '1', 'Category': 'Missing data', 'OnPhases': []},
        {'DeviceId': '2', 'Category': 'Missing data', 'OnPhases': []},
        {'DeviceId': '1', 'Category': 'Detectors', 'OnPhases': [2]},     # not judged here
    ])
    yesterday, today = end - pd.Timedelta(days=1, hours=-2), end + pd.Timedelta(hours=2)
    terms = _terminations([
        (yesterday, 1, 2, 'MaxOut', 8), (yesterday, 1, 2, 'GapOut', 2),      # 80% yesterday
        (today, 1, 2, 'MaxOut', 1), (today, 1, 2, 'GapOut', 9),              # 10% today: cleared
        (yesterday, 1, 4, 'MaxOut', 6), (yesterday, 1, 4, 'GapOut', 4),      # 60% yesterday
        (today, 1, 4, 'MaxOut', 5), (today, 1, 4, 'GapOut', 5),              # 50% today: still
        (yesterday, 1, 6, 'GapOut', 10), (today, 1, 6, 'GapOut', 10),
        # a max out after as_of does not count
        (as_of + pd.Timedelta(hours=1), 1, 2, 'MaxOut', 50),
    ])
    waits = pd.DataFrame([(yesterday, 1, 3, 2), (today, 1, 3, 0)], columns=['TimeStamp', 'DeviceId', 'Phase', 'TotalSkips'])
    bins = pd.date_range(end, as_of, freq='15min', inclusive='left')
    has_data = pd.concat([pd.DataFrame({'TimeStamp': bins, 'DeviceId': 1}),                  # all of today
                          pd.DataFrame({'TimeStamp': bins[:8], 'DeviceId': 2})])            # a third of today
    flags = today_check.today_flags(problems, end, as_of, terms, waits, has_data)
    assert list(flags) == [False, True, None, False, False, True, None]
    # under an hour of today: nothing to tell
    assert today_check.today_flags(problems, end, end + pd.Timedelta(minutes=45), terms, waits, has_data).isna().all()
