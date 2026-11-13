import pandas as pd

from atspm_report.statistical_analysis import drain_maxout_alerts, maxout_end_of_day_totals

DAY = pd.Timestamp('2026-11-11')


def _terminations(today_share):
    """Phase 1 of one signal: 0% max outs every hour of 20 earlier days, then today's shares by hour."""
    rows = []
    for day in pd.date_range(DAY - pd.Timedelta(days=20), DAY):
        for hour in range(24):
            share = today_share.get(hour, 0.0) if day == DAY else 0.0
            maxed = round(share * 20)
            rows.append((day + pd.Timedelta(hours=hour), 'A', 1, 'MaxOut', maxed))
            rows.append((day + pd.Timedelta(hours=hour), 'A', 1, 'GapOut', 20 - maxed))
    return pd.DataFrame(rows, columns=['TimeStamp', 'DeviceId', 'Phase', 'PerformanceMeasure', 'Total'])


def _alerts():
    return pd.DataFrame({'DeviceId': ['A', 'A'], 'Phase': [1, 1], 'Date': [DAY - pd.Timedelta(days=1), DAY],
                         'Alert': [0, 1], 'days_from_max': [1, 0]})


def test_a_midday_spike_that_went_back_to_normal_drains_away():
    spike = _terminations({11: 0.6, 12: 0.6, 13: 0.6})
    assert drain_maxout_alerts(_alerts(), spike, 0.1, 1.0).empty


def test_max_outs_still_going_at_the_end_of_the_day_stay_flagged():
    late = _terminations({hour: 0.6 for hour in range(14, 24)})
    kept = drain_maxout_alerts(_alerts(), late, 0.1, 1.0)
    assert len(kept) == 2
    assert kept.loc[kept['Date'] == DAY, 'MaxOutEndTotal'].iloc[0] == 5.0


def test_each_hour_is_measured_against_its_usual_share():
    terminations = _terminations({})
    usual_peak = terminations['TimeStamp'].dt.hour == 17
    terminations.loc[usual_peak & (terminations['PerformanceMeasure'] == 'MaxOut'), 'Total'] = 10
    terminations.loc[usual_peak & (terminations['PerformanceMeasure'] == 'GapOut'), 'Total'] = 10
    days = pd.DataFrame({'DeviceId': ['A'], 'Phase': [1], 'Date': [DAY]})
    assert maxout_end_of_day_totals(days, terminations, 0.1).iloc[0] == 0.0
