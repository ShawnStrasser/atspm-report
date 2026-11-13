"""Missing data and system outages shown hour by hour, so an outage can be found in time. Which signals alert stays daily."""

import pandas as pd

from atspm_report.data_processing import missing_data_by_hour, region_missing_by_hour
from atspm_report.table_generation import hours_text, prepare_missing_data_alerts_table, prepare_system_outages_table

SIGNALS = pd.DataFrame({'DeviceId': ['A', 'B'], 'Name': ['Alpha', 'Bravo'], 'Region': ['Region 1', 'Region 1']})


def bins(device, start, end):
    return pd.DataFrame({'DeviceId': device, 'TimeStamp': pd.date_range(start, end, freq='15min', inclusive='left')})


def test_each_hour_is_its_share_of_missing_records():
    # A reports all of 11/3 but 02:00-04:00 and one record of 10:00; B reports all day.
    has_data = pd.concat([bins('A', '2026-11-03', '2026-11-03 02:00'), bins('A', '2026-11-03 04:00', '2026-11-04'),
                          bins('B', '2026-11-03', '2026-11-04')])
    has_data = has_data[(has_data['DeviceId'] != 'A') | (has_data['TimeStamp'] != pd.Timestamp('2026-11-03 10:15'))]
    hourly = missing_data_by_hour(has_data).set_index(['DeviceId', 'TimeStamp'])['MissingData']
    assert len(hourly) == 2 * 24
    assert hourly[('A', pd.Timestamp('2026-11-03 02:00'))] == 1.0 and hourly[('A', pd.Timestamp('2026-11-03 03:00'))] == 1.0
    assert hourly[('A', pd.Timestamp('2026-11-03 10:00'))] == 0.25
    assert hourly['B'].eq(0).all()


def test_the_repeated_dst_hour_and_what_follows_a_feed_stop_are_not_missing():
    # 11/1/2026 is the first Sunday of November: its 1:00 am hour is left out of the data.
    has_data = bins('A', '2026-11-01', '2026-11-02')
    has_data = has_data[has_data['TimeStamp'].dt.hour != 1]
    hourly = missing_data_by_hour(has_data)
    assert len(hourly) == 23 and hourly['MissingData'].eq(0).all()
    # The feed stopped at 18:30: 18:00 expects its two records before the stop, nothing after counts.
    stopped = missing_data_by_hour(bins('A', '2026-11-03', '2026-11-03 18:30'), stop=pd.Timestamp('2026-11-03 18:30'))
    assert stopped['TimeStamp'].max() == pd.Timestamp('2026-11-03 18:00') and stopped['MissingData'].eq(0).all()


def test_hours_text_gives_the_runs_newest_first():
    hours = pd.to_datetime(['2026-11-02 22:00', '2026-11-02 23:00', '2026-11-03 00:00', '2026-11-03 05:00',
                            '2026-11-01 01:00', '2026-11-03 23:00'])
    assert hours_text(hours, shown=3).split('\n') == [
        'Tue 11/3 23:00-24:00', 'Tue 11/3 05:00-06:00', 'Mon 11/2 22:00 to Tue 11/3 01:00', 'and 1 more time']


def test_missing_data_table_places_the_loss_in_time():
    alerts = pd.DataFrame({'DeviceId': ['A', 'A'], 'Date': pd.to_datetime(['2026-11-02', '2026-11-03']), 'MissingData': [0.0, 0.2],
                           'Alert': [0, 1]})
    hourly = pd.DataFrame({'DeviceId': 'A', 'TimeStamp': pd.date_range('2026-11-03', periods=24, freq='h'), 'MissingData': 0.0})
    hourly.loc[[2, 3], 'MissingData'] = 1.0
    hourly.loc[10, 'MissingData'] = 0.75
    table, total = prepare_missing_data_alerts_table(alerts, SIGNALS, hourly=hourly)
    assert total == 1
    assert list(table.columns) == ['Signal', 'When Missing', 'Hours Missing', 'Sparkline_Data']
    row = table.iloc[0]
    assert row['When Missing'] == 'Tue 11/3 10:00-11:00\nTue 11/3 02:00-04:00'
    assert row['Hours Missing'] == '2.75' and len(row['Sparkline_Data']) == 24
    # Without the hourly view the table is the daily one, as before.
    daily, _ = prepare_missing_data_alerts_table(alerts, SIGNALS)
    assert 'Missing Data %' in daily.columns


def test_system_outage_gives_its_hours():
    days = pd.DataFrame({'Date': pd.to_datetime(['2026-11-03']), 'Region': ['Region 1'], 'MissingData': [0.4],
                         'LatestDataDay': pd.to_datetime(['2026-11-03'])})
    has_data = pd.concat([bins(device, '2026-11-03', '2026-11-03 06:00') for device in 'AB']
                         + [bins(device, '2026-11-03 16:00', '2026-11-04') for device in 'AB'])
    hourly = region_missing_by_hour(has_data, SIGNALS)
    table, total = prepare_system_outages_table(days, hourly=hourly)
    assert total == 1 and list(table.columns) == ['Dates', 'Regions', 'When', 'Hours Out', 'Ongoing']
    assert table.iloc[0]['When'] == 'Tue 11/3 06:00-16:00' and table.iloc[0]['Hours Out'] == 10
    assert 'Missing Data %' in prepare_system_outages_table(days)[0].columns
