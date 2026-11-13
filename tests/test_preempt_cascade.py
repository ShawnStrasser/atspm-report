import pandas as pd

from atspm_report.preempt_cascade import (
    build_cascade_break_alerts,
    build_cascade_links,
    build_preempt_duration_alerts,
    cascade_follows,
    evaluate_preempt_cascades,
    reporting_bins,
    review_preempt_alerts,
    signal_neighbours,
    update_preempt_calls,
)

# Three signals half a mile apart on one north-south line: A -> B -> C.
SIGNALS = pd.DataFrame({
    'DeviceId': ['A', 'B', 'C', 'FAR'],
    'Name': ['A', 'B', 'C', 'FAR'],
    'Region': ['R'] * 4,
    'Latitude': [45.0, 45.00724, 45.01448, 46.0],
    'Longitude': [-122.0] * 4,
})
REPORT_DAY = pd.Timestamp('2026-09-28')


def _run(start, skip=(), duration=15.0):
    """One vehicle driving A -> B -> C at 30 mph (60 s per half mile)."""
    rows = []
    for i, device in enumerate(['A', 'B', 'C']):
        if device in skip:
            continue
        rows.append({'DeviceId': device, 'Preempt': 1, 'StartTime': start + pd.Timedelta(seconds=60 * i),
                     'Duration': duration, 'IsValid': True})
    return rows


def _calls(recent_skip=(), recent_runs=True):
    rows = []
    for day in pd.date_range('2026-09-01', REPORT_DAY):
        recent = day > REPORT_DAY - pd.Timedelta(days=7)
        if recent and not recent_runs:
            continue
        for hour in (9, 15):
            rows += _run(day + pd.Timedelta(hours=hour), skip=recent_skip if recent else ())
    return pd.DataFrame(rows)


def _decrease_alert(device):
    return pd.DataFrame([{
        'DeviceId': device, 'Preempt': 1, 'Date': REPORT_DAY, 'Direction': 'Decrease',
        'BaselinePerDay': 2.0, 'RecentPerDay': 0.0, 'BaselineDays': 20, 'RecentDays': 7,
        'Score': 5.0, 'DailyCounts': [2] * 28,
    }])


def _links(calls):
    neighbours = signal_neighbours(SIGNALS)
    follows = cascade_follows(calls, neighbours)
    links = build_cascade_links(calls, follows, before=REPORT_DAY - pd.Timedelta(days=6))
    return follows, links


def test_neighbours_need_coordinates():
    pairs = signal_neighbours(SIGNALS)
    assert set(zip(pairs['DeviceId'], pairs['NextDeviceId'])) >= {('A', 'B'), ('B', 'C'), ('A', 'C')}
    assert 'FAR' not in set(pairs['DeviceId'])
    assert signal_neighbours(SIGNALS.drop(columns=['Latitude', 'Longitude'])).empty


def test_follows_take_the_earliest_call_inside_the_travel_window():
    # A to B is half a mile: at 20-45 mph a vehicle arrives 40 s (less 30 s slack) to 90 s (plus 30 s) later.
    start = pd.Timestamp('2026-09-10 09:00:00')
    rows = [{'DeviceId': 'A', 'Preempt': 1, 'StartTime': start, 'Duration': 15.0, 'IsValid': True}]
    for preempt, seconds in ((1, 5), (1, 60), (1, 100), (2, 200)):
        rows.append({'DeviceId': 'B', 'Preempt': preempt, 'StartTime': start + pd.Timedelta(seconds=seconds),
                     'Duration': 15.0, 'IsValid': True})
    follows = cascade_follows(pd.DataFrame(rows), signal_neighbours(SIGNALS))
    from_a = follows[(follows['DeviceId'] == 'A') & (follows['NextDeviceId'] == 'B')]
    # The call 5 s later is too soon to be the same vehicle; 60 s is the first in the window
    # (100 s is in it too); preempt 2 at 200 s is past the window, so it has no row.
    assert from_a[['NextPreempt', 'NextStartTime']].values.tolist() == [[1, start + pd.Timedelta(seconds=60)]]


def test_links_follow_the_driving_direction():
    _, links = _links(_calls())
    pairs = set(zip(links['DeviceId'], links['NextDeviceId']))
    assert {('A', 'B'), ('B', 'C'), ('A', 'C')} <= pairs
    assert ('C', 'A') not in pairs


def test_decrease_overruled_when_whole_cascade_quiet():
    calls = _calls(recent_runs=False)
    follows, links = _links(calls)
    reviewed = review_preempt_alerts(_decrease_alert('B'), calls, follows, links, REPORT_DAY)
    assert reviewed.loc[0, 'Cascade'] == 'Overruled'
    assert 'quiet' in reviewed.loc[0, 'Note']


def test_decrease_confirmed_when_neighbours_keep_calling():
    calls = _calls(recent_skip=('B',))
    follows, links = _links(calls)
    reviewed = review_preempt_alerts(_decrease_alert('B'), calls, follows, links, REPORT_DAY)
    assert reviewed.loc[0, 'Cascade'] == 'Confirmed'


def test_cascade_break_on_skipped_middle_signal():
    calls = _calls(recent_skip=('B',))
    follows, links = _links(calls)
    breaks = build_cascade_break_alerts(calls, follows, links, REPORT_DAY)
    assert list(breaks['DeviceId']) == ['B']
    assert breaks.loc[0, 'Called'] == 0

    healthy = _calls()
    follows, links = _links(healthy)
    assert build_cascade_break_alerts(healthy, follows, links, REPORT_DAY).empty


def test_stuck_call_vetoed_when_neighbour_held_too():
    long_start = REPORT_DAY + pd.Timedelta(hours=2)
    stuck = pd.DataFrame([
        {'DeviceId': 'A', 'Preempt': 5, 'StartTime': long_start, 'Duration': 7200.0, 'IsValid': True},
    ])
    neighbours = signal_neighbours(SIGNALS)
    alerts = build_preempt_duration_alerts(stuck, neighbours, REPORT_DAY, rail_preempts=[5])
    assert list(alerts['Direction']) == ['Stuck On']

    train = pd.concat([stuck, stuck.assign(DeviceId='B', Duration=7000.0)], ignore_index=True)
    assert build_preempt_duration_alerts(train, neighbours, REPORT_DAY, rail_preempts=[5]).empty
    # Under the rail limit, an EV limit applies only to non-rail numbers.
    ev = stuck.assign(Preempt=1, Duration=900.0)
    assert list(build_preempt_duration_alerts(ev, neighbours, REPORT_DAY, rail_preempts=[5])['Direction']) == ['Stuck On']
    assert build_preempt_duration_alerts(ev, neighbours, REPORT_DAY, rail_preempts=None).empty


def test_chatter_needs_mostly_short_calls():
    starts = pd.date_range(REPORT_DAY - pd.Timedelta(days=6), periods=30, freq='5h')
    chatter = pd.DataFrame({'DeviceId': 'A', 'Preempt': 2, 'StartTime': starts, 'Duration': 0.5, 'IsValid': True})
    alerts = build_preempt_duration_alerts(chatter, None, REPORT_DAY)
    assert list(alerts['Direction']) == ['Chatter']
    normal = chatter.assign(Duration=15.0)
    assert build_preempt_duration_alerts(normal, None, REPORT_DAY).empty


def test_evaluate_drops_overruled_and_keeps_record():
    calls = _calls(recent_runs=False)
    result = evaluate_preempt_cascades(_decrease_alert('B'), calls, SIGNALS, REPORT_DAY, verbosity=0)
    assert result['alerts'].empty
    assert list(result['overruled']['DeviceId']) == ['B']
    assert not result['links'].empty


def test_update_calls_replaces_rerun_days():
    calls = _calls()
    day = calls[calls['StartTime'].dt.normalize() == REPORT_DAY]
    merged = update_preempt_calls(day, calls)
    assert len(merged) == len(calls)


def test_break_counts_each_vehicle_once():
    # A fourth signal extends the line, giving B two chains (A-B-C, A-B-D)
    # that see the same vehicle.
    signals = pd.concat([SIGNALS, pd.DataFrame([{
        'DeviceId': 'D', 'Name': 'D', 'Region': 'R', 'Latitude': 45.02172, 'Longitude': -122.0,
    }])], ignore_index=True)
    rows = []
    for day in pd.date_range('2026-09-01', REPORT_DAY):
        recent = day > REPORT_DAY - pd.Timedelta(days=7)
        for hour in (9, 15):
            start = day + pd.Timedelta(hours=hour)
            for i, device in enumerate(['A', 'B', 'C', 'D']):
                if recent and device == 'B':
                    continue
                rows.append({'DeviceId': device, 'Preempt': 1, 'StartTime': start + pd.Timedelta(seconds=60 * i),
                             'Duration': 15.0, 'IsValid': True})
    calls = pd.DataFrame(rows)
    neighbours = signal_neighbours(signals)
    follows = cascade_follows(calls, neighbours)
    links = build_cascade_links(calls, follows, before=REPORT_DAY - pd.Timedelta(days=6))
    breaks = build_cascade_break_alerts(calls, follows, links, REPORT_DAY)
    assert list(breaks['DeviceId']) == ['B']
    assert breaks.loc[0, 'Passes'] == 14


def test_rail_hold_with_crossing_still_serving_is_information():
    hold_start = REPORT_DAY + pd.Timedelta(hours=2)
    calls = pd.DataFrame([
        {'DeviceId': 'A', 'Preempt': 6, 'StartTime': hold_start, 'Duration': 6 * 3600.0, 'IsValid': True},
        {'DeviceId': 'A', 'Preempt': 5, 'StartTime': hold_start + pd.Timedelta(hours=1), 'Duration': 120.0, 'IsValid': True},
    ])
    alerts = build_preempt_duration_alerts(calls, None, REPORT_DAY, rail_preempts=[5, 6])
    assert list(alerts['Direction']) == ['Long Hold']
    # Nothing served inside the hold: the crossing may be stuck.
    alone = build_preempt_duration_alerts(calls.iloc[:1], None, REPORT_DAY, rail_preempts=[5, 6])
    assert list(alone['Direction']) == ['Stuck On']


def _has_data(days, gaps=()):
    """has_data bins for A, B and C over ``days``, minus ``gaps`` (device, start, end)."""
    bins = pd.date_range(days[0], days[1] + pd.Timedelta(days=1), freq='15min', inclusive='left')
    rows = pd.DataFrame([(d, t) for d in ['A', 'B', 'C'] for t in bins], columns=['DeviceId', 'TimeStamp'])
    for device, start, end in gaps:
        rows = rows[~((rows['DeviceId'] == device) & (rows['TimeStamp'] >= start) & (rows['TimeStamp'] < end))]
    return rows


def _recent_outage(device):
    """``device`` not reporting 8:00-17:00 every day of the recent week."""
    days = pd.date_range(REPORT_DAY - pd.Timedelta(days=6), REPORT_DAY)
    gaps = [(device, d + pd.Timedelta(hours=8), d + pd.Timedelta(hours=17)) for d in days]
    return _has_data((pd.Timestamp('2026-09-01'), REPORT_DAY), gaps)


def test_no_break_when_middle_signal_was_not_reporting():
    calls = _calls(recent_skip=('B',))
    follows, links = _links(calls)
    reporting = reporting_bins(_recent_outage('B'))
    assert build_cascade_break_alerts(calls, follows, links, REPORT_DAY, reporting=reporting).empty
    # The same gap at another signal leaves the break standing.
    reporting = reporting_bins(_recent_outage('FAR'))
    assert list(build_cascade_break_alerts(calls, follows, links, REPORT_DAY, reporting=reporting)['DeviceId']) == ['B']


def test_decrease_not_confirmed_by_vehicles_during_outage():
    calls = _calls(recent_skip=('B',))
    follows, links = _links(calls)
    reporting = reporting_bins(_recent_outage('B'))
    reviewed = review_preempt_alerts(_decrease_alert('B'), calls, follows, links, REPORT_DAY, reporting=reporting)
    assert reviewed.loc[0, 'Cascade'] != 'Confirmed'


def test_long_call_across_outage_is_not_stuck():
    start = REPORT_DAY + pd.Timedelta(hours=2)
    calls = pd.DataFrame([{'DeviceId': 'A', 'Preempt': 5, 'StartTime': start, 'Duration': 7200.0, 'IsValid': True}])
    gap = _has_data((REPORT_DAY, REPORT_DAY), [('A', start + pd.Timedelta(hours=1), start + pd.Timedelta(hours=1, minutes=30))])
    assert build_preempt_duration_alerts(calls, None, REPORT_DAY, rail_preempts=[5], reporting=reporting_bins(gap)).empty
    full = _has_data((REPORT_DAY, REPORT_DAY))
    alerts = build_preempt_duration_alerts(calls, None, REPORT_DAY, rail_preempts=[5], reporting=reporting_bins(full))
    assert list(alerts['Direction']) == ['Stuck On']


def test_times_outside_has_data_count_as_reporting():
    calls = _calls(recent_skip=('B',))
    follows, links = _links(calls)
    # has_data only covers a day before the call history: nothing is known.
    early = reporting_bins(_has_data((pd.Timestamp('2026-08-01'), pd.Timestamp('2026-08-01'))))
    assert list(build_cascade_break_alerts(calls, follows, links, REPORT_DAY, reporting=early)['DeviceId']) == ['B']


def _false_calls(per_day=3, preempt=4, days=7, device='B', lengths=(5.2,)):
    """The usual A -> B -> C runs, plus calls of 5.2 s on another input at B that no vehicle makes."""
    rows = _calls().to_dict('records')
    for day in pd.date_range(end=REPORT_DAY, periods=days):
        for i in range(per_day):
            rows.append({'DeviceId': device, 'Preempt': preempt, 'StartTime': day + pd.Timedelta(hours=1 + 2 * i),
                         'Duration': lengths[i % len(lengths)], 'IsValid': True})
    return pd.DataFrame(rows)


def _increase_alert(device, preempt):
    return _decrease_alert(device).assign(Preempt=preempt, Direction='Increase', BaselinePerDay=0.5, RecentPerDay=3.0)


def test_same_length_calls_no_nearby_signal_sees_are_false_calls():
    calls = _false_calls()
    result = evaluate_preempt_cascades(_decrease_alert('B').head(0), calls, SIGNALS, REPORT_DAY, false_calls=True)
    alerts = result['alerts']
    row = alerts[alerts['Direction'] == 'False Calls'].iloc[0]
    assert (row['DeviceId'], row['Preempt'], row['Class']) == ('B', 4, 'Emergency Vehicle')
    # Nearby signals are not named: the input has no linked preempts.
    assert row['Note'] == ('21 of 21 calls in the last 7 days had the same duration, lasting about 5.2 s. '
                           'This does not represent normal EVP calls.')
    # Off unless asked for; and the runs' own calls, every one 15 s long, are seen at the next signal, so they are vehicles.
    assert 'False Calls' not in set(evaluate_preempt_cascades(_decrease_alert('B').head(0), calls, SIGNALS, REPORT_DAY)['alerts'].get('Direction', []))
    assert set(alerts.loc[alerts['Direction'] == 'False Calls', 'Preempt']) == {4}


def _false_call_inputs(calls):
    alerts = evaluate_preempt_cascades(_decrease_alert('B').head(0), calls, SIGNALS, REPORT_DAY, false_calls=True)['alerts']
    return [] if alerts.empty else list(zip(alerts['DeviceId'], alerts['Preempt'], alerts['Direction']))


def test_false_calls_need_one_a_day():
    assert _false_call_inputs(_false_calls(per_day=1)) == [('B', 4, 'False Calls')]
    assert _false_call_inputs(_false_calls(per_day=1, days=6)) == []


def test_false_calls_need_most_calls_at_the_shortest_length():
    # Two of every three calls are longer: some calls of the shortest length are normal.
    assert _false_call_inputs(_false_calls(lengths=(5.2, 9.0, 14.0))) == []
    # Calls of one longer length are not calls of a moment.
    mixed = pd.concat([_false_calls(), _false_calls(preempt=3, lengths=(10.1,))]).drop_duplicates()
    assert _false_call_inputs(mixed) == [('B', 4, 'False Calls')]


def test_false_calls_with_no_nearby_signal_to_check_against_need_more():
    # 11 or more calls in 7 days, and 8 in 10 of the input's calls.
    assert _false_call_inputs(_false_calls(device='FAR')) == [('FAR', 4, 'False Calls')]
    assert _false_call_inputs(_false_calls(device='FAR', per_day=1)) == []
    assert _false_call_inputs(_false_calls(device='FAR', per_day=6, lengths=(5.2, 5.2, 9.0))) == []


def test_false_calls_name_only_linked_preempts():
    # Input 1 at B is linked to A and C by the runs; its extra calls of 5.2 s reach neither.
    alerts = evaluate_preempt_cascades(_decrease_alert('B').head(0), _false_calls(preempt=1), SIGNALS, REPORT_DAY, false_calls=True)['alerts']
    row = alerts[alerts['Direction'] == 'False Calls'].iloc[0]
    assert row['Note'] == ('21 of 35 calls in the last 7 days had the same duration, lasting about 5.2 s. '
                           'This does not represent normal EVP calls. Its 2 linked preempts saw none of them.')


def test_an_increase_on_an_input_with_false_calls_is_told_as_false_calls():
    result = evaluate_preempt_cascades(_increase_alert('B', 4), _false_calls(), SIGNALS, REPORT_DAY, false_calls=True)
    alerts = result['alerts']
    assert alerts['Direction'].tolist() == ['False Calls']
    assert alerts.loc[0, 'Note'].endswith('Up from 0.5 to 3.0 calls a day.')
    assert (alerts.loc[0, 'BaselinePerDay'], alerts.loc[0, 'RecentPerDay']) == (0.0, 3.0)


def test_an_increase_whose_calls_nearby_signals_saw_is_not_an_alert():
    result = evaluate_preempt_cascades(_increase_alert('B', 1), _false_calls(), SIGNALS, REPORT_DAY, false_calls=True)
    assert 'Increase' not in set(result['alerts']['Direction'])
    assert result['overruled'][['Direction', 'Note']].values.tolist() == [['Increase', 'nearby signals had 14 of its 14 calls']]
    # Without the option the cascade review alone decides.
    assert 'Increase' in set(evaluate_preempt_cascades(_increase_alert('B', 1), _false_calls(), SIGNALS, REPORT_DAY)['alerts']['Direction'])


def test_an_increase_of_calls_no_nearby_signal_saw_stays():
    calls = _false_calls(preempt=3, lengths=(9.0, 14.0, 22.0))
    result = evaluate_preempt_cascades(_increase_alert('B', 3), calls, SIGNALS, REPORT_DAY, false_calls=True)
    row = result['alerts'].iloc[0]
    assert (row['Direction'], row['Note']) == ('Increase', 'no linked preempts to compare with')


def test_false_calls_check_with_nothing_to_find():
    result = evaluate_preempt_cascades(_increase_alert('B', 1), _calls(), SIGNALS, REPORT_DAY, false_calls=True, overrule=False)
    assert result['alerts']['Direction'].tolist() == ['Increase']
    assert evaluate_preempt_cascades(_increase_alert('B', 1), _calls(), SIGNALS, REPORT_DAY, false_calls=True)['alerts'].empty
