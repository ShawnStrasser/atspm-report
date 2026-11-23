"""Timing Insights: green use by phase from the 15-minute tables, judged over the last six weekdays."""

import json

import pandas as pd
import pytest

from atspm_report import timing_insights as ti

DEVICE = 'dev-1'
REPORT_DAY = pd.Timestamp('2026-11-20')     # a Friday; its six weekdays run back to 11-13 (11-11 is Veterans Day)
DAYS = ti.judged_days(REPORT_DAY)
PHASES = (2, 4, 6, 8)


def light(day, phase, hour):
    """A phase gapping out every cycle with light traffic: 6 cycles a bin, occupancy 20 %, no split failures, free."""
    return dict(gap=6, mx=0, fo=0, sf=0, cycles=6, gor=0.2, green=20.0, ped=0, cl=0, veh=10)


def tables(rule=light, phases=PHASES, days=DAYS):
    sf, term, ped, coord, counts = [], [], [], [], []
    for day in days:
        for hour in range(24):
            for quarter in range(4):
                ts = day + pd.Timedelta(hours=hour, minutes=15 * quarter)
                cl = 0
                for phase in phases:
                    b = rule(day, phase, hour)
                    cl = b['cl']
                    for measure, key in (('GapOut', 'gap'), ('MaxOut', 'mx'), ('ForceOff', 'fo')):
                        if b[key]:
                            term.append((ts, DEVICE, phase, measure, b[key]))
                    sf.append((ts, DEVICE, phase, b['green'], b['gor'], 0.1, b['sf'], b['cycles']))
                    ped.append((ts, DEVICE, phase, b['ped']))
                    counts.append((ts, DEVICE, 10 + phase, b['veh']))
                coord.append((ts, DEVICE, cl))
    return dict(
        split_failures=pd.DataFrame(sf, columns=['TimeStamp', 'DeviceId', 'Phase', 'Green_Time', 'Green_Occupancy',
                                                 'Red_Occupancy', 'Split_Failure', 'Cycles']),
        terminations=pd.DataFrame(term, columns=['TimeStamp', 'DeviceId', 'Phase', 'PerformanceMeasure', 'Total']),
        pedestrian=pd.DataFrame(ped, columns=['TimeStamp', 'DeviceId', 'Phase', 'PedServices']),
        coordination_agg=pd.DataFrame(coord, columns=['TimeStamp', 'DeviceId', 'CycleLength']),
        actuations=pd.DataFrame(counts, columns=['TimeStamp', 'DeviceId', 'Detector', 'Total']))


SIGNALS = pd.DataFrame({'DeviceId': [DEVICE], 'Name': ['01001-Main at First'], 'Region': ['Region 1']})
DETECTORS = pd.DataFrame({'DeviceId': DEVICE, 'Detector': [10 + p for p in PHASES], 'Phase': list(PHASES),
                          'Function': 'Count', 'Lanes': 1.0})


def run(rule=light, faults=None, **kw):
    return ti.build_insight_alerts(report_day=REPORT_DAY, signals=SIGNALS, detectors=DETECTORS, faults=faults,
                                   **tables(rule, **kw))


def found(alerts, check):
    return sorted(alerts.loc[alerts['Check'] == check, 'Phase'].astype(int))


def test_judged_days_are_weekdays_without_holidays():
    days = ti.judged_days('2026-11-15')     # a Sunday
    assert [d.strftime('%m-%d') for d in days] == ['11-13', '11-12', '11-10', '11-09', '11-06', '11-05']


def test_light_traffic_alerts_nothing():
    assert run().empty


def recall_4(day, phase, hour):
    b = light(day, phase, hour)
    if phase == 4:
        b.update(gap=0, mx=6)          # runs to its max every cycle with light traffic
    return b


def test_max_recall_with_light_traffic():
    alerts = run(recall_4)
    assert found(alerts, ti.MAX_RECALL) == [4]
    row = alerts[alerts['Check'] == ti.MAX_RECALL].iloc[0]
    assert row['DaysSeen'] == 6 and row['Severity'] == pytest.approx(12.0)
    assert found(alerts, ti.MAXOUT_LOW) == []       # on recall is its own table


def test_coordinated_phase_is_not_max_recall():
    def held(day, phase, hour):
        b = light(day, phase, hour)
        b['cl'] = 100
        if phase in (2, 6):
            b.update(gap=0, fo=6)      # coordinated: held to the force-off
        return b
    alerts = run(held)
    assert found(alerts, ti.MAX_RECALL) == [] and found(alerts, ti.MAXOUT_LOW) == []


def test_max_out_low_demand():
    def long_4(day, phase, hour):
        b = light(day, phase, hour)
        if phase == 4:
            b.update(gap=2, mx=4)      # runs to its limit on two thirds of greens, little traffic
        return b
    assert found(run(long_4), ti.MAXOUT_LOW) == [4]


def test_barrier_partner_busy_is_not_low_demand():
    def long_4_busy_8(day, phase, hour):
        b = light(day, phase, hour)
        if phase == 4:
            b.update(gap=2, mx=4)
        if phase == 8:
            b.update(gor=0.7, sf=2)    # the partner is busy: phase 4 is held at the barrier
        return b
    assert found(run(long_4_busy_8), ti.MAXOUT_LOW) == []


def test_needs_more_green_names_donors_across_the_barrier():
    def short_side(day, phase, hour):
        b = light(day, phase, hour)
        if phase in (4, 8):
            b.update(gap=2, mx=4, sf=2, gor=0.9)      # short of green; 2 and 6 have time to spare
        return b
    alerts = run(short_side)
    assert found(alerts, ti.NEEDS_GREEN) == [4, 8]
    assert set(alerts.loc[alerts['Check'] == ti.NEEDS_GREEN, 'Donors']) == {'Ph 2 and 6'}
    assert found(alerts, ti.CAPACITY) == []


def test_max_capacity_is_one_row_per_signal():
    def all_short(day, phase, hour):
        b = light(day, phase, hour)
        b.update(gap=2, mx=4, sf=2, gor=0.9)
        return b
    alerts = run(all_short)
    cap = alerts[alerts['Check'] == ti.CAPACITY]
    assert len(cap) == 1 and int(cap['Phase'].iloc[0]) == 0
    assert 'Phases 2, 4, 6 and 8' in cap['Detail'].iloc[0]
    assert '12.0 hours a day' in cap['Detail'].iloc[0]


def test_premature_gap_out_from_bins():
    def failing_4(day, phase, hour):
        b = light(day, phase, hour)
        if phase == 4:
            b.update(sf=1)             # one gap-out in six leaves a queue
        return b
    alerts = run(failing_4)
    assert found(alerts, ti.PREMATURE) == [4]
    row = alerts[alerts['Check'] == ti.PREMATURE].iloc[0]
    assert '17 % of its gap-outs' in row['Detail']
    data = json.loads(row['ChartData'])
    assert data['phases']['4']['sfgo'][0] == pytest.approx(100 / 6, abs=0.1)


def test_premature_shares_split_failures_by_gap_outs():
    def mixed_4(day, phase, hour):
        b = light(day, phase, hour)
        if phase == 4:
            b.update(gap=3, mx=3, sf=2)     # half the greens gapped out: half the failures count at a gap-out
        return b
    row = run(mixed_4)
    row = row[row['Check'] == ti.PREMATURE].iloc[0]
    assert '33 % of its gap-outs' in row['Detail']


def test_faulted_hours_are_left_out():
    faults = pd.DataFrame({'DeviceId': [DEVICE], 'Phase': [4], 'Zone': ['stop bar'],
                           's': [DAYS[-1]], 'e': [DAYS[0] + pd.Timedelta(days=1)]})
    faults = faults.astype({'s': 'datetime64[us]', 'e': 'datetime64[us]'})
    assert found(run(recall_4, faults=faults), ti.MAX_RECALL) == []


def blocked_4(day, phase, hour):
    """Phase 4: free-flowing at night (about 36 vehicles a bin of the 45 the green can carry), 8-11 h occupied with
    split failures but only 9 vehicles a bin, 11-15 h occupied and flowing."""
    b = light(day, phase, hour)
    if phase == 4:
        b.update(veh=36, gor=0.3)
        if 8 <= hour < 11:
            b.update(gor=0.9, sf=3, veh=9)
        elif 11 <= hour < 15:
            b.update(gor=0.9, veh=36)
    return b


def test_downstream_bottleneck():
    alerts = run(blocked_4)
    assert found(alerts, ti.BOTTLENECK) == [4]
    data = json.loads(alerts.loc[alerts['Check'] == ti.BOTTLENECK, 'ChartData'].iloc[0])
    assert sum(1 for p in data['points'] if p[2]) == 6 * 12


def test_bottleneck_signal_is_not_max_capacity():
    def all_short_4_blocked(day, phase, hour):
        b = blocked_4(day, phase, hour)
        if 7 <= hour < 19:
            b.update(gap=2, mx=4, sf=max(b['sf'], 2), gor=max(b['gor'], 0.9))
        return b
    alerts = run(all_short_4_blocked)
    assert found(alerts, ti.BOTTLENECK) == [4]
    assert found(alerts, ti.CAPACITY) == []


def test_without_tables_nothing():
    assert ti.build_insight_alerts(None, None, REPORT_DAY).empty


def test_listed_alerts_keep_the_worst_rows_per_check():
    alerts = pd.DataFrame({'DeviceId': [DEVICE] * 3, 'Phase': [2, 4, 6], 'Check': ti.PREMATURE,
                           'Severity': [5.0, 9.0, 1.0], 'Rank': 0})
    assert sorted(ti.listed_alerts(alerts, SIGNALS, 2)['Phase']) == [2, 4]


def test_table_and_charts():
    alerts = pd.concat([run(recall_4), run(blocked_4)], ignore_index=True)
    rows, total = ti.prepare_insight_table(alerts, SIGNALS, 'Region 1', 5, ti.MAX_RECALL)
    assert total == 1 and list(rows.columns) == ['Signal', 'Phase', 'Details']
    figures = ti.create_insight_plots(alerts, SIGNALS, 5)
    checks = {check for _, scope, check in figures if scope == 'All Regions'}
    assert {ti.MAX_RECALL, ti.BOTTLENECK} <= checks
