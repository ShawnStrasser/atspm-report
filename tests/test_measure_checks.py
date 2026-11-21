"""atspm's per-phase measures as inputs: roll-ups, the fault rule, the delay rise and trend checks, and the report unchanged
without them."""

import duckdb
import numpy as np
import pandas as pd
import pytest

from atspm_report import measure_inputs as mi
from atspm_report import measure_checks as mc
from atspm_report.statistical_analysis import alert, cusum

DETECTORS = pd.DataFrame([('A', 1, 4, 'Presence'), ('A', 2, 4, 'Advance'), ('A', 3, 2, 'Count')],
                         columns=['DeviceId', 'Detector', 'Phase', 'Function'])


def finding(detector, start, end, label='stuck on', confidence=0.9, kind='Vehicle'):
    return {'DeviceId': 'A', 'DetectorType': kind, 'Detector': detector, 'Start': pd.Timestamp(start),
            'End': pd.Timestamp(end), 'Label': label, 'Confidence': confidence}


# ---- measure_inputs ----------------------------------------------------------------------------------------------------

def test_fault_spans_name_the_zone_each_measure_comes_from():
    findings = pd.DataFrame([finding(1, '2026-11-12 08:00', '2026-11-12 09:00'), finding(2, '2026-11-12 10:00', '2026-11-12 11:00'),
                             finding(2, '2026-11-12 12:00', '2026-11-12 13:00', confidence=0.3),        # under the suspect limit
                             finding(1, '2026-11-12 14:00', '2026-11-12 15:00', label='lane closure'),  # not a fault
                             finding(3, '2026-11-12 16:00', '2026-11-12 17:00')])                       # no measure from it
    timeline = pd.DataFrame({'DeviceId': ['A', 'A'], 'StartTime': pd.to_datetime(['2026-11-12 18:00', '2026-11-12 19:00']),
                             'EndTime': pd.to_datetime(['2026-11-12 18:30', '2026-11-12 19:30']),
                             'EventClass': ['Stuck On', 'Green'], 'EventValue': [2, 4]})
    minutes = pd.DataFrame({'DeviceId': ['A', 'A'], 'DetectorType': ['Vehicle'] * 2, 'Detector': [1, 2],
                            'Date': pd.to_datetime(['2026-11-05', '2026-11-06']), 'BadMinutes': [30.0, 5.0], 'SuspectMinutes': [0.0, 0.0]})
    spans = mi.fault_spans(findings, DETECTORS, timeline, minutes).sort_values('s')
    assert list(zip(spans['Zone'], spans['s'].dt.strftime('%m-%d %H:%M'))) == [
        (mi.STOP_BAR, '11-05 00:00'), (mi.STOP_BAR, '11-12 08:00'), (mi.ADVANCE, '11-12 10:00'), (mi.ADVANCE, '11-12 18:00')]
    assert mi.fault_spans(findings, None).empty


def test_delay_per_vehicle_joins_the_bins_before_summing():
    ts = pd.date_range('2026-11-12 08:00', periods=4, freq='15min')
    aog = pd.DataFrame({'TimeStamp': ts, 'DeviceId': 'A', 'Phase': 4, 'Total_Actuations': [10, 10, 10, 50], 'Percent_AOG': 0.5})
    # approach_delay ends a bin earlier: the last bin's 50 vehicles are not counted against delay it does not have.
    ad = pd.DataFrame({'TimeStamp': ts[:3], 'DeviceId': 'A', 'Phase': 4, 'Delay_Seconds': 100.0, 'Queued_Delay_Seconds': 20.0})
    h = mi.hourly_delay(ad, aog, '2026-11-12', '2026-11-13')
    assert h['dsum'].iloc[0] / h['veh'].iloc[0] == pytest.approx(12.0) and h['bins'].iloc[0] == 3
    con = duckdb.connect()
    r = mi.hourly_delay(con.from_df(ad), con.from_df(aog), '2026-11-12', '2026-11-13', devices=['A'])
    assert r[['veh', 'dsum']].values.tolist() == h[['veh', 'dsum']].values.tolist()
    assert mi.hourly_delay(None, aog, '2026-11-12', '2026-11-13').empty


def test_delay_comes_from_the_queue_model_when_approach_delay_has_it():
    ts = pd.date_range('2026-11-12 08:00', periods=3, freq='15min')
    aog = pd.DataFrame({'TimeStamp': ts, 'DeviceId': 'A', 'Phase': 4, 'Total_Actuations': 10, 'Percent_AOG': 0.5})
    ad = pd.DataFrame({'TimeStamp': ts, 'DeviceId': 'A', 'Phase': 4, 'Delay_Seconds': 100.0, 'Queued_Delay_Seconds': 20.0})
    # Without the column: measured plus queued, 120 s a bin.
    assert mi.hourly_delay(ad, aog, '2026-11-12', '2026-11-13')['dsum'].iloc[0] == pytest.approx(360.0)
    # With it: the Queue Model's delay is the whole delay, not added to the other two; a NULL row (a day before the
    # Queue Model was on) takes measured plus queued.
    queued = ad.assign(Queue_Delay_Seconds=[300.0, 250.0, None])
    expected = 300.0 + 250.0 + 120.0
    assert mi.hourly_delay(queued, aog, '2026-11-12', '2026-11-13')['dsum'].iloc[0] == pytest.approx(expected)
    con = duckdb.connect()
    same = mi.hourly_delay(con.from_df(queued), con.from_df(aog), '2026-11-12', '2026-11-13', devices=['A'])
    assert same['dsum'].iloc[0] == pytest.approx(expected)
    other = duckdb.connect()   # relations of two connections are read and joined here
    apart = mi.hourly_delay(con.from_df(queued), other.from_df(aog), '2026-11-12', '2026-11-13', devices=['A'])
    assert apart['dsum'].iloc[0] == pytest.approx(expected)
    # The generator's DeviceId cast keeps the column on a relation.
    from atspm_report.generator import _normalize_deviceid
    assert mi.has_queue_delay(_normalize_deviceid(con.from_df(queued)))
    assert not mi.has_queue_delay(_normalize_deviceid(con.from_df(ad))) and not mi.has_queue_delay(None)


def test_platoon_ratio_is_summed_then_divided_once():
    ts = pd.date_range('2026-11-12 08:00', periods=2, freq='15min')
    pr = pd.DataFrame({'TimeStamp': ts, 'DeviceId': 'A', 'Phase': 2, 'Total_Actuations': [100, 10],
                       'Green_Actuations': [80, 1], 'Green_Seconds': [450.0, 90.0]})
    h = mi.hourly_platoon(pr, '2026-11-12', '2026-11-13').iloc[0]
    assert h.gre / h.arr / (h.gs / (h.bins * 900)) == pytest.approx((81 / 110) / (540 / 1800))


def test_unmasked_drops_hours_their_zone_faulted_in():
    hours = pd.DataFrame({'DeviceId': 'A', 'Phase': 4, 'hr': pd.date_range('2026-11-12 08:00', periods=3, freq='h')})
    faults = mi.fault_spans(pd.DataFrame([finding(2, '2026-11-12 09:10', '2026-11-12 09:20')]), DETECTORS)
    assert mi.unmasked(hours, faults, mi.ADVANCE)['hr'].dt.hour.tolist() == [8, 10]
    assert len(mi.unmasked(hours, faults, mi.STOP_BAR)) == 3


def test_holidays_and_day_kinds():
    days = mi.holidays('2026-11-01', '2026-12-31')
    assert [str(d) for d in days] == ['2026-11-11', '2026-11-26', '2026-11-27', '2026-12-25']
    assert mi.day_kind('2026-11-14') == 'Saturdays' and mi.day_kind('2026-11-13') == 'Weekdays'


# ---- sudden rise in delay per vehicle ----------------------------------------------------------------------------------

def delay_tables(report_day='2026-11-20', rise_hours=range(8, 14), usual=10.0, risen=25.0, vehicles=20, days=None,
                 pr_fall=False):
    """15-minute arrival_on_green, approach_delay and platoon_ratio of signal A phase 4 on the weekdays before report_day
    and on it; on the report day delay per vehicle is `risen` in rise_hours."""
    report = pd.Timestamp(report_day)
    days = days or [d for d in pd.date_range(report - pd.Timedelta(days=14), report) if d.day_of_week < 5]
    rows = []
    for day in days:
        for ts in pd.date_range(day, periods=96, freq='15min'):
            up = day == report and ts.hour in rise_hours
            jitter = 0.3 * np.sin(day.day + ts.hour)
            rows.append((ts, (risen if up else usual + jitter), 0.3 if up and pr_fall else 0.6))
    ts, per, aog = zip(*rows)
    base = pd.DataFrame({'TimeStamp': ts, 'DeviceId': 'A', 'Phase': 4, 'Total_Actuations': vehicles})
    ad = base.drop(columns='Total_Actuations').assign(Delay_Seconds=[vehicles * p for p in per], Queued_Delay_Seconds=0.0)
    aog_t = base.assign(Percent_AOG=aog)
    pr = base.assign(Green_Actuations=[round(vehicles * a) for a in aog], Green_Seconds=270.0)
    return ad, aog_t, pr


def test_a_sudden_rise_in_delay_alerts_and_tells_arrivals_on_green_when_they_fell_too():
    ad, aog, pr = delay_tables(pr_fall=True)
    alerts = mc.delay_rise_alerts(ad, aog, '2026-11-20', platoon_ratio=pr)
    assert len(alerts) == 1
    row = alerts.iloc[0]
    assert row.Phase == 4 and row.Hours == list(range(8, 14))
    assert row.Text.startswith('Delay per vehicle on phase 4 rose to 25 s from 10 s in the same hours of the last 5 weekdays '
                               '(6 hours: 8:00-14:00).')
    assert 'Arrivals on green fell too, from 60 % to 30 %' in row.Text


def test_no_supporting_words_when_arrivals_on_green_held():
    ad, aog, pr = delay_tables()
    assert 'Arrivals on green' not in mc.delay_rise_alerts(ad, aog, '2026-11-20', platoon_ratio=pr).iloc[0].Text


def test_too_few_hours_small_rises_and_quiet_approaches_do_not_alert():
    assert mc.delay_rise_alerts(*delay_tables(rise_hours=range(8, 11))[:2], '2026-11-20').empty
    assert mc.delay_rise_alerts(*delay_tables(risen=14.0)[:2], '2026-11-20').empty      # under 5 s
    assert mc.delay_rise_alerts(*delay_tables(vehicles=5)[:2], '2026-11-20').empty      # 20 an hour


def test_needs_three_earlier_days_of_the_kind_and_skips_holidays():
    few = [pd.Timestamp(d) for d in ('2026-11-17', '2026-11-18', '2026-11-20')]
    assert mc.delay_rise_alerts(*delay_tables(days=few)[:2], '2026-11-20').empty
    # Veterans Day is left out of the earlier days: 11-12 has only 11-09 and 11-10 before it.
    early = [pd.Timestamp(d) for d in ('2026-11-09', '2026-11-10', '2026-11-11', '2026-11-12')]
    assert mc.delay_rise_alerts(*delay_tables('2026-11-12', days=early)[:2], '2026-11-12').empty
    # No check on a holiday.
    assert mc.delay_rise_alerts(*delay_tables('2026-11-26')[:2], '2026-11-26').empty


def test_faulting_advance_zones_are_neither_flagged_hours_nor_baseline():
    ad, aog, pr = delay_tables()
    faults = mi.fault_spans(pd.DataFrame([finding(2, '2026-11-20 08:00', '2026-11-20 11:00')]), DETECTORS)
    assert mc.delay_rise_alerts(ad, aog, '2026-11-20', faults).empty     # three hours left: under four
    stop_bar = mi.fault_spans(pd.DataFrame([finding(1, '2026-11-20 08:00', '2026-11-20 11:00')]), DETECTORS)
    assert len(mc.delay_rise_alerts(ad, aog, '2026-11-20', stop_bar)) == 1
    # The earlier days' hours faulting too leaves too few baseline days.
    whole = pd.DataFrame([finding(2, f'2026-11-{d} 00:00', f'2026-11-{d} 23:59') for d in ('13', '16', '17')])
    assert mc.delay_rise_alerts(ad, aog, '2026-11-20', mi.fault_spans(whole, DETECTORS)).empty


def test_delay_rise_without_its_tables_is_empty():
    assert mc.delay_rise_alerts(None, None, '2026-11-20').empty
    assert mc.delay_rise_alerts(delay_tables()[0], None, '2026-11-20').empty


def plans(report_day='2026-11-20', report_plan=5, usual_plan=4):
    """coordination_agg of signal A: usual_plan every day, report_plan on the report day."""
    stamps = pd.date_range(pd.Timestamp(report_day) - pd.Timedelta(days=14), pd.Timestamp(report_day) + pd.Timedelta(hours=23, minutes=45),
                           freq='15min')
    return pd.DataFrame({'TimeStamp': stamps, 'DeviceId': 'A',
                         'Pattern': np.where(stamps >= pd.Timestamp(report_day), report_plan, usual_plan)})


def test_earlier_hours_count_only_when_they_ran_the_same_plan():
    ad, aog, pr = delay_tables()
    assert mc.delay_rise_alerts(ad, aog, '2026-11-20', platoon_ratio=pr, coordination=plans()).empty
    assert len(mc.delay_rise_alerts(ad, aog, '2026-11-20', platoon_ratio=pr, coordination=plans(report_plan=4))) == 1
    # No Pattern column: every earlier hour counts.
    no_plan = plans().drop(columns='Pattern')
    assert len(mc.delay_rise_alerts(ad, aog, '2026-11-20', platoon_ratio=pr, coordination=no_plan)) == 1


def test_a_phase_barely_served_on_the_earlier_days_is_not_judged():
    ad, aog, pr = delay_tables()
    earlier = pr['TimeStamp'] < pd.Timestamp('2026-11-20')
    pr.loc[earlier, 'Green_Seconds'] = 1.0          # 4 s of green an hour
    assert mc.delay_rise_alerts(ad, aog, '2026-11-20', platoon_ratio=pr).empty
    pr.loc[earlier, 'Green_Seconds'] = 15.0         # 60 s an hour
    assert len(mc.delay_rise_alerts(ad, aog, '2026-11-20', platoon_ratio=pr)) == 1


def test_a_fault_on_another_phase_is_told_not_left_out():
    ad, aog, pr = delay_tables()
    other = pd.DataFrame([('A', 5, 8, 'Presence')], columns=DETECTORS.columns)
    faults = mi.fault_spans(pd.DataFrame([finding(5, '2026-11-20 09:00', '2026-11-20 10:00')]),
                            pd.concat([DETECTORS, other], ignore_index=True))
    row = mc.delay_rise_alerts(ad, aog, '2026-11-20', faults, platoon_ratio=pr).iloc[0]
    assert row.Text.endswith('A detector of phase 8 had a known fault in some of these hours, which can change how this '
                             'phase is served.')
    assert 'known fault' not in mc.delay_rise_alerts(ad, aog, '2026-11-20', platoon_ratio=pr).iloc[0].Text
    # A few minutes of faults on the other phase are not told.
    brief = mi.fault_spans(pd.DataFrame([finding(5, '2026-11-20 09:00', '2026-11-20 09:10')]),
                           pd.concat([DETECTORS, other], ignore_index=True))
    assert 'known fault' not in mc.delay_rise_alerts(ad, aog, '2026-11-20', brief, platoon_ratio=pr).iloc[0].Text


def test_earlier_days_since_the_last_report_are_judged_and_named():
    ad, aog, pr = delay_tables()                     # the rise is on Friday 11-20
    # A Monday run judges Friday to Sunday; the alert names its day.
    alerts = mc.delay_rise_alerts(ad, aog, '2026-11-22', platoon_ratio=pr, check_days=3)
    assert len(alerts) == 1 and alerts.iloc[0].Date == pd.Timestamp('2026-11-20')
    assert alerts.iloc[0].Text.startswith('On Fri 11/20: Delay per vehicle on phase 4 rose')
    assert mc.delay_rise_alerts(ad, aog, '2026-11-22', platoon_ratio=pr).empty
    assert mc.judged_days('2026-11-22', 3) == list(pd.date_range('2026-11-20', '2026-11-22'))


# ---- trends ------------------------------------------------------------------------------------------------------------

def split_table(days=21, last=0.70, usual=0.10, cycles=100, wobble=0.01, night=0.0, end='2026-12-10', last_days=1,
                climb=0.0):
    """split_failures of A phase 4: `usual` of cycles failing (a little day to day spread), `last` on the last last_days
    days (from 6 h); climb adds that much a day from the first day on (from 6 h)."""
    rows = []
    stamps = pd.date_range(pd.Timestamp(end) - pd.Timedelta(days=days - 1), periods=days * 96, freq='15min')
    for ts in stamps:
        day = (ts.normalize() - stamps[0].normalize()).days
        share = usual + (climb * day if ts.hour >= 6 else 0) + wobble * ((day * 7) % 5 - 2)
        share = last if day >= days - last_days and ts.hour >= 6 else share
        share = night if ts.hour < 5 and night else share
        rows.append((ts, 'A', 4, round(share * cycles), cycles))
    return pd.DataFrame(rows, columns=['TimeStamp', 'DeviceId', 'Phase', 'Split_Failure', 'Cycles'])


def test_rising_split_failures_alert_with_enough_history():
    alerts = mc.trend_alerts(mc.SPLIT_FAIL_TREND, split_table(), '2026-12-10')
    assert len(alerts) == 1 and alerts.iloc[0].Text == 'Phase 4 left a queue on 55 % of cycles, against 10 % on the 10 weekdays before.'


def test_trends_quietly_do_nothing_without_enough_earlier_days_of_the_kind():
    # 16 days to Thursday 12-10 hold 10 weekdays (Thanksgiving and the day after left out): 3 recent, 7 earlier, under 8.
    assert mc.trend_alerts(mc.SPLIT_FAIL_TREND, split_table(days=16), '2026-12-10').empty
    assert len(mc.trend_alerts(mc.SPLIT_FAIL_TREND, split_table(days=17), '2026-12-10')) == 1
    assert mc.trend_alerts(mc.SPLIT_FAIL_TREND, None, '2026-12-10').empty


def test_a_one_day_rise_needs_the_jump_but_a_few_days_need_only_the_rise():
    # One day 12 points up: under the one-day jump of 20.
    assert mc.trend_alerts(mc.SPLIT_FAIL_TREND, split_table(last=0.25, wobble=0), '2026-12-10').empty
    # The same 12 points held for the last three weekdays: flagged, told over the recent days.
    three = mc.trend_alerts(mc.SPLIT_FAIL_TREND, split_table(last=0.25, wobble=0, last_days=3), '2026-12-10')
    assert len(three) == 1 and 'over its last 3 weekdays' in three.iloc[0].Text
    # Six points held for three days: under the rise.
    assert mc.trend_alerts(mc.SPLIT_FAIL_TREND, split_table(last=0.17, wobble=0, last_days=3), '2026-12-10').empty


def test_a_steady_climb_is_flagged_and_a_bad_day_does_not_echo():
    climb = mc.trend_alerts(mc.SPLIT_FAIL_TREND, split_table(last=0.0, last_days=0, usual=0.05, climb=0.015), '2026-12-10')
    assert len(climb) == 1
    # A one-day jump two days ago is in the recent average but the day judged is back to normal.
    table = split_table(end='2026-12-10')
    table = table[table['TimeStamp'] < '2026-12-09']
    later = split_table(days=2, last=0.10, wobble=0, end='2026-12-10')
    assert mc.trend_alerts(mc.SPLIT_FAIL_TREND, pd.concat([table, later]), '2026-12-10').empty


def test_weekdays_are_judged_against_weekdays_only():
    # Weekends always fail more: no alert on a weekday, and a weekend day judged against weekend days only.
    table = split_table(days=28, last=0.10, wobble=0)
    weekend = table['TimeStamp'].dt.day_of_week >= 5
    table.loc[weekend, 'Split_Failure'] = 60
    assert mc.trend_alerts(mc.SPLIT_FAIL_TREND, table, '2026-12-10').empty
    assert mc.trend_alerts(mc.SPLIT_FAIL_TREND, table, '2026-12-06', history_days=28).empty


def test_split_failure_trend_leaves_out_overnight_failing_zones_and_faults():
    assert mc.trend_alerts(mc.SPLIT_FAIL_TREND, split_table(night=0.9), '2026-12-10').empty
    faults = mi.fault_spans(pd.DataFrame([finding(1, '2026-12-10 12:00', '2026-12-10 12:30')]), DETECTORS)
    assert mc.trend_alerts(mc.SPLIT_FAIL_TREND, split_table(), '2026-12-10', faults).empty
    advance = mi.fault_spans(pd.DataFrame([finding(2, '2026-12-10 12:00', '2026-12-10 12:30')]), DETECTORS)
    assert len(mc.trend_alerts(mc.SPLIT_FAIL_TREND, split_table(), '2026-12-10', advance)) == 1


def test_falling_platoon_ratio_alerts():
    days = 21
    stamps = pd.date_range('2026-11-20', periods=days * 96, freq='15min')
    last = stamps >= pd.Timestamp('2026-12-10')
    wobble = 0.02 * ((stamps.day * 7) % 5 - 2)
    green = np.where(last, 0.25, 0.6 + wobble) * 40
    pr = pd.DataFrame({'TimeStamp': stamps, 'DeviceId': 'A', 'Phase': 4, 'Total_Actuations': 40,
                       'Green_Actuations': green, 'Green_Seconds': 450.0})
    alerts = mc.trend_alerts(mc.PLATOON_TREND, pr, '2026-12-10')
    assert len(alerts) == 1 and alerts.iloc[0].Text.startswith('Phase 4 platoon ratio fell to 0.50')
    assert mc.trend_alerts(mc.PLATOON_TREND, pr[pr['TimeStamp'] >= '2026-11-25'], '2026-12-10').empty


def test_the_max_out_method_is_unchanged_for_max_outs():
    days = pd.date_range('2026-11-01', periods=21)
    t = pd.DataFrame({'DeviceId': 'A', 'Phase': 2, 'Date': days, 'Percent MaxOut': [0.05] * 20 + [0.6], 'Services': 100})
    flagged = alert(cusum(t, k_value=1))
    assert flagged['Alert'].iloc[-1] == 1 and 'CUSUM_Percent MaxOut' in flagged.columns


def test_falling_platoon_ratio_leaves_out_faulting_advance_zones_and_needs_the_report_day():
    stamps = pd.date_range('2026-11-20', periods=21 * 96, freq='15min')
    last = stamps >= pd.Timestamp('2026-12-10')
    green = np.where(last, 0.25, 0.6 + 0.02 * ((stamps.day * 7) % 5 - 2)) * 40
    pr = pd.DataFrame({'TimeStamp': stamps, 'DeviceId': 'A', 'Phase': 4, 'Total_Actuations': 40,
                       'Green_Actuations': green, 'Green_Seconds': 450.0})
    advance = mi.fault_spans(pd.DataFrame([finding(2, '2026-12-10 12:00', '2026-12-10 12:30')]), DETECTORS)
    assert mc.trend_alerts(mc.PLATOON_TREND, pr, '2026-12-10', advance).empty
    stop_bar = mi.fault_spans(pd.DataFrame([finding(1, '2026-12-10 12:00', '2026-12-10 12:30')]), DETECTORS)
    assert len(mc.trend_alerts(mc.PLATOON_TREND, pr, '2026-12-10', stop_bar)) == 1
    # A report day after the fall: no alert dated an earlier day, unless that day is judged too.
    assert mc.trend_alerts(mc.PLATOON_TREND, pr, '2026-12-11').empty
    assert mc.trend_alerts(mc.PLATOON_TREND, pr, '2026-12-11', check_days=2).iloc[0].Text.startswith('On Thu 12/10: Phase 4')


def test_delay_rise_numbers_are_sums_divided_once():
    ad, aog, pr = delay_tables(pr_fall=True)
    busy = ad['TimeStamp'].dt.hour == 8
    # One flagged hour twice as busy: the told delay is weighted by its vehicles, not an average of the hours.
    ad.loc[busy, 'Delay_Seconds'] *= 2
    aog.loc[busy, 'Total_Actuations'] *= 2
    pr.loc[busy, ['Total_Actuations', 'Green_Actuations']] *= 2
    row = mc.delay_rise_alerts(ad, aog, '2026-11-20', platoon_ratio=pr).iloc[0]
    report = (ad['TimeStamp'] >= '2026-11-20') & ad['TimeStamp'].dt.hour.isin(row.Hours)
    assert row.Value == pytest.approx(ad.loc[report, 'Delay_Seconds'].sum() / aog.loc[report, 'Total_Actuations'].sum())
