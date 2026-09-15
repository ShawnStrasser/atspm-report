from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd
import pytest

from atspm_report import ReportGenerator
from atspm_report.preempt_processing import (
    DIRECTION_DECREASE,
    DIRECTION_INCREASE,
    PREEMPT_ALERT_COLUMNS,
    PREEMPT_HISTORY_COLUMNS,
    PRESENCE_PREEMPT,
    build_preempt_alerts,
    summarize_daily_preempts,
    update_preempt_history,
)
from atspm_report.table_generation import prepare_preempt_alerts_table


DAY = pd.Timestamp('2026-08-01')


def _timeline_row(device, start, duration, event_class='Preempt', value=3, is_valid=True):
    return {
        'DeviceId': device,
        'StartTime': start,
        'EndTime': start + timedelta(seconds=duration),
        'Duration': float(duration),
        'IsValid': is_valid,
        'EventClass': event_class,
        'EventValue': value,
    }


def _history(device, counts, preempt=3, start=DAY, present=None):
    """Build a history frame: one presence row per day plus call rows where count > 0.

    ``counts`` is a list of daily call counts, oldest first. ``present`` optionally
    lists booleans for whether the device reported that day.
    """
    rows = []
    for i, count in enumerate(counts):
        date = start + pd.Timedelta(days=i)
        if present is not None and not present[i]:
            continue
        rows.append({'DeviceId': device, 'Preempt': PRESENCE_PREEMPT, 'Date': date,
                     'Count': 0, 'ValidCount': 0, 'TotalDuration': 0.0, 'MaxDuration': None})
        if count > 0:
            rows.append({'DeviceId': device, 'Preempt': preempt, 'Date': date,
                         'Count': count, 'ValidCount': count,
                         'TotalDuration': 20.0 * count, 'MaxDuration': 20.0})
    return pd.DataFrame(rows, columns=PREEMPT_HISTORY_COLUMNS)


# ---------------------------------------------------------------------------
# summarize_daily_preempts
# ---------------------------------------------------------------------------

def test_summarize_counts_calls_and_valid_durations_only():
    base = DAY + pd.Timedelta(hours=1)
    timeline = pd.DataFrame([
        # Non-preempt rows spanning the day so the device counts as present.
        _timeline_row('sig-1', base, 10, event_class='Green', value=2),
        _timeline_row('sig-1', base + timedelta(hours=14), 10, event_class='Green', value=2),
        _timeline_row('sig-1', base + timedelta(hours=2), 30, value=3, is_valid=True),
        _timeline_row('sig-1', base + timedelta(hours=3), 50, value=3, is_valid=True),
        # Unmatched call: counted, but its duration is ignored.
        _timeline_row('sig-1', base + timedelta(hours=4), 9999, value=3, is_valid=False),
        _timeline_row('sig-1', base + timedelta(hours=5), 15, value=5, is_valid=True),
    ])

    daily = summarize_daily_preempts(timeline)

    assert list(daily.columns) == PREEMPT_HISTORY_COLUMNS
    presence = daily[daily['Preempt'] == PRESENCE_PREEMPT]
    assert len(presence) == 1
    assert presence.iloc[0]['DeviceId'] == 'sig-1'
    assert presence.iloc[0]['Date'] == DAY

    p3 = daily[daily['Preempt'] == 3].iloc[0]
    assert p3['Count'] == 3
    assert p3['ValidCount'] == 2
    assert p3['TotalDuration'] == pytest.approx(80.0)
    assert p3['MaxDuration'] == pytest.approx(50.0)

    p5 = daily[daily['Preempt'] == 5].iloc[0]
    assert p5['Count'] == 1
    assert p5['TotalDuration'] == pytest.approx(15.0)


def test_summarize_skips_presence_for_short_coverage():
    base = DAY + pd.Timedelta(hours=1)
    timeline = pd.DataFrame([
        _timeline_row('sig-1', base, 10, event_class='Green', value=2),
        _timeline_row('sig-1', base + timedelta(hours=3), 10, event_class='Green', value=2),
        _timeline_row('sig-1', base + timedelta(hours=2), 30, value=3),
    ])

    daily = summarize_daily_preempts(timeline)

    # Calls still recorded, but the device-day is not treated as fully reported.
    assert (daily['Preempt'] == PRESENCE_PREEMPT).sum() == 0
    assert daily[daily['Preempt'] == 3].iloc[0]['Count'] == 1


def test_summarize_handles_empty_timeline():
    daily = summarize_daily_preempts(pd.DataFrame())
    assert daily.empty
    assert list(daily.columns) == PREEMPT_HISTORY_COLUMNS


# ---------------------------------------------------------------------------
# update_preempt_history
# ---------------------------------------------------------------------------

def test_update_replaces_same_date_and_applies_retention():
    past = _history('sig-1', [1, 2, 3])
    rerun = _history('sig-1', [7], start=DAY + pd.Timedelta(days=2))  # same date as day 3

    updated = update_preempt_history(rerun, past, retention_days=42)

    day3 = updated[(updated['Date'] == DAY + pd.Timedelta(days=2)) & (updated['Preempt'] == 3)]
    assert len(day3) == 1
    assert day3.iloc[0]['Count'] == 7
    # Days 1 and 2 untouched.
    assert updated[updated['Preempt'] == 3]['Count'].tolist() == [1, 2, 7]

    later = _history('sig-1', [1], start=DAY + pd.Timedelta(days=50))
    trimmed = update_preempt_history(later, updated, retention_days=42)
    assert trimmed['Date'].min() >= DAY + pd.Timedelta(days=50) - pd.Timedelta(days=41)
    assert len(trimmed) == 2  # presence + call rows for the new day only


def test_update_with_no_past_history_returns_new_rows():
    new = _history('sig-1', [2])
    updated = update_preempt_history(new, pd.DataFrame())
    assert len(updated) == 2
    assert list(updated.columns) == PREEMPT_HISTORY_COLUMNS


# ---------------------------------------------------------------------------
# build_preempt_alerts
# ---------------------------------------------------------------------------

def test_increase_is_flagged():
    history = _history('sig-1', [2] * 14 + [30] * 7)

    alerts = build_preempt_alerts(history)

    assert list(alerts.columns) == PREEMPT_ALERT_COLUMNS
    assert len(alerts) == 1
    row = alerts.iloc[0]
    assert row['Direction'] == DIRECTION_INCREASE
    assert row['BaselinePerDay'] == 2.0
    assert row['RecentPerDay'] == 30.0
    assert row['BaselineDays'] == 14
    assert row['RecentDays'] == 7
    assert row['Date'] == DAY + pd.Timedelta(days=20)
    assert row['DailyCounts'] == [2] * 14 + [30] * 7


def test_decrease_is_flagged():
    history = _history('sig-1', [10] * 14 + [0] * 7)

    alerts = build_preempt_alerts(history)

    assert len(alerts) == 1
    assert alerts.iloc[0]['Direction'] == DIRECTION_DECREASE
    assert alerts.iloc[0]['RecentPerDay'] == 0.0


def test_new_preempt_number_compares_against_zero_baseline():
    # Preempt 3 fires steadily; preempt 7 only appears in the last week.
    p7 = _history('sig-1', [0] * 14 + [4] * 7, preempt=7)
    history = pd.concat([
        _history('sig-1', [2] * 21, preempt=3),
        p7[p7['Preempt'] != PRESENCE_PREEMPT],
    ], ignore_index=True)

    alerts = build_preempt_alerts(history)

    assert alerts['Preempt'].tolist() == [7]
    assert alerts.iloc[0]['BaselinePerDay'] == 0.0
    assert alerts.iloc[0]['Direction'] == DIRECTION_INCREASE


def test_steady_pair_does_not_alert():
    history = _history('sig-1', [3, 2, 4, 3, 3, 2, 5, 3, 4, 2, 3, 3, 4, 2, 3, 4, 2, 3, 3, 5, 2])
    assert build_preempt_alerts(history).empty


def test_requires_minimum_baseline_days():
    history = _history('sig-1', [2] * 9 + [30] * 7)  # only 9 baseline days
    assert build_preempt_alerts(history).empty

    history = _history('sig-1', [2] * 10 + [30] * 7)
    assert len(build_preempt_alerts(history)) == 1


def test_decrease_ignored_for_rare_preempts():
    # Median baseline of 0.5 < 1 call/day: going quiet is not evidence of a fault.
    history = _history('sig-1', [1, 0] * 7 + [0] * 7)
    assert build_preempt_alerts(history).empty


def test_device_absent_on_report_date_is_skipped():
    history = pd.concat([
        _history('sig-1', [2] * 14 + [30] * 7),
        # sig-2 stops reporting before the report date, so it isn't evaluated.
        _history('sig-2', [2] * 14 + [30] * 6 + [0], present=[True] * 20 + [False]),
    ], ignore_index=True)

    alerts = build_preempt_alerts(history)

    assert alerts['DeviceId'].tolist() == ['sig-1']


def test_unreported_days_are_not_counted_as_zero():
    # Device missed 7 days in the recent window; those days are neither zeros
    # nor part of the window, so the preempt is judged only on reported days.
    present = [True] * 14 + [False] * 7 + [True] * 7
    history = _history('sig-1', [10] * 14 + [0] * 7 + [10] * 7, present=present)

    assert build_preempt_alerts(history).empty


def test_empty_history_returns_empty_alerts():
    alerts = build_preempt_alerts(pd.DataFrame())
    assert alerts.empty
    assert list(alerts.columns) == PREEMPT_ALERT_COLUMNS


# ---------------------------------------------------------------------------
# table preparation
# ---------------------------------------------------------------------------

def test_prepare_preempt_alerts_table_filters_region_and_formats():
    alerts = build_preempt_alerts(pd.concat([
        _history('sig-1', [2] * 14 + [30] * 7),
        _history('sig-2', [10] * 14 + [0] * 7),
    ], ignore_index=True))
    signals = pd.DataFrame([
        {'DeviceId': 'sig-1', 'Name': 'Main & 1st', 'Region': 'Region 1'},
        {'DeviceId': 'sig-2', 'Name': 'Main & 2nd', 'Region': 'Region 2'},
    ])

    table, total = prepare_preempt_alerts_table(alerts, signals, region='Region 1')

    assert total == 1
    assert list(table.columns) == ['Signal', 'Preempt', 'Change', 'Baseline/Day', 'Recent/Day', 'Sparkline_Data']
    row = table.iloc[0]
    assert row['Signal'] == 'Main & 1st'
    assert row['Preempt'] == 3
    assert row['Change'] == DIRECTION_INCREASE
    assert row['Baseline/Day'] == '2.0'
    assert row['Recent/Day'] == '30.0'
    assert row['Sparkline_Data'] == [2] * 14 + [30] * 7

    all_table, all_total = prepare_preempt_alerts_table(alerts, signals, region='All Regions')
    assert all_total == 2


# ---------------------------------------------------------------------------
# end to end through ReportGenerator
# ---------------------------------------------------------------------------

def _generate(preempt_history, past_alerts, config=None):
    cfg = {
        'suppress_repeated_alerts': True,
        'alert_suppression_days': 21,
        'alert_flagging_days': 7,
        'verbosity': 0,
    }
    if config:
        cfg.update(config)
    with patch('atspm_report.generator.generate_pdf_report', return_value={}) as pdf:
        result = ReportGenerator(cfg).generate(
            signals=pd.DataFrame([{'DeviceId': 'sig-1', 'Name': 'Signal 1', 'Region': 'Region 1'}]),
            timeline=pd.DataFrame(),
            past_alerts=past_alerts,
            preempt_history=preempt_history,
        )
    return result, pdf


def test_generate_reports_preempt_alerts_and_updates_history():
    today = pd.Timestamp(datetime.now().date())
    history = _history('sig-1', [2] * 14 + [30] * 7, start=today - pd.Timedelta(days=20))

    result, pdf = _generate(history, past_alerts={})

    alerts = result['alerts']['preempts']
    assert len(alerts) == 1
    assert alerts.iloc[0]['Direction'] == DIRECTION_INCREASE
    assert pdf.call_args.kwargs['preempt_alerts_df'] is alerts
    # Empty timeline: history passes through unchanged.
    assert len(result['updated_preempt_history']) == len(history)

    stored = result['updated_past_alerts']['preempts']
    assert set(['DeviceId', 'Preempt', 'Direction', 'Date']).issubset(stored.columns)
    assert len(stored) == 1


def test_generate_suppresses_repeat_but_not_opposite_direction():
    today = pd.Timestamp(datetime.now().date())
    history = _history('sig-1', [2] * 14 + [30] * 7, start=today - pd.Timedelta(days=20))
    past_alerts = {
        'preempts': pd.DataFrame([
            {'DeviceId': 'sig-1', 'Preempt': 3, 'Direction': DIRECTION_INCREASE,
             'Date': today - pd.Timedelta(days=3)},
        ]),
    }

    result, _ = _generate(history, past_alerts)
    assert result['alerts']['preempts'].empty

    # Same pair, but the past alert was for a drop: the increase is new.
    past_alerts['preempts']['Direction'] = DIRECTION_DECREASE
    result, _ = _generate(history, past_alerts)
    assert len(result['alerts']['preempts']) == 1
