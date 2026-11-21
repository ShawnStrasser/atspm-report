from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd

from atspm_report import ReportGenerator


def test_overlap_dual_indications_are_never_suppressed():
    now = datetime.now().replace(microsecond=0)
    timeline = pd.DataFrame([
        {
            'DeviceId': 'signal-1',
            'StartTime': now - timedelta(seconds=20),
            'EndTime': now,
            'Duration': 20.0,
            'IsValid': True,
            'EventClass': 'Green',
            'EventValue': 5,
        },
        {
            'DeviceId': 'signal-1',
            'StartTime': now - timedelta(seconds=10),
            'EndTime': now - timedelta(seconds=5),
            'Duration': 5.0,
            'IsValid': True,
            'EventClass': 'Overlap Yellow',
            'EventValue': 5,
        },
    ])
    past_alerts = {
        'overlap_dual_indications': pd.DataFrame([
            {'DeviceId': 'signal-1', 'Phase': 5, 'Date': now - timedelta(days=1)},
        ]),
    }
    config = {
        'overlap_dual_indications_enabled': True,
        'overlap_dual_indication_phases': [5],
        'suppress_repeated_alerts': True,
        'alert_suppression_days': 21,
        'alert_flagging_days': 7,
        'verbosity': 0,
    }

    with patch('atspm_report.generator.generate_pdf_report', return_value={}):
        result = ReportGenerator(config).generate(
            signals=pd.DataFrame([
                {'DeviceId': 'signal-1', 'Name': 'Signal 1', 'Region': 'Region 1'},
            ]),
            timeline=timeline,
            past_alerts=past_alerts,
        )

    alerts = result['alerts']['overlap_dual_indications']
    assert len(alerts) == 1
    assert alerts.iloc[0]['Phase'] == 5


def _clearance_config(**overrides):
    config = {
        'suppress_repeated_alerts': True,
        'alert_suppression_days': 21,
        'alert_flagging_days': 7,
        'verbosity': 0,
    }
    config.update(overrides)
    return config


def _short_yellow_timeline(now, count=3):
    """A repeated 3.0s phase-8 yellow, which is below the 3.5s minimum."""
    rows = []
    for index in range(count):
        start = now - timedelta(minutes=10 * (index + 1))
        rows.append({
            'DeviceId': 'signal-1',
            'StartTime': start,
            'EndTime': start + timedelta(seconds=3),
            'Duration': 3.0,
            'IsValid': True,
            'EventClass': 'Yellow',
            'EventValue': 8,
        })
    return pd.DataFrame(rows)


def _generate(config, timeline, past_alerts):
    signals = pd.DataFrame([
        {'DeviceId': 'signal-1', 'Name': 'Signal 1', 'Region': 'Region 1'},
    ])
    with patch('atspm_report.generator.generate_pdf_report', return_value={}):
        return ReportGenerator(config).generate(
            signals=signals,
            timeline=timeline,
            past_alerts=past_alerts,
        )


def test_ongoing_alerts_are_empty_unless_requested():
    now = datetime.now().replace(microsecond=0)
    past_alerts = {
        'clearance_intervals': pd.DataFrame([
            {'DeviceId': 'signal-1', 'EventClass': 'Yellow', 'EventValue': 8,
             'Date': now - timedelta(days=2)},
        ]),
    }

    result = _generate(_clearance_config(), _short_yellow_timeline(now), past_alerts)

    assert result['alerts']['clearance_intervals'].empty  # suppressed as a repeat
    assert result['ongoing_alerts']['clearance_intervals'].empty


def test_ongoing_alerts_collect_suppressed_repeats_with_a_start_date():
    now = datetime.now().replace(microsecond=0)
    first_seen = (now - timedelta(days=5)).replace(hour=0, minute=0, second=0)
    past_alerts = {
        'clearance_intervals': pd.DataFrame([
            {'DeviceId': 'signal-1', 'EventClass': 'Yellow', 'EventValue': 8,
             'Date': first_seen + timedelta(days=offset)}
            for offset in range(4)
        ]),
    }

    result = _generate(
        _clearance_config(include_ongoing_issues=True),
        _short_yellow_timeline(now),
        past_alerts,
    )

    ongoing = result['ongoing_alerts']['clearance_intervals']
    assert result['alerts']['clearance_intervals'].empty
    assert len(ongoing) == 1
    assert ongoing.iloc[0]['EventValue'] == 8
    assert ongoing.iloc[0]['OngoingSince'] == pd.Timestamp(first_seen).normalize()


def test_ongoing_since_restarts_after_a_quiet_gap():
    """An issue that cleared and came back is dated from its return, not its first ever alert."""
    now = datetime.now().replace(microsecond=0)
    returned_on = (now - timedelta(days=3)).replace(hour=0, minute=0, second=0)
    past_alerts = {
        'clearance_intervals': pd.DataFrame([
            {'DeviceId': 'signal-1', 'EventClass': 'Yellow', 'EventValue': 8,
             'Date': now - timedelta(days=200)},
            {'DeviceId': 'signal-1', 'EventClass': 'Yellow', 'EventValue': 8,
             'Date': now - timedelta(days=199)},
            {'DeviceId': 'signal-1', 'EventClass': 'Yellow', 'EventValue': 8,
             'Date': returned_on},
        ]),
    }

    result = _generate(
        _clearance_config(include_ongoing_issues=True),
        _short_yellow_timeline(now),
        past_alerts,
    )

    ongoing = result['ongoing_alerts']['clearance_intervals']
    assert len(ongoing) == 1
    assert ongoing.iloc[0]['OngoingSince'] == pd.Timestamp(returned_on).normalize()


# ---- An old report day is judged from that day, not from the clock ----

OLD_DAY = datetime(2026, 10, 14, 12, 0)   # more than alert_flagging_days before any run of these tests


def test_rerun_of_an_old_report_day_still_flags_its_alerts():
    result = _generate(_clearance_config(), _short_yellow_timeline(OLD_DAY), {})
    alerts = result['alerts']['clearance_intervals']
    assert len(alerts) == 1 and alerts.iloc[0]['EventValue'] == 8


def test_rerun_of_an_old_report_day_is_suppressed_by_the_history_before_it():
    # Told 19 days before the report day, 20 before the morning after it: inside the 21 suppression days a daily run
    # that morning reads, and long before the clock's.
    past_alerts = {'clearance_intervals': pd.DataFrame([
        {'DeviceId': 'signal-1', 'EventClass': 'Yellow', 'EventValue': 8, 'Date': OLD_DAY - timedelta(days=19)}])}
    result = _generate(_clearance_config(include_ongoing_issues=True), _short_yellow_timeline(OLD_DAY), past_alerts)
    assert result['alerts']['clearance_intervals'].empty
    assert len(result['ongoing_alerts']['clearance_intervals']) == 1
    # 21 days before that morning is outside them.
    past_alerts['clearance_intervals']['Date'] = OLD_DAY - timedelta(days=20)
    result = _generate(_clearance_config(), _short_yellow_timeline(OLD_DAY), past_alerts)
    assert len(result['alerts']['clearance_intervals']) == 1


def test_report_today_is_the_day_after_the_report_day():
    from atspm_report.generator import _report_today
    assert _report_today(pd.Timestamp('2026-10-14 00:00'), None) == pd.Timestamp('2026-10-15')
    has_data = pd.DataFrame({'DeviceId': ['a', 'a'], 'TimeStamp': pd.to_datetime(['2026-10-13 08:00', '2026-10-14 23:45'])})
    assert _report_today(None, has_data) == pd.Timestamp('2026-10-15')
    assert _report_today(None, None) == pd.Timestamp(datetime.now().date())



def test_ongoing_day_counts_count_to_the_report_day():
    from atspm_report.table_generation import _ONGOING_TODAY, _format_ongoing_since, ongoing_counted_to
    with ongoing_counted_to(pd.Timestamp('2026-10-15')):
        assert _format_ongoing_since(pd.Timestamp('2026-10-10')) == '10/10/26 (5d)'
    assert _ONGOING_TODAY.get() is None
    today = pd.Timestamp(datetime.now().date())
    assert _format_ongoing_since(today - timedelta(days=3)).endswith('(3d)')   # unset: the clock's today


def test_old_rerun_report_counts_ongoing_days_to_its_report_day():
    from atspm_report import report_generation
    from atspm_report.table_generation import _format_ongoing_since
    past_alerts = {'clearance_intervals': pd.DataFrame([
        {'DeviceId': 'signal-1', 'EventClass': 'Yellow', 'EventValue': 8, 'Date': OLD_DAY - timedelta(days=4)}])}
    told = {}

    def render(*args, **kwargs):   # inside the report, as its tables format the Ongoing column
        since = kwargs['ongoing_alerts']['clearance_intervals']['OngoingSince'].min()
        told['text'] = _format_ongoing_since(since)
        return {}

    with patch.object(report_generation, '_generate_pdf_report', side_effect=render):
        result = ReportGenerator(_clearance_config(include_ongoing_issues=True)).generate(
            signals=pd.DataFrame([{'DeviceId': 'signal-1', 'Name': 'Signal 1', 'Region': 'Region 1'}]),
            timeline=_short_yellow_timeline(OLD_DAY), past_alerts=past_alerts)
    assert len(result['ongoing_alerts']['clearance_intervals']) == 1
    assert told['text'] == '10/10/26 (5d)'   # since 10/10, counted to the morning after 10/14, not to today
