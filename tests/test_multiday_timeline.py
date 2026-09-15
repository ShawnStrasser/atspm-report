"""A timeline spanning several days rebuilds the alarm and preempt counts each run."""

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd

from atspm_report import ReportGenerator, ALARM_HISTORY_DAYS


REPORT_DAY = datetime.combine(datetime.now().date() - timedelta(days=1), datetime.min.time())
DEVICE = "1"
SIGNALS = pd.DataFrame([{"DeviceId": DEVICE, "Name": "Sig 1", "Region": "R1"}])


def interval(start, seconds, event_class, value, device=DEVICE, valid=True):
    return {
        "DeviceId": device, "StartTime": start, "EndTime": start + timedelta(seconds=seconds),
        "Duration": float(seconds), "IsValid": valid, "EventClass": event_class, "EventValue": value,
    }


def full_day(day):
    """Rows spanning the day so it counts as reported (>= 12h of timeline)."""
    return [interval(day + timedelta(hours=1), 20, "Green", 2),
            interval(day + timedelta(hours=22), 20, "Green", 2)]


def yellows(day, durations):
    return [interval(day + timedelta(hours=8, minutes=i), d, "Yellow", 2) for i, d in enumerate(durations)]


def generate(**kwargs):
    generator = ReportGenerator({"verbosity": 0, "suppress_repeated_alerts": False})
    with patch("atspm_report.generator.create_device_plots", return_value=[]), \
         patch("atspm_report.generator.create_phase_skip_plots", return_value=[]), \
         patch("atspm_report.generator.generate_pdf_report", return_value={}):
        return generator.generate(signals=SIGNALS, past_alerts={}, **kwargs)


class TestMultiDayTimeline(unittest.TestCase):

    def test_alarm_counts_are_rebuilt_from_every_day_present(self):
        rows = full_day(REPORT_DAY)
        rows.append(interval(REPORT_DAY + timedelta(hours=3), 30, "MMU Flash", 1))
        older = REPORT_DAY - timedelta(days=3)
        rows += [interval(older + timedelta(hours=h), 30, "MMU Flash", 1) for h in (4, 5)]
        expired = REPORT_DAY - timedelta(days=ALARM_HISTORY_DAYS + 5)
        rows.append(interval(expired, 30, "MMU Flash", 1))

        result = generate(timeline=pd.DataFrame(rows))

        alarms = result["alarms"]
        self.assertEqual(len(alarms), 1)
        self.assertEqual(alarms.iloc[0]["DayCount"], 1)
        self.assertEqual(alarms.iloc[0]["TotalCount"], 3)
        self.assertEqual(pd.Timestamp(alarms.iloc[0]["Date"]), pd.Timestamp(REPORT_DAY))

        history = result["updated_alarm_history"]
        self.assertEqual(sorted(pd.to_datetime(history["Date"]).dt.normalize().unique()),
                         [pd.Timestamp(older), pd.Timestamp(REPORT_DAY)])

    def test_alarm_that_did_not_fire_on_the_report_day_is_not_listed(self):
        rows = full_day(REPORT_DAY)
        rows.append(interval(REPORT_DAY - timedelta(days=2), 30, "MMU Flash", 1))

        result = generate(timeline=pd.DataFrame(rows))

        self.assertTrue(result["alarms"].empty)
        self.assertEqual(len(result["updated_alarm_history"]), 1)

    def test_passed_history_is_kept_for_days_the_timeline_does_not_cover(self):
        rows = full_day(REPORT_DAY)
        rows.append(interval(REPORT_DAY + timedelta(hours=3), 30, "MMU Flash", 1))
        in_timeline = REPORT_DAY - timedelta(days=1)
        rows.append(interval(in_timeline + timedelta(hours=3), 30, "MMU Flash", 1))
        not_in_timeline = REPORT_DAY - timedelta(days=10)
        alarm_history = pd.DataFrame([
            {"DeviceId": DEVICE, "AlarmType": "MMU Flash", "Date": not_in_timeline, "Count": 5,
             "LatestAlarm": not_in_timeline + timedelta(hours=1)},
            {"DeviceId": DEVICE, "AlarmType": "MMU Flash", "Date": in_timeline, "Count": 99,
             "LatestAlarm": in_timeline + timedelta(hours=1)},
        ])

        result = generate(timeline=pd.DataFrame(rows), alarm_history=alarm_history)

        history = result["updated_alarm_history"].set_index("Date")["Count"]
        self.assertEqual(history[pd.Timestamp(not_in_timeline)], 5)   # kept
        self.assertEqual(history[pd.Timestamp(in_timeline)], 1)       # replaced by the timeline
        self.assertEqual(result["alarms"].iloc[0]["TotalCount"], 7)

    def test_clearance_checks_only_look_at_the_latest_day(self):
        short_on_older_day = pd.DataFrame(
            yellows(REPORT_DAY, [3.5, 3.5, 3.5, 3.5])
            + yellows(REPORT_DAY - timedelta(days=1), [3.5, 3.5, 3.5, 3.3])
        )
        self.assertTrue(generate(timeline=short_on_older_day)["alerts"]["clearance_intervals"].empty)

        short_on_report_day = pd.DataFrame(
            yellows(REPORT_DAY, [3.5, 3.5, 3.5, 3.3])
            + yellows(REPORT_DAY - timedelta(days=1), [3.5, 3.5, 3.5, 3.5])
        )
        alerts = generate(timeline=short_on_report_day)["alerts"]["clearance_intervals"]
        self.assertEqual(len(alerts), 1)
        self.assertEqual(pd.Timestamp(alerts.iloc[0]["Date"]), pd.Timestamp(REPORT_DAY))

    def test_device_days_lets_trimmed_older_days_count_as_reported(self):
        # A preempt that fired once a day for 20 days, then went silent for a week.
        rows = full_day(REPORT_DAY)
        days = [REPORT_DAY - timedelta(days=i) for i in range(27)]
        for day in days[7:]:
            rows.append(interval(day + timedelta(hours=10), 30, "Preempt", 1))
        timeline = pd.DataFrame(rows)
        device_days = pd.DataFrame({"DeviceId": DEVICE, "Date": days})

        with_presence = generate(timeline=timeline, device_days=device_days)
        alerts = with_presence["alerts"]["preempts"]
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]["Direction"], "Decrease")
        self.assertEqual(alerts.iloc[0]["BaselinePerDay"], 1.0)
        self.assertEqual(alerts.iloc[0]["RecentPerDay"], 0.0)
        presence = with_presence["updated_preempt_history"]
        self.assertEqual(int((presence["Preempt"] == 0).sum()), len(days))

        # Older days trimmed to their Preempt rows cover far less than 12h, so
        # without device_days they do not count as reported days.
        without_presence = generate(timeline=timeline)
        self.assertTrue(without_presence["alerts"]["preempts"].empty)


if __name__ == "__main__":
    unittest.main()
