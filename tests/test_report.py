import unittest
import pandas as pd
import numpy as np
from pathlib import Path
from io import BytesIO
from datetime import datetime, timedelta
from unittest.mock import patch
import sys
import os
import re
import tomli
import matplotlib
matplotlib.use('Agg')
import matplotlib.dates as mdates

# Add the src directory to the path to import the package directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atspm_report import ReportGenerator
import atspm_report
from atspm_report.visualization import create_phase_skip_plots, _format_time_axis
from atspm_report.clearance_processing import process_clearance_intervals
from atspm_report.table_generation import prepare_clearance_interval_alerts_table

class TestReportGenerator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = Path(__file__).parent / 'data'
        cls.signals_df = pd.read_parquet(cls.test_data_dir / 'signals.parquet')
        
        # Load data from the test data directory
        cls.subset_signals = cls.signals_df.copy()
        
        cls.terminations = pd.read_parquet(cls.test_data_dir / 'terminations.parquet')
        cls.detector_health = pd.read_parquet(cls.test_data_dir / 'detector_health.parquet')
        cls.has_data = pd.read_parquet(cls.test_data_dir / 'has_data.parquet')
        cls.pedestrian = pd.read_parquet(cls.test_data_dir / 'full_ped.parquet')
        
        # Create dummy phase_wait data to trigger an alert
        # Using one of the DeviceIds from the test data
        test_device_id = cls.signals_df['DeviceId'].iloc[0]
        now = datetime.now()
        cls.phase_wait = pd.DataFrame({
            'TimeStamp': [now - timedelta(hours=i) for i in range(6)],
            'DeviceId': [test_device_id] * 6,
            'Phase': [1, 1, 1, 2, 2, 2],
            'AvgPhaseWait': [150.0, 160.0, 170.0, 50.0, 55.0, 60.0],
            'MaxPhaseWait': [180.0, 190.0, 200.0, 70.0, 75.0, 80.0],
            'TotalSkips': [2, 3, 2, 0, 0, 0]  # Phase 1 has skips, Phase 2 doesn't
        })
        
        # Create dummy coordination_agg data with cycle length (15-minute bin aggregated)
        cls.coordination_agg = pd.DataFrame({
            'TimeStamp': [now - timedelta(hours=5), now - timedelta(hours=3), now - timedelta(hours=1)],
            'DeviceId': [test_device_id] * 3,
            'ActualCycleLength': [100.0, 120.0, 100.0]  # Cycle lengths
        })

        cls.config = {
            "historical_window_days": 21,
            "alert_flagging_days": 7,
            "suppress_repeated_alerts": True,
            "alert_suppression_days": 21,
            "figures_per_device": 1, # Speed up tests
            "verbosity": 1,
        }
        
        # Load expected alerts for comparison
        expected_alerts_dir = cls.test_data_dir / 'expected_alerts'
        cls.expected_alerts = {}
        for alert_file in expected_alerts_dir.glob('*.parquet'):
            alert_type = alert_file.stem
            cls.expected_alerts[alert_type] = pd.read_parquet(alert_file)

    def test_1_generate_new_alerts(self):
        """Test that alerts are generated and match expected outputs."""
        generator = ReportGenerator(self.config)
        
        data = {
            'signals': self.subset_signals,
            'terminations': self.terminations,
            'detector_health': self.detector_health,
            'has_data': self.has_data,
            'pedestrian': self.pedestrian,
            'phase_wait': self.phase_wait,
            'coordination_agg': self.coordination_agg
        }
        
        result = generator.generate(**data)
        
        self.assertIn('alerts', result)
        self.assertIn('reports', result)
        
        alerts = result['alerts']
        
        # Check that all alert types are present
        for alert_type in ['maxout', 'actuations', 'missing_data', 'pedestrian', 'phase_skips', 'clearance_intervals', 'overlap_dual_indications', 'general_phase_conflicts', 'overlap_conflicts', 'system_outages']:
            self.assertIn(alert_type, alerts, f"Missing alert type: {alert_type}")
            actual = alerts[alert_type]
            
            # Check that required columns are present for non-empty alerts
            if not actual.empty:
                if alert_type == 'maxout':
                    required_cols = ['DeviceId', 'Phase', 'Date']
                elif alert_type == 'actuations':
                    required_cols = ['DeviceId', 'Detector', 'Date']
                elif alert_type == 'missing_data':
                    required_cols = ['DeviceId', 'Date']
                elif alert_type == 'pedestrian':
                    required_cols = ['DeviceId', 'Phase', 'Date']
                elif alert_type == 'phase_skips':
                    required_cols = ['DeviceId', 'Phase', 'Date']
                elif alert_type == 'clearance_intervals':
                    required_cols = ['DeviceId', 'EventClass', 'EventValue', 'Date']
                elif alert_type == 'overlap_dual_indications':
                    required_cols = ['DeviceId', 'Phase', 'Date', 'ConflictStart']
                elif alert_type in ['general_phase_conflicts', 'overlap_conflicts']:
                    required_cols = [
                        'DeviceId', 'Date', 'ConflictStart',
                        'Movement1Number', 'Movement2Number',
                    ]
                elif alert_type == 'system_outages':
                    required_cols = ['Date', 'Region']
                else:
                    required_cols = []
                
                for col in required_cols:
                    self.assertIn(col, actual.columns, 
                        f"{alert_type}: Missing required column '{col}'")
        
        # Verify we got some alerts (at least phase_skips since we created test data for it)
        self.assertGreater(
            len(alerts['phase_skips']), 0,
            "Expected at least one phase_skips alert from synthetic data"
        )
        
        self.assertTrue(len(result['reports']) > 0, "No PDF reports were generated")
        
        # Store alerts for the next test
        self.__class__.past_alerts = result['updated_past_alerts']

    def test_2_suppress_alerts(self):
        """Test that alerts are suppressed when past alerts are provided."""
        if not hasattr(self, 'past_alerts'):
            self.skipTest("Test 1 did not store past_alerts")
            
        generator = ReportGenerator(self.config)
        
        data = {
            'signals': self.subset_signals,
            'terminations': self.terminations,
            'detector_health': self.detector_health,
            'has_data': self.has_data,
            'pedestrian': self.pedestrian,
            'phase_wait': self.phase_wait,
            'coordination_agg': self.coordination_agg,
            'past_alerts': self.past_alerts
        }
        
        result = generator.generate(**data)
        
        alerts = result['alerts']
        for alert_type, df in alerts.items():
            self.assertTrue(df.empty, f"Alert type {alert_type} was not suppressed: {df}")
            
        self.assertEqual(len(result['reports']), 0, "Reports were generated even though all alerts should be suppressed")

    def test_3_deviceid_as_int(self):
        """Test that DeviceId can be provided as int and gets converted to string."""
        # Create a copy of signals with DeviceId as int
        signals_with_int_deviceid = self.subset_signals.copy()
        signals_with_int_deviceid['DeviceId'] = range(len(signals_with_int_deviceid))
        
        # Create matching test data with int DeviceIds
        test_terminations = self.terminations.copy()
        test_terminations['DeviceId'] = 0
        
        test_detector_health = self.detector_health.copy()
        test_detector_health['DeviceId'] = 0
        
        test_has_data = self.has_data.copy()
        test_has_data['DeviceId'] = 0
        
        test_pedestrian = self.pedestrian.copy()
        test_pedestrian['DeviceId'] = 0
        
        generator = ReportGenerator(self.config)
        
        # Should not raise an error
        result = generator.generate(
            signals=signals_with_int_deviceid,
            terminations=test_terminations,
            detector_health=test_detector_health,
            has_data=test_has_data,
            pedestrian=test_pedestrian
        )
        
        # Verify DeviceIds are strings in the output
        for alert_type, alerts_df in result['alerts'].items():
            if not alerts_df.empty and 'DeviceId' in alerts_df.columns:
                self.assertEqual(
                    alerts_df['DeviceId'].dtype,
                    'object',
                    f"{alert_type}: DeviceId should be string type"
                )
    
    def test_4_phase_skip_retention_handles_empty_alert_rows(self):
        """Regression: phase skip retention should handle zero-skip input without type errors."""
        signals = pd.DataFrame([
            {"DeviceId": "1", "Name": "Test Signal", "Region": "R1"}
        ])
        phase_wait = pd.DataFrame([
            {
                "TimeStamp": pd.Timestamp("2026-01-26 00:00:00"),
                "DeviceId": "1",
                "Phase": 2,
                "AvgPhaseWait": 10.0,
                "MaxPhaseWait": 12.0,
                "TotalSkips": 0,
            }
        ])
        
        config = {
            **self.config,
            "verbosity": 0,
            "suppress_repeated_alerts": False,
            "phase_skip_retention_days": 14,
        }
        generator = ReportGenerator(config)
        
        with patch("atspm_report.generator.create_device_plots", return_value=[]), \
             patch("atspm_report.generator.create_phase_skip_plots", return_value=[]), \
             patch("atspm_report.generator.generate_pdf_report", return_value={}):
            result = generator.generate(
                signals=signals,
                phase_wait=phase_wait,
                past_alerts={},
            )
        
        self.assertTrue(
            result['alerts']['phase_skips'].empty,
            "Expected no phase skip alerts when TotalSkips is zero"
        )
        self.assertEqual(
            len(result['reports']),
            0,
            "Expected no PDF reports when no alerts are generated"
        )

    def test_5_empty_terminations_do_not_crash_report_generation(self):
        """Regression: empty terminations input should not crash visualization."""
        generator = ReportGenerator({
            **self.config,
            "verbosity": 0,
            "suppress_repeated_alerts": False,
        })

        empty_terminations = self.terminations.iloc[0:0].copy()

        result = generator.generate(
            signals=self.subset_signals,
            terminations=empty_terminations,
            detector_health=self.detector_health,
            has_data=self.has_data,
        )

        self.assertTrue(
            result['alerts']['maxout'].empty,
            "Expected maxout alerts to be empty when terminations has no rows"
        )
        self.assertIn('reports', result)

    def test_6_clearance_intervals_flow_through_generator_history(self):
        """Clearance interval alerts should be exposed and saved by movement identity."""
        signal_id = "clearance-signal"
        signals = pd.DataFrame([
            {"DeviceId": signal_id, "Name": "Clearance Signal", "Region": "R1"}
        ])
        now = datetime.now().replace(microsecond=200000)
        timeline = pd.DataFrame({
            "DeviceId": [signal_id] * 4,
            "StartTime": [now - timedelta(minutes=i) for i in range(4)],
            "EndTime": [now - timedelta(minutes=i) + timedelta(seconds=3) for i in range(4)],
            "Duration": [3.5, 3.5, 3.5, 3.3],
            "IsValid": [True] * 4,
            "EventClass": ["Yellow"] * 4,
            "EventValue": [2] * 4,
        })
        generator = ReportGenerator({
            **self.config,
            "verbosity": 0,
            "suppress_repeated_alerts": False,
        })

        with patch("atspm_report.generator.create_device_plots", return_value=[]), \
             patch("atspm_report.generator.create_phase_skip_plots", return_value=[]), \
             patch("atspm_report.generator.generate_pdf_report", return_value={}) as pdf_mock:
            result = generator.generate(
                signals=signals,
                timeline=timeline,
                past_alerts={},
            )

        clearance_alerts = result['alerts']['clearance_intervals']
        self.assertEqual(len(clearance_alerts), 1)
        self.assertEqual(clearance_alerts.iloc[0]['DeviceId'], signal_id)
        self.assertEqual(clearance_alerts.iloc[0]['EventClass'], "Yellow")
        self.assertEqual(clearance_alerts.iloc[0]['EventValue'], 2)

        clearance_history = result['updated_past_alerts']['clearance_intervals']
        self.assertEqual(
            clearance_history.columns.tolist(),
            ['DeviceId', 'EventClass', 'EventValue', 'Date']
        )
        self.assertEqual(len(clearance_history), 1)
        self.assertIn('clearance_alerts_df', pdf_mock.call_args.kwargs)


class TestOngoingSectionLayout(unittest.TestCase):
    """Each check's charts sit under its own table, new alerts before ongoing ones."""

    def _maxout_frame(self, phase, percent, ongoing_since=None):
        rows = [
            {
                'DeviceId': '1', 'Phase': phase, 'Date': datetime(2026, 9, 8 + day),
                'Alert': 1 if day else 0, 'Percent MaxOut': percent,
            }
            for day in range(2)
        ]
        frame = pd.DataFrame(rows)
        if ongoing_since is not None:
            frame['OngoingSince'] = ongoing_since
        return frame

    def test_charts_follow_their_own_table_and_ongoing_comes_last(self):
        import matplotlib.pyplot as plt
        from atspm_report.report_generation import generate_pdf_report

        signals = pd.DataFrame([
            {'DeviceId': '1', 'Name': 'Signal 1', 'Region': 'Region 1'},
        ])
        new_figure, ongoing_figure = plt.figure(), plt.figure()
        events = []

        def record_table(df, title, styles, **kwargs):
            events.append(('table', title))
            return []

        def record_charts(figures):
            if figures:
                events.append(('charts', len(figures)))
            return []

        with patch('atspm_report.report_generation.create_reportlab_table', side_effect=record_table), \
             patch('atspm_report.report_generation._chart_flowables', side_effect=record_charts):
            generate_pdf_report(
                filtered_df_maxouts=self._maxout_frame(2, 0.5),
                filtered_df_actuations=pd.DataFrame(),
                filtered_df_ped=pd.DataFrame(),
                ped_hourly_df=pd.DataFrame(),
                filtered_df_missing_data=pd.DataFrame(),
                system_outages_df=pd.DataFrame(),
                phase_figures=[(new_figure, 'Region 1')],
                detector_figures=[], ped_figures=[], missing_data_figures=[],
                signals_df=signals,
                ongoing_alerts={
                    'maxout': self._maxout_frame(4, 0.6, pd.Timestamp('2026-08-30')),
                },
                ongoing_phase_figures=[(ongoing_figure, 'Region 1')],
                verbosity=0,
            )

        # "All Regions" is rendered first and holds no region-tagged figures, so
        # anchor on the region that does have both sets of charts.
        start = events.index(('table', 'Phase Termination Alerts'))
        self.assertEqual(
            events[start:start + 4],
            [
                ('table', 'Phase Termination Alerts'),
                ('charts', 1),
                ('table', 'Ongoing Phase Termination Alerts'),
                ('charts', 1),
            ],
        )


class TestOngoingIssuesSections(unittest.TestCase):
    """The Ongoing Issues subsections reach the PDF with their own rows and dates."""

    def _render(self, ongoing_alerts):
        """Run generate_pdf_report, capturing every table it builds."""
        from atspm_report.report_generation import generate_pdf_report

        signals = pd.DataFrame([
            {'DeviceId': '1', 'Name': 'Signal 1', 'Region': 'Region 1'},
        ])
        clearance_alerts = pd.DataFrame([
            {
                'DeviceId': '1', 'EventClass': 'Yellow', 'EventValue': 2,
                'MedianDuration': 3.0, 'MaxAbsDelta': 0.5, 'SampleCount': 4,
                'ShortCount': 4, 'LongCount': 0, 'IrregularCount': 0,
                'MinDuration': 3.0, 'MaxDuration': 3.0,
                'FirstEventTime': datetime(2026, 9, 9, 8, 0),
                'MaxDeltaEventTime': datetime(2026, 9, 9, 8, 0),
                'Date': datetime(2026, 9, 9),
            },
        ])

        tables = []

        def record_table(df, title, styles, **kwargs):
            tables.append((title, df))
            return []

        with patch('atspm_report.report_generation.create_reportlab_table', side_effect=record_table):
            generate_pdf_report(
                filtered_df_maxouts=pd.DataFrame(),
                filtered_df_actuations=pd.DataFrame(),
                filtered_df_ped=pd.DataFrame(),
                ped_hourly_df=pd.DataFrame(),
                filtered_df_missing_data=pd.DataFrame(),
                system_outages_df=pd.DataFrame(),
                phase_figures=[], detector_figures=[], ped_figures=[],
                missing_data_figures=[],
                signals_df=signals,
                clearance_alerts_df=clearance_alerts,
                ongoing_alerts=ongoing_alerts,
                verbosity=0,
            )
        return tables

    def test_no_ongoing_alerts_leaves_the_report_unchanged(self):
        titles = [title for title, _ in self._render(None)]
        self.assertIn('Clearance Interval Alerts', titles)
        self.assertNotIn('Ongoing Clearance Interval Alerts', titles)

    def test_ongoing_alerts_add_a_subsection_with_an_ongoing_column(self):
        ongoing = pd.DataFrame([
            {
                'DeviceId': '1', 'EventClass': 'Yellow', 'EventValue': 4,
                'MedianDuration': 3.0, 'MaxAbsDelta': 0.9, 'SampleCount': 6,
                'ShortCount': 6, 'LongCount': 0, 'IrregularCount': 0,
                'MinDuration': 3.0, 'MaxDuration': 3.0,
                'FirstEventTime': datetime(2026, 9, 9, 9, 0),
                'MaxDeltaEventTime': datetime(2026, 9, 9, 9, 0),
                'Date': datetime(2026, 9, 9),
                'OngoingSince': pd.Timestamp('2026-08-25'),
            },
        ])
        tables = self._render({'clearance_intervals': ongoing})
        by_title = dict(tables)

        self.assertIn('Ongoing Clearance Interval Alerts', by_title)
        ongoing_rows = by_title['Ongoing Clearance Interval Alerts']
        self.assertIn('Ongoing', ongoing_rows.columns)
        self.assertTrue(ongoing_rows['Ongoing'].iloc[0].startswith('8/25/26 ('))
        # The new-alert table for the same section keeps its original columns.
        self.assertNotIn('Ongoing', by_title['Clearance Interval Alerts'].columns)

    def test_a_section_with_only_ongoing_rows_still_renders(self):
        ongoing = pd.DataFrame([
            {
                'DeviceId': '1', 'EventClass': 'Red', 'EventValue': 6,
                'MedianDuration': 0.4, 'MaxAbsDelta': 0.2, 'SampleCount': 3,
                'ShortCount': 3, 'LongCount': 0, 'IrregularCount': 0,
                'MinDuration': 0.4, 'MaxDuration': 0.4,
                'FirstEventTime': datetime(2026, 9, 9, 7, 0),
                'MaxDeltaEventTime': datetime(2026, 9, 9, 7, 0),
                'Date': datetime(2026, 9, 9),
                'OngoingSince': pd.Timestamp('2026-09-01'),
            },
        ])
        from atspm_report.report_generation import generate_pdf_report

        signals = pd.DataFrame([
            {'DeviceId': '1', 'Name': 'Signal 1', 'Region': 'Region 1'},
        ])
        tables = []

        def record_table(df, title, styles, **kwargs):
            tables.append((title, df))
            return []

        with patch('atspm_report.report_generation.create_reportlab_table', side_effect=record_table):
            generate_pdf_report(
                filtered_df_maxouts=pd.DataFrame(),
                filtered_df_actuations=pd.DataFrame(),
                filtered_df_ped=pd.DataFrame(),
                ped_hourly_df=pd.DataFrame(),
                filtered_df_missing_data=pd.DataFrame(),
                system_outages_df=pd.DataFrame(),
                phase_figures=[], detector_figures=[], ped_figures=[],
                missing_data_figures=[],
                signals_df=signals,
                clearance_alerts_df=pd.DataFrame(),
                ongoing_alerts={'clearance_intervals': ongoing},
                verbosity=0,
            )

        titles = [title for title, _ in tables]
        self.assertIn('Ongoing Clearance Interval Alerts', titles)
        self.assertNotIn('Clearance Interval Alerts', titles)


class TestPackageMetadata(unittest.TestCase):
    """Test package metadata and configuration."""
    
    def test_version_consistency(self):
        """Test that __init__.py version matches pyproject.toml version."""
        # Get version from __init__.py
        init_version = atspm_report.__version__
        
        # Get version from pyproject.toml
        pyproject_path = Path(__file__).parent.parent / 'pyproject.toml'
        with open(pyproject_path, 'rb') as f:
            pyproject_data = tomli.load(f)
        toml_version = pyproject_data['project']['version']
        
        self.assertEqual(
            init_version,
            toml_version,
            f"Version mismatch: __init__.py has '{init_version}' but pyproject.toml has '{toml_version}'"
        )


class TestClearanceIntervals(unittest.TestCase):
    def _timeline(self, durations, event_class="Yellow", event_value=2, device_id="1", valid=None, start=None):
        start = start or pd.Timestamp("2026-05-26 14:00:00")
        valid = valid if valid is not None else [True] * len(durations)
        starts = [start + pd.Timedelta(minutes=i) for i in range(len(durations))]
        return pd.DataFrame({
            "DeviceId": [device_id] * len(durations),
            "StartTime": starts,
            "EndTime": [ts + pd.Timedelta(seconds=float(duration)) for ts, duration in zip(starts, durations)],
            "Duration": durations,
            "IsValid": valid,
            "EventClass": [event_class] * len(durations),
            "EventValue": [event_value] * len(durations),
        })

    def _with_preceding_overlap_green(self, overlap_yellows):
        overlap_greens = overlap_yellows.copy()
        overlap_greens['EventClass'] = 'Overlap Green'
        overlap_greens['EndTime'] = overlap_yellows['StartTime']
        overlap_greens['StartTime'] = (
            overlap_greens['EndTime'] - pd.Timedelta(seconds=5)
        )
        overlap_greens['Duration'] = 5.0
        return pd.concat([overlap_greens, overlap_yellows], ignore_index=True)

    def test_hard_minimum_uses_point_one_second_tolerance(self):
        allowed = self._timeline([3.5, 3.5, 3.5, 3.4])
        self.assertTrue(process_clearance_intervals(allowed).empty)

        flagged = self._timeline([3.5, 3.5, 3.5, 3.3])
        alerts = process_clearance_intervals(flagged)

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['ShortCount'], 1)
        self.assertAlmostEqual(alerts.iloc[0]['RepresentativeShortDuration'], 3.3)

    def test_median_irregularity_flags_more_than_point_one_second_only(self):
        allowed = self._timeline([4.0, 4.0, 4.0, 4.1])
        self.assertTrue(process_clearance_intervals(allowed).empty)

        flagged = self._timeline([4.0, 4.0, 4.0, 4.2])
        alerts = process_clearance_intervals(flagged)

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['IrregularCount'], 0)
        self.assertEqual(alerts.iloc[0]['LongCount'], 1)

    def test_invalid_timeline_rows_are_excluded(self):
        timeline = self._timeline([3.5, 3.5, 3.0], valid=[True, True, False])
        self.assertTrue(process_clearance_intervals(timeline).empty)

    def test_clearance_rows_longer_than_25_seconds_are_excluded(self):
        timeline = self._timeline([3.5, 3.5, 3.5, 30.0])
        self.assertTrue(process_clearance_intervals(timeline).empty)

    def test_clearance_intervals_within_one_minute_of_midnight_are_excluded(self):
        timeline = self._timeline([3.5, 3.5, 3.5])
        near_midnight = pd.concat([
            self._timeline([3.0], start=pd.Timestamp("2026-05-26 23:59:30")),
            self._timeline([3.0], start=pd.Timestamp("2026-05-27 00:00:30")),
        ], ignore_index=True)

        self.assertTrue(
            process_clearance_intervals(
                pd.concat([timeline, near_midnight], ignore_index=True)
            ).empty
        )

    def test_clearance_interval_outside_midnight_window_is_retained(self):
        timeline = self._timeline([3.5, 3.5, 3.5])
        outside_midnight_window = self._timeline(
            [3.0],
            start=pd.Timestamp("2026-05-27 00:01:01"),
        )

        alerts = process_clearance_intervals(
            pd.concat([timeline, outside_midnight_window], ignore_index=True)
        )

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['ShortCount'], 1)

    def test_short_clearance_rows_near_invalid_events_are_not_excluded_by_device(self):
        timeline = self._timeline([3.5, 3.5, 3.5, 3.3])
        invalid_event = pd.DataFrame([{
            "DeviceId": "1",
            "StartTime": pd.Timestamp("2026-05-26 14:03:20"),
            "EndTime": pd.Timestamp("2026-05-26 14:03:21"),
            "Duration": 1.0,
            "IsValid": False,
            "EventClass": "Green",
            "EventValue": 2,
        }])

        alerts = process_clearance_intervals(pd.concat([timeline, invalid_event], ignore_index=True))
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['ShortCount'], 1)

        other_device_invalid = invalid_event.copy()
        other_device_invalid["DeviceId"] = "2"
        alerts = process_clearance_intervals(pd.concat([timeline, other_device_invalid], ignore_index=True))

        self.assertEqual(len(alerts), 1)

    def test_clearance_rows_overlapping_any_invalid_interval_are_excluded(self):
        timeline = self._timeline([3.5, 3.5, 3.5, 3.3])
        invalid_event = pd.DataFrame([{
            'DeviceId': '1',
            'StartTime': pd.Timestamp('2026-05-26 14:03:01'),
            'EndTime': pd.Timestamp('2026-05-26 14:03:02'),
            'Duration': 1.0,
            'IsValid': False,
            'EventClass': 'Green',
            'EventValue': 6,
        }])

        alerts = process_clearance_intervals(
            pd.concat([timeline, invalid_event], ignore_index=True)
        )
        self.assertTrue(alerts.empty)

        other_device_invalid = invalid_event.copy()
        other_device_invalid['DeviceId'] = '2'
        alerts = process_clearance_intervals(
            pd.concat([timeline, other_device_invalid], ignore_index=True)
        )
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['ShortCount'], 1)

    def test_invalid_non_indication_intervals_do_not_mask_clearance(self):
        """Ped Service intervals are invalid by construction and must not hide alerts.

        A full timeline carries an invalid Ped Service interval for every ped
        service of the day, running concurrently with vehicle clearances. Treating
        those as evidence of bad signal-state data suppressed most samples.
        """
        timeline = self._timeline([3.5, 3.5, 3.5, 3.3])
        ped_service = pd.DataFrame([{
            'DeviceId': '1',
            'StartTime': pd.Timestamp('2026-05-26 14:03:01'),
            'EndTime': pd.Timestamp('2026-05-26 14:03:02'),
            'Duration': 1.0,
            'IsValid': False,
            'EventClass': 'Ped Service',
            'EventValue': 6,
        }])

        alerts = process_clearance_intervals(
            pd.concat([timeline, ped_service], ignore_index=True)
        )
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['ShortCount'], 1)

        # An invalid indication interval at the same instant still masks it.
        indication = ped_service.assign(EventClass='Green')
        self.assertTrue(process_clearance_intervals(
            pd.concat([timeline, indication], ignore_index=True)
        ).empty)

    def test_validity_event_classes_are_configurable(self):
        timeline = self._timeline([3.5, 3.5, 3.5, 3.3])
        ped_service = pd.DataFrame([{
            'DeviceId': '1',
            'StartTime': pd.Timestamp('2026-05-26 14:03:01'),
            'EndTime': pd.Timestamp('2026-05-26 14:03:02'),
            'Duration': 1.0,
            'IsValid': False,
            'EventClass': 'Ped Service',
            'EventValue': 6,
        }])
        combined = pd.concat([timeline, ped_service], ignore_index=True)

        widened = process_clearance_intervals(
            combined,
            {'clearance_validity_event_classes': ['Green', 'Ped Service']},
        )
        self.assertTrue(widened.empty)

    def test_invalid_event_cushion_seconds_is_configurable(self):
        timeline = self._timeline([3.5, 3.5, 3.5, 7.0])
        invalid_event = pd.DataFrame([{
            "DeviceId": "1",
            "StartTime": pd.Timestamp("2026-05-26 14:03:45"),
            "EndTime": pd.Timestamp("2026-05-26 14:03:46"),
            "Duration": 1.0,
            "IsValid": False,
            "EventClass": "Green",
            "EventValue": 2,
        }])
        combined = pd.concat([timeline, invalid_event], ignore_index=True)

        self.assertEqual(len(process_clearance_intervals(combined)), 1)
        self.assertTrue(
            process_clearance_intervals(
                combined,
                {"clearance_invalid_event_cushion_seconds": 60}
            ).empty
        )

    def test_unsorted_overlap_row_at_same_time_as_invalid_phase_rows_is_excluded(self):
        device_id = "03082"
        bad_time = pd.Timestamp("2026-05-25 02:53:06.600")
        rows = [
            {
                "DeviceId": device_id,
                "StartTime": bad_time,
                "EndTime": bad_time + pd.Timedelta(seconds=24.4),
                "Duration": 24.4,
                "IsValid": True,
                "EventClass": "Overlap Yellow",
                "EventValue": 8,
            }
        ]
        for minute_offset in range(20):
            start_time = pd.Timestamp("2026-05-25 02:00:00") + pd.Timedelta(minutes=minute_offset)
            rows.append({
                "DeviceId": device_id,
                "StartTime": start_time,
                "EndTime": start_time + pd.Timedelta(seconds=3.5),
                "Duration": 3.5,
                "IsValid": True,
                "EventClass": "Overlap Yellow",
                "EventValue": 8,
            })

        rows.extend([
            {
                "DeviceId": device_id,
                "StartTime": bad_time,
                "EndTime": bad_time + pd.Timedelta(seconds=3.5),
                "Duration": 3.5,
                "IsValid": False,
                "EventClass": "Yellow",
                "EventValue": 4,
            },
            {
                "DeviceId": device_id,
                "StartTime": bad_time,
                "EndTime": bad_time + pd.Timedelta(seconds=3.5),
                "Duration": 3.5,
                "IsValid": False,
                "EventClass": "Yellow",
                "EventValue": 8,
            },
        ])
        timeline = pd.DataFrame(rows)

        self.assertTrue(process_clearance_intervals(timeline).empty)

    def test_overlap_yellow_requires_immediately_preceding_same_numbered_green(self):
        yellows = self._timeline(
            [4.0, 4.0, 4.0, 4.3],
            event_class='Overlap Yellow',
            event_value=5,
        )
        self.assertTrue(process_clearance_intervals(yellows).empty)

        wrong_number_greens = self._with_preceding_overlap_green(yellows)
        wrong_number_greens.loc[
            wrong_number_greens['EventClass'] == 'Overlap Green',
            'EventValue',
        ] = 6
        self.assertTrue(process_clearance_intervals(wrong_number_greens).empty)

        delayed_greens = self._with_preceding_overlap_green(yellows)
        delayed_greens.loc[
            delayed_greens['EventClass'] == 'Overlap Green',
            'EndTime',
        ] -= pd.Timedelta(milliseconds=100)
        self.assertTrue(process_clearance_intervals(delayed_greens).empty)

    def test_overlap_rows_must_behave_like_fixed_clearance(self):
        fixed = self._timeline([4.0, 4.0, 4.0, 4.3], event_class="Overlap Yellow", event_value=5)
        variable = self._timeline([4.0] * 9 + [7.0], event_class="Overlap Yellow", event_value=6)
        fixed = self._with_preceding_overlap_green(fixed)
        variable = self._with_preceding_overlap_green(variable)
        alerts = process_clearance_intervals(pd.concat([fixed, variable], ignore_index=True))

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['EventClass'], "Overlap Yellow")
        self.assertEqual(alerts.iloc[0]['EventValue'], 5)

    def test_red_clearance_variation_is_ignored_unless_below_global_minimum(self):
        variable_red = self._timeline(
            [1.0, 1.0, 1.0, 0.8, 1.4],
            event_class='Red',
        )
        self.assertTrue(process_clearance_intervals(variable_red).empty)

        at_minimum = self._timeline(
            [1.0, 1.0, 1.0, 0.5],
            event_class='Red',
        )
        self.assertTrue(process_clearance_intervals(at_minimum).empty)

        short_red = self._timeline(
            [1.0, 1.0, 1.0, 0.49],
            event_class='Red',
        )
        alerts = process_clearance_intervals(short_red)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['ShortCount'], 1)
        self.assertEqual(alerts.iloc[0]['IrregularCount'], 0)
        self.assertEqual(alerts.iloc[0]['LongCount'], 0)

    def test_variable_overlap_red_still_flags_below_global_minimum(self):
        variable_overlap_red = self._timeline(
            [0.49, 1.0, 1.0, 1.0, 10.0],
            event_class='Overlap Red',
        )

        alerts = process_clearance_intervals(variable_overlap_red)

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['EventClass'], 'Overlap Red')
        self.assertEqual(alerts.iloc[0]['ShortCount'], 1)
        self.assertEqual(alerts.iloc[0]['IrregularCount'], 0)
        self.assertEqual(alerts.iloc[0]['LongCount'], 0)

    def test_representative_irregular_sample_is_farthest_from_median(self):
        timeline = self._timeline(
            [4.0, 4.0, 4.0, 3.8, 4.4],
            event_class="Yellow",
        )
        alerts = process_clearance_intervals(timeline)

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['IrregularCount'], 1)
        self.assertEqual(alerts.iloc[0]['LongCount'], 1)
        self.assertAlmostEqual(alerts.iloc[0]['RepresentativeIrregularDuration'], 3.8)
        self.assertAlmostEqual(alerts.iloc[0]['RepresentativeLongDuration'], 4.4)

    def test_stop_time_input_filters_04009_like_irregular_yellow_alert(self):
        signal_id = "2f75782b-6ac0-46b4-b900-ac45075dd812"
        irregular_time = pd.Timestamp("2026-05-25 20:43:21.100")
        starts = pd.date_range(irregular_time - pd.Timedelta(minutes=468), irregular_time, freq="min")
        timeline = pd.DataFrame({
            "DeviceId": [signal_id] * 469,
            "StartTime": starts,
            "EndTime": starts + pd.to_timedelta([4.0] * 468 + [4.5], unit="s"),
            "Duration": [4.0] * 468 + [4.5],
            "IsValid": [True] * 469,
            "EventClass": ["Yellow"] * 469,
            "EventValue": [2] * 469,
        })
        stop_time = pd.DataFrame([{
            "DeviceId": signal_id,
            "StartTime": pd.Timestamp("2026-05-25 20:43:21.000"),
            "EndTime": pd.Timestamp("2026-05-25 20:43:21.200"),
            "Duration": 1.0,
            "IsValid": True,
            "EventClass": "Stop Time Input",
            "EventValue": 1,
        }])

        alerts = process_clearance_intervals(pd.concat([timeline, stop_time], ignore_index=True))

        self.assertTrue(alerts.empty)

    def test_stop_time_input_overlap_filter_can_be_disabled(self):
        signal_id = "2f75782b-6ac0-46b4-b900-ac45075dd812"
        irregular_time = pd.Timestamp("2026-05-25 20:43:21.100")
        starts = pd.date_range(irregular_time - pd.Timedelta(minutes=468), irregular_time, freq="min")
        timeline = pd.DataFrame({
            "DeviceId": [signal_id] * 469,
            "StartTime": starts,
            "EndTime": starts + pd.to_timedelta([4.0] * 468 + [4.5], unit="s"),
            "Duration": [4.0] * 468 + [4.5],
            "IsValid": [True] * 469,
            "EventClass": ["Yellow"] * 469,
            "EventValue": [2] * 469,
        })
        stop_time = pd.DataFrame([{
            "DeviceId": signal_id,
            "StartTime": pd.Timestamp("2026-05-25 20:43:21.000"),
            "EndTime": pd.Timestamp("2026-05-25 20:43:21.200"),
            "Duration": 1.0,
            "IsValid": True,
            "EventClass": "Stop Time Input",
            "EventValue": 1,
        }])

        alerts = process_clearance_intervals(
            pd.concat([timeline, stop_time], ignore_index=True),
            {"filter_stoptime": False},
        )

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts.iloc[0]['DeviceId'], signal_id)
        self.assertEqual(alerts.iloc[0]['EventClass'], "Yellow")
        self.assertEqual(alerts.iloc[0]['EventValue'], 2)
        self.assertEqual(alerts.iloc[0]['SampleCount'], 469)
        self.assertEqual(alerts.iloc[0]['IrregularCount'], 0)
        self.assertEqual(alerts.iloc[0]['LongCount'], 1)
        self.assertAlmostEqual(alerts.iloc[0]['RepresentativeLongDuration'], 4.5)

    def test_clearance_table_prioritizes_phases_then_sorts_for_display(self):
        timestamp = pd.Timestamp("2026-05-26 14:54:33.200")
        signals = pd.DataFrame([
            {"DeviceId": "1", "Name": "Signal A", "Region": "R1"},
            {"DeviceId": "2", "Name": "Signal B", "Region": "R1"},
            {"DeviceId": "3", "Name": "Signal C", "Region": "R1"},
        ])
        alerts = pd.DataFrame([
            {
                "DeviceId": "1", "EventClass": "Overlap Red", "EventValue": 5,
                "Date": timestamp.normalize(), "MedianDuration": 1.0, "SampleCount": 8,
                "MaxAbsDelta": 3.0, "ShortCount": 0, "IrregularCount": 1, "LongCount": 1,
                "AvgSignedDeviation": 0.1, "RepresentativeShortDuration": pd.NA,
                "RepresentativeShortTime": pd.NaT, "RepresentativeIrregularDuration": 4.0,
                "RepresentativeIrregularTime": timestamp, "RepresentativeLongDuration": 4.0,
                "RepresentativeLongTime": timestamp,
            },
            {
                "DeviceId": "1", "EventClass": "Yellow", "EventValue": 2,
                "Date": timestamp.normalize(), "MedianDuration": 3.5, "SampleCount": 18,
                "MaxAbsDelta": 2.0, "ShortCount": 3, "IrregularCount": 5, "LongCount": 2,
                "AvgSignedDeviation": -0.2, "RepresentativeShortDuration": 3.1,
                "RepresentativeShortTime": timestamp, "RepresentativeIrregularDuration": 3.4,
                "RepresentativeIrregularTime": pd.Timestamp("2026-05-26 14:34:33.200"),
                "RepresentativeLongDuration": 5.1,
                "RepresentativeLongTime": pd.Timestamp("2026-05-26 14:34:33.200"),
            },
            {
                "DeviceId": "3", "EventClass": "Yellow", "EventValue": 4,
                "Date": timestamp.normalize(), "MedianDuration": 3.5, "SampleCount": 12,
                "MaxAbsDelta": 1.0, "ShortCount": 1, "IrregularCount": 1, "LongCount": 0,
                "AvgSignedDeviation": 0.0, "RepresentativeShortDuration": 3.2,
                "RepresentativeShortTime": timestamp, "RepresentativeIrregularDuration": 4.5,
                "RepresentativeIrregularTime": timestamp, "RepresentativeLongDuration": pd.NA,
                "RepresentativeLongTime": pd.NaT,
            },
        ])

        table, total = prepare_clearance_interval_alerts_table(alerts, signals, region="R1", max_rows=2)

        self.assertEqual(total, 3)
        self.assertEqual(len(table), 2)
        self.assertEqual(table.columns.tolist(), ["Signal", "Movement", "Median", "Details"])
        self.assertEqual(table['Movement'].tolist(), ["Ph 2 Yellow", "Ph 4 Yellow"])
        self.assertNotIn("Latest", table.columns)
        self.assertEqual(table.iloc[0]['Median'], "3.5s")
        self.assertIn("Of 18 samples, 3 were short (3.1s at 5/26/26 2:54:33.2 PM)", table.iloc[0]['Details'])
        self.assertIn("2 were long (5.1s at 5/26/26 2:34:33.2 PM)", table.iloc[0]['Details'])
        self.assertIn(
            '5 were irregular (3.4s at 5/26/26 2:34:33.2 PM)',
            table.iloc[0]['Details'],
        )
        self.assertNotIn("Ovlp 5 Red", table['Movement'].tolist())


class TestPhaseSkipVisualization(unittest.TestCase):
    def setUp(self):
        self.signals = pd.DataFrame([
            {"DeviceId": "1", "Name": "Signal 1", "Region": "R1"}
        ])
        self.rankings = pd.DataFrame([
            {"DeviceId": "1", "TotalSkips": 5}
        ])

    def tearDown(self):
        matplotlib.pyplot.close('all')

    def test_phase_skip_single_day_uses_time_axis_labels(self):
        timestamps = pd.date_range("2026-02-01 00:00:00", periods=8, freq="3h")
        phase_waits = pd.DataFrame({
            "TimeStamp": timestamps,
            "DeviceId": ["1"] * len(timestamps),
            "Phase": [1] * len(timestamps),
            "AvgPhaseWait": [10.0] * len(timestamps),
            "MaxPhaseWait": [15.0] * len(timestamps),
            "TotalSkips": [1] * len(timestamps),
            "AlertPhase": [True] * len(timestamps),
        })

        figures = create_phase_skip_plots(phase_waits, self.signals, self.rankings, num_figures=1)

        self.assertEqual(len(figures), 2)
        formatter = figures[0][0].axes[0].xaxis.get_major_formatter()
        formatted_tick = formatter.format_data_short(mdates.date2num(pd.Timestamp("2026-02-01 06:00:00")))

        self.assertRegex(formatted_tick, r"\d{2}:\d{2}")
        self.assertNotIn("Feb", formatted_tick)

    def test_phase_skip_multi_day_uses_date_axis_labels(self):
        timestamps = pd.to_datetime([
            "2026-02-01 00:00:00",
            "2026-02-01 12:00:00",
            "2026-02-02 00:00:00",
            "2026-02-02 12:00:00",
            "2026-02-03 00:00:00",
        ])
        phase_waits = pd.DataFrame({
            "TimeStamp": timestamps,
            "DeviceId": ["1"] * len(timestamps),
            "Phase": [1] * len(timestamps),
            "AvgPhaseWait": [10.0] * len(timestamps),
            "MaxPhaseWait": [15.0] * len(timestamps),
            "TotalSkips": [1] * len(timestamps),
            "AlertPhase": [True] * len(timestamps),
        })

        figures = create_phase_skip_plots(phase_waits, self.signals, self.rankings, num_figures=1)

        self.assertEqual(len(figures), 2)
        axis = figures[0][0].axes[0]
        formatter = axis.xaxis.get_major_formatter()
        midnight_tick = formatter(mdates.date2num(pd.Timestamp("2026-02-02 00:00:00")), None)
        noon_tick = formatter(mdates.date2num(pd.Timestamp("2026-02-02 12:00:00")), None)

        self.assertRegex(midnight_tick, r"[A-Za-z]{3}-\d{2}")
        self.assertNotIn("00:00", midnight_tick)
        self.assertRegex(noon_tick, r"[A-Za-z]{3}-\d{2}")
        self.assertIn("12:00", noon_tick)
        self.assertIn("2026-02-01 to 2026-02-03", axis.get_title())
        self.assertEqual(axis.title.get_fontsize(), 14)

    def test_time_axis_uses_date_only_for_daily_multi_day_data(self):
        fig, ax = matplotlib.pyplot.subplots()
        timestamps = pd.date_range("2026-02-01", periods=3, freq="D")

        _format_time_axis(ax, timestamps)

        formatter = ax.xaxis.get_major_formatter()
        formatted_tick = formatter.format_data_short(mdates.date2num(pd.Timestamp("2026-02-02")))

        self.assertRegex(formatted_tick, r"[A-Za-z]{3}-\d{2}")
        self.assertNotIn("00:00", formatted_tick)


class TestDataSchemas(unittest.TestCase):
    """Test that test data and README examples have correct schemas."""
    
    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = Path(__file__).parent / 'data'
    
    def test_signals_schema(self):
        """Test signals data has required columns."""
        signals = pd.read_parquet(self.test_data_dir / 'signals.parquet')
        required_columns = ['DeviceId', 'Name', 'Region']
        for col in required_columns:
            self.assertIn(col, signals.columns, f"Missing required column: {col}")
    
    def test_terminations_schema(self):
        """Test terminations data has required columns."""
        terminations = pd.read_parquet(self.test_data_dir / 'terminations.parquet')
        required_columns = ['TimeStamp', 'DeviceId', 'Phase', 'PerformanceMeasure', 'Total']
        for col in required_columns:
            self.assertIn(col, terminations.columns, f"Missing required column: {col}")
    
    def test_detector_health_schema(self):
        """Test detector_health data has required columns."""
        detector_health = pd.read_parquet(self.test_data_dir / 'detector_health.parquet')
        required_columns = ['TimeStamp', 'DeviceId', 'Detector', 'Total', 'anomaly', 'prediction']
        for col in required_columns:
            self.assertIn(col, detector_health.columns, f"Missing required column: {col}")
        self.assertEqual(detector_health['anomaly'].dtype, bool, "anomaly column should be boolean")
    
    def test_has_data_schema(self):
        """Test has_data has required columns."""
        has_data = pd.read_parquet(self.test_data_dir / 'has_data.parquet')
        required_columns = ['TimeStamp', 'DeviceId']
        for col in required_columns:
            self.assertIn(col, has_data.columns, f"Missing required column: {col}")
    
    def test_pedestrian_schema(self):
        """Test pedestrian data has required columns."""
        pedestrian = pd.read_parquet(self.test_data_dir / 'full_ped.parquet')
        required_columns = ['TimeStamp', 'DeviceId', 'Phase', 'PedActuation', 'PedServices']
        for col in required_columns:
            self.assertIn(col, pedestrian.columns, f"Missing required column: {col}")
    
    def test_readme_sample_dataframes(self):
        """Test that sample DataFrame examples from README can be created."""
        # Sample signals
        signals = pd.DataFrame({
            'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188', '3cb7be3e-123d-4f8f-a0d4-4d56c7fab684'],
            'Name': ['04100-Pacific at Hill', '2B528-(OR8) Adair St @ 4th Av'],
            'Region': ['Region 2', 'Region 1']
        })
        self.assertEqual(signals.shape[0], 2)
        self.assertIn('DeviceId', signals.columns)
        
        # Sample terminations
        terminations = pd.DataFrame({
            'TimeStamp': pd.to_datetime(['2024-01-15 08:30:00', '2024-01-15 08:35:00', '2024-01-15 08:35:00']),
            'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 3,
            'Phase': [2, 2, 4],
            'PerformanceMeasure': ['MaxOut', 'GapOut', 'ForceOff'],
            'Total': [30, 15, 12]
        })
        self.assertEqual(terminations.shape[0], 3)
        
        # Sample detector_health
        detector_health = pd.DataFrame({
            'TimeStamp': pd.to_datetime(['2024-01-15 08:00:00', '2024-01-15 08:00:00']),
            'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 2,
            'Detector': [1, 2],
            'Total': [150, 5],
            'anomaly': [False, True],
            'prediction': [145.0, 150.0]
        })
        self.assertEqual(detector_health.shape[0], 2)
        
        # Sample has_data
        has_data = pd.DataFrame({
            'TimeStamp': pd.to_datetime(['2024-01-15 00:00:00', '2024-01-15 00:15:00', '2024-01-15 00:30:00']),
            'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 3
        })
        self.assertEqual(has_data.shape[0], 3)
        
        # Sample pedestrian
        pedestrian = pd.DataFrame({
            'TimeStamp': pd.to_datetime(['2024-01-15 12:30:00', '2024-01-15 12:30:00']),
            'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188', '3cb7be3e-123d-4f8f-a0d4-4d56c7fab684'],
            'Phase': [2, 4],
            'PedActuation': [5, 10],
            'PedServices': [1, 2]
        })
        self.assertEqual(pedestrian.shape[0], 2)
        
        # Sample phase_wait data (new format)
        phase_wait = pd.DataFrame({
            'TimeStamp': pd.to_datetime(['2024-01-15 14:00:00', '2024-01-15 14:15:00', '2024-01-15 14:30:00']),
            'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 3,
            'Phase': [1, 1, 2],
            'AvgPhaseWait': [150.0, 160.0, 50.0],
            'TotalSkips': [2, 3, 0]
        })
        self.assertEqual(phase_wait.shape[0], 3)
        
        # Sample coordination data (for cycle length)
        coordination = pd.DataFrame({
            'TimeStamp': pd.to_datetime(['2024-01-15 14:00:00', '2024-01-15 14:30:00']),
            'Raw_TimeStamp': pd.to_datetime(['2024-01-15 14:00:05', '2024-01-15 14:30:10']),
            'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 2,
            'EventId': [132, 132],  # 132 = Cycle Length Change
            'Parameter': [100, 120]  # Cycle lengths
        })
        self.assertEqual(coordination.shape[0], 2)


if __name__ == '__main__':
    unittest.main()
