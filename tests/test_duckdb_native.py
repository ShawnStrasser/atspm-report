import unittest
import pandas as pd
import duckdb
from datetime import datetime, timedelta
import sys
import os
import matplotlib
matplotlib.use('Agg')

# Add the src directory to the path to import the package directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atspm_report.data_processing import process_maxout_data, process_missing_data
from atspm_report.phase_skip_processing import process_phase_wait_data


def _relation(con, name, df):
    """A DuckDB relation over a pandas DataFrame registered on con."""
    con.register(name, df)
    return con.table(name)


class TestDuckDBNative(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        # Setup basic data
        dates = pd.date_range(start='2023-01-01', periods=24*7, freq='h')
        self.maxout_df = pd.DataFrame({
            'TimeStamp': dates,
            'DeviceId': [1] * len(dates),
            'Phase': [2] * len(dates),
            'PerformanceMeasure': ['MaxOut'] * len(dates),
            'Total': [10] * len(dates)
        })

        self.missing_df = pd.DataFrame({
            'TimeStamp': dates,
            'DeviceId': [1] * len(dates)
        })

        # Phase skip data (pre-aggregated format expected by process_phase_wait_data)
        now = datetime.now()
        self.phase_skip_df = pd.DataFrame({
            'TimeStamp': [now - timedelta(hours=i) for i in range(6)],
            'DeviceId': [1, 1, 1, 1, 1, 1],
            'Phase': [1, 1, 1, 2, 2, 2],
            'AvgPhaseWait': [150.0, 160.0, 170.0, 50.0, 55.0, 60.0],
            'MaxPhaseWait': [180.0, 190.0, 200.0, 70.0, 75.0, 80.0],
            'TotalSkips': [2, 3, 2, 0, 0, 0]  # Phase 1 has skips, Phase 2 doesn't
        })

    def tearDown(self):
        self.con.close()

    def test_process_maxout_relation_input(self):
        """process_maxout_data accepts a DuckDB relation and matches the pandas result"""
        t = _relation(self.con, 'maxout', self.maxout_df)

        daily_df, hourly_df = process_maxout_data(t)

        self.assertIsInstance(daily_df, pd.DataFrame)
        self.assertIsInstance(hourly_df, pd.DataFrame)
        self.assertFalse(daily_df.empty)
        self.assertFalse(hourly_df.empty)
        self.assertIn('Percent MaxOut', daily_df.columns)

        daily_pd, hourly_pd = process_maxout_data(self.maxout_df)
        pd.testing.assert_frame_equal(daily_df, daily_pd)
        pd.testing.assert_frame_equal(hourly_df, hourly_pd)

    def test_process_missing_data_relation_input(self):
        """process_missing_data accepts a DuckDB relation and matches the pandas result"""
        t = _relation(self.con, 'has_data', self.missing_df)

        result_df = process_missing_data(t)

        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertFalse(result_df.empty)
        self.assertIn('MissingData', result_df.columns)
        pd.testing.assert_frame_equal(result_df, process_missing_data(self.missing_df))

    def test_phase_skip_relation_input(self):
        """process_phase_wait_data accepts a DuckDB relation and returns DataFrames"""
        t = _relation(self.con, 'phase_wait', self.phase_skip_df)

        pw_df, alerts_df, cycle_length = process_phase_wait_data(t)

        for frame in (pw_df, alerts_df, cycle_length):
            self.assertIsInstance(frame, pd.DataFrame)

        # Check columns match expected schema
        self.assertIn('AvgPhaseWait', pw_df.columns)
        self.assertIn('TotalSkips', pw_df.columns)
        self.assertIn('TotalSkips', alerts_df.columns)
        self.assertIn('DeviceId', alerts_df.columns)
        self.assertIn('Phase', alerts_df.columns)

        # Verify we got alerts for Phase 1 (which has skips)
        self.assertTrue(len(alerts_df) > 0, "Expected at least one alert")
        self.assertTrue((alerts_df['Phase'] == 1).all(), "Alerts should only be for Phase 1")

        pw_pd, alerts_pd, _ = process_phase_wait_data(self.phase_skip_df)
        pd.testing.assert_frame_equal(pw_df, pw_pd)
        pd.testing.assert_frame_equal(alerts_df, alerts_pd)

    def test_relation_left_idle(self):
        """Reading a relation leaves its connection free for the next query"""
        t = _relation(self.con, 'maxout', self.maxout_df)
        process_maxout_data(t)
        self.assertEqual(self.con.execute('SELECT 42').fetchall(), [(42,)])


class TestDuckDBEndToEnd(unittest.TestCase):
    """Test end-to-end ReportGenerator with DuckDB relations."""

    @classmethod
    def setUpClass(cls):
        from pathlib import Path

        cls.test_data_dir = Path(__file__).parent / 'data'
        cls.con = duckdb.connect()

        # Load data as DuckDB relations
        def rel(name):
            return cls.con.read_parquet(str(cls.test_data_dir / f'{name}.parquet'))
        cls.signals_rel = rel('signals')
        cls.terminations_rel = rel('terminations')
        cls.detector_counts_rel = rel('detector_counts')
        cls.has_data_rel = rel('has_data')
        cls.pedestrian_rel = rel('full_ped')

        # Load pandas versions for comparison
        cls.signals_pd = pd.read_parquet(cls.test_data_dir / 'signals.parquet')
        cls.terminations_pd = pd.read_parquet(cls.test_data_dir / 'terminations.parquet')
        cls.detector_counts_pd = pd.read_parquet(cls.test_data_dir / 'detector_counts.parquet')
        cls.has_data_pd = pd.read_parquet(cls.test_data_dir / 'has_data.parquet')
        cls.pedestrian_pd = pd.read_parquet(cls.test_data_dir / 'full_ped.parquet')

        cls.config = {
            "suppress_repeated_alerts": True,
            "alert_suppression_days": 21,
            "figures_per_device": 1,
            "verbosity": 0,  # Silent for faster tests
        }

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    def test_relations_generate_same_alerts_as_pandas(self):
        """DuckDB relations produce the same alerts as pandas DataFrames."""
        from atspm_report import ReportGenerator

        generator = ReportGenerator(self.config)

        result_rel = generator.generate(
            signals=self.signals_rel,
            terminations=self.terminations_rel,
            detector_counts=self.detector_counts_rel,
            has_data=self.has_data_rel,
            pedestrian=self.pedestrian_rel
        )

        result_pandas = generator.generate(
            signals=self.signals_pd,
            terminations=self.terminations_pd,
            detector_counts=self.detector_counts_pd,
            has_data=self.has_data_pd,
            pedestrian=self.pedestrian_pd
        )

        for alert_type in ['maxout', 'missing_data', 'system_outages']:
            rel_alerts = result_rel['alerts'][alert_type]
            pandas_alerts = result_pandas['alerts'][alert_type]

            self.assertEqual(
                len(rel_alerts),
                len(pandas_alerts),
                f"{alert_type}: relations produced {len(rel_alerts)} alerts but pandas produced {len(pandas_alerts)}"
            )

            if not rel_alerts.empty and 'DeviceId' in rel_alerts.columns:
                self.assertEqual(
                    set(rel_alerts['DeviceId'].values),
                    set(pandas_alerts['DeviceId'].values),
                    f"{alert_type}: DeviceId mismatch between relations and pandas"
                )

        for key in ['maxout_hourly', 'missing_hourly', 'outage_hourly']:
            pd.testing.assert_frame_equal(result_rel['hourly_data'][key], result_pandas['hourly_data'][key])

    def test_relations_generate_pdf_reports(self):
        """DuckDB relation input generates PDF reports when alerts exist.

        Note: PDF reports are only generated when there are non-suppressed alerts.
        If no alerts are generated from the test data, the reports dict will be empty,
        which is correct behavior.
        """
        from atspm_report import ReportGenerator

        # Use a fresh config without suppression to ensure we get reports
        config = {
            "suppress_repeated_alerts": False,  # Disable suppression to see all alerts
            "figures_per_device": 1,
            "verbosity": 0,
        }
        generator = ReportGenerator(config)

        result = generator.generate(
            signals=self.signals_rel,
            terminations=self.terminations_rel,
            detector_counts=self.detector_counts_rel,
            has_data=self.has_data_rel,
            pedestrian=self.pedestrian_rel
        )

        self.assertIn('reports', result)

        any_alerts = any(
            not alerts.empty
            for alerts in result['alerts'].values()
        )

        # PDF reports should be generated if and only if there are alerts
        if any_alerts:
            self.assertTrue(len(result['reports']) > 0,
                "Alerts exist but no PDF reports were generated with DuckDB relation input")
            for region, pdf_bytes in result['reports'].items():
                pdf_bytes.seek(0)
                header = pdf_bytes.read(4)
                self.assertEqual(header, b'%PDF',
                    f"Report for region '{region}' is not a valid PDF")
        else:
            self.assertEqual(len(result['reports']), 0,
                "No alerts but PDF reports were generated")


if __name__ == '__main__':
    unittest.main()
