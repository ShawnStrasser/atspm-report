import unittest
import pandas as pd
import ibis
import ibis.expr.types as ir
from datetime import datetime, timedelta
import sys
import os
import matplotlib
matplotlib.use('Agg')

# Add the src directory to the path to import the package directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atspm_report.data_processing import process_maxout_data, process_actuations_data, process_missing_data
from atspm_report.phase_skip_processing import process_phase_wait_data

class TestIbisNative(unittest.TestCase):
    def setUp(self):
        # Setup basic data
        dates = pd.date_range(start='2023-01-01', periods=24*7, freq='h')
        self.maxout_df = pd.DataFrame({
            'TimeStamp': dates,
            'DeviceId': [1] * len(dates),
            'Phase': [2] * len(dates),
            'PerformanceMeasure': ['MaxOut'] * len(dates),
            'Total': [10] * len(dates)
        })
        
        self.actuations_df = pd.DataFrame({
            'TimeStamp': dates,
            'DeviceId': [1] * len(dates),
            'Detector': [1] * len(dates),
            'Total': [100] * len(dates),
            'anomaly': [0] * len(dates),
            'prediction': [90] * len(dates)
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

    def test_process_maxout_ibis_input(self):
        """Test that process_maxout_data accepts Ibis table and returns Ibis expressions"""
        # Convert to Ibis
        t = ibis.memtable(self.maxout_df)
        
        # Run processing
        daily, hourly = process_maxout_data(t)
        
        # Verify return types are Ibis expressions, not DataFrames
        self.assertIsInstance(daily, ir.Table)
        self.assertIsInstance(hourly, ir.Table)
        
        # Verify we can execute them
        daily_df = daily.execute()
        hourly_df = hourly.execute()
        
        self.assertFalse(daily_df.empty)
        self.assertFalse(hourly_df.empty)
        self.assertIn('Percent MaxOut', daily_df.columns)

    def test_process_actuations_ibis_input(self):
        """Test that process_actuations_data accepts Ibis table and returns Ibis expressions"""
        t = ibis.memtable(self.actuations_df)
        
        daily, hourly = process_actuations_data(t)
        
        self.assertIsInstance(daily, ir.Table)
        self.assertIsInstance(hourly, ir.Table)
        
        daily_df = daily.execute()
        self.assertFalse(daily_df.empty)
        self.assertIn('PercentAnomalous', daily_df.columns)

    def test_process_missing_data_ibis_input(self):
        """Test that process_missing_data accepts Ibis table and returns Ibis expressions"""
        t = ibis.memtable(self.missing_df)
        
        result = process_missing_data(t)
        
        self.assertIsInstance(result, ir.Table)
        
        result_df = result.execute()
        self.assertFalse(result_df.empty)
        self.assertIn('MissingData', result_df.columns)

    def test_phase_skip_ibis_input(self):
        """Test that process_phase_wait_data accepts Ibis table and returns Ibis expressions"""
        t = ibis.memtable(self.phase_skip_df)
        
        phase_waits, alerts, cycle_length = process_phase_wait_data(t)
        
        # Verify return types are Ibis tables
        self.assertIsInstance(phase_waits, ir.Table)
        self.assertIsInstance(alerts, ir.Table)
        self.assertIsInstance(cycle_length, ir.Table)
        
        # Execute to verify validity
        pw_df = phase_waits.execute()
        alerts_df = alerts.execute()
        
        # Check columns match expected schema
        self.assertIn('AvgPhaseWait', pw_df.columns)
        self.assertIn('TotalSkips', pw_df.columns)
        self.assertIn('TotalSkips', alerts_df.columns)
        self.assertIn('DeviceId', alerts_df.columns)
        self.assertIn('Phase', alerts_df.columns)
        
        # Verify we got alerts for Phase 1 (which has skips)
        self.assertTrue(len(alerts_df) > 0, "Expected at least one alert")
        self.assertTrue((alerts_df['Phase'] == 1).all(), "Alerts should only be for Phase 1")


class TestIbisEndToEnd(unittest.TestCase):
    """Test end-to-end ReportGenerator with Ibis tables."""
    
    @classmethod
    def setUpClass(cls):
        from pathlib import Path
        from atspm_report import ReportGenerator
        
        cls.test_data_dir = Path(__file__).parent / 'data'
        cls.con = ibis.duckdb.connect()
        
        # Load data as Ibis tables
        cls.signals_ibis = cls.con.read_parquet(str(cls.test_data_dir / 'signals.parquet'))
        cls.terminations_ibis = cls.con.read_parquet(str(cls.test_data_dir / 'terminations.parquet'))
        cls.detector_health_ibis = cls.con.read_parquet(str(cls.test_data_dir / 'detector_health.parquet'))
        cls.has_data_ibis = cls.con.read_parquet(str(cls.test_data_dir / 'has_data.parquet'))
        cls.pedestrian_ibis = cls.con.read_parquet(str(cls.test_data_dir / 'full_ped.parquet'))
        
        # Load pandas versions for comparison
        cls.signals_pd = pd.read_parquet(cls.test_data_dir / 'signals.parquet')
        cls.terminations_pd = pd.read_parquet(cls.test_data_dir / 'terminations.parquet')
        cls.detector_health_pd = pd.read_parquet(cls.test_data_dir / 'detector_health.parquet')
        cls.has_data_pd = pd.read_parquet(cls.test_data_dir / 'has_data.parquet')
        cls.pedestrian_pd = pd.read_parquet(cls.test_data_dir / 'full_ped.parquet')
        
        cls.config = {
            "suppress_repeated_alerts": True,
            "alert_suppression_days": 21,
            "figures_per_device": 1,
            "verbosity": 0,  # Silent for faster tests
        }
    
    def test_ibis_generates_same_alerts_as_pandas(self):
        """Test that Ibis tables produce the same alerts as pandas DataFrames."""
        from atspm_report import ReportGenerator
        
        generator = ReportGenerator(self.config)
        
        # Generate with Ibis
        result_ibis = generator.generate(
            signals=self.signals_ibis,
            terminations=self.terminations_ibis,
            detector_health=self.detector_health_ibis,
            has_data=self.has_data_ibis,
            pedestrian=self.pedestrian_ibis
        )
        
        # Generate with pandas
        result_pandas = generator.generate(
            signals=self.signals_pd,
            terminations=self.terminations_pd,
            detector_health=self.detector_health_pd,
            has_data=self.has_data_pd,
            pedestrian=self.pedestrian_pd
        )
        
        # Compare alert counts
        for alert_type in ['maxout', 'actuations', 'missing_data', 'pedestrian', 'system_outages']:
            ibis_alerts = result_ibis['alerts'][alert_type]
            pandas_alerts = result_pandas['alerts'][alert_type]
            
            self.assertEqual(
                len(ibis_alerts),
                len(pandas_alerts),
                f"{alert_type}: Ibis produced {len(ibis_alerts)} alerts but pandas produced {len(pandas_alerts)}"
            )
            
            # Compare DeviceIds if not empty
            if not ibis_alerts.empty and 'DeviceId' in ibis_alerts.columns:
                self.assertEqual(
                    set(ibis_alerts['DeviceId'].values),
                    set(pandas_alerts['DeviceId'].values),
                    f"{alert_type}: DeviceId mismatch between Ibis and pandas"
                )
    
    def test_ibis_generates_pdf_reports(self):
        """Test that Ibis input generates PDF reports when alerts exist.
        
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
            signals=self.signals_ibis,
            terminations=self.terminations_ibis,
            detector_health=self.detector_health_ibis,
            has_data=self.has_data_ibis,
            pedestrian=self.pedestrian_ibis
        )
        
        # Verify reports key exists
        self.assertIn('reports', result)
        
        # Check if any alerts were generated
        any_alerts = any(
            not alerts.empty 
            for alerts in result['alerts'].values()
        )
        
        # PDF reports should be generated if and only if there are alerts
        if any_alerts:
            self.assertTrue(len(result['reports']) > 0, 
                "Alerts exist but no PDF reports were generated with Ibis input")
            # Verify each report is a valid BytesIO with PDF content
            for region, pdf_bytes in result['reports'].items():
                pdf_bytes.seek(0)
                header = pdf_bytes.read(4)
                self.assertEqual(header, b'%PDF', 
                    f"Report for region '{region}' is not a valid PDF")
        else:
            # No alerts means no reports - this is correct behavior
            self.assertEqual(len(result['reports']), 0, 
                "No alerts but PDF reports were generated")


if __name__ == '__main__':
    unittest.main()
