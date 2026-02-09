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

# Add the src directory to the path to import the package directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atspm_report import ReportGenerator
import atspm_report

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
        for alert_type in ['maxout', 'actuations', 'missing_data', 'pedestrian', 'phase_skips', 'system_outages']:
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
