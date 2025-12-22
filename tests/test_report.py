import unittest
import pandas as pd
import numpy as np
from pathlib import Path
from io import BytesIO
from datetime import datetime, timedelta
import sys
import os
import re
import tomllib
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
        
        # Create dummy phase skip events to trigger an alert
        # Using one of the DeviceIds from the test data
        test_device_id = cls.signals_df['DeviceId'].iloc[0]
        cls.phase_skip_events = pd.DataFrame({
            'deviceid': [test_device_id] * 3,
            'timestamp': [datetime.now() - timedelta(hours=i) for i in range(3)],
            'eventid': [612, 612, 132], # 612 is phase 1 wait, 132 is max cycle
            'parameter': [200, 200, 120] # 200s wait, 120s cycle
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
            'phase_skip_events': self.phase_skip_events
        }
        
        result = generator.generate(**data)
        
        self.assertIn('alerts', result)
        self.assertIn('reports', result)
        
        alerts = result['alerts']
        
        # Compare each alert type against expected outputs
        for alert_type in ['maxout', 'actuations', 'missing_data', 'pedestrian', 'phase_skips', 'system_outages']:
            actual = alerts[alert_type]
            
            if alert_type in self.expected_alerts:
                expected = self.expected_alerts[alert_type]
                
                # Check we have the same number of alerts
                self.assertEqual(
                    len(actual), 
                    len(expected),
                    f"{alert_type}: Expected {len(expected)} alerts but got {len(actual)}"
                )
                
                if not actual.empty:
                    # Check that key columns match
                    # We can't compare exact values due to timestamps, but we can compare structure
                    self.assertEqual(
                        set(actual.columns), 
                        set(expected.columns),
                        f"{alert_type}: Column mismatch"
                    )
                    
                    # Check DeviceId matches for non-system_outages
                    if alert_type != 'system_outages' and 'DeviceId' in actual.columns:
                        self.assertEqual(
                            set(actual['DeviceId'].values),
                            set(expected['DeviceId'].values),
                            f"{alert_type}: DeviceId mismatch"
                        )
            else:
                # If no expected alerts exist, verify actual is empty
                self.assertTrue(actual.empty, f"{alert_type}: Expected no alerts but got {len(actual)}")
        
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
            'phase_skip_events': self.phase_skip_events,
            'past_alerts': self.past_alerts
        }
        
        result = generator.generate(**data)
        
        alerts = result['alerts']
        for alert_type, df in alerts.items():
            self.assertTrue(df.empty, f"Alert type {alert_type} was not suppressed: {df}")
            
        self.assertEqual(len(result['reports']), 0, "Reports were generated even though all alerts should be suppressed")


class TestPackageMetadata(unittest.TestCase):
    """Test package metadata and configuration."""
    
    def test_version_consistency(self):
        """Test that __init__.py version matches pyproject.toml version."""
        # Get version from __init__.py
        init_version = atspm_report.__version__
        
        # Get version from pyproject.toml
        pyproject_path = Path(__file__).parent.parent / 'pyproject.toml'
        with open(pyproject_path, 'rb') as f:
            pyproject_data = tomllib.load(f)
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
        
        # Sample phase_skip_events
        phase_skip_events = pd.DataFrame({
            'deviceid': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 3,
            'timestamp': pd.to_datetime(['2024-01-15 14:22:30', '2024-01-15 14:22:31', '2024-01-15 14:22:35']),
            'eventid': [612, 612, 132],
            'parameter': [200, 200, 120]
        })
        self.assertEqual(phase_skip_events.shape[0], 3)


if __name__ == '__main__':
    unittest.main()
