import unittest
import pandas as pd
import numpy as np
from pathlib import Path
from io import BytesIO
from datetime import datetime, timedelta
import sys
import os
import matplotlib
matplotlib.use('Agg')

# Add the src directory to the path to import the package directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atspm_report import ReportGenerator

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
            "figures_per_device": 0, # Speed up tests
            "verbosity": 1,
        }

    def test_1_generate_new_alerts(self):
        """Test that alerts are generated when no past alerts are provided."""
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
        # Check that we have at least some alerts
        found_any = False
        for alert_type, df in alerts.items():
            if not df.empty:
                found_any = True
        
        self.assertTrue(found_any, "No alerts were generated in Test 1")
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

if __name__ == '__main__':
    unittest.main()
