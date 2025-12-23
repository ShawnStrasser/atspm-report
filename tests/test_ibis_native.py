import unittest
import pandas as pd
import ibis
import ibis.expr.types as ir
from datetime import datetime, timedelta
import sys
import os

# Add the src directory to the path to import the package directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atspm_report.data_processing import process_maxout_data, process_actuations_data, process_missing_data
from atspm_report.phase_skip_processing import transform_phase_skip_raw_data

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

        # Phase skip data
        self.phase_skip_df = pd.DataFrame({
            'deviceid': [1, 1, 1],
            'timestamp': [datetime.now(), datetime.now(), datetime.now()],
            'eventid': [612, 612, 132],
            'parameter': [200, 200, 120]
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
        """Test that transform_phase_skip_raw_data accepts Ibis table and returns Ibis expressions"""
        t = ibis.memtable(self.phase_skip_df)
        
        phase_waits, alerts = transform_phase_skip_raw_data(t)
        
        self.assertIsInstance(phase_waits, ir.Table)
        self.assertIsInstance(alerts, ir.Table)
        
        # Execute to verify validity
        pw_df = phase_waits.execute()
        alerts_df = alerts.execute()
        
        # Check columns match expected schema (renaming happened correctly)
        self.assertIn('PhaseWaitTime', pw_df.columns)
        self.assertIn('TotalSkips', alerts_df.columns)

if __name__ == '__main__':
    unittest.main()
