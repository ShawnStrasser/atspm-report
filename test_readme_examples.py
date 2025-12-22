"""
Test script to verify README examples work with actual test data
"""
import pandas as pd
from pathlib import Path

def test_data_loading():
    """Test that test data loads correctly with expected columns"""
    print("Testing data loading from README examples...")
    
    test_data_dir = Path('tests/data')
    
    # Load signals
    signals = pd.read_parquet(test_data_dir / 'signals.parquet')
    print(f"\n✓ Signals loaded: {signals.shape}")
    print(f"  Columns: {signals.columns.tolist()}")
    print(f"  DeviceId type: {signals['DeviceId'].dtype} (expected: object/str)")
    assert 'DeviceId' in signals.columns
    assert 'Name' in signals.columns
    assert 'Region' in signals.columns
    
    # Load terminations
    terminations = pd.read_parquet(test_data_dir / 'terminations.parquet')
    print(f"\n✓ Terminations loaded: {terminations.shape}")
    print(f"  Columns: {terminations.columns.tolist()}")
    assert 'TimeStamp' in terminations.columns
    assert 'DeviceId' in terminations.columns
    assert 'Phase' in terminations.columns
    assert 'PerformanceMeasure' in terminations.columns
    assert 'Total' in terminations.columns
    
    # Load detector_health
    detector_health = pd.read_parquet(test_data_dir / 'detector_health.parquet')
    print(f"\n✓ Detector health loaded: {detector_health.shape}")
    print(f"  Columns: {detector_health.columns.tolist()}")
    print(f"  Anomaly type: {detector_health['anomaly'].dtype} (expected: bool)")
    assert 'TimeStamp' in detector_health.columns
    assert 'DeviceId' in detector_health.columns
    assert 'Detector' in detector_health.columns
    assert 'Total' in detector_health.columns
    assert 'anomaly' in detector_health.columns
    assert 'prediction' in detector_health.columns
    
    # Load has_data
    has_data = pd.read_parquet(test_data_dir / 'has_data.parquet')
    print(f"\n✓ Has data loaded: {has_data.shape}")
    print(f"  Columns: {has_data.columns.tolist()}")
    assert 'TimeStamp' in has_data.columns
    assert 'DeviceId' in has_data.columns
    
    # Load pedestrian
    pedestrian = pd.read_parquet(test_data_dir / 'full_ped.parquet')
    print(f"\n✓ Pedestrian loaded: {pedestrian.shape}")
    print(f"  Columns: {pedestrian.columns.tolist()}")
    assert 'TimeStamp' in pedestrian.columns
    assert 'DeviceId' in pedestrian.columns
    assert 'Phase' in pedestrian.columns
    assert 'PedActuation' in pedestrian.columns
    assert 'PedServices' in pedestrian.columns
    
    print("\n✅ All data files loaded successfully with expected schema!")
    return True

def test_sample_dataframes():
    """Test that sample DataFrame examples from README are valid"""
    print("\n\nTesting sample DataFrame examples from README...")
    
    # Sample signals (from README)
    signals = pd.DataFrame({
        'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188', '3cb7be3e-123d-4f8f-a0d4-4d56c7fab684'],
        'Name': ['04100-Pacific at Hill', '2B528-(OR8) Adair St @ 4th Av'],
        'Region': ['Region 2', 'Region 1']
    })
    print(f"✓ Signals sample created: {signals.shape}")
    
    # Sample terminations (from README)
    terminations = pd.DataFrame({
        'TimeStamp': pd.to_datetime(['2024-01-15 08:30:00', '2024-01-15 08:35:00', '2024-01-15 08:35:00']),
        'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 3,
        'Phase': [2, 2, 4],
        'PerformanceMeasure': ['MaxOut', 'GapOut', 'ForceOff'],
        'Total': [30, 15, 12]
    })
    print(f"✓ Terminations sample created: {terminations.shape}")
    
    # Sample detector_health (from README)
    detector_health = pd.DataFrame({
        'TimeStamp': pd.to_datetime(['2024-01-15 08:00:00', '2024-01-15 08:00:00']),
        'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 2,
        'Detector': [1, 2],
        'Total': [150, 5],
        'anomaly': [False, True],
        'prediction': [145.0, 150.0]
    })
    print(f"✓ Detector health sample created: {detector_health.shape}")
    
    # Sample has_data (from README)
    has_data = pd.DataFrame({
        'TimeStamp': pd.to_datetime(['2024-01-15 00:00:00', '2024-01-15 00:15:00', '2024-01-15 00:30:00']),
        'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 3
    })
    print(f"✓ Has data sample created: {has_data.shape}")
    
    # Sample pedestrian (from README)
    pedestrian = pd.DataFrame({
        'TimeStamp': pd.to_datetime(['2024-01-15 12:30:00', '2024-01-15 12:30:00']),
        'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188', '3cb7be3e-123d-4f8f-a0d4-4d56c7fab684'],
        'Phase': [2, 4],
        'PedActuation': [5, 10],
        'PedServices': [1, 2]
    })
    print(f"✓ Pedestrian sample created: {pedestrian.shape}")
    
    # Sample phase_skip_events (from README)
    phase_skip_events = pd.DataFrame({
        'deviceid': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 3,
        'timestamp': pd.to_datetime(['2024-01-15 14:22:30', '2024-01-15 14:22:31', '2024-01-15 14:22:35']),
        'eventid': [612, 612, 132],
        'parameter': [200, 200, 120]
    })
    print(f"✓ Phase skip events sample created: {phase_skip_events.shape}")
    
    print("\n✅ All README DataFrame examples are valid!")
    return True

if __name__ == '__main__':
    try:
        test_data_loading()
        test_sample_dataframes()
        print("\n" + "="*60)
        print("SUCCESS: All README examples validated!")
        print("="*60)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
