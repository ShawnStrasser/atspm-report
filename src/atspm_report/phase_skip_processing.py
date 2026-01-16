"""
Phase Skip Processing Module

Processes pre-aggregated phase_wait data from the atspm package to generate phase skip alerts.

Expected Input Schema (phase_wait table):
    - TimeStamp (DATETIME): Bin start time
    - DeviceId (INTEGER or TEXT): Unique identifier for the controller
    - Phase (INT16): Phase number
    - AvgPhaseWait (FLOAT): Average wait time in seconds
    - TotalSkips (BIGINT): Count of skipped phases

Expected Input Schema (coordination_agg table - for cycle length):
    - TimeStamp (DATETIME): Bin start time (15-minute bins)
    - DeviceId (INTEGER or TEXT): Unique identifier for the controller
    - ActualCycleLength (FLOAT): Actual cycle length in seconds
"""

import pandas as pd
from typing import Union, Tuple, Optional

# Output column definitions
PHASE_WAIT_COLUMNS = [
    'DeviceId', 'TimeStamp', 'Phase', 'AvgPhaseWait', 'MaxPhaseWait', 'TotalSkips'
]

PHASE_SKIP_ALERT_COLUMNS = [
    'DeviceId', 'Phase', 'Date', 'TotalSkips'
]

COORDINATION_COLUMNS = [
    'DeviceId', 'TimeStamp', 'CycleLength'
]


def process_phase_wait_data(
    phase_wait: Union[pd.DataFrame, None],
    coordination_agg: Optional[pd.DataFrame] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Process phase_wait data to generate phase skip alerts.

    Args:
        phase_wait: DataFrame with columns TimeStamp, DeviceId, Phase, AvgPhaseWait, MaxPhaseWait, TotalSkips
        coordination_agg: Optional DataFrame with columns TimeStamp, DeviceId, ActualCycleLength
                         Used to plot cycle length as a step function (15-minute bins)

    Returns:
        Tuple of (phase_waits_df, alert_rows_df, cycle_length_df):
            - phase_waits_df: Processed phase wait data for plotting
            - alert_rows_df: Alert candidates with DeviceId, Phase, Date, TotalSkips
            - cycle_length_df: Cycle length data for plotting (DeviceId, TimeStamp, CycleLength)
    """
    # Handle empty/None input
    if phase_wait is None or phase_wait.empty:
        return (
            pd.DataFrame(columns=PHASE_WAIT_COLUMNS),
            pd.DataFrame(columns=PHASE_SKIP_ALERT_COLUMNS),
            pd.DataFrame(columns=COORDINATION_COLUMNS)
        )
    
    # Make a copy to avoid modifying original
    phase_waits_df = phase_wait.copy()
    
    # Normalize column names (handle case variations)
    phase_waits_df.columns = [col.strip() for col in phase_waits_df.columns]
    
    # Ensure required columns exist
    required_cols = ['TimeStamp', 'DeviceId', 'Phase', 'AvgPhaseWait', 'MaxPhaseWait', 'TotalSkips']
    for col in required_cols:
        if col not in phase_waits_df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # Type conversions
    phase_waits_df['DeviceId'] = phase_waits_df['DeviceId'].astype(str)
    phase_waits_df['Phase'] = phase_waits_df['Phase'].astype(int)
    phase_waits_df['TimeStamp'] = pd.to_datetime(phase_waits_df['TimeStamp'])
    phase_waits_df['AvgPhaseWait'] = phase_waits_df['AvgPhaseWait'].astype(float)
    phase_waits_df['MaxPhaseWait'] = phase_waits_df['MaxPhaseWait'].astype(float)
    phase_waits_df['TotalSkips'] = phase_waits_df['TotalSkips'].astype(int)
    
    # Create Date column from TimeStamp
    phase_waits_df['Date'] = phase_waits_df['TimeStamp'].dt.normalize()
    
    # Generate alert rows by aggregating by DeviceId, Phase, Date
    alert_rows_df = (
        phase_waits_df[phase_waits_df['TotalSkips'] > 0]
        .groupby(['DeviceId', 'Phase', 'Date'], as_index=False)
        .agg(TotalSkips=('TotalSkips', 'sum'))
    )
    
    # Ensure alert columns are in correct order
    if not alert_rows_df.empty:
        alert_rows_df = alert_rows_df[PHASE_SKIP_ALERT_COLUMNS]
    else:
        alert_rows_df = pd.DataFrame(columns=PHASE_SKIP_ALERT_COLUMNS)
    
    # Process coordination_agg data for cycle length
    cycle_length_df = _extract_cycle_length(coordination_agg)
    
    # Return data in expected format
    return (
        phase_waits_df[PHASE_WAIT_COLUMNS],
        alert_rows_df,
        cycle_length_df
    )


def _extract_cycle_length(coordination_agg: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Extract cycle length data from coordination_agg table.
    
    Args:
        coordination_agg: DataFrame with TimeStamp, DeviceId, ActualCycleLength columns
                         (15-minute bin aggregated data)
    
    Returns:
        DataFrame with DeviceId, TimeStamp, CycleLength columns
    """
    if coordination_agg is None or coordination_agg.empty:
        return pd.DataFrame(columns=COORDINATION_COLUMNS)
    
    # Make a copy
    coord_df = coordination_agg.copy()
    
    # Check if ActualCycleLength column exists
    if 'ActualCycleLength' not in coord_df.columns:
        return pd.DataFrame(columns=COORDINATION_COLUMNS)
    
    # Filter out rows where ActualCycleLength is 0 or null (no coordination)
    coord_df = coord_df[coord_df['ActualCycleLength'] > 0].copy()
    
    if coord_df.empty:
        return pd.DataFrame(columns=COORDINATION_COLUMNS)
    
    # Ensure TimeStamp is datetime
    coord_df['TimeStamp'] = pd.to_datetime(coord_df['TimeStamp'])
    
    # Extract and rename columns
    cycle_length_df = coord_df[['DeviceId', 'TimeStamp', 'ActualCycleLength']].copy()
    cycle_length_df.columns = ['DeviceId', 'TimeStamp', 'CycleLength']
    
    # Type conversions
    cycle_length_df['DeviceId'] = cycle_length_df['DeviceId'].astype(str)
    cycle_length_df['CycleLength'] = cycle_length_df['CycleLength'].astype(float)
    
    # Sort by device and timestamp
    cycle_length_df = cycle_length_df.sort_values(['DeviceId', 'TimeStamp']).reset_index(drop=True)
    
    return cycle_length_df
