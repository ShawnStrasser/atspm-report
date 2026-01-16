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
import ibis
import ibis.expr.types as ir
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


def _to_ibis(data: Union[pd.DataFrame, ir.Table, None]) -> Optional[ir.Table]:
    """Convert pandas DataFrame to Ibis table if needed."""
    if data is None:
        return None
    if isinstance(data, ir.Table):
        return data
    if isinstance(data, pd.DataFrame):
        if data.empty:
            return None
        return ibis.memtable(data)
    return None


def _is_empty(data: Union[pd.DataFrame, ir.Table, None]) -> bool:
    """Check if data is None or empty, works for both pandas and Ibis."""
    if data is None:
        return True
    if isinstance(data, pd.DataFrame):
        return data.empty
    if isinstance(data, ir.Table):
        return data.count().execute() == 0
    return True


def process_phase_wait_data(
    phase_wait: Union[pd.DataFrame, ir.Table, None],
    coordination_agg: Optional[Union[pd.DataFrame, ir.Table]] = None
) -> Tuple[ir.Table, ir.Table, ir.Table]:
    """
    Process phase_wait data to generate phase skip alerts.
    
    All processing is done using Ibis. Pandas inputs are converted to Ibis tables.

    Args:
        phase_wait: DataFrame or Ibis table with columns TimeStamp, DeviceId, Phase, AvgPhaseWait, MaxPhaseWait, TotalSkips
        coordination_agg: Optional DataFrame or Ibis table with columns TimeStamp, DeviceId, ActualCycleLength
                         Used to plot cycle length as a step function (15-minute bins)

    Returns:
        Tuple of (phase_waits, alert_rows, cycle_length) as Ibis tables:
            - phase_waits: Processed phase wait data for plotting
            - alert_rows: Alert candidates with DeviceId, Phase, Date, TotalSkips
            - cycle_length: Cycle length data for plotting
    """
    # Convert to Ibis if pandas
    phase_wait_tbl = _to_ibis(phase_wait)
    
    # Handle empty/None input
    if phase_wait_tbl is None:
        empty_phase_waits = ibis.memtable(pd.DataFrame(columns=PHASE_WAIT_COLUMNS))
        empty_alerts = ibis.memtable(pd.DataFrame(columns=PHASE_SKIP_ALERT_COLUMNS))
        empty_cycle = ibis.memtable(pd.DataFrame(columns=COORDINATION_COLUMNS))
        return (empty_phase_waits, empty_alerts, empty_cycle)
    
    # Verify required columns exist
    required_cols = ['TimeStamp', 'DeviceId', 'Phase', 'AvgPhaseWait', 'MaxPhaseWait', 'TotalSkips']
    for col in required_cols:
        if col not in phase_wait_tbl.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # Cast columns to correct types and add Date column
    phase_waits = phase_wait_tbl.mutate(
        DeviceId=phase_wait_tbl.DeviceId.cast('string'),
        Phase=phase_wait_tbl.Phase.cast('int32'),
        AvgPhaseWait=phase_wait_tbl.AvgPhaseWait.cast('float64'),
        MaxPhaseWait=phase_wait_tbl.MaxPhaseWait.cast('float64'),
        TotalSkips=phase_wait_tbl.TotalSkips.cast('int64'),
        Date=phase_wait_tbl.TimeStamp.truncate('D')
    )
    
    # Select output columns for phase_waits
    phase_waits_out = phase_waits.select(PHASE_WAIT_COLUMNS)
    
    # Generate alert rows by filtering and aggregating
    alert_rows = (
        phase_waits
        .filter(phase_waits.TotalSkips > 0)
        .group_by(['DeviceId', 'Phase', 'Date'])
        .aggregate(TotalSkips=phase_waits.TotalSkips.sum())
        .select(PHASE_SKIP_ALERT_COLUMNS)
    )
    
    # Process coordination_agg data for cycle length
    cycle_length = _extract_cycle_length(coordination_agg)
    
    return (phase_waits_out, alert_rows, cycle_length)


def _extract_cycle_length(coordination_agg: Optional[Union[pd.DataFrame, ir.Table]]) -> ir.Table:
    """
    Extract cycle length data from coordination_agg table using Ibis.
    
    Args:
        coordination_agg: DataFrame or Ibis table with TimeStamp, DeviceId, ActualCycleLength columns
    
    Returns:
        Ibis table with DeviceId, TimeStamp, CycleLength columns
    """
    coord_tbl = _to_ibis(coordination_agg)
    
    if coord_tbl is None:
        return ibis.memtable(pd.DataFrame(columns=COORDINATION_COLUMNS))
    
    # Check if ActualCycleLength column exists
    if 'ActualCycleLength' not in coord_tbl.columns:
        return ibis.memtable(pd.DataFrame(columns=COORDINATION_COLUMNS))
    
    # Filter, cast, rename columns
    cycle_length = (
        coord_tbl
        .filter(coord_tbl.ActualCycleLength > 0)
        .mutate(
            DeviceId=coord_tbl.DeviceId.cast('string'),
            CycleLength=coord_tbl.ActualCycleLength.cast('float64')
        )
        .select(['DeviceId', 'TimeStamp', 'CycleLength'])
        .order_by(['DeviceId', 'TimeStamp'])
    )
    
    return cycle_length
