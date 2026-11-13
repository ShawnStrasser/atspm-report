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
from typing import Optional, Tuple

from .duckdb_utils import TableLike, columns, run_sql

# Output column definitions
PHASE_WAIT_COLUMNS = [
    'DeviceId', 'TimeStamp', 'Phase', 'AvgPhaseWait', 'MaxPhaseWait', 'TotalSkips'
]

PHASE_SKIP_ALERT_COLUMNS = [
    'DeviceId', 'Phase', 'Date', 'TotalSkips'
]

COORDINATION_COLUMNS = [
    'DeviceId', 'TimeStamp', 'CycleLength', 'IsAssumed'
]


def _empty(cols) -> pd.DataFrame:
    return pd.DataFrame(columns=cols)


def process_phase_wait_data(
    phase_wait: Optional[TableLike],
    coordination_agg: Optional[TableLike] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Process phase_wait data to generate phase skip alerts.

    Args:
        phase_wait: DataFrame or DuckDB relation with columns TimeStamp, DeviceId, Phase, AvgPhaseWait, MaxPhaseWait, TotalSkips
        coordination_agg: Optional DataFrame or DuckDB relation with columns TimeStamp, DeviceId, ActualCycleLength
                         Used to plot cycle length as a step function (15-minute bins)

    Returns:
        Tuple of (phase_waits, alert_rows, cycle_length) DataFrames:
            - phase_waits: Processed phase wait data for plotting
            - alert_rows: Alert candidates with DeviceId, Phase, Date, TotalSkips
            - cycle_length: Cycle length data for plotting
    """
    # Handle empty/None input
    if phase_wait is None or (isinstance(phase_wait, pd.DataFrame) and phase_wait.empty):
        return (_empty(PHASE_WAIT_COLUMNS), _empty(PHASE_SKIP_ALERT_COLUMNS), _empty(COORDINATION_COLUMNS))

    # Verify required columns exist
    required_cols = ['TimeStamp', 'DeviceId', 'Phase', 'AvgPhaseWait', 'MaxPhaseWait', 'TotalSkips']
    present = columns(phase_wait)
    for col in required_cols:
        if col not in present:
            raise ValueError(f"Missing required column: {col}")

    typed = """
        SELECT
            CAST(DeviceId AS VARCHAR) AS DeviceId,
            CAST(TimeStamp AS TIMESTAMP) AS TimeStamp,
            CAST(Phase AS INTEGER) AS Phase,
            CAST(AvgPhaseWait AS DOUBLE) AS AvgPhaseWait,
            CAST(MaxPhaseWait AS DOUBLE) AS MaxPhaseWait,
            CAST(TotalSkips AS BIGINT) AS TotalSkips
        FROM t
    """
    phase_waits_out = run_sql(typed, t=phase_wait)

    # Alert rows: days with skips, summed per DeviceId/Phase/day
    alert_rows = run_sql(f"""
        SELECT DeviceId, Phase, DATE_TRUNC('day', TimeStamp) AS Date,
               CAST(SUM(TotalSkips) AS BIGINT) AS TotalSkips
        FROM ({typed})
        WHERE TotalSkips > 0
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """, t=phase_wait)

    # Process coordination_agg data for cycle length
    cycle_length = _extract_cycle_length(coordination_agg)

    return (phase_waits_out, alert_rows, cycle_length)


def _extract_cycle_length(coordination_agg: Optional[TableLike]) -> pd.DataFrame:
    """
    Extract cycle length data from coordination_agg.

    Args:
        coordination_agg: DataFrame or DuckDB relation with TimeStamp, DeviceId, ActualCycleLength columns

    Returns:
        DataFrame with DeviceId, TimeStamp, CycleLength, IsAssumed columns
    """
    if coordination_agg is None or (isinstance(coordination_agg, pd.DataFrame) and coordination_agg.empty):
        return _empty(COORDINATION_COLUMNS)

    # Check if ActualCycleLength column exists
    if 'ActualCycleLength' not in columns(coordination_agg):
        return _empty(COORDINATION_COLUMNS)

    # A cycle length of zero means the signal was running free. The skip test does
    # not go blind there - it falls back to the assumed cycle length - so neither
    # should the chart. Dropping those bins made the trace disappear exactly where
    # a reader most needs to know what threshold was being applied.
    return run_sql("""
        SELECT
            CAST(DeviceId AS VARCHAR) AS DeviceId,
            CAST(TimeStamp AS TIMESTAMP) AS TimeStamp,
            CAST(ActualCycleLength AS DOUBLE) AS CycleLength,
            ActualCycleLength <= 0 AS IsAssumed
        FROM t
        ORDER BY 1, 2
    """, t=coordination_agg)
