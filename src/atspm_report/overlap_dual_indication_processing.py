"""Detect phase-green / same-numbered overlap yellow-or-red conflicts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

import pandas as pd
from .phase_conflict_processing import run_conflict_queries, validate_timeline

if TYPE_CHECKING:
    import duckdb


OVERLAP_DUAL_INDICATION_COLUMNS = [
    "DeviceId",
    "Phase",
    "Date",
    "ConflictStart",
    "ConflictEnd",
    "DurationSeconds",
    "OverlapIndication",
]


# Reads the clean indications phase_conflict_processing builds (valid intervals
# overlapping no invalid interval of their signal).
CONFLICT_SQL = """
WITH clean_timeline AS (
    SELECT * FROM clean_indications
),
phase_green AS (
    SELECT
        CAST(DeviceId AS VARCHAR) AS DeviceId,
        TRY_CAST(EventValue AS INTEGER) AS Phase,
        StartTime,
        EndTime
    FROM clean_timeline
    WHERE EventClass = 'Green'
      AND TRY_CAST(EventValue AS INTEGER) IN ({phase_numbers})
),
overlap_not_green AS (
    SELECT
        CAST(DeviceId AS VARCHAR) AS DeviceId,
        TRY_CAST(EventValue AS INTEGER) AS Phase,
        EventClass,
        StartTime,
        EndTime
    FROM clean_timeline
    WHERE EventClass IN ('Overlap Yellow', 'Overlap Red')
      AND TRY_CAST(EventValue AS INTEGER) IN ({phase_numbers})
)
SELECT
    green.DeviceId,
    green.Phase,
    GREATEST(green.StartTime, overlap.StartTime) AS ConflictStart,
    LEAST(green.EndTime, overlap.EndTime) AS ConflictEnd,
    DATE_DIFF(
        'millisecond',
        GREATEST(green.StartTime, overlap.StartTime),
        LEAST(green.EndTime, overlap.EndTime)
    ) / 1000.0 AS DurationSeconds,
    overlap.EventClass AS OverlapIndication
FROM phase_green AS green
JOIN overlap_not_green AS overlap
  ON green.DeviceId = overlap.DeviceId
 AND green.Phase = overlap.Phase
 AND green.StartTime < overlap.EndTime
 AND overlap.StartTime < green.EndTime
ORDER BY green.DeviceId, ConflictStart, green.Phase
"""


def _empty_conflicts() -> pd.DataFrame:
    return pd.DataFrame(columns=OVERLAP_DUAL_INDICATION_COLUMNS)


def overlap_dual_indication_sql(config: Optional[dict] = None) -> Optional[str]:
    """The overlap dual indication query for this config, None when the check is off."""
    config = config or {}
    if not bool(config.get("overlap_dual_indications_enabled", False)):
        return None
    phases = sorted({
        int(phase)
        for phase in config.get("overlap_dual_indication_phases", [])
        if pd.notna(phase) and int(phase) > 0
    })
    if not phases:
        return None
    return CONFLICT_SQL.format(phase_numbers=", ".join(str(phase) for phase in phases))


def dual_indications_frame(conflicts: pd.DataFrame) -> pd.DataFrame:
    """The query's rows as the report's overlap dual indication alerts."""
    if conflicts.empty:
        return _empty_conflicts()

    conflicts["ConflictStart"] = pd.to_datetime(conflicts["ConflictStart"], errors="coerce")
    conflicts["ConflictEnd"] = pd.to_datetime(conflicts["ConflictEnd"], errors="coerce")
    conflicts["Date"] = conflicts["ConflictStart"].dt.normalize()
    conflicts["Phase"] = pd.to_numeric(conflicts["Phase"], errors="coerce").astype("Int64")
    conflicts = conflicts.dropna(subset=["ConflictStart", "ConflictEnd", "Date", "Phase"])
    conflicts = conflicts[conflicts["DurationSeconds"] > 0].copy()

    return conflicts.reindex(columns=OVERLAP_DUAL_INDICATION_COLUMNS)


def process_overlap_dual_indications(
    timeline: Union[pd.DataFrame, "duckdb.DuckDBPyRelation", None],
    config: Optional[dict] = None,
) -> pd.DataFrame:
    """Return valid same-numbered phase-green/overlap-yellow-or-red conflicts."""
    sql = overlap_dual_indication_sql(config)
    if sql is None:
        return _empty_conflicts()
    df = validate_timeline(timeline, " for overlap dual indications")
    if df is None:
        return _empty_conflicts()
    return dual_indications_frame(run_conflict_queries(df, {"dual": sql})["dual"])
