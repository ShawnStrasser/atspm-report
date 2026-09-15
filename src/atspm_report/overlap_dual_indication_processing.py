"""Detect phase-green / same-numbered overlap yellow-or-red conflicts."""

from __future__ import annotations

from typing import Optional, Union

import duckdb
import pandas as pd
import ibis.expr.types as ir


OVERLAP_DUAL_INDICATION_COLUMNS = [
    "DeviceId",
    "Phase",
    "Date",
    "ConflictStart",
    "ConflictEnd",
    "DurationSeconds",
    "OverlapIndication",
]


CONFLICT_SQL = """
WITH invalid_intervals AS (
    SELECT
        CAST(DeviceId AS VARCHAR) AS DeviceId,
        StartTime,
        EndTime
    FROM timeline
    WHERE IsValid IS FALSE
      AND StartTime IS NOT NULL
      AND EndTime IS NOT NULL
),
clean_timeline AS (
    SELECT candidate.*
    FROM timeline AS candidate
    WHERE candidate.IsValid IS TRUE
      AND NOT EXISTS (
          SELECT 1
          FROM invalid_intervals AS invalid
          WHERE invalid.DeviceId = CAST(candidate.DeviceId AS VARCHAR)
            AND candidate.StartTime < invalid.EndTime
            AND invalid.StartTime < candidate.EndTime
      )
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


def _to_pandas(
    data: Union[pd.DataFrame, "ir.Table", None],
) -> Optional[pd.DataFrame]:
    if data is None:
        return None
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, ir.Table):
        return data.execute()
    return None


def process_overlap_dual_indications(
    timeline: Union[pd.DataFrame, "ir.Table", None],
    config: Optional[dict] = None,
) -> pd.DataFrame:
    """Return valid same-numbered phase-green/overlap-yellow-or-red conflicts."""
    config = config or {}
    if not bool(config.get("overlap_dual_indications_enabled", False)):
        return _empty_conflicts()

    phases = sorted({
        int(phase)
        for phase in config.get("overlap_dual_indication_phases", [])
        if pd.notna(phase) and int(phase) > 0
    })
    if not phases:
        return _empty_conflicts()

    df = _to_pandas(timeline)
    if df is None or df.empty:
        return _empty_conflicts()

    required_cols = [
        "DeviceId",
        "StartTime",
        "EndTime",
        "IsValid",
        "EventClass",
        "EventValue",
    ]
    missing_cols = [column for column in required_cols if column not in df.columns]
    if missing_cols:
        raise ValueError(
            "Missing required timeline column(s) for overlap dual indications: "
            + ", ".join(missing_cols)
        )

    # DuckDB needs real timestamps for the interval arithmetic; callers may
    # hand over strings (or Parquet-typed columns), so coerce here.
    df = df.copy()
    df["StartTime"] = pd.to_datetime(df["StartTime"], errors="coerce")
    df["EndTime"] = pd.to_datetime(df["EndTime"], errors="coerce")

    phase_numbers = ", ".join(str(phase) for phase in phases)
    con = duckdb.connect()
    try:
        con.register("timeline", df)
        conflicts = con.execute(
            CONFLICT_SQL.format(phase_numbers=phase_numbers)
        ).df()
    finally:
        con.close()

    if conflicts.empty:
        return _empty_conflicts()

    conflicts["ConflictStart"] = pd.to_datetime(conflicts["ConflictStart"], errors="coerce")
    conflicts["ConflictEnd"] = pd.to_datetime(conflicts["ConflictEnd"], errors="coerce")
    conflicts["Date"] = conflicts["ConflictStart"].dt.normalize()
    conflicts["Phase"] = pd.to_numeric(conflicts["Phase"], errors="coerce").astype("Int64")
    conflicts = conflicts.dropna(subset=["ConflictStart", "ConflictEnd", "Date", "Phase"])
    conflicts = conflicts[conflicts["DurationSeconds"] > 0].copy()

    return conflicts.reindex(columns=OVERLAP_DUAL_INDICATION_COLUMNS)
