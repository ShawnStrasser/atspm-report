"""Detect standard dual-ring phase and overlap conflicts from timeline data."""

from __future__ import annotations

from typing import Optional

import duckdb
import pandas as pd
from .duckdb_utils import quote_literal, to_pandas


STANDARD_CONFLICT_PAIRS = [
    (1, 2), (1, 3), (1, 4), (1, 7), (1, 8),
    (2, 3), (2, 4), (2, 7), (2, 8),
    (3, 4), (3, 5), (3, 6),
    (4, 5), (4, 6),
    (5, 6), (5, 7), (5, 8),
    (6, 7), (6, 8),
    (7, 8),
]

OVERLAP_COMPATIBLE_GROUPS = [
    {1, 2, 5, 6},
    {3, 4, 7, 8},
]

OVERLAP_CONFLICT_PAIRS = sorted({
    tuple(sorted((left, right)))
    for left in OVERLAP_COMPATIBLE_GROUPS[0]
    for right in OVERLAP_COMPATIBLE_GROUPS[1]
})

# Overlap yellow/red indications longer than this are treated as stale rather
# than real. A lost overlap termination event (61-66) leaves the timeline holding
# the last indication until the next one arrives, which can be minutes later, and
# that stale interval then overlaps a legitimately-green conflicting movement.
# Phase yellow/red staleness is handled upstream in the atspm package; overlap
# indications are not, because an overlap yellow can legitimately be a flashing
# yellow arrow or a clearance interval. Six seconds is above any real overlap
# clearance but far below the multi-minute intervals a dropped event produces.
OVERLAP_STALE_INDICATION_SECONDS = 6.0

CONFLICT_COLUMNS = [
    'DeviceId',
    'Date',
    'ConflictStart',
    'ConflictEnd',
    'DurationSeconds',
    'Movement1Type',
    'Movement1Number',
    'Movement1Indication',
    'Movement2Type',
    'Movement2Number',
    'Movement2Indication',
]

PAIR_VALUES = ', '.join(f'({left}, {right})' for left, right in STANDARD_CONFLICT_PAIRS)
OVERLAP_PAIR_VALUES = ', '.join(
    f'({left}, {right})' for left, right in OVERLAP_CONFLICT_PAIRS
)

# Interval classes the conflict checks compare.
INDICATION_CLASSES = ('Green', 'Yellow', 'Red', 'Overlap Green', 'Overlap Yellow', 'Overlap Red')

# The valid indication intervals that overlap no invalid interval of their
# signal. Built once per timeline and shared by every conflict check (and the
# overlap dual indication check): a signal's clean intervals do not depend on
# any other signal or on which check reads them.
CLEAN_TIMELINE_SQL = """
CREATE TABLE clean_indications AS
WITH timeline AS (
    SELECT * FROM timeline_input
),
invalid_intervals AS (
    SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, StartTime, EndTime
    FROM timeline
    WHERE IsValid IS FALSE
      AND StartTime IS NOT NULL
      AND EndTime IS NOT NULL
)
SELECT candidate.*
FROM timeline AS candidate
WHERE candidate.IsValid IS TRUE
  AND candidate.EventClass IN ({indication_classes})
  AND NOT EXISTS (
      SELECT 1
      FROM invalid_intervals AS invalid
      WHERE invalid.DeviceId = CAST(candidate.DeviceId AS VARCHAR)
        AND candidate.StartTime < invalid.EndTime
        AND invalid.StartTime < candidate.EndTime
  )
""".format(indication_classes=', '.join(f"'{name}'" for name in INDICATION_CLASSES))

CLEAN_TIMELINE_CTES = """
clean_timeline AS (
    SELECT * FROM clean_indications{device_filter}
),
conflict_pairs(Phase1, Phase2) AS (
    VALUES {pair_values}
)
"""

GENERAL_PHASE_CONFLICT_SQL = """
WITH
{clean_timeline_ctes},
phase_indications AS (
    SELECT
        CAST(DeviceId AS VARCHAR) AS DeviceId,
        TRY_CAST(EventValue AS INTEGER) AS MovementNumber,
        EventClass AS Indication,
        StartTime,
        EndTime
    FROM clean_timeline
    WHERE EventClass IN ('Green', 'Yellow', 'Red')
      AND TRY_CAST(EventValue AS INTEGER) BETWEEN 1 AND 8
),
conflicts AS (
    SELECT
        first.DeviceId,
        GREATEST(first.StartTime, second.StartTime) AS ConflictStart,
        LEAST(first.EndTime, second.EndTime) AS ConflictEnd,
        first.MovementNumber AS Movement1Number,
        first.Indication AS Movement1Indication,
        second.MovementNumber AS Movement2Number,
        second.Indication AS Movement2Indication
    FROM phase_indications AS first
    JOIN phase_indications AS second
      ON first.DeviceId = second.DeviceId
     AND first.MovementNumber < second.MovementNumber
     AND first.StartTime < second.EndTime
     AND second.StartTime < first.EndTime
    JOIN conflict_pairs AS pairs
      ON pairs.Phase1 = first.MovementNumber
     AND pairs.Phase2 = second.MovementNumber
)
SELECT
    DeviceId,
    ConflictStart,
    ConflictEnd,
    DATE_DIFF('millisecond', ConflictStart, ConflictEnd) / 1000.0 AS DurationSeconds,
    'Phase' AS Movement1Type,
    Movement1Number,
    Movement1Indication,
    'Phase' AS Movement2Type,
    Movement2Number,
    Movement2Indication
FROM conflicts
ORDER BY DeviceId, ConflictStart, Movement1Number, Movement2Number
"""

OVERLAP_CONFLICT_SQL = """
WITH
{clean_timeline_ctes},
configured_overlaps(OverlapNumber) AS (
    VALUES {overlap_values}
),
phase_indications AS (
    SELECT
        CAST(DeviceId AS VARCHAR) AS DeviceId,
        'Phase' AS MovementType,
        TRY_CAST(EventValue AS INTEGER) AS MovementNumber,
        EventClass AS Indication,
        StartTime,
        EndTime
    FROM clean_timeline
    WHERE EventClass IN ('Green', 'Yellow')
      AND TRY_CAST(EventValue AS INTEGER) BETWEEN 1 AND 8
),
overlap_indications AS (
    SELECT
        CAST(indication.DeviceId AS VARCHAR) AS DeviceId,
        'Overlap' AS MovementType,
        TRY_CAST(indication.EventValue AS INTEGER) AS MovementNumber,
        indication.EventClass AS Indication,
        indication.StartTime,
        indication.EndTime
    FROM clean_timeline AS indication
    JOIN configured_overlaps
      ON configured_overlaps.OverlapNumber = TRY_CAST(indication.EventValue AS INTEGER)
    WHERE indication.EventClass IN ('Overlap Green', 'Overlap Yellow')
      AND NOT (
          indication.EventClass IN ('Overlap Yellow', 'Overlap Red')
          AND DATE_DIFF(
                  'millisecond', indication.StartTime, indication.EndTime
              ) > {stale_overlap_ms}
      )
),
all_indications AS (
    SELECT * FROM phase_indications
    UNION ALL
    SELECT * FROM overlap_indications
),
raw_conflicts AS (
    SELECT
        source.DeviceId,
        GREATEST(source.StartTime, target.StartTime) AS ConflictStart,
        LEAST(source.EndTime, target.EndTime) AS ConflictEnd,
        source.MovementType AS SourceType,
        source.MovementNumber AS SourceNumber,
        source.Indication AS SourceIndication,
        target.MovementType AS TargetType,
        target.MovementNumber AS TargetNumber,
        target.Indication AS TargetIndication
    FROM overlap_indications AS source
    JOIN all_indications AS target
      ON source.DeviceId = target.DeviceId
     AND source.MovementNumber <> target.MovementNumber
     AND source.StartTime < target.EndTime
     AND target.StartTime < source.EndTime
    JOIN conflict_pairs AS pairs
      ON pairs.Phase1 = LEAST(source.MovementNumber, target.MovementNumber)
     AND pairs.Phase2 = GREATEST(source.MovementNumber, target.MovementNumber)
),
canonical_conflicts AS (
    SELECT DISTINCT
        DeviceId,
        ConflictStart,
        ConflictEnd,
        CASE
            WHEN TargetType = 'Overlap' AND TargetNumber < SourceNumber THEN TargetType
            ELSE SourceType
        END AS Movement1Type,
        CASE
            WHEN TargetType = 'Overlap' AND TargetNumber < SourceNumber THEN TargetNumber
            ELSE SourceNumber
        END AS Movement1Number,
        CASE
            WHEN TargetType = 'Overlap' AND TargetNumber < SourceNumber THEN TargetIndication
            ELSE SourceIndication
        END AS Movement1Indication,
        CASE
            WHEN TargetType = 'Overlap' AND TargetNumber < SourceNumber THEN SourceType
            ELSE TargetType
        END AS Movement2Type,
        CASE
            WHEN TargetType = 'Overlap' AND TargetNumber < SourceNumber THEN SourceNumber
            ELSE TargetNumber
        END AS Movement2Number,
        CASE
            WHEN TargetType = 'Overlap' AND TargetNumber < SourceNumber THEN SourceIndication
            ELSE TargetIndication
        END AS Movement2Indication
    FROM raw_conflicts
)
SELECT
    DeviceId,
    ConflictStart,
    ConflictEnd,
    DATE_DIFF('millisecond', ConflictStart, ConflictEnd) / 1000.0 AS DurationSeconds,
    Movement1Type,
    Movement1Number,
    Movement1Indication,
    Movement2Type,
    Movement2Number,
    Movement2Indication
FROM canonical_conflicts
ORDER BY DeviceId, ConflictStart, Movement1Type, Movement1Number, Movement2Type, Movement2Number
"""


SAME_MOVEMENT_COLOR_CONFLICT_SQL = '''
WITH
{clean_timeline_ctes},
indications AS (
    SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, 'Phase' AS MovementType,
           TRY_CAST(EventValue AS INTEGER) AS MovementNumber, EventClass AS Indication,
           CASE EventClass WHEN 'Green' THEN 1 WHEN 'Yellow' THEN 2 WHEN 'Red' THEN 3 END AS ColorRank,
           StartTime, EndTime
    FROM clean_timeline
    WHERE EventClass IN ('Green', 'Yellow', 'Red') AND TRY_CAST(EventValue AS INTEGER) IS NOT NULL
    UNION ALL
    SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, 'Overlap' AS MovementType,
           TRY_CAST(EventValue AS INTEGER) AS MovementNumber, EventClass AS Indication,
           CASE EventClass WHEN 'Overlap Green' THEN 1 WHEN 'Overlap Yellow' THEN 2 WHEN 'Overlap Red' THEN 3 END AS ColorRank,
           StartTime, EndTime
    FROM clean_timeline
    WHERE EventClass IN ('Overlap Green', 'Overlap Yellow', 'Overlap Red') AND TRY_CAST(EventValue AS INTEGER) IS NOT NULL
),
conflicts AS (
    SELECT first.DeviceId, GREATEST(first.StartTime, second.StartTime) AS ConflictStart,
           LEAST(first.EndTime, second.EndTime) AS ConflictEnd, first.MovementType,
           first.MovementNumber, first.Indication AS Movement1Indication,
           second.Indication AS Movement2Indication
    FROM indications AS first
    JOIN indications AS second
      ON first.DeviceId = second.DeviceId
     AND first.MovementType = second.MovementType
     AND first.MovementNumber = second.MovementNumber
     AND first.ColorRank < second.ColorRank
     AND first.StartTime < second.EndTime
     AND second.StartTime < first.EndTime
)
SELECT DeviceId, ConflictStart, ConflictEnd,
       DATE_DIFF('millisecond', ConflictStart, ConflictEnd) / 1000.0 AS DurationSeconds,
       MovementType AS Movement1Type, MovementNumber AS Movement1Number, Movement1Indication,
       MovementType AS Movement2Type, MovementNumber AS Movement2Number, Movement2Indication
FROM conflicts
ORDER BY DeviceId, ConflictStart, Movement1Type, Movement1Number, Movement1Indication, Movement2Indication
'''


def _empty_conflicts() -> pd.DataFrame:
    return pd.DataFrame(columns=CONFLICT_COLUMNS)


def validate_timeline(timeline, context: str = '') -> Optional[pd.DataFrame]:
    """The timeline as a DataFrame ready for the conflict queries, None when empty."""
    df = to_pandas(timeline)
    if df is None or df.empty:
        return None
    required = ['DeviceId', 'StartTime', 'EndTime', 'IsValid', 'EventClass', 'EventValue']
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f'Missing required timeline column(s){context}: ' + ', '.join(missing))
    # DuckDB needs real timestamps for the interval arithmetic; callers may
    # hand over strings (or Parquet-typed columns), so coerce here.
    if not all(pd.api.types.is_datetime64_dtype(df[column]) for column in ('StartTime', 'EndTime')):
        df = df.copy()
        df['StartTime'] = pd.to_datetime(df['StartTime'], errors='coerce')
        df['EndTime'] = pd.to_datetime(df['EndTime'], errors='coerce')
    return df


def run_conflict_queries(timeline: pd.DataFrame, queries: dict) -> dict:
    """Run each query (name -> SQL reading clean_indications) on one connection,
    with the timeline's clean indications built once -> name -> DataFrame."""
    con = duckdb.connect()
    try:
        con.register('timeline_input', timeline)
        con.execute(CLEAN_TIMELINE_SQL)
        con.unregister('timeline_input')
        return {name: con.execute(sql).df() for name, sql in queries.items()}
    finally:
        con.close()


def _device_filter(device_ids) -> str:
    excluded = sorted({
        str(device_id)
        for device_id in (device_ids or [])
        if pd.notna(device_id) and str(device_id).strip()
    })
    if not excluded:
        return ''
    ids = ', '.join(quote_literal(device_id) for device_id in excluded)
    return f' WHERE CAST(DeviceId AS VARCHAR) NOT IN ({ids})'


def conflicts_frame(conflicts: pd.DataFrame) -> pd.DataFrame:
    """A conflict query's rows as the report's conflict alerts."""
    if conflicts.empty:
        return _empty_conflicts()
    conflicts['ConflictStart'] = pd.to_datetime(conflicts['ConflictStart'], errors='coerce')
    conflicts['ConflictEnd'] = pd.to_datetime(conflicts['ConflictEnd'], errors='coerce')
    conflicts['Date'] = conflicts['ConflictStart'].dt.normalize()
    conflicts = conflicts.dropna(subset=['ConflictStart', 'ConflictEnd', 'Date'])
    conflicts = conflicts[conflicts['DurationSeconds'] > 0].copy()
    return conflicts.reindex(columns=CONFLICT_COLUMNS)


def general_phase_conflict_sql(config: Optional[dict] = None) -> Optional[str]:
    """The general phase conflict query for this config, None when the check is off."""
    config = config or {}
    if not bool(config.get('general_phase_conflicts_enabled', False)):
        return None
    clean_ctes = CLEAN_TIMELINE_CTES.format(
        pair_values=PAIR_VALUES,
        device_filter=_device_filter(config.get('general_phase_conflict_excluded_device_ids')),
    )
    return GENERAL_PHASE_CONFLICT_SQL.format(clean_timeline_ctes=clean_ctes)


def overlap_conflict_sql(config: Optional[dict] = None) -> Optional[str]:
    """The phase/overlap conflict query for this config, None when the check is off."""
    config = config or {}
    if not bool(config.get('overlap_conflicts_enabled', False)):
        return None
    overlaps = sorted({
        int(number)
        for number in config.get('overlap_conflict_numbers', [])
        if pd.notna(number) and 1 <= int(number) <= 8
    })
    if not overlaps:
        return None
    clean_ctes = CLEAN_TIMELINE_CTES.format(
        pair_values=OVERLAP_PAIR_VALUES,
        device_filter=_device_filter(config.get('overlap_conflict_excluded_device_ids')),
    )
    overlap_values = ', '.join(f'({number})' for number in overlaps)
    return OVERLAP_CONFLICT_SQL.format(
        clean_timeline_ctes=clean_ctes,
        overlap_values=overlap_values,
        stale_overlap_ms=int(OVERLAP_STALE_INDICATION_SECONDS * 1000),
    )


def same_movement_color_conflict_sql(config: Optional[dict] = None) -> Optional[str]:
    """The same phase/overlap multi-color query for this config, None when the check is off."""
    config = config or {}
    if not bool(config.get('same_movement_color_conflicts_enabled', False)):
        return None
    clean_ctes = CLEAN_TIMELINE_CTES.format(pair_values=PAIR_VALUES, device_filter='')
    return SAME_MOVEMENT_COLOR_CONFLICT_SQL.format(clean_timeline_ctes=clean_ctes)


def _process(timeline, sql: Optional[str]) -> pd.DataFrame:
    if sql is None:
        return _empty_conflicts()
    df = validate_timeline(timeline)
    if df is None:
        return _empty_conflicts()
    return conflicts_frame(run_conflict_queries(df, {'conflicts': sql})['conflicts'])


def process_general_phase_conflicts(timeline, config: Optional[dict] = None) -> pd.DataFrame:
    return _process(timeline, general_phase_conflict_sql(config))


def process_overlap_conflicts(timeline, config: Optional[dict] = None) -> pd.DataFrame:
    return _process(timeline, overlap_conflict_sql(config))


def process_same_movement_color_conflicts(timeline, config: Optional[dict] = None) -> pd.DataFrame:
    return _process(timeline, same_movement_color_conflict_sql(config))
