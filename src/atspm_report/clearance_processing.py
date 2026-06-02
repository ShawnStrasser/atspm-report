"""Clearance interval monitoring derived from ATSPM timeline rows."""

from __future__ import annotations

from typing import Optional, Union

import numpy as np
import pandas as pd
import ibis.expr.types as ir


CLEARANCE_EVENT_CLASSES = ["Yellow", "Red", "Overlap Yellow", "Overlap Red"]
FLOAT_EPSILON = 1e-9
DEFAULT_INVALID_EVENT_CUSHION_SECONDS = 30
INVALID_NEIGHBOR_MIN_DURATION_SECONDS = 6.0

CLEARANCE_ALERT_COLUMNS = [
    "DeviceId",
    "EventClass",
    "EventValue",
    "Date",
    "MedianDuration",
    "SampleCount",
    "MaxAbsDelta",
    "ShortCount",
    "IrregularCount",
    "LongCount",
    "AvgSignedDeviation",
    "RepresentativeShortDuration",
    "RepresentativeShortTime",
    "RepresentativeIrregularDuration",
    "RepresentativeIrregularTime",
    "RepresentativeLongDuration",
    "RepresentativeLongTime",
]


def _empty_alerts() -> pd.DataFrame:
    return pd.DataFrame(columns=CLEARANCE_ALERT_COLUMNS)


def _to_pandas(data: Union[pd.DataFrame, "ir.Table", None]) -> Optional[pd.DataFrame]:
    if data is None:
        return None
    if isinstance(data, pd.DataFrame):
        return data
    if ir is not None and isinstance(data, ir.Table):
        return data.execute()
    return None


def process_clearance_intervals(
    timeline: Union[pd.DataFrame, "ir.Table", None],
    config: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Create clearance interval alert rows from ATSPM timeline output.

    Alerts are generated for phase yellow/red rows and for overlap yellow/red rows
    that behave like fixed clearance timing.
    """
    config = config or {}
    df = _to_pandas(timeline)
    if df is None or df.empty:
        return _empty_alerts()

    required_cols = [
        "DeviceId",
        "StartTime",
        "Duration",
        "IsValid",
        "EventClass",
        "EventValue",
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required timeline column(s): {', '.join(missing_cols)}")

    df = df.copy()
    df["DeviceId"] = df["DeviceId"].astype(str)
    df["StartTime"] = pd.to_datetime(df["StartTime"], errors="coerce")
    df["Duration"] = pd.to_numeric(df["Duration"], errors="coerce")
    df["EventValue"] = pd.to_numeric(df["EventValue"], errors="coerce").astype("Int64")
    invalid_starts = df.loc[
        (df["IsValid"] == False) &
        df["StartTime"].notna(),
        ["DeviceId", "StartTime"]
    ].copy()

    df = df[
        (df["IsValid"] == True) &
        df["StartTime"].notna() &
        df["Duration"].notna() &
        (df["Duration"] <= 25) &
        df["EventClass"].isin(CLEARANCE_EVENT_CLASSES) &
        df["EventValue"].notna()
    ].copy()
    if df.empty:
        return _empty_alerts()

    invalid_event_cushion_seconds = float(
        config.get("clearance_invalid_event_cushion_seconds", DEFAULT_INVALID_EVENT_CUSHION_SECONDS)
    )
    if not invalid_starts.empty and invalid_event_cushion_seconds >= 0:
        invalid_event_cushion = pd.Timedelta(seconds=invalid_event_cushion_seconds)
        df = _drop_long_rows_near_invalid_events(df, invalid_starts, invalid_event_cushion)
        if df.empty:
            return _empty_alerts()

    yellow_min = float(config.get("clearance_yellow_min_seconds", 3.5))
    red_min = float(config.get("clearance_red_min_seconds", 0.5))
    tolerance = float(config.get("clearance_tolerance_seconds", 0.1))
    overlap_fixed_median_max = float(config.get("overlap_fixed_median_max_seconds", 6.0))
    overlap_fixed_within = float(config.get("overlap_fixed_within_seconds", 2.0))
    overlap_fixed_ratio = float(config.get("overlap_fixed_within_ratio", 0.95))

    alert_rows = []
    group_cols = ["DeviceId", "EventClass", "EventValue"]
    for (device_id, event_class, event_value), group in df.groupby(group_cols, sort=False):
        group = group.sort_values("StartTime").copy()
        durations = group["Duration"].astype(float)
        median_duration = float(durations.median())

        if str(event_class).startswith("Overlap"):
            within_ratio = float((durations.sub(median_duration).abs() <= overlap_fixed_within).mean())
            if median_duration > overlap_fixed_median_max or within_ratio < overlap_fixed_ratio:
                continue

        if "Yellow" in str(event_class):
            short_threshold = yellow_min - tolerance
        else:
            short_threshold = red_min - tolerance

        deltas = durations - median_duration
        abs_deltas = deltas.abs()
        short_mask = durations < (short_threshold - FLOAT_EPSILON)
        irregular_mask = abs_deltas > (tolerance + FLOAT_EPSILON)
        long_mask = irregular_mask & ~short_mask

        short_count = int(short_mask.sum())
        irregular_count = int(irregular_mask.sum())
        long_count = int(long_mask.sum())
        if short_count == 0 and irregular_count == 0:
            continue

        bad_times = group.loc[short_mask | irregular_mask, "StartTime"]
        alert_date = pd.to_datetime(bad_times.max()).normalize()

        short_row = _representative_row(group, abs_deltas, short_mask)
        irregular_row = _representative_row(group, abs_deltas, irregular_mask)
        long_row = _representative_row(group, abs_deltas, long_mask)

        alert_rows.append({
            "DeviceId": str(device_id),
            "EventClass": str(event_class),
            "EventValue": int(event_value),
            "Date": alert_date,
            "MedianDuration": median_duration,
            "SampleCount": int(len(group)),
            "MaxAbsDelta": float(abs_deltas.max()) if len(abs_deltas) else np.nan,
            "ShortCount": short_count,
            "IrregularCount": irregular_count,
            "LongCount": long_count,
            "AvgSignedDeviation": float(deltas.mean()) if len(deltas) else np.nan,
            "RepresentativeShortDuration": _row_value(short_row, "Duration"),
            "RepresentativeShortTime": _row_value(short_row, "StartTime"),
            "RepresentativeIrregularDuration": _row_value(irregular_row, "Duration"),
            "RepresentativeIrregularTime": _row_value(irregular_row, "StartTime"),
            "RepresentativeLongDuration": _row_value(long_row, "Duration"),
            "RepresentativeLongTime": _row_value(long_row, "StartTime"),
        })

    if not alert_rows:
        return _empty_alerts()

    return pd.DataFrame(alert_rows).reindex(columns=CLEARANCE_ALERT_COLUMNS)


def _representative_row(group: pd.DataFrame, abs_deltas: pd.Series, mask: pd.Series) -> Optional[pd.Series]:
    if not bool(mask.any()):
        return None
    candidate_index = abs_deltas[mask].idxmax()
    return group.loc[candidate_index]


def _drop_long_rows_near_invalid_events(
    clearance_rows: pd.DataFrame,
    invalid_starts: pd.DataFrame,
    cushion: pd.Timedelta,
) -> pd.DataFrame:
    long_rows = clearance_rows[clearance_rows["Duration"] > INVALID_NEIGHBOR_MIN_DURATION_SECONDS]
    if long_rows.empty:
        return clearance_rows.copy()

    clearance_sorted = (
        long_rows
        .reset_index()
        .rename(columns={"index": "_original_index"})
        .sort_values(["StartTime", "DeviceId"])
    )
    invalid_sorted = (
        invalid_starts
        .rename(columns={"StartTime": "InvalidStartTime"})
        .sort_values(["InvalidStartTime", "DeviceId"])
    )

    previous_invalid = pd.merge_asof(
        clearance_sorted[["_original_index", "DeviceId", "StartTime"]],
        invalid_sorted,
        left_on="StartTime",
        right_on="InvalidStartTime",
        by="DeviceId",
        direction="backward",
        tolerance=cushion,
    )
    next_invalid = pd.merge_asof(
        clearance_sorted[["_original_index", "DeviceId", "StartTime"]],
        invalid_sorted,
        left_on="StartTime",
        right_on="InvalidStartTime",
        by="DeviceId",
        direction="forward",
        tolerance=cushion,
    )

    near_invalid = (
        previous_invalid["InvalidStartTime"].notna() |
        next_invalid["InvalidStartTime"].notna()
    )
    excluded_indices = previous_invalid.loc[near_invalid, "_original_index"]

    return clearance_rows.drop(index=excluded_indices).copy()


def _row_value(row: Optional[pd.Series], column: str):
    if row is None:
        return pd.NA
    return row[column]
