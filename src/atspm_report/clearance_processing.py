"""Clearance interval monitoring derived from ATSPM timeline rows."""

from __future__ import annotations

from typing import Optional, Union

import numpy as np
import pandas as pd
import ibis.expr.types as ir

from .alarm_processing import FLASH_EVENT_CLASSES


CLEARANCE_EVENT_CLASSES = ["Yellow", "Red", "Overlap Yellow", "Overlap Red"]
DEFAULT_STOP_TIME_EVENT_CLASSES = ["Stop Time Input", "Preempt"]
# Any flash state, from either the EventId 174 bitmap or the EventId 173 enum. A
# controller entering, sitting in, or leaving flash produces clearance intervals
# that are real but meaningless to review, so they are excluded for a window
# either side of the flash.
DEFAULT_FLASH_EVENT_CLASSES = list(FLASH_EVENT_CLASSES)
DEFAULT_FLASH_CUSHION_SECONDS = 600
# Only an invalid vehicle indication interval says the signal-state record is
# untrustworthy at that moment. Other interval types in a full timeline do not:
# atspm marks every Ped Service interval invalid by construction, and those run
# concurrently with vehicle clearances all day, so treating them as evidence of
# bad data masked most of the samples and hid roughly three quarters of alerts.
DEFAULT_VALIDITY_EVENT_CLASSES = [
    "Green",
    "Yellow",
    "Red",
    "Overlap Green",
    "Overlap Yellow",
    "Overlap Red",
    "Overlap Trail Green",
]
FLOAT_EPSILON = 1e-9
DEFAULT_INVALID_EVENT_CUSHION_SECONDS = 30
INVALID_NEIGHBOR_MIN_DURATION_SECONDS = 6.0
MIDNIGHT_EXCLUSION_SECONDS = 60

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
    if "EndTime" in df.columns:
        df["EndTime"] = pd.to_datetime(df["EndTime"], errors="coerce")
    else:
        df["EndTime"] = pd.NaT
    df["EndTime"] = df["EndTime"].fillna(
        df["StartTime"] + pd.to_timedelta(df["Duration"], unit="s")
    )
    df["EventValue"] = pd.to_numeric(df["EventValue"], errors="coerce").astype("Int64")
    stop_events = (
        _extract_stop_time_events(
            df,
            config.get("clearance_stop_event_classes", DEFAULT_STOP_TIME_EVENT_CLASSES),
        )
        if bool(config.get("filter_stoptime", True))
        else pd.DataFrame(columns=["DeviceId", "StopStartTime", "StopEndTime"])
    )
    flash_intervals = _extract_flash_intervals(
        df,
        config.get("clearance_flash_event_classes", DEFAULT_FLASH_EVENT_CLASSES),
        float(config.get("clearance_flash_cushion_seconds", DEFAULT_FLASH_CUSHION_SECONDS)),
    )
    validity_classes = config.get(
        "clearance_validity_event_classes", DEFAULT_VALIDITY_EVENT_CLASSES
    )
    is_state_row = df['EventClass'].isin(validity_classes)

    invalid_intervals = df.loc[
        (df['IsValid'] == False) &
        is_state_row &
        df['StartTime'].notna() &
        df['EndTime'].notna(),
        ['DeviceId', 'StartTime', 'EndTime']
    ].copy()
    invalid_intervals = invalid_intervals[
        invalid_intervals['EndTime'] >= invalid_intervals['StartTime']
    ]

    valid_overlap_greens = df.loc[
        (df['IsValid'] == True) &
        (df['EventClass'] == 'Overlap Green') &
        df['StartTime'].notna() &
        df['EndTime'].notna() &
        df['EventValue'].notna()
    ].copy()
    if not invalid_intervals.empty and not valid_overlap_greens.empty:
        valid_overlap_greens = _drop_rows_overlapping_invalid_intervals(
            valid_overlap_greens,
            invalid_intervals,
        )

    invalid_starts = df.loc[
        (df["IsValid"] == False) &
        is_state_row &
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

    if not invalid_intervals.empty:
        df = _drop_rows_overlapping_invalid_intervals(df, invalid_intervals)
        if df.empty:
            return _empty_alerts()

    if not flash_intervals.empty:
        df = _drop_rows_overlapping_invalid_intervals(df, flash_intervals)
        if df.empty:
            return _empty_alerts()

    df = _drop_rows_near_midnight(df)
    if df.empty:
        return _empty_alerts()

    df = _drop_overlap_yellows_without_preceding_green(df, valid_overlap_greens)
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
        is_red = "Red" in str(event_class)

        if str(event_class) == "Overlap Yellow":
            within_ratio = float((durations.sub(median_duration).abs() <= overlap_fixed_within).mean())
            if median_duration > overlap_fixed_median_max or within_ratio < overlap_fixed_ratio:
                continue

        if is_red:
            # Red clearance may vary. Only values below the configured global
            # minimum are alertable; tolerance does not lower this threshold.
            short_mask = durations < (red_min - FLOAT_EPSILON)
            irregular_only_mask = pd.Series(False, index=group.index)
            long_mask = pd.Series(False, index=group.index)
        else:
            short_threshold = yellow_min - tolerance
            short_mask = durations < (short_threshold - FLOAT_EPSILON)
            irregular_only_mask = (
                (durations - median_duration < -(tolerance + FLOAT_EPSILON)) & ~short_mask
            )
            long_mask = (
                (durations - median_duration > (tolerance + FLOAT_EPSILON)) & ~short_mask
            )

        deltas = durations - median_duration
        abs_deltas = deltas.abs()

        non_short_deviation_mask = irregular_only_mask | long_mask
        if bool(non_short_deviation_mask.any()) and not stop_events.empty:
            overlaps = _stop_time_overlap_mask(
                group.loc[
                    non_short_deviation_mask,
                    ['DeviceId', 'StartTime', 'EndTime'],
                ],
                stop_events,
            )
            if bool(overlaps.any()):
                irregular_only_mask = irregular_only_mask.copy()
                long_mask = long_mask.copy()
                irregular_only_mask.loc[overlaps[overlaps].index] = False
                long_mask.loc[overlaps[overlaps].index] = False

        anomaly_mask = irregular_only_mask | long_mask

        short_count = int(short_mask.sum())
        long_count = int(long_mask.sum())
        irregular_count = int(irregular_only_mask.sum())
        if short_count == 0 and irregular_count == 0 and long_count == 0:
            continue

        bad_times = group.loc[short_mask | anomaly_mask, "StartTime"]
        alert_date = pd.to_datetime(bad_times.max()).normalize()

        short_row = _representative_row(group, abs_deltas, short_mask)
        irregular_row = _representative_row(group, abs_deltas, irregular_only_mask)
        long_row = _representative_row(group, abs_deltas, long_mask)

        alert_rows.append({
            "DeviceId": str(device_id),
            "EventClass": str(event_class),
            "EventValue": int(event_value),
            "Date": alert_date,
            "MedianDuration": median_duration,
            "SampleCount": int(len(group)),
            "MaxAbsDelta": float(abs_deltas[short_mask | anomaly_mask].max()),
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


def _extract_flash_intervals(
    timeline: pd.DataFrame,
    event_classes,
    cushion_seconds: float,
) -> pd.DataFrame:
    """Return flash alarm intervals widened by `cushion_seconds` on both sides.

    The result is shaped like the invalid-interval frame so the same overlap
    filter can drop clearance rows that fall inside a flash window.
    """
    flash_keys = _normalized_event_class_keys(event_classes)
    if not flash_keys or cushion_seconds < 0:
        return pd.DataFrame(columns=["DeviceId", "StartTime", "EndTime"])

    event_class_key = (
        timeline["EventClass"]
        .astype(str)
        .str.replace(r"[\s_-]+", "", regex=True)
        .str.lower()
    )
    flash_rows = timeline.loc[
        event_class_key.isin(flash_keys) &
        timeline["StartTime"].notna(),
        ["DeviceId", "StartTime", "EndTime"],
    ].copy()
    if flash_rows.empty:
        return pd.DataFrame(columns=["DeviceId", "StartTime", "EndTime"])

    # A flash interval whose end is missing still marks a moment worth excluding.
    flash_rows["EndTime"] = flash_rows["EndTime"].fillna(flash_rows["StartTime"])
    flash_rows = flash_rows[flash_rows["EndTime"] >= flash_rows["StartTime"]]
    if flash_rows.empty:
        return pd.DataFrame(columns=["DeviceId", "StartTime", "EndTime"])

    # Timedelta arithmetic promotes to nanosecond resolution, which would not
    # match the microsecond timestamps read from Parquet when these intervals are
    # later merged against clearance rows. Cast back to the original resolution.
    cushion = pd.Timedelta(seconds=cushion_seconds)
    start_dtype = flash_rows["StartTime"].dtype
    end_dtype = flash_rows["EndTime"].dtype
    flash_rows["StartTime"] = (flash_rows["StartTime"] - cushion).astype(start_dtype)
    flash_rows["EndTime"] = (flash_rows["EndTime"] + cushion).astype(end_dtype)
    return flash_rows[["DeviceId", "StartTime", "EndTime"]]


def _extract_stop_time_events(timeline: pd.DataFrame, event_classes) -> pd.DataFrame:
    stop_event_keys = _normalized_event_class_keys(event_classes)
    if not stop_event_keys:
        return pd.DataFrame(columns=["DeviceId", "StopStartTime", "StopEndTime"])

    event_class_key = (
        timeline["EventClass"]
        .astype(str)
        .str.replace(r"[\s_-]+", "", regex=True)
        .str.lower()
    )
    stop_events = timeline.loc[
        event_class_key.isin(stop_event_keys) &
        timeline["StartTime"].notna() &
        timeline["EndTime"].notna(),
        ["DeviceId", "StartTime", "EndTime"],
    ].copy()
    if stop_events.empty:
        return pd.DataFrame(columns=["DeviceId", "StopStartTime", "StopEndTime"])

    stop_events = stop_events[stop_events["EndTime"] >= stop_events["StartTime"]]
    if stop_events.empty:
        return pd.DataFrame(columns=["DeviceId", "StopStartTime", "StopEndTime"])

    stop_events = stop_events.rename(columns={
        "StartTime": "StopStartTime",
        "EndTime": "StopEndTime",
    })
    return stop_events[["DeviceId", "StopStartTime", "StopEndTime"]]


def _normalized_event_class_keys(event_classes) -> set[str]:
    if isinstance(event_classes, str):
        event_classes = [event_classes]
    return {
        str(event_class).replace(" ", "").replace("_", "").replace("-", "").lower()
        for event_class in event_classes
        if pd.notna(event_class)
    }


def _stop_time_overlap_mask(clearance_rows: pd.DataFrame, stop_events: pd.DataFrame) -> pd.Series:
    if clearance_rows.empty or stop_events.empty:
        return pd.Series(False, index=clearance_rows.index)

    clearance_sorted = (
        clearance_rows
        .reset_index()
        .rename(columns={"index": "_original_index"})
        .sort_values(["EndTime", "DeviceId"])
    )
    stop_sorted = (
        stop_events
        .sort_values(["StopStartTime", "DeviceId"])
        .assign(
            _MaxStopEndTime=lambda rows: rows
            .groupby("DeviceId", sort=False)["StopEndTime"]
            .cummax()
        )
    )

    prior_stop = pd.merge_asof(
        clearance_sorted[["_original_index", "DeviceId", "StartTime", "EndTime"]],
        stop_sorted[["DeviceId", "StopStartTime", "_MaxStopEndTime"]],
        left_on="EndTime",
        right_on="StopStartTime",
        by="DeviceId",
        direction="backward",
    )
    overlaps = prior_stop["_MaxStopEndTime"].notna() & (
        prior_stop["_MaxStopEndTime"] >= prior_stop["StartTime"]
    )

    return pd.Series(
        overlaps.to_numpy(),
        index=prior_stop["_original_index"],
    ).reindex(clearance_rows.index, fill_value=False)


def _drop_overlap_yellows_without_preceding_green(
    clearance_rows: pd.DataFrame,
    valid_overlap_greens: pd.DataFrame,
) -> pd.DataFrame:
    '''Require same-numbered overlap green to end exactly at yellow start.'''
    yellow_mask = clearance_rows['EventClass'] == 'Overlap Yellow'
    if not bool(yellow_mask.any()):
        return clearance_rows.copy()
    if valid_overlap_greens.empty:
        return clearance_rows.loc[~yellow_mask].copy()

    green_end_keys = (
        valid_overlap_greens[['DeviceId', 'EventValue', 'EndTime']]
        .drop_duplicates()
        .rename(columns={'EndTime': 'StartTime'})
        .assign(_HasPrecedingOverlapGreen=True)
    )
    yellow_rows = (
        clearance_rows.loc[yellow_mask, ['DeviceId', 'EventValue', 'StartTime']]
        .reset_index()
        .rename(columns={'index': '_original_index'})
        .merge(
            green_end_keys,
            on=['DeviceId', 'EventValue', 'StartTime'],
            how='left',
        )
    )
    excluded_indices = yellow_rows.loc[
        yellow_rows['_HasPrecedingOverlapGreen'].isna(),
        '_original_index',
    ]
    return clearance_rows.drop(index=excluded_indices).copy()


def _drop_rows_near_midnight(clearance_rows: pd.DataFrame) -> pd.DataFrame:
    """Remove clearance intervals intersecting the minute around midnight."""
    if clearance_rows.empty:
        return clearance_rows.copy()

    cushion = pd.Timedelta(seconds=MIDNIGHT_EXCLUSION_SECONDS)
    start_midnight = clearance_rows["StartTime"].dt.normalize()
    next_midnight = start_midnight + pd.Timedelta(days=1)
    near_start_midnight = clearance_rows["StartTime"] <= (start_midnight + cushion)
    near_next_midnight = clearance_rows["EndTime"] >= (next_midnight - cushion)

    return clearance_rows.loc[~(near_start_midnight | near_next_midnight)].copy()


def _drop_rows_overlapping_invalid_intervals(
    clearance_rows: pd.DataFrame,
    invalid_intervals: pd.DataFrame,
) -> pd.DataFrame:
    '''Remove rows with a positive-duration intersection with invalid data.'''
    if clearance_rows.empty or invalid_intervals.empty:
        return clearance_rows.copy()

    clearance_sorted = (
        clearance_rows
        .reset_index()
        .rename(columns={'index': '_original_index'})
        .sort_values(['EndTime', 'DeviceId'])
    )
    invalid_sorted = (
        invalid_intervals
        .rename(columns={
            'StartTime': 'InvalidStartTime',
            'EndTime': 'InvalidEndTime',
        })
        .sort_values(['InvalidStartTime', 'DeviceId'])
        .assign(
            _MaxInvalidEndTime=lambda rows: rows
            .groupby('DeviceId', sort=False)['InvalidEndTime']
            .cummax()
        )
    )

    prior_invalid = pd.merge_asof(
        clearance_sorted[['_original_index', 'DeviceId', 'StartTime', 'EndTime']],
        invalid_sorted[['DeviceId', 'InvalidStartTime', '_MaxInvalidEndTime']],
        left_on='EndTime',
        right_on='InvalidStartTime',
        by='DeviceId',
        direction='backward',
        allow_exact_matches=False,
    )
    overlaps_invalid = prior_invalid['_MaxInvalidEndTime'].notna() & (
        prior_invalid['_MaxInvalidEndTime'] > prior_invalid['StartTime']
    )
    excluded_indices = prior_invalid.loc[overlaps_invalid, '_original_index']
    return clearance_rows.drop(index=excluded_indices).copy()


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
