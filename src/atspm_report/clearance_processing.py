"""Clearance interval monitoring derived from ATSPM timeline rows."""

from __future__ import annotations

from typing import Optional, Union

import numpy as np
import pandas as pd

from .duckdb_utils import is_relation

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
# How close an overlap yellow must land to the yellow median plus the red median
# before it is read as a missing Overlap Red begin rather than a long yellow.
DEFAULT_OVERLAP_MISSING_RED_TOLERANCE_SECONDS = 0.2
# Share of overlap yellows that must have a matching Overlap Red before the red
# median is trusted. Many overlaps - every FYA one, and a few traditional ones -
# run yellow straight to green and emit almost no red at all, which leaves a
# "median" drawn from one or two samples. Measured over a day, overlaps above this
# mark absorb reds at 0.008% of samples against a 0.013% invalid rate on the phase
# yellows at the same signals, so what the filter removes there is the same event
# loss the phase check already sees. Below it the rate is thirty times higher and
# tracks nothing.
DEFAULT_OVERLAP_RED_COVERAGE_MIN = 0.95
INVALID_NEIGHBOR_MIN_DURATION_SECONDS = 6.0
MIDNIGHT_EXCLUSION_SECONDS = 60
# datetime64 units, coarsest to finest.
_TIME_UNITS = ["s", "ms", "us", "ns"]

CLEARANCE_ALERT_COLUMNS = [
    "DeviceId",
    "EventClass",
    "EventValue",
    "Date",
    "MedianDuration",
    "SampleCount",
    "MaxAbsDelta",
    "ShortCount",
    "LongCount",
    "AvgSignedDeviation",
    "RepresentativeShortDuration",
    "RepresentativeShortTime",
    "RepresentativeLongDuration",
    "RepresentativeLongTime",
]


def _empty_alerts() -> pd.DataFrame:
    return pd.DataFrame(columns=CLEARANCE_ALERT_COLUMNS)


def _to_pandas(data: Union[pd.DataFrame, "duckdb.DuckDBPyRelation", None]) -> Optional[pd.DataFrame]:
    if data is None:
        return None
    if isinstance(data, pd.DataFrame):
        return data
    if is_relation(data):
        return data.df()
    return None


def process_clearance_intervals(
    timeline: Union[pd.DataFrame, "duckdb.DuckDBPyRelation", None],
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

    filter_stoptime = bool(config.get("filter_stoptime", True))
    stop_classes = config.get("clearance_stop_event_classes", DEFAULT_STOP_TIME_EVENT_CLASSES)
    flash_classes = config.get("clearance_flash_event_classes", DEFAULT_FLASH_EVENT_CLASSES)
    validity_classes = config.get(
        "clearance_validity_event_classes", DEFAULT_VALIDITY_EVENT_CLASSES
    )

    if not all(pd.api.types.is_datetime64_dtype(df[col])
               for col in ("StartTime", "EndTime") if col in df.columns):
        # Text times are parsed on the whole column, which their format is read from.
        df = df.copy()
        df["StartTime"] = pd.to_datetime(df["StartTime"], errors="coerce")
        if "EndTime" in df.columns:
            df["EndTime"] = pd.to_datetime(df["EndTime"], errors="coerce")
    # Only the clearance intervals, Overlap Green, the stop-time and flash
    # intervals and the invalid indications (which mark data untrustworthy) are
    # read below. A full timeline is mostly other rows (valid greens, phase calls,
    # detector and pedestrian intervals), so they are dropped before any work is
    # done on them.
    read_classes = (
        CLEARANCE_EVENT_CLASSES + ["Overlap Green"]
        + _matching_event_classes(df["EventClass"], (stop_classes if filter_stoptime else []))
        + _matching_event_classes(df["EventClass"], flash_classes)
    )
    is_read = df["EventClass"].isin(read_classes) | (
        df["EventClass"].isin(validity_classes) & (df["IsValid"] == False)
    ).fillna(False)
    df = df[is_read].copy()
    df["DeviceId"] = df["DeviceId"].astype(str)
    df["StartTime"] = pd.to_datetime(df["StartTime"], errors="coerce")
    df["Duration"] = pd.to_numeric(df["Duration"], errors="coerce")
    if "EndTime" in df.columns:
        df["EndTime"] = pd.to_datetime(df["EndTime"], errors="coerce")
    else:
        df["EndTime"] = pd.NaT
    missing_end = df["EndTime"].isna()
    if missing_end.any():
        df["EndTime"] = _fill_times(
            df["EndTime"],
            df.loc[missing_end, "StartTime"] + pd.to_timedelta(df.loc[missing_end, "Duration"], unit="s"),
        )
    # pandas 3 keeps the resolution each column was built with (a parsed
    # timestamp is microseconds, float seconds make nanoseconds), and the
    # merge_asof calls below need start and end times of one resolution.
    if df["StartTime"].dtype != df["EndTime"].dtype:
        finest = max((df["StartTime"].dtype, df["EndTime"].dtype),
                     key=lambda dtype: _TIME_UNITS.index(np.datetime_data(dtype)[0]))
        df["StartTime"] = df["StartTime"].astype(finest)
        df["EndTime"] = df["EndTime"].astype(finest)
    df["EventValue"] = pd.to_numeric(df["EventValue"], errors="coerce").astype("Int64")
    stop_events = (
        _extract_stop_time_events(df, stop_classes)
        if filter_stoptime
        else pd.DataFrame(columns=["DeviceId", "StopStartTime", "StopEndTime"])
    )
    flash_intervals = _extract_flash_intervals(
        df,
        flash_classes,
        float(config.get("clearance_flash_cushion_seconds", DEFAULT_FLASH_CUSHION_SECONDS)),
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

    df = _drop_overlap_yellows_absorbing_red(
        df,
        float(config.get(
            "overlap_missing_red_tolerance_seconds",
            DEFAULT_OVERLAP_MISSING_RED_TOLERANCE_SECONDS,
        )),
        float(config.get(
            "overlap_red_coverage_min",
            DEFAULT_OVERLAP_RED_COVERAGE_MIN,
        )),
    )
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

    group_cols = ["DeviceId", "EventClass", "EventValue"]
    df = _movements_that_can_alert(df, group_cols, yellow_min, red_min, tolerance)
    if df.empty:
        return _empty_alerts()
    # Whether each interval overlaps a stop-time interval depends on that
    # interval alone, so it is worked out for all of them in one pass.
    if not stop_events.empty:
        df = df.assign(_StopOverlap=_stop_time_overlap_mask(
            df[['DeviceId', 'StartTime', 'EndTime']].reset_index(drop=True), stop_events
        ).to_numpy())

    alert_rows = []
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
            below_min_mask = durations < (red_min - FLOAT_EPSILON)
            below_median_mask = pd.Series(False, index=group.index)
            long_mask = pd.Series(False, index=group.index)
        else:
            short_threshold = yellow_min - tolerance
            below_min_mask = durations < (short_threshold - FLOAT_EPSILON)
            below_median_mask = (
                (durations - median_duration < -(tolerance + FLOAT_EPSILON)) & ~below_min_mask
            )
            long_mask = (
                (durations - median_duration > (tolerance + FLOAT_EPSILON)) & ~below_min_mask
            )

        deltas = durations - median_duration
        abs_deltas = deltas.abs()

        median_deviation_mask = below_median_mask | long_mask
        if bool(median_deviation_mask.any()) and not stop_events.empty:
            overlaps = group.loc[median_deviation_mask, '_StopOverlap']
            if bool(overlaps.any()):
                below_median_mask = below_median_mask.copy()
                long_mask = long_mask.copy()
                below_median_mask.loc[overlaps[overlaps].index] = False
                long_mask.loc[overlaps[overlaps].index] = False

        # An interval under the minimum and one merely under the median are both
        # just short to a reader; they are tracked apart only because stop-time
        # filtering applies to the second and not the first.
        short_mask = below_min_mask | below_median_mask
        flagged_mask = short_mask | long_mask

        short_count = int(short_mask.sum())
        long_count = int(long_mask.sum())
        if short_count == 0 and long_count == 0:
            continue

        bad_times = group.loc[flagged_mask, "StartTime"]
        alert_date = pd.to_datetime(bad_times.max()).normalize()

        short_row = _representative_row(group, abs_deltas, short_mask)
        long_row = _representative_row(group, abs_deltas, long_mask)

        alert_rows.append({
            "DeviceId": str(device_id),
            "EventClass": str(event_class),
            "EventValue": int(event_value),
            "Date": alert_date,
            "MedianDuration": median_duration,
            "SampleCount": int(len(group)),
            "MaxAbsDelta": float(abs_deltas[flagged_mask].max()),
            "ShortCount": short_count,
            "LongCount": long_count,
            "AvgSignedDeviation": float(deltas.mean()) if len(deltas) else np.nan,
            "RepresentativeShortDuration": _row_value(short_row, "Duration"),
            "RepresentativeShortTime": _row_value(short_row, "StartTime"),
            "RepresentativeLongDuration": _row_value(long_row, "Duration"),
            "RepresentativeLongTime": _row_value(long_row, "StartTime"),
        })

    if not alert_rows:
        return _empty_alerts()

    return pd.DataFrame(alert_rows).reindex(columns=CLEARANCE_ALERT_COLUMNS)


def _movements_that_can_alert(
    clearance_rows: pd.DataFrame,
    group_cols: list,
    yellow_min: float,
    red_min: float,
    tolerance: float,
) -> pd.DataFrame:
    """Keep the movements with an interval the alert loop could flag.

    A movement alerts only on an interval under the minimum or, for yellow, off
    its median by more than the tolerance. Nearly all movements have neither, so
    they are found at once and dropped rather than run through the loop. The
    bounds are a hair looser than the loop's, so a borderline movement is kept
    and the loop has the final say.
    """
    slack = 1e-6
    durations = clearance_rows["Duration"].astype(float)
    medians = durations.groupby([clearance_rows[col] for col in group_cols], sort=False).transform("median")
    classes = clearance_rows["EventClass"]
    red_classes = [value for value in classes.unique() if "Red" in str(value)]
    is_red = classes.isin(red_classes)
    candidate = (
        (is_red & (durations < red_min - FLOAT_EPSILON + slack))
        | (~is_red & (
            (durations < yellow_min - tolerance - FLOAT_EPSILON + slack)
            | ((durations - medians).abs() > tolerance + FLOAT_EPSILON - slack)
        ))
    )
    keep = candidate.groupby([clearance_rows[col] for col in group_cols], sort=False).transform("any")
    return clearance_rows[keep.astype(bool)]


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

    flash_rows = timeline.loc[
        timeline["EventClass"].isin(_matching_event_classes(timeline["EventClass"], event_classes)) &
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

    stop_events = timeline.loc[
        timeline["EventClass"].isin(_matching_event_classes(timeline["EventClass"], event_classes)) &
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


def _fill_times(times: pd.Series, fill: pd.Series) -> pd.Series:
    """times.fillna(fill) when the two are of different resolutions.

    As pandas 2 does it: the fill takes the resolution of times when it fits
    exactly, otherwise times move to the fill's finer one. pandas 3 deprecates
    filling with times of another resolution, so the cast is done here.
    """
    if fill.dtype != times.dtype:
        fitted = fill.astype(times.dtype)
        if fitted.astype(fill.dtype).equals(fill):
            fill = fitted
        else:
            times = times.astype(fill.dtype)
    return times.fillna(fill)


def _matching_event_classes(event_class: pd.Series, event_classes) -> list:
    """The EventClass values present that name one of event_classes, ignoring case,
    spaces, underscores and hyphens. Matched on the distinct values, not every row."""
    keys = _normalized_event_class_keys(event_classes)
    if not keys:
        return []
    present = pd.Series(event_class.unique(), dtype=object)
    normalized = present.astype(str).str.replace(r"[\s_-]+", "", regex=True).str.lower()
    return present[normalized.isin(keys)].tolist()


def _normalized_event_class_keys(event_classes) -> set[str]:
    if isinstance(event_classes, str):
        event_classes = [event_classes]
    return {
        str(event_class).replace(" ", "").replace("_", "").replace("-", "").lower()
        for event_class in event_classes
        if pd.notna(event_class)
    }


# The merge_asof helpers below sort the clearance rows by time alone: merge_asof
# needs no more, and each row's match depends only on that row (the by= key
# keeps it to its own signal), so the order among equal times does not matter.
def _stop_time_overlap_mask(clearance_rows: pd.DataFrame, stop_events: pd.DataFrame) -> pd.Series:
    if clearance_rows.empty or stop_events.empty:
        return pd.Series(False, index=clearance_rows.index)

    clearance_sorted = (
        clearance_rows
        .reset_index()
        .rename(columns={"index": "_original_index"})
        .sort_values("EndTime")
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


def _drop_overlap_yellows_absorbing_red(
    clearance_rows: pd.DataFrame,
    tolerance_seconds: float,
    red_coverage_min: float,
) -> pd.DataFrame:
    """Drop overlap yellows that ran as long as the yellow plus the red.

    When the Overlap Red begin event goes missing, the log reads as a single
    yellow running from where the yellow started to where the red should have
    ended, so the interval lands on the yellow median plus the red median. That
    is a hole in the record rather than a long yellow, and it accounted for the
    long-overlap alerts being reported, so the sample is thrown out instead.

    Only a sample already reading long is eligible, which keeps a signal whose
    red is shorter than the tolerance from losing its ordinary yellows. The
    overlap must also emit a red for red_coverage_min of its yellows: one that
    runs yellow to green instead has no meaningful red median, and its longer
    yellows are how it operates rather than a hole in the record.
    """
    yellows = clearance_rows["EventClass"] == "Overlap Yellow"
    reds = clearance_rows["EventClass"] == "Overlap Red"
    if not yellows.any() or not reds.any():
        return clearance_rows

    key = ["DeviceId", "EventValue"]
    yellow_stats = clearance_rows.loc[yellows].groupby(key)["Duration"].agg(
        YellowMedian="median", YellowCount="size"
    )
    red_stats = clearance_rows.loc[reds].groupby(key)["Duration"].agg(
        RedMedian="median", RedCount="size"
    )
    medians = yellow_stats.join(red_stats, how="inner").reset_index()
    medians = medians[
        medians["RedCount"] >= medians["YellowCount"] * red_coverage_min
    ]
    if medians.empty:
        return clearance_rows

    candidates = (
        clearance_rows.loc[yellows, key + ["Duration"]]
        .reset_index()
        .merge(medians, on=key, how="inner")
    )
    if candidates.empty:
        return clearance_rows

    limit = tolerance_seconds + FLOAT_EPSILON
    absorbed = (
        (candidates["Duration"] - candidates["YellowMedian"] > limit)
        & (
            (candidates["Duration"] - (candidates["YellowMedian"] + candidates["RedMedian"]))
            .abs() <= limit
        )
    )
    drop_index = candidates.loc[absorbed, "index"]
    if drop_index.empty:
        return clearance_rows
    return clearance_rows.drop(index=drop_index)


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
        clearance_rows[['DeviceId', 'StartTime', 'EndTime']]
        .reset_index()
        .rename(columns={'index': '_original_index'})
        .sort_values('EndTime')
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
        long_rows[["DeviceId", "StartTime"]]
        .reset_index()
        .rename(columns={"index": "_original_index"})
        .sort_values("StartTime")
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
