"""Main ReportGenerator class for ATSPM anomaly detection."""

from typing import Optional, Dict
import numpy as np
import pandas as pd
from reportlab.lib.units import inch

from datetime import datetime, timedelta

from .data_processing import (
    process_maxout_data,
    process_missing_data,
    missing_data_by_hour,
    region_missing_by_hour,
    detect_feed_stop,
    mark_open_periods,
    trim_findings_to_stop,
)
from .statistical_analysis import (
    cusum, alert, drain_maxout_alerts, MAXOUT_ALERT_DEFAULTS, MAXOUT_DRAIN_DEFAULTS, DEFAULT_ALERT_RECENCY_DAYS,
)
from .visualization import create_atspm_detector_health_plots, create_device_plots, create_phase_skip_plots, create_preempt_plots
from .focus_overview import (
    BASELINE_DAYS as OVERVIEW_BASELINE_DAYS,
    GREEN as OVERVIEW_GREEN,
    MAX_OUT as OVERVIEW_MAX_OUT,
    NO_WAIT as OVERVIEW_NO_WAIT,
    PANEL_TITLES as OVERVIEW_TITLES,
    WAIT as OVERVIEW_WAIT,
    create_focus_overview,
    fault_rows,
    hourly_cycles,
    hourly_terminations,
    measure as overview_measure,
    no_data_hours,
    normal_range,
    phase_words as overview_phase_words,
    signal_phases,
)
from .report_generation import generate_pdf_report
from .table_generation import ONGOING_SINCE_COLUMN
from .phase_skip_processing import process_phase_wait_data
from .clearance_processing import process_clearance_intervals
from .alarm_processing import (
    FLASH_EVENT_CLASSES,
    summarize_daily_alarms,
    update_alarm_history,
    build_alarm_alerts,
)
from .preempt_cascade import (
    CLASS_BUS_PREEMPT,
    CLASS_TSP,
    BUS_DIRECTIONS,
    PREEMPT_SECTIONS,
    PULSE_MAX_SECONDS,
    SECTION_EV,
    SECTION_TSP,
    TSP_CLASSES,
    USUAL_LENGTH_QUANTILES,
    build_bus_preempt_alerts,
    call_section,
    classify_preempt_calls,
    preempt_calls_from_timeline,
    preempt_episode_days,
    update_preempt_calls,
    evaluate_preempt_cascades,
)
from .preempt_processing import (
    PRESENCE_PREEMPT,
    summarize_daily_preempts,
    update_preempt_history,
    build_section_preempt_alerts,
    rebuild_history_classes,
)
from .overlap_dual_indication_processing import dual_indications_frame, overlap_dual_indication_sql
from .atspm_detector_health import (
    ALERT_TYPE as ATSPM_DETECTOR_HEALTH,
    FAULT_STATUSES as ATSPM_FAULT_STATUSES,
    PED as ATSPM_PED,
    VEHICLE as ATSPM_VEHICLE,
    build_atspm_detector_health_alerts,
    detector_info as atspm_detector_info,
    findings_frame as atspm_findings_frame,
    limit_to_window as atspm_limit_to_window,
    listed_alerts as atspm_listed_alerts,
    affects_operations as atspm_affects_operations,
    report_window as atspm_report_window,
)
from .phase_conflict_processing import (
    conflicts_frame,
    general_phase_conflict_sql,
    overlap_conflict_sql,
    run_conflict_queries,
    same_movement_color_conflict_sql,
    validate_timeline,
)
from .detector_triage import attach_triage, rank_detectors, silent_ped_phases, usual_detector_counts
from .signal_focus import (
    active_detectors,
    detectors_down_together,
    known_detectors,
    CHART_DETECTOR as FOCUS_CHART_DETECTOR,
    OTHER_DETECTORS_ROW as FOCUS_OTHER_DETECTORS_ROW,
    CHART_GREEN as FOCUS_CHART_GREEN,
    CHART_MAX_OUT as FOCUS_CHART_MAX_OUT,
    CHART_MISSING as FOCUS_CHART_MISSING,
    CHART_PED as FOCUS_CHART_PED,
    CHART_PREEMPT as FOCUS_CHART_PREEMPT,
    CHART_SKIPS as FOCUS_CHART_SKIPS,
    CHART_WAIT as FOCUS_CHART_WAIT,
    HARM_TIERS as FOCUS_HARM_TIERS,
    PREEMPT as FOCUS_PREEMPT,
    SKIPS as FOCUS_SKIPS,
    build_signal_focus,
    collect_problems,
    hidden_signals,
)
from .coordination_check import (
    ALERT_TYPE as COORDINATION,
    CONFIDENCE_HIGH,
    COORDINATION_HISTORY_DAYS,
    build_coordination_alerts,
    listed_alerts,
    create_coordination_plots,
    reset_base_map,
)
from . import measure_inputs
from .measure_checks import (
    DELAY_RISE, PLATOON_TREND, SPLIT_FAIL_TREND, delay_rise_alerts, trend_alerts,
    STEP_LOOKBACK_DAYS as DELAY_RISE_LOOKBACK_DAYS,
)
from . import timing_insights
from . import table_limits
from .utils import log_message
from . import duckdb_utils
from .duckdb_utils import TableLike, run_sql


def _is_empty(data: Optional[TableLike]) -> bool:
    """Check if data is None or empty, for a pandas DataFrame or DuckDB relation."""
    return duckdb_utils.is_empty(data)


def _to_pandas(data: Optional[TableLike]) -> pd.DataFrame:
    """Read a DuckDB relation into a pandas DataFrame; DataFrames pass through."""
    return duckdb_utils.to_pandas(data)


def _get_shape_str(data: Optional[TableLike]) -> str:
    """Get a shape string for logging, for a pandas DataFrame or DuckDB relation."""
    if isinstance(data, pd.DataFrame):
        return str(data.shape)
    if duckdb_utils.is_relation(data):
        return f"({duckdb_utils.row_count(data)}, {len(data.columns)})"
    return "unknown"


def _split_timeline_days(timeline: pd.DataFrame):
    """Return (all rows, rows on the latest day, latest day) for a pandas timeline.

    The latest day in the timeline is the report day. Clearance and conflict
    checks only ever look at that day; the alarm and preempt counters use every
    day present, so a caller with its own timeline store can pass several weeks
    and have the six-week counts rebuilt each run instead of accumulated.
    """
    starts = pd.to_datetime(timeline['StartTime'], errors='coerce')
    report_day = starts.max()
    if pd.isna(report_day):
        return timeline, timeline.head(0), None
    report_day = pd.Timestamp(report_day).normalize()
    return timeline, timeline[starts >= report_day], report_day


def _history_sections(history: pd.DataFrame, rail_preempts=None) -> np.ndarray:
    """The report section of each daily-count row (rows from before the classes count by their input)."""
    if history is None or history.empty:
        return np.array([], dtype=object)
    classes = history['Class'] if 'Class' in history.columns else [None] * len(history)
    return call_section(classes, history['Preempt'], rail_preempts)


def _attach_daily_counts(alerts: pd.DataFrame, history: pd.DataFrame, report_day, rail_preempts=None) -> pd.DataFrame:
    """Fill DailyCounts (the trend sparkline) for preempt alerts that lack it, from the calls of the alert's section (Class)."""
    alerts = alerts.copy()
    if 'DailyCounts' not in alerts.columns:
        alerts['DailyCounts'] = None
    missing = alerts['DailyCounts'].isna()
    if not missing.any() or history is None or history.empty:
        return alerts
    hist = history.copy()
    hist['DeviceId'] = hist['DeviceId'].astype(str)
    hist['Date'] = pd.to_datetime(hist['Date'])
    hist['Section'] = _history_sections(hist, rail_preempts)
    hist = hist[hist['Date'] <= pd.Timestamp(report_day)]
    for idx in alerts.index[missing]:
        device, preempt = str(alerts.at[idx, 'DeviceId']), int(alerts.at[idx, 'Preempt'])
        section = alerts.at[idx, 'Class'] if 'Class' in alerts.columns else None
        days = hist.loc[hist['DeviceId'] == device, 'Date'].drop_duplicates().sort_values()
        mine = hist[(hist['DeviceId'] == device) & (hist['Preempt'] == preempt)]
        if isinstance(section, str):
            mine = mine[mine['Section'] == section]
        rows = mine.groupby('Date')['Count'].sum()
        alerts.at[idx, 'DailyCounts'] = [int(rows.get(d, 0)) for d in days]
    return alerts


def _preempt_alert_classes(alerts: pd.DataFrame, rail_preempts=None) -> pd.DataFrame:
    """Preempt alerts with Class (the report section) filled in where it is missing, as in history from before the split.

    Old rows were all emergency vehicle or rail alerts: rail by their input, else emergency vehicle. So an old 'Chatter' on an
    input whose bus calls now raise a TSP alert does not hold that new alert back.
    """
    if alerts is None or alerts.empty or not {'Preempt', 'Direction'} <= set(alerts.columns):
        return alerts
    alerts = alerts.copy()
    guess = pd.Series(call_section([None] * len(alerts), alerts['Preempt'], rail_preempts), index=alerts.index)
    guess[alerts['Direction'].isin(BUS_DIRECTIONS)] = SECTION_TSP
    alerts['Class'] = alerts['Class'].where(alerts['Class'].notna(), guess) if 'Class' in alerts.columns else guess
    return alerts


# The dtype astype(str) gives: object on pandas 2, the str dtype on pandas 3.
_TEXT_DTYPE = pd.Series(['']).astype(str).dtype


def _normalize_deviceid(data: Optional[TableLike]) -> Optional[TableLike]:
    """Convert DeviceId column to string type if present, for a DataFrame or DuckDB relation."""
    if data is None:
        return None
    
    if isinstance(data, pd.DataFrame):
        # Ids already held as astype(str) would leave them are not converted:
        # copying a timeline of tens of millions of rows costs seconds and gigabytes.
        if 'DeviceId' in data.columns and not (
            data['DeviceId'].dtype == _TEXT_DTYPE
            and pd.api.types.infer_dtype(data['DeviceId'], skipna=False) == 'string'
        ):
            data = data.copy()
            data['DeviceId'] = data['DeviceId'].astype(str)
        return data
    
    if duckdb_utils.is_relation(data):
        if 'DeviceId' in data.columns:
            data = data.project('* REPLACE (CAST(DeviceId AS VARCHAR) AS DeviceId)')
        return data
    
    return data


def _report_today(report_day, has_data: Optional[TableLike]) -> pd.Timestamp:
    """Midnight after the report day: the 'today' the windows that count back from the report count from (alert flagging,
    suppression, phase skip retention), so a re-run of an old day is judged as it was on its day. In a daily run, whose
    report day is yesterday, this is today. Without a timeline, the day after the latest has_data day; without either,
    today."""
    if report_day is not None:
        return pd.Timestamp(report_day).normalize() + pd.Timedelta(days=1)
    if not _is_empty(has_data):
        latest = run_sql("SELECT MAX(TimeStamp) AS Latest FROM t", t=has_data)['Latest'].iloc[0]
        if pd.notna(latest):
            return pd.Timestamp(latest).normalize() + pd.Timedelta(days=1)
    return pd.Timestamp(datetime.now().date())


def _reported_days(has_data: Optional[TableLike]) -> Optional[pd.DataFrame]:
    """The days each signal reported data (DeviceId, Date), or None without has_data."""
    if _is_empty(has_data):
        return None
    return run_sql("SELECT DISTINCT DeviceId, CAST(TimeStamp AS DATE) AS Date FROM t", t=has_data)


def _no_calls_note(number, times, history, start) -> str:
    """What a preempt's row of the overview says when it has no calls in its days: the day of its last call before them, from the
    stored calls (times) or the daily counts (history: Preempt, Date, Count), when known."""
    before = [stamp for stamp in pd.to_datetime(pd.Series(times, dtype=object)) if pd.notna(stamp) and stamp < start]
    if history is not None and not history.empty and {'Preempt', 'Date', 'Count'} <= set(history.columns):
        days = history[(pd.to_numeric(history['Preempt'], errors='coerce') == number) & (pd.to_numeric(history['Count'], errors='coerce') > 0)]
        before += [day for day in pd.to_datetime(days['Date']) if pd.notna(day) and day < start]
    if not before:
        return 'no calls in these days'
    last = max(before)
    return f"no calls in these days (last call {last:%a} {last.month}/{last.day})"


def _window_counts(counts: TableLike, devices: list, start, end) -> pd.DataFrame:
    """The detector counts of these devices in [start, end) (DeviceId already text)."""
    if duckdb_utils.is_relation(counts):
        ids = ', '.join("'" + d.replace("'", "''") + "'" for d in devices)
        return counts.filter(
            f"DeviceId IN ({ids}) AND TimeStamp >= TIMESTAMP '{pd.Timestamp(start)}' "
            f"AND TimeStamp < TIMESTAMP '{pd.Timestamp(end)}'"
        ).project('TimeStamp, DeviceId, Detector, Total').df()
    stamps = pd.to_datetime(counts['TimeStamp'])
    in_window = counts[(stamps >= start) & (stamps < end)]
    return in_window[in_window['DeviceId'].isin(devices)]


def _window_services(pedestrian: TableLike, devices: list, start, end) -> pd.DataFrame:
    """The walks of these devices in [start, end) (TimeStamp, DeviceId, Phase, PedServices)."""
    frame = _to_pandas(pedestrian)
    stamps = pd.to_datetime(frame['TimeStamp'])
    return frame.loc[(stamps >= start) & (stamps < end) & frame['DeviceId'].isin(devices), ['TimeStamp', 'DeviceId', 'Phase', 'PedServices']]


def _hourly_actuations(counts: Optional[pd.DataFrame], start, end, fill=None) -> dict:
    """{(DeviceId, Detector): actuations per hour from start to end}; an hour without data stays empty (NaN), or takes fill:
    a button has no row for an hour nobody pressed it."""
    if counts is None or counts.empty:
        return {}
    hours = pd.date_range(start, end, freq='h', inclusive='left')
    by_hour = counts.assign(Hour=pd.to_datetime(counts['TimeStamp']).dt.floor('h')).pivot_table(
        index=['DeviceId', 'Detector'], columns='Hour', values='Total', aggfunc='sum').reindex(columns=hours)
    if fill is not None:
        by_hour = by_hour.fillna(fill)
    return {(str(device), int(detector)): values.tolist() for (device, detector), values in by_hour.iterrows()}


def _usual_by_hour(usual: Optional[pd.DataFrame], devices: list, start, end, stop=None) -> dict:
    """{(DeviceId, Detector): what the detector usually counts in each hour from start to end}, from detector_usual_counts, for the
    health sparklines. Left out for a detector with under USUAL_MIN_SAMPLES samples of a typical quarter hour; empty past the data's end."""
    if usual is None or usual.empty or not devices:
        return {}
    rows = usual[usual['DeviceId'].astype(str).isin(devices)].copy()
    if rows.empty:
        return {}
    rows['DeviceId'] = rows['DeviceId'].astype(str)
    rows['Hour'] = [getattr(clock, 'hour', 0) for clock in rows['clock']]
    if 'Samples' in rows:
        enough = rows.groupby(['DeviceId', 'Detector'])['Samples'].transform('median') >= USUAL_MIN_SAMPLES
        rows = rows[enough]
    by_hour = rows.groupby(['DeviceId', 'Detector', 'weekend', 'Hour'])['Total'].sum()
    hours = pd.date_range(start, end, freq='h', inclusive='left')
    expected = {}
    for (device, detector), one in by_hour.groupby(level=['DeviceId', 'Detector']):
        table = one.droplevel(['DeviceId', 'Detector']).to_dict()
        expected[(device, int(detector))] = [
            float('nan') if stop is not None and hour >= stop else table.get((hour.day_of_week >= 5, hour.hour), float('nan'))
            for hour in hours]
    return expected


def _signal_match_key(value) -> str:
    if pd.isna(value):
        return ''
    return str(value).strip().upper()


def _resolve_signal_exclusion_device_ids(
    signals: pd.DataFrame,
    excluded_signals,
) -> list[str]:
    '''Resolve configured signal codes/names to DeviceIds using the signals table.'''
    if signals is None or signals.empty or not excluded_signals:
        return []
    if 'DeviceId' not in signals.columns or 'Name' not in signals.columns:
        return []

    excluded_keys = {
        _signal_match_key(signal)
        for signal in excluded_signals
        if _signal_match_key(signal)
    }
    if not excluded_keys:
        return []

    resolved = []
    for _, row in signals.iterrows():
        name = str(row.get('Name', '')).strip()
        name_key = _signal_match_key(name)
        code_key = _signal_match_key(name.split('-', 1)[0])
        if name_key in excluded_keys or code_key in excluded_keys:
            resolved.append(str(row['DeviceId']))
    return sorted(set(resolved))


def _config_with_signal_exclusions(
    config: dict,
    signals: pd.DataFrame,
    signal_key: str,
    device_key: str,
) -> dict:
    '''Return a config copy with signal-code exclusions resolved to DeviceIds.'''
    resolved_device_ids = _resolve_signal_exclusion_device_ids(
        signals,
        config.get(signal_key, []),
    )
    explicit_device_ids = [
        str(device_id)
        for device_id in config.get(device_key, [])
        if pd.notna(device_id) and str(device_id).strip()
    ]
    updated = dict(config)
    updated[device_key] = sorted(set(explicit_device_ids + resolved_device_ids))
    return updated


# Alert configuration
ALERT_CONFIG = {
    'maxout': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'maxout_alerts'},
    'missing_data': {'id_cols': ['DeviceId'], 'file_suffix': 'missing_data_alerts'},
    'phase_skips': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'phase_skips_alerts'},
    'clearance_intervals': {'id_cols': ['DeviceId', 'EventClass', 'EventValue'], 'file_suffix': 'clearance_interval_alerts'},
    'overlap_dual_indications': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'overlap_dual_indication_alerts'},
    'general_phase_conflicts': {
        'id_cols': ['DeviceId', 'Movement1Number', 'Movement2Number'],
        'file_suffix': 'general_phase_conflict_alerts',
    },
    'overlap_conflicts': {
        'id_cols': [
            'DeviceId', 'Movement1Type', 'Movement1Number',
            'Movement2Type', 'Movement2Number',
        ],
        'file_suffix': 'overlap_conflict_alerts',
    },
    'same_movement_color_conflicts': {
        'id_cols': [
            'DeviceId', 'Movement1Type', 'Movement1Number',
            'Movement1Indication', 'Movement2Indication',
        ],
        'file_suffix': 'same_movement_color_conflict_alerts',
    },
    'system_outages': {'id_cols': ['Region'], 'file_suffix': 'system_outages_alerts'},
    # Direction is part of the key so a preempt that drops after an earlier
    # increase (or vice versa) is a new alert rather than a suppressed repeat.
    # Class is the report section (Emergency Vehicle, TSP, Rail); history from
    # before the split gets it filled in on load (_preempt_alert_classes).
    'preempts': {'id_cols': ['DeviceId', 'Preempt', 'Class', 'Direction'], 'file_suffix': 'preempt_alerts'},
    # Detector and ped button health from atspm-detector. Issue is 'fault' for
    # bad and suspect alike (a broken detector wobbles between the two), or the
    # information status itself. Vehicle and ped numbers overlap.
    ATSPM_DETECTOR_HEALTH: {
        'id_cols': ['DeviceId', 'DetectorType', 'Detector', 'Issue'],
        'file_suffix': 'atspm_detector_health_alerts',
    },
    # Coordination plans set wrong (Cause 'Schedule') and controller clocks off (Cause 'Clock'), see coordination_check.
    COORDINATION: {'id_cols': ['DeviceId', 'Cause'], 'file_suffix': 'coordination_alerts'},
    # Checks on atspm's per-phase measures (measure_checks), each behind its own switch, off by default.
    DELAY_RISE: {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'delay_rise_alerts'},
    SPLIT_FAIL_TREND: {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'split_failure_trend_alerts'},
    PLATOON_TREND: {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'platoon_ratio_trend_alerts'},
    # Timing Insights (timing_insights): how each phase uses its green; Phase 0 for Max Capacity (the whole signal).
    timing_insights.ALERT_TYPE: {'id_cols': timing_insights.ID_COLUMNS, 'file_suffix': 'timing_insight_alerts'},
}

# A detector's usual count needs this many samples of a typical quarter hour before a sparkline shows it.
USUAL_MIN_SAMPLES = 5

# Safety-critical signal conflicts must be reported every time they are observed.
UNSUPPRESSED_ALERT_TYPES = {'overlap_dual_indications'}


def _split_system_outages(outages: pd.DataFrame, recent: pd.DataFrame, past: pd.DataFrame) -> tuple:
    """(new, ongoing) rows of the recent outage days, one row per region and day as the alert history keeps them.

    Days in a row in a region are one outage; a day that recovered ends it, so the next one is a new outage however
    soon it comes. An outage none of whose days was reported before is new. One that was reported is ongoing while it
    is still out on the latest day of data, with OngoingSince the day it began. Once over, it is not shown again.
    """
    columns = ['Region', 'Date']
    runs = outages[columns].assign(Date=pd.to_datetime(outages['Date']).dt.normalize()).drop_duplicates().sort_values(columns)
    runs['Run'] = (runs.groupby('Region')['Date'].diff() != pd.Timedelta(days=1)).cumsum()
    runs = runs.join(runs.groupby('Run')['Date'].agg(RunStart='min', RunEnd='max'), on='Run')

    reported = pd.DataFrame(columns=columns)
    if past is not None and not past.empty:
        reported = past[columns].assign(Date=pd.to_datetime(past['Date']).dt.tz_localize(None).dt.normalize())
    seen_runs = runs.merge(reported.drop_duplicates(), on=columns)['Run'].unique()

    days = recent.assign(_date=pd.to_datetime(recent['Date']).dt.normalize()).merge(
        runs.rename(columns={'Date': '_date'}), on=['Region', '_date'], how='left').drop(columns='_date')
    latest = pd.to_datetime(days['LatestDataDay']).max()
    seen = days['Run'].isin(seen_runs)
    new = days[~seen].drop(columns=['Run', 'RunStart', 'RunEnd'])
    ongoing = days[seen & (days['RunEnd'] >= latest)]
    ongoing = ongoing.assign(**{ONGOING_SINCE_COLUMN: ongoing['RunStart']}).drop(columns=['Run', 'RunStart', 'RunEnd'])
    return new.reset_index(drop=True), ongoing.reset_index(drop=True)


class ReportGenerator:
    """
    Generate ATSPM anomaly detection reports.
    
    Accepts DataFrames as input and returns reports as BytesIO objects along with
    alert DataFrames. All inputs are optional except 'signals' (required for regional grouping).
    """
    
    # Keys of the `alerts`, `ongoing_alerts`, and `updated_past_alerts` dicts;
    # also the keys `past_alerts` is expected to carry.
    ALERT_TYPES = list(ALERT_CONFIG)

    def __init__(self, config: dict):
        """
        Initialize with configuration dict.
        
        Config keys (all optional with defaults):
            - historical_window_days (int): Days of data to analyze. Default: 21
            - alert_flagging_days (int): Max age for new alerts, counted from the report day, not
              the clock (as are alert_suppression_days and phase_skip_retention_days). Default: 7
            - suppress_repeated_alerts (bool): Enable alert suppression. Default: True
            - alert_suppression_days (int): Days to suppress repeat alerts. Default: 21
            - include_ongoing_issues (bool): Add an 'Ongoing Issues' section at the end of the
              report listing the repeat alerts suppression removed, one subsection per check in
              the checks' order. Default: False
            - maxout_cusum_threshold / maxout_zscore_threshold / maxout_percent_threshold /
              maxout_min_services: phase termination alerting thresholds. A day is flagged
              only when all four are exceeded, so raising any one reduces sensitivity.
              Defaults: 0.25, 4.0, 0.2, 30
            - maxout_drain_enabled (bool): A flagged max-out day must also still be high at the day's end: an hourly
              running total of max outs above that hour's usual share, less maxout_drain_per_hour each hour, must end
              above maxout_drain_threshold, so a spike that went back to normal is not reported. Default: True
            - maxout_drain_per_hour / maxout_drain_threshold: Defaults: 0.1, 1.0
            - alert_retention_weeks (int): Weeks to retain alert history. Default: 104
            - figures_per_device (int): Plots per device in report. Default: 3
            - ongoing_figures (int): Charts per Ongoing Issues table and report, for its first rows. Default: None
              (figures_per_device)
            - verbosity (int): 0=silent, 1=info, 2=debug. Default: 1
            - phase_skip_alert_threshold (int): Min skips to trigger alert. Default: 1
            - phase_skip_retention_days (int): Days to retain phase skip data. Default: 14
            - max_table_rows (int): Maximum rows to show in each report table. Default: 10
            - clearance_yellow_min_seconds (float): Minimum yellow clearance time. Default: 3.5
            - clearance_red_min_seconds (float): Minimum red clearance time. Default: 0.5
            - clearance_tolerance_seconds (float): Clearance timing tolerance. Default: 0.1
            - clearance_invalid_event_cushion_seconds (float): Seconds around invalid timeline events to exclude. Default: 30
            - filter_stoptime (bool): Filter deviating clearance intervals overlapping stop-time events. Default: True
            - clearance_stop_event_classes (list[str]): Timeline event classes used by filter_stoptime. Default: Stop Time Input, Preempt
            - clearance_flash_event_classes (list[str]): Flash classes whose surroundings are excluded. Default: every flash class (see alarm_processing.FLASH_EVENT_CLASSES)
            - clearance_flash_cushion_seconds (float): Seconds either side of a flash alarm to exclude. Default: 600
            - overlap_missing_red_tolerance_seconds (float): How close an overlap yellow must land to
              the yellow median plus the red median before it is discarded as a missing Overlap Red
              begin event rather than reported as long. Default: 0.2
            - overlap_dual_indications_enabled (bool): Enable overlap dual indication detection. Default: False
            - overlap_dual_indication_phases (list[int]): Same-numbered phases/overlaps to analyze. Default: []
            - general_phase_conflicts_enabled (bool): Enable standard phase conflict checks. Default: False
            - general_phase_conflict_excluded_signals (list[str]): Signal names/codes to skip for non-standard phasing. Default: []
            - general_phase_conflict_excluded_device_ids (list[str]): DeviceIds to skip for non-standard phasing. Default: []
            - overlap_conflicts_enabled (bool): Enable phase/overlap conflict checks. Default: False
            - overlap_conflict_numbers (list[int]): Overlaps 1-8 to include. Default: []
            - overlap_conflict_excluded_signals (list[str]): Signal names/codes to skip for non-standard phasing. Default: []
            - overlap_conflict_excluded_device_ids (list[str]): DeviceIds to skip for non-standard phasing. Default: []
            - same_movement_color_conflicts_enabled (bool): Enable same phase/overlap multi-color checks. Default: False
            - signal_focus_signals (int): At most this many signals in each report's Signal Focus section, which then leave
              its other tables and charts; 0 leaves the section out. Default: 3
            - signal_focus_related_only (bool): A signal's problems must belong together (share a phase, such as a
              detector fault and max outs on its phase, or a flash alarm and a conflict) and reach the score by
              themselves, unless there are 3 or more different kinds of them. False: any problems in two areas. Default: True
            - signal_focus_charts_per_signal (int): At most this many charts each, in the order of the signal's table; a row whose chart falls past the limit says No chart. Default: 10
            - table_row_limits (dict): Table -> its own row limit instead of max_table_rows. Keys: 'detector_faults',
              'lane_closure', 'timing_plan', 'ped_faults', 'ped_setup', and 'ongoing' for every Ongoing table. Default: {}
            - data_only_table_signals (int): Signals listed in each report's Detector Health 'Data Only' table (new and
              ongoing); new alerts left out are not filed in the history, so they come up on a later run. 0: no limit. Default: 10
            - phase_skip_table_signals (int): Signals listed in each report's phase skip tables, new and ongoing; new alerts
              left out are not filed. 0: no limit. Default: 5
            - alarm_table_signals (int): Signals in each report's Controller Alarms table, the most alarms over the six weeks
              first. 0: no limit. Default: 10
            - ongoing_rotation_enabled (bool): Each region's Ongoing tables show the signals waiting longest since they were
              last shown, the worse ones sooner (table_limits.rotate_ongoing): as many as the table's limit ('ongoing' in
              table_row_limits, else max_table_rows; the signal limits above where lower). The rest are still filed as told.
              The state goes in and out as ongoing_last_shown / 'updated_ongoing_last_shown'. Default: True
            - detector_triage_enabled (bool): Rank detectors and ped buttons together by urgency (service lost, then harm to
              other phases) and add the Impact column; needs atspm_detector_config, terminations, phase_wait and has_data,
              and for all its measures pedestrian, atspm_ped_inputs, detector_usual_counts and coordination_agg. It never
              changes which detectors alert. Default: False
            - coordination_clock_check_enabled (bool): Alert a signal whose clock is off against nearby signals' (its
              emergency vehicle calls line up with theirs a fixed time apart, as after a missed clock change); reads
              the stored preempt calls and needs Latitude/Longitude. Default: True
            - coordination_plan_check_enabled (bool): Alert a signal that, on one kind of day, runs another plan, runs
              free or changes plans at other hours than the coordinated signals around it; needs coordination_agg with
              Pattern and CycleLength over six weeks. Groups come from the signals table's optional 'corridor' column
              where a signal has one, else from what the signals run and Latitude/Longitude. Default: False
            - coordination_figures (int): Coordination groups shown per report in a run (new and ongoing each), each
              with its map and one chart per recent day that shows the difference; no table. Alerts of groups not
              shown are not held back as told and come up on later runs. Default: 5
            - delay_rise_check_enabled (bool): Alert a phase whose delay per vehicle rose suddenly against the same hours
              of earlier days of the same kind (measure_checks); needs approach_delay and arrival_on_green over
              measure_checks.STEP_LOOKBACK_DAYS. Default: False
            - split_failure_trend_enabled / platoon_ratio_trend_enabled (bool): Each phase's daily split failures or
              platoon ratio against its earlier days of the same kind, one bad day or several (measure_checks); needs
              split_failures or platoon_ratio over historical_window_days, and quietly does nothing before 11 weekdays
              (5 weekend days) of them. Default: False
            - measure_check_days (int): Most days those three checks judge in one run: the report day and the earlier
              days the last report did not cover (by the detector health history, as its window), so a Monday run after
              a Friday run judges Friday to Sunday. Without that history, all of them. Default: 3
            - timing_insights_enabled (bool): The Timing Insights section at the end of the report (timing_insights):
              Premature Gap-Out, Maxing Out despite Low Demand, Max Recall despite Detection, Needs More Green, Max Capacity
              and Downstream Bottleneck, judged over the last six weekdays; needs split_failures (with Green_Time) and
              terminations over those days, and uses pedestrian and coordination_agg, and actuations with the detector
              table's count zones for Downstream Bottleneck. Default: False
            - timing_insights_rows (int): Rows (each with its chart) per Timing Insights table in each report, new and
              ongoing; new alerts left out are not filed, so they come up on a later run. 0: no limit. Default: 3
            - joke_index (int): Specific joke index. Default: None (auto-cycle by date)
            - custom_logo_path (str): Path to custom logo. Default: None (use ODOT logo)
        """
        self.config = self._set_defaults(config)
    
    def _set_defaults(self, config: dict) -> dict:
        """Set default values for missing config keys."""
        defaults = {
            'historical_window_days': 21,
            'alert_flagging_days': 7,
            'suppress_repeated_alerts': True,
            'alert_suppression_days': 21,
            'include_ongoing_issues': False,
            **MAXOUT_ALERT_DEFAULTS,
            **MAXOUT_DRAIN_DEFAULTS,
            'alert_retention_weeks': 104,
            'figures_per_device': 3,
            # Charts per Ongoing Issues table and report; None: figures_per_device.
            'ongoing_figures': None,
            'verbosity': 1,
            'phase_skip_alert_threshold': 1,
            'phase_skip_retention_days': 14,
            'alert_recency_days': DEFAULT_ALERT_RECENCY_DAYS,
            # Trailing days of phase wait drawn on each chart.
            'phase_skip_new_chart_days': 1,
            'phase_skip_ongoing_chart_days': 7,
            'max_table_rows': 10,
            'clearance_yellow_min_seconds': 3.5,
            'clearance_red_min_seconds': 0.5,
            'clearance_tolerance_seconds': 0.1,
            'clearance_invalid_event_cushion_seconds': 30,
            'filter_stoptime': True,
            'clearance_stop_event_classes': ['Stop Time Input', 'Preempt'],
            'clearance_flash_event_classes': list(FLASH_EVENT_CLASSES),
            'clearance_flash_cushion_seconds': 600,
            'overlap_missing_red_tolerance_seconds': 0.2,
            'overlap_dual_indications_enabled': False,
            'overlap_dual_indication_phases': [],
            'general_phase_conflicts_enabled': False,
            'general_phase_conflict_excluded_signals': [],
            'general_phase_conflict_excluded_device_ids': [],
            'overlap_conflicts_enabled': False,
            'overlap_conflict_numbers': [],
            'overlap_conflict_excluded_signals': [],
            'overlap_conflict_excluded_device_ids': [],
            'same_movement_color_conflicts_enabled': False,
            'overlap_fixed_median_max_seconds': 6.0,
            'overlap_fixed_within_seconds': 2.0,
            'overlap_fixed_within_ratio': 0.95,
            'signal_focus_signals': 3,
            'signal_focus_related_only': True,
            'signal_focus_charts_per_signal': 10,
            'table_row_limits': {},
            # Report length (signals per table, each report): see table_limits.
            'data_only_table_signals': 10,
            'phase_skip_table_signals': 5,
            'alarm_table_signals': 10,
            'ongoing_rotation_enabled': True,
            # Ranks the health tables by urgency. Off until approved.
            'detector_triage_enabled': False,
            'joke_index': None,
            'custom_logo_path': None,
            # Preempt numbers that are railroad preempts. Rail calls get the
            # longer stuck-on limit; None holds every preempt to the rail limit.
            'preempt_rail_numbers': None,
            # Cascade review of preempt alerts needs Latitude/Longitude on the
            # signals table; without them it is skipped.
            'preempt_cascade_enabled': True,
            'preempt_cascade_max_miles': 2.0,
            'preempt_cascade_min_mph': 20.0,
            'preempt_cascade_max_mph': 45.0,
            # False Calls: an input with most of its calls at the shortest call length, not seen at nearby
            # signals. Also drops an Increase whose calls nearby signals saw (real traffic) and marks the usual
            # call lengths on the charts. Needs the cascade review.
            'preempt_false_calls_enabled': True,
            # Preempt alert charts per region (alerts and information each).
            'preempt_figures': 6,
            # Controller clocks off against nearby signals, from the stored preempt calls: approved 2026-11-16.
            'coordination_clock_check_enabled': True,
            # Coordination plans set wrong against the signal's group: needs coordination_agg with Pattern and
            # CycleLength over COORDINATION_HISTORY_DAYS and signal coordinates. Off until approved.
            'coordination_plan_check_enabled': False,
            # Plan check: also judge signals that sit between two signals of a group while running none of its plans
            # (free all day in the middle of a corridor). Off until reviewed.
            'coordination_position_check_enabled': False,
            # Coordination groups shown per report in a run (new and ongoing each), each with its map and charts; the
            # rest wait for a later run and are not held back as told until they are shown.
            'coordination_figures': 5,
            # Checks on atspm's per-phase measures. Off until approved.
            'delay_rise_check_enabled': False,
            'split_failure_trend_enabled': False,
            'platoon_ratio_trend_enabled': False,
            'measure_check_days': 3,
            # Timing Insights: off until approved.
            timing_insights.CONFIG_SWITCH: False,
            timing_insights.ROWS_CONFIG: 3,
        }
        return {**defaults, **config}
    
    def generate(
        self,
        signals: TableLike,  # REQUIRED
        terminations: Optional[TableLike] = None,
        detector_counts: Optional[TableLike] = None,
        has_data: Optional[TableLike] = None,
        pedestrian: Optional[TableLike] = None,
        phase_wait: Optional[TableLike] = None,
        coordination_agg: Optional[TableLike] = None,
        timeline: Optional[TableLike] = None,
        past_alerts: Optional[Dict[str, pd.DataFrame]] = None,
        alarm_history: Optional[pd.DataFrame] = None,
        preempt_history: Optional[pd.DataFrame] = None,
        device_days: Optional[TableLike] = None,
        preempt_calls: Optional[pd.DataFrame] = None,
        preempt_events: Optional[TableLike] = None,
        atspm_detector_findings: Optional[TableLike] = None,
        atspm_detector_minutes: Optional[pd.DataFrame] = None,
        atspm_detector_config: Optional[pd.DataFrame] = None,
        ped_press_counts: Optional[TableLike] = None,
        atspm_ped_inputs: Optional[pd.DataFrame] = None,
        detector_usual_counts: Optional[TableLike] = None,
        expected_data_end=None,
        split_failures: Optional[TableLike] = None,
        arrival_on_green: Optional[TableLike] = None,
        platoon_ratio: Optional[TableLike] = None,
        approach_delay: Optional[TableLike] = None,
        ongoing_last_shown: Optional[pd.DataFrame] = None,
        actuations: Optional[TableLike] = None,
    ) -> dict:
        """
        Generate reports from provided pandas DataFrames or DuckDB relations.
        
        Args:
            signals: REQUIRED. Signal metadata with columns: DeviceId, Name, Region.
                Can be a pandas DataFrame or DuckDB relation.
            terminations: Phase termination data with columns:
                TimeStamp, DeviceId, Phase, Total, PerformanceMeasure
            detector_counts: 15-minute detector actuation counts with columns
                TimeStamp, DeviceId, Detector, Total, covering at least the
                atspm-detector health window. Only used to draw the Detector
                Health charts.
            has_data: Data availability records with columns:
                TimeStamp, DeviceId (one row per 15-minute bin a signal reported in).
                Also keeps the preempt cascade and duration checks from reading an
                outage as missed or stuck calls, so it should span the preempt calls.
            pedestrian: Pedestrian phase data with columns:
                TimeStamp, DeviceId, Phase, PedServices, PedActuation. Read by the detector
                triage and the ped button charts (walks); it raises no alerts of its own.
            phase_wait: Phase wait data with columns:
                TimeStamp, DeviceId, Phase, AvgPhaseWait, MaxPhaseWait, TotalSkips
            coordination_agg: Coordination aggregation data with columns:
                TimeStamp, DeviceId, ActualCycleLength
                (15-minute bin aggregated data for cycle length plotting). The coordination plan check also needs
                Pattern and CycleLength over six weeks (COORDINATION_HISTORY_DAYS); the phase skip charts then
                draw only the span phase_wait covers.
            timeline: ATSPM timeline data with columns:
                DeviceId, StartTime, EndTime, Duration, IsValid, EventClass, EventValue
                Usually the report day alone. The latest day present is the report
                day and is the only day the clearance and conflict checks look at;
                the alarm and preempt counters use every day present, so passing
                up to six weeks (older days trimmed to alarm and Preempt rows is
                fine) rebuilds those counts each run with no history to persist.
            alarm_history: Accumulated daily controller alarm counts with columns:
                DeviceId, AlarmType, Date, Count, LatestAlarm. For callers that
                pass one day of timeline: six-week totals are carried across runs
                here. Days present in the timeline replace the matching history rows.
            preempt_history: Accumulated daily preempt call counts with columns:
                DeviceId, Preempt, Date, Count, ValidCount, TotalDuration, MaxDuration.
                Carried across runs like alarm_history; the preempt frequency
                baseline is built from it.
            device_days: Which days each signal reported data, columns DeviceId, Date.
                Only needed with a multi-day timeline whose older days were trimmed:
                the preempt check treats a reported day with no calls as zero, and
                without this it infers reported days from each day's timeline span.
            preempt_calls: Stored preempt calls with columns DeviceId, Preempt,
                StartTime, Duration, IsValid, carried across runs like
                preempt_history (returned as 'updated_preempt_calls'). Used to
                learn preempt cascades between neighbouring signals and to check
                call durations. Signals need Latitude/Longitude for the cascades.
                Each call gets a Class (emergency vehicle, TSP, bus call that ran as
                a preempt, rail, unclassified), which the returned record carries.
            preempt_events: Raw preempt events (DeviceId, TimeStamp, EventId,
                Parameter = the preempt number) over the days of the stored calls:
                at least 105 (preempt entry), and 112 (TSP check-in) where logged.
                They tell a bus call that ran as a full preempt (an entry) from a
                TSP request. Without them a burst of short pulses is taken as TSP.
            atspm_detector_findings: atspm-detector's findings table for vehicle
                detectors and ped buttons together, one row per finding (no row =
                no finding): DeviceId, DetectorType (Vehicle or Ped), Detector,
                Start, End, Label, Confidence, Detail. A detector or button is
                reported when its bad minutes (Confidence 0.8 or more) or, more
                slowly, its suspect minutes (0.5 to under 0.8) add up (running
                totals over atspm_detector_minutes and this window), or at once
                for a finding with no time of its own at Confidence 0.8 or more.
                Lane closures of an hour or more and timing plan changes are
                listed as information, closures every run, and so are ped set-up
                issues. Vehicle rows go in the Detector Health section, ped rows
                in the Pedestrian Button Health section. The report day (latest
                timeline day, else the last finding's day) is the alert date. An
                empty table is a run that found nothing: clean days lower the
                running totals. None is a run without a health check.
            atspm_detector_minutes: Stored daily minutes per detector (DeviceId,
                DetectorType, Detector, Date, BadMinutes, SuspectMinutes), carried
                across runs like alarm_history (returned as
                'updated_atspm_detector_minutes').
            atspm_detector_config: The detector table the health run read
                (DeviceId, Detector, phase, function, lanes). The findings table
                names no phase, so this puts 'Ph6 Advance' in the tables and groups
                the charts by phase. Without it they name the detector only.
            ped_press_counts: Presses per 15 minutes of ped inputs (TimeStamp,
                DeviceId, Detector = the input, Total) over the health window. Only
                used to draw the Pedestrian Button Health charts and their sparklines.
            expected_data_end: When the data should end (the midnight after the last day the report covers), to tell when the
                source stopped being updated. By default the end of the day most signals last reported on.
            atspm_ped_inputs: The phase each ped input calls (DeviceId, Input, Phase), from
                atspm-detector's ped health. Needed to rank ped buttons with the detectors.
            detector_usual_counts: What each detector usually counts (DeviceId, Detector,
                weekend, clock, Total), the traffic weights of the detector triage; see
                detector_triage.usual_detector_counts. Without it the triage reads them
                off detector_counts, which only reach over the health window.
            split_failures, arrival_on_green, platoon_ratio, approach_delay: atspm's
                per-phase measures in 15-minute bins, each optional; a check or chart that
                needs one is left out without it. A DuckDB relation is filtered to the
                signals and days needed before anything is read, so weeks can be passed.
                Hours whose zones had a known fault (health findings, timeline Stuck On
                and Erratic, atspm_detector_minutes) are never used.
                - split_failures: TimeStamp, DeviceId, Phase, Split_Failure (cycles
                  failed), Cycles (cycles ended in the bin), Green_Occupancy and
                  Red_Occupancy (0-1), from the stop-bar zones. The split failure trend
                  (historical_window_days) and the phase timing checks (the report day and
                  the two earlier days of its kind, up to three weeks back).
                - arrival_on_green: TimeStamp, DeviceId, Phase, Total_Actuations (vehicles
                  from the advance zones reaching the stop bar), Percent_AOG (0-1).
                - approach_delay: TimeStamp, DeviceId, Phase, Delay_Seconds (measured),
                  Queued_Delay_Seconds (estimated) and optionally Queue_Delay_Seconds (the
                  whole delay by atspm 3.1's Queue Model, queue_model=True), seconds summed
                  over the bin's vehicles. The delay is Queue_Delay_Seconds where present,
                  else (also on a row where it is NULL) Delay_Seconds + Queued_Delay_Seconds.
                  With arrival_on_green: delay per vehicle = SUM(delay) /
                  SUM(Total_Actuations), bins joined first. Told
                  in the detector triage's Impact (wait_days before its window), drawn on
                  the coordination impact charts (as many days as given, three weeks is
                  enough) and judged by the delay rise check (STEP_LOOKBACK_DAYS).
                - platoon_ratio: TimeStamp, DeviceId, Phase, Total_Actuations,
                  Green_Actuations, Green_Seconds (seconds of green in the bin); never
                  averaged over bins: SUM(Green_Actuations) / SUM(Total_Actuations) /
                  (SUM(Green_Seconds) / (bins x 900)). Supporting evidence of the delay
                  rise check, and the platoon ratio trend (historical_window_days).
            past_alerts: Dict of alert_type -> DataFrame for suppression.
                Keys: 'maxout', 'missing_data',
                      'phase_skips', 'clearance_intervals', 'overlap_dual_indications',
                      'general_phase_conflicts', 'overlap_conflicts', 'system_outages',
                      'preempts', 'atspm_detector_health', 'coordination'
            ongoing_last_shown: When each ongoing alert was last shown in an Ongoing table (AlertType, Key = its id
                columns joined by '|', LastShown), for the rotation (ongoing_rotation_enabled); returned updated as
                'updated_ongoing_last_shown'. None: nothing shown yet.
            actuations: 15-minute detector counts (TimeStamp, DeviceId, Detector, Total) over the Timing Insights'
                weekdays, for Downstream Bottleneck; only the detector table's count zones (Function 'Count') are read.

        Returns:
            dict with keys:
                - 'reports': Dict[str, BytesIO] - region name -> PDF bytes (empty if no alerts)
                - 'alerts': Dict[str, pd.DataFrame] - alert type -> alert DataFrame
                    Keys: 'maxout', 'missing_data',
                          'phase_skips', 'clearance_intervals', 'overlap_dual_indications',
                          'general_phase_conflicts', 'overlap_conflicts', 'system_outages',
                          'preempts', 'atspm_detector_health', 'coordination'
                - 'ongoing_alerts': Dict[str, pd.DataFrame] - repeat alerts suppression
                    removed, carrying an OngoingSince column; empty unless
                    include_ongoing_issues is set. With the rotation, only the ones the reports show
                - 'updated_past_alerts': Dict[str, pd.DataFrame] - for next run's suppression
                - 'updated_ongoing_last_shown': pd.DataFrame - for next run's rotation (unchanged on a run without
                    Ongoing Issues)
                - 'updated_alarm_history': pd.DataFrame - for next run's alarm totals
                - 'updated_preempt_history': pd.DataFrame - for next run's preempt baseline
                - 'updated_preempt_calls': pd.DataFrame - for next run's cascade and duration checks
                - 'updated_atspm_detector_minutes': pd.DataFrame - for next run's detector health running totals
                - 'preempt_cascade_links': pd.DataFrame - cascade patterns learned this run
                - 'overruled_preempt_alerts': pd.DataFrame - frequency alerts the cascade review dropped
                - 'alarms': pd.DataFrame - controller alarms listed this run
                - 'data_stopped_at': pd.Timestamp or None - when the data stopped for the whole network, if it did
                - 'signal_focus': Dict[str, pd.DataFrame] - report (region or 'All Regions') -> its Signal Focus rows
                - 'silent_ped_phases': pd.DataFrame - with the detector triage, ped phases with no press at all that normally
                    get many: for review, not alerts, as a button with no press is not on the ped health list
                - 'hourly_data': Dict[str, pd.DataFrame] - intermediate hourly aggregates
                    Keys: 'maxout_hourly', and with has_data 'missing_hourly' (DeviceId, TimeStamp,
                    MissingData: the share of each hour's 15-minute records missing over the last 7 days) and 'outage_hourly'
                    (the same per Region over every day), which the missing data and system outage tables and charts show
        """
        # Validate required input
        if _is_empty(signals):
            raise ValueError("'signals' is required and cannot be None or empty")
        
        # Normalize DeviceId to string in all inputs
        signals = _normalize_deviceid(signals)
        terminations = _normalize_deviceid(terminations)
        detector_counts = _normalize_deviceid(detector_counts)
        has_data = _normalize_deviceid(has_data)
        pedestrian = _normalize_deviceid(pedestrian)
        phase_wait = _normalize_deviceid(phase_wait)
        coordination_agg = _normalize_deviceid(coordination_agg)
        timeline = _normalize_deviceid(timeline)
        device_days = _normalize_deviceid(device_days)
        preempt_events = _normalize_deviceid(preempt_events)
        atspm_detector_findings = _normalize_deviceid(atspm_detector_findings)
        ped_press_counts = _normalize_deviceid(ped_press_counts)
        atspm_ped_inputs = _normalize_deviceid(atspm_ped_inputs)
        detector_usual_counts = _normalize_deviceid(detector_usual_counts)
        split_failures, arrival_on_green, platoon_ratio, approach_delay = (
            _normalize_deviceid(table) if measure_inputs.present(table) else None
            for table in (split_failures, arrival_on_green, platoon_ratio, approach_delay))

        # Convert signals to pandas (needed for downstream operations)
        signals = _to_pandas(signals)

        # Latest day = report day for the clearance and conflict checks; every day
        # feeds the alarm and preempt counters (see _split_timeline_days).
        timeline_latest, report_day = None, None
        if not _is_empty(timeline):
            timeline, timeline_latest, report_day = _split_timeline_days(_to_pandas(timeline))
        device_days = _to_pandas(device_days) if not _is_empty(device_days) else None
        today = self.report_today = _report_today(report_day, has_data)
        reset_base_map()   # a base map that failed in an earlier report is tried again
        
        verbosity = self.config['verbosity']
        log_message("Starting signal analysis...", 1, verbosity)
        
        # Initialize past alerts if not provided
        if past_alerts is None:
            past_alerts = {}
        for alert_type in ALERT_CONFIG:
            if alert_type not in past_alerts:
                past_alerts[alert_type] = pd.DataFrame()
        past_alerts = dict(past_alerts)
        past_alerts['preempts'] = _preempt_alert_classes(past_alerts['preempts'], self.config['preempt_rail_numbers'])
        # History from before ped health holds vehicle detectors only.
        past_health = past_alerts[ATSPM_DETECTOR_HEALTH]
        if not past_health.empty and 'DetectorType' not in past_health.columns:
            past_alerts[ATSPM_DETECTOR_HEALTH] = past_health.assign(DetectorType=ATSPM_VEHICLE)
        
        # Initialize result containers
        new_alerts = {}
        hourly_data = {}

        # The source itself can stop being updated: nothing after that is missing data, and a fault's end there is only the data's.
        data_stopped_at = detect_feed_stop(has_data, expected_data_end) if not _is_empty(has_data) else None
        if data_stopped_at is not None:
            log_message(f"Data stopped updating at {data_stopped_at}", 1, verbosity)
        
        # Process maxout data if provided
        if not _is_empty(terminations):
            log_message("Processing max out data...", 1, verbosity)
            maxout_daily, maxout_hourly = process_maxout_data(terminations)
            hourly_data['maxout_hourly'] = _to_pandas(maxout_hourly)
            log_message(f"Processed max out data. Shape: {_get_shape_str(maxout_daily)}", 1, verbosity)
            
            log_message("Calculating CUSUM statistics for maxout...", 1, verbosity)
            t = cusum(maxout_daily, k_value=1)
            new_alerts['maxout'] = alert(
                t, self._maxout_thresholds(), self.config['alert_recency_days']
            )
            if self.config['maxout_drain_enabled']:
                new_alerts['maxout'] = drain_maxout_alerts(
                    new_alerts['maxout'], terminations, self.config['maxout_drain_per_hour'],
                    self.config['maxout_drain_threshold'], self.config['alert_recency_days'])
        else:
            new_alerts['maxout'] = pd.DataFrame()
            hourly_data['maxout_hourly'] = pd.DataFrame()
        
        # Process missing data if provided
        if not _is_empty(has_data):
            log_message("Processing missing data...", 1, verbosity)
            missing_data = process_missing_data(has_data, data_stopped_at)
            log_message(f"Processed missing data. Shape: {_get_shape_str(missing_data)}", 1, verbosity)
            
            # Filter out dates with system-wide missing data: a region-day whose
            # signals average 30% or more missing is a system outage, reported on
            # its own and kept out of the per-signal check.
            missing_data_filtered = run_sql("""
                WITH valid_date_regions AS (
                    SELECT md.Date, s.Region
                    FROM md JOIN sig AS s ON md.DeviceId = s.DeviceId
                    GROUP BY 1, 2
                    HAVING AVG(md.MissingData) < 0.3
                ),
                valid_combos AS (
                    SELECT v.Date, s.DeviceId
                    FROM valid_date_regions AS v JOIN sig AS s ON v.Region = s.Region
                )
                SELECT md.DeviceId, md.Date, md.MissingData
                FROM md JOIN valid_combos AS v ON md.DeviceId = v.DeviceId AND md.Date = v.Date
                ORDER BY md.Date, md.DeviceId
            """, single_thread=True, md=missing_data, sig=signals)

            # Get system outages (dates/regions where avg missing data >= 0.3)
            system_outages = run_sql("""
                SELECT md.Date, s.Region, AVG(md.MissingData) AS MissingData,
                       (SELECT MAX(Date) FROM md) AS LatestDataDay
                FROM md JOIN sig AS s ON md.DeviceId = s.DeviceId
                GROUP BY 1, 2
                HAVING AVG(md.MissingData) >= 0.3
                ORDER BY 1, 2
            """, single_thread=True, md=missing_data, sig=signals)

            log_message("Calculating CUSUM statistics for missing data...", 1, verbosity)
            t_missing_data = cusum(missing_data_filtered, k_value=1)
            new_alerts['missing_data'] = alert(
                t_missing_data, recency_days=self.config['alert_recency_days']
            )
            new_alerts['system_outages'] = system_outages
            # What the report shows of them, hour by hour: where in time the data went missing. The alerts stay daily.
            hourly_data['missing_hourly'] = missing_data_by_hour(has_data, data_stopped_at)
            hourly_data['outage_hourly'] = region_missing_by_hour(has_data, signals, data_stopped_at)
        else:
            new_alerts['missing_data'] = pd.DataFrame()
            new_alerts['system_outages'] = pd.DataFrame()
        
        # Process phase wait data if provided
        if not _is_empty(phase_wait):
            log_message("Processing phase wait data...", 1, verbosity)
            
            phase_coordination = coordination_agg
            if self.config['coordination_plan_check_enabled'] and not _is_empty(coordination_agg):
                # The plan check reads weeks of coordination_agg; the cycle line under the phase waits covers their span.
                phase_coordination = run_sql("SELECT * FROM c WHERE TimeStamp >= (SELECT min(TimeStamp) FROM w)",
                                             c=coordination_agg, w=phase_wait)
            phase_skip_waits, phase_skip_alert_rows, cycle_length_data = process_phase_wait_data(
                phase_wait,
                phase_coordination
            )
            
            # Convert to pandas for downstream processing
            phase_skip_waits_pd = _to_pandas(phase_skip_waits)
            phase_skip_alert_rows_pd = _to_pandas(phase_skip_alert_rows)
            cycle_length_data_pd = _to_pandas(cycle_length_data)
            
            # Apply retention to phase skip alert rows
            if self.config['phase_skip_retention_days'] > 0 and 'Date' in phase_skip_alert_rows_pd.columns:
                cutoff_datetime = (today - pd.Timedelta(days=self.config['phase_skip_retention_days'])).to_pydatetime()
                # Coerce to pandas datetime for robust comparisons, including empty frames.
                phase_skip_dates = pd.to_datetime(phase_skip_alert_rows_pd['Date'], errors='coerce').dt.tz_localize(None)
                phase_skip_alert_rows_pd = phase_skip_alert_rows_pd[phase_skip_dates >= cutoff_datetime]
            
            # Summarize and generate alerts
            phase_skip_summary, phase_skip_alert_candidates = self._summarize_phase_skip_alerts(
                phase_skip_alert_rows_pd,
                self.config['phase_skip_alert_threshold']
            )
            new_alerts['phase_skips'] = phase_skip_alert_candidates
            
            # Store for report generation
            self.phase_skip_waits = phase_skip_waits_pd
            self.phase_skip_all_rows = phase_skip_alert_rows_pd
            self.phase_skip_summary = phase_skip_summary
            self.cycle_length_data = cycle_length_data_pd
        else:
            new_alerts['phase_skips'] = pd.DataFrame()
            self.phase_skip_waits = pd.DataFrame()
            self.phase_skip_all_rows = pd.DataFrame()
            self.phase_skip_summary = pd.DataFrame()
            self.cycle_length_data = pd.DataFrame()

        # Process clearance interval data if provided
        if not _is_empty(timeline_latest):
            log_message("Processing clearance interval data...", 1, verbosity)
            clearance_alerts = process_clearance_intervals(timeline_latest, self.config)
            new_alerts['clearance_intervals'] = clearance_alerts
            log_message(f"Processed clearance interval data. Shape: {clearance_alerts.shape}", 1, verbosity)
        else:
            new_alerts['clearance_intervals'] = pd.DataFrame()

        # Controller alarms are deliberately kept out of `new_alerts`: they are
        # never suppressed against past alerts. The section is self-clearing
        # because a pair is only listed when it alarmed again on the report day.
        log_message("Processing controller alarms...", 1, verbosity)
        daily_alarms = summarize_daily_alarms(timeline)
        updated_alarm_history = update_alarm_history(
            daily_alarms,
            alarm_history if alarm_history is not None else pd.DataFrame(),
            verbosity=verbosity,
        )
        alarm_alerts = build_alarm_alerts(updated_alarm_history, report_date=report_day)
        log_message(
            f"Processed controller alarms. Shape: {alarm_alerts.shape}",
            1,
            verbosity,
        )

        # Preempt frequency uses the same accumulator pattern as alarms, but
        # its alerts DO go through suppression (keyed on direction) so each
        # signal/preempt shows up once per change rather than daily.
        log_message("Processing preempt frequency...", 1, verbosity)
        rail_numbers = self.config['preempt_rail_numbers']
        # Every call with its timestamp, kept like the daily counts, feeds the
        # cascade review (neighbouring preempts on one route or rail line) and
        # the duration checks. Each call is classed (emergency vehicle, TSP, bus
        # call that ran as a preempt, rail) by the shape of its episode and the
        # raw preempt events, and the daily counts are kept per class.
        updated_preempt_calls = classify_preempt_calls(
            update_preempt_calls(preempt_calls_from_timeline(timeline), preempt_calls),
            events=_to_pandas(preempt_events) if not _is_empty(preempt_events) else None,
            rail_preempts=rail_numbers,
        )
        daily_preempts = summarize_daily_preempts(timeline, device_days=device_days)
        updated_preempt_history = rebuild_history_classes(
            update_preempt_history(
                daily_preempts,
                preempt_history if preempt_history is not None else pd.DataFrame(),
                verbosity=verbosity,
            ),
            updated_preempt_calls,
        )
        preempt_alerts = build_section_preempt_alerts(
            updated_preempt_history, report_date=report_day, rail_preempts=rail_numbers,
        )

        preempt_cascade_links = pd.DataFrame()
        overruled_preempt_alerts = pd.DataFrame()
        if self.config['preempt_cascade_enabled'] and report_day is not None:
            cascade = evaluate_preempt_cascades(
                preempt_alerts,
                updated_preempt_calls,
                signals,
                report_day,
                rail_preempts=self.config['preempt_rail_numbers'],
                max_miles=self.config['preempt_cascade_max_miles'],
                min_mph=self.config['preempt_cascade_min_mph'],
                max_mph=self.config['preempt_cascade_max_mph'],
                verbosity=verbosity,
                has_data=_to_pandas(has_data) if not _is_empty(has_data) else None,
                false_calls=self.config['preempt_false_calls_enabled'],
            )
            preempt_alerts = cascade['alerts']
            overruled_preempt_alerts = cascade['overruled']
            preempt_cascade_links = cascade['links']
        # TSP: bus priority calls that pulsed a preempt input, whether or not they ran as preempts.
        if report_day is not None:
            bus_alerts = build_bus_preempt_alerts(updated_preempt_calls, report_day)
            if not bus_alerts.empty:
                bus_alerts['ChartSeries'] = [[[d, int(p)]] for d, p in zip(bus_alerts['DeviceId'], bus_alerts['Preempt'])]
                preempt_alerts = pd.concat([df for df in (preempt_alerts, bus_alerts) if not df.empty], ignore_index=True)
        preempt_alerts = _preempt_alert_classes(preempt_alerts, rail_numbers)
        if not preempt_alerts.empty:
            preempt_alerts = _attach_daily_counts(preempt_alerts, updated_preempt_history, report_day, rail_numbers)
        new_alerts['preempts'] = preempt_alerts
        log_message(
            f"Processed preempt frequency. Shape: {preempt_alerts.shape}",
            1,
            verbosity,
        )

        # Coordination: clocks off against nearby signals (from the preempt calls above) and plans set wrong against
        # the signal's group (from coordination_agg history), each behind its own switch.
        new_alerts[COORDINATION] = pd.DataFrame()
        plan_check = self.config['coordination_plan_check_enabled'] and not _is_empty(coordination_agg)
        clock_check = self.config['coordination_clock_check_enabled']
        if report_day is not None and (plan_check or clock_check):
            log_message("Processing coordination plans and clocks...", 1, verbosity)
            new_alerts[COORDINATION] = build_coordination_alerts(
                signals, report_day,
                coordination=coordination_agg if plan_check else None,
                preempt_calls=updated_preempt_calls if clock_check else None,
                plan_check=plan_check, clock_check=clock_check, verbosity=verbosity,
                by_position=self.config['coordination_position_check_enabled'],
            )['alerts']

        # The atspm-detector health check judged each detector and ped button before
        # it got here; this adds its minutes to the running totals, picks the ones
        # to report and dates them to the report day.
        atspm_window = None
        updated_atspm_minutes = atspm_detector_minutes
        atspm_info = atspm_detector_info(atspm_detector_config)
        silent_phases = pd.DataFrame()
        # No measure from a detector while it is faulting: when each phase's stop-bar and advance zones had a known fault.
        measure_faults = None
        if any(table is not None for table in (split_failures, arrival_on_green, platoon_ratio, approach_delay)):
            measure_faults = measure_inputs.fault_spans(
                trim_findings_to_stop(_to_pandas(atspm_detector_findings), data_stopped_at)
                if atspm_detector_findings is not None else None,
                atspm_info, timeline, atspm_detector_minutes)
        if atspm_detector_findings is not None:
            log_message("Processing atspm-detector health...", 1, verbosity)
            atspm_detector_findings = trim_findings_to_stop(_to_pandas(atspm_detector_findings), data_stopped_at)
            atspm_alerts, updated_atspm_minutes = build_atspm_detector_health_alerts(
                _to_pandas(atspm_detector_findings), report_date=report_day,
                minutes_history=atspm_detector_minutes,
                reported=_reported_days(has_data), info=atspm_detector_config,
            )
            # The health judges several days for context; the report shows only the
            # days no earlier report showed (the report day, plus a skipped one).
            if not atspm_alerts.empty:
                atspm_window = atspm_report_window(atspm_alerts['Date'].max(),
                                                   past_alerts.get(ATSPM_DETECTOR_HEALTH))
                atspm_alerts = mark_open_periods(atspm_limit_to_window(atspm_alerts, *atspm_window), data_stopped_at)
            if self.config['detector_triage_enabled'] and not (
                    atspm_alerts.empty or _is_empty(terminations) or _is_empty(phase_wait) or _is_empty(has_data)):
                log_message("Ranking detectors and ped buttons by urgency...", 1, verbosity)
                usual = _to_pandas(detector_usual_counts) if not _is_empty(detector_usual_counts) else usual_detector_counts(detector_counts)
                triage_rows = rank_detectors(
                    atspm_findings_frame(_to_pandas(atspm_detector_findings)), atspm_info, terminations, phase_wait, has_data,
                    atspm_alerts['Date'].max(),
                    baseline_days=max(self.config['historical_window_days'] - 3, 0),
                    wait_days=max(self.config['phase_skip_retention_days'] - 3, 0),
                    pedestrian=pedestrian, ped_inputs=_to_pandas(atspm_ped_inputs), usual_counts=usual,
                    coordination=coordination_agg, approach_delay=approach_delay, arrival_on_green=arrival_on_green,
                    measure_faults=measure_faults,
                )
                atspm_alerts = attach_triage(atspm_alerts, triage_rows)
                silent_phases = silent_ped_phases(pedestrian, terminations, has_data, atspm_alerts['Date'].max())
            new_alerts[ATSPM_DETECTOR_HEALTH] = atspm_alerts
            log_message(
                f"Processed atspm-detector health. Shape: {new_alerts[ATSPM_DETECTOR_HEALTH].shape}",
                1,
                verbosity,
            )
        else:
            new_alerts[ATSPM_DETECTOR_HEALTH] = pd.DataFrame()

        # Checks on atspm's per-phase measures, each behind its own switch; none takes part in Signal Focus.
        measure_devices = signals['DeviceId'].astype(str).unique().tolist()
        for alert_type in (DELAY_RISE, SPLIT_FAIL_TREND, PLATOON_TREND, timing_insights.ALERT_TYPE):
            new_alerts[alert_type] = pd.DataFrame()
        if report_day is not None:
            # The report day and the earlier days the last report did not cover (a Monday run judges Friday to Sunday).
            check_from = atspm_report_window(report_day, past_alerts.get(ATSPM_DETECTOR_HEALTH),
                                             self.config['measure_check_days'])[0]
            check_days = (pd.Timestamp(report_day).normalize() - check_from).days + 1
            if self.config['delay_rise_check_enabled'] and approach_delay is not None and arrival_on_green is not None:
                log_message("Checking for sudden rises in delay per vehicle...", 1, verbosity)
                new_alerts[DELAY_RISE] = delay_rise_alerts(approach_delay, arrival_on_green, report_day, measure_faults,
                                                           platoon_ratio, devices=measure_devices,
                                                           coordination=coordination_agg, check_days=check_days)
            for alert_type, switch, table in ((SPLIT_FAIL_TREND, 'split_failure_trend_enabled', split_failures),
                                              (PLATOON_TREND, 'platoon_ratio_trend_enabled', platoon_ratio)):
                if self.config[switch] and table is not None:
                    log_message(f"Checking {alert_type.replace('_', ' ')}s...", 1, verbosity)
                    new_alerts[alert_type] = trend_alerts(alert_type, table, report_day, measure_faults,
                                                          history_days=self.config['historical_window_days'],
                                                          recency_days=self.config['alert_recency_days'],
                                                          devices=measure_devices, check_days=check_days)
            if self.config[timing_insights.CONFIG_SWITCH] and split_failures is not None and not _is_empty(terminations):
                log_message("Checking timing insights (green use by phase)...", 1, verbosity)
                # the measure faults with the count zones too (Downstream Bottleneck reads them)
                insight_faults = measure_inputs.fault_spans(
                    trim_findings_to_stop(_to_pandas(atspm_detector_findings), data_stopped_at)
                    if atspm_detector_findings is not None else None,
                    atspm_info, timeline, atspm_detector_minutes, zones_of=measure_inputs.WITH_COUNT_ZONES)
                new_alerts[timing_insights.ALERT_TYPE] = timing_insights.build_insight_alerts(
                    split_failures, terminations, report_day, signals=signals, pedestrian=pedestrian,
                    coordination_agg=coordination_agg, actuations=actuations, detectors=atspm_info,
                    faults=insight_faults, verbosity=verbosity)

        # The overlap dual indication and signal conflict checks read the report
        # day's clean indications, which are built once for all of them. The dual
        # indication check is opt-in because not every agency uses same-numbered
        # phase/overlap relationships.
        conflict_checks = {}
        if self.config['overlap_dual_indications_enabled']:
            conflict_checks['overlap_dual_indications'] = (
                overlap_dual_indication_sql(self.config), dual_indications_frame)
        if self.config['general_phase_conflicts_enabled']:
            general_phase_config = _config_with_signal_exclusions(
                self.config,
                signals,
                'general_phase_conflict_excluded_signals',
                'general_phase_conflict_excluded_device_ids',
            )
            conflict_checks['general_phase_conflicts'] = (
                general_phase_conflict_sql(general_phase_config), conflicts_frame)
        if self.config['overlap_conflicts_enabled']:
            overlap_conflict_config = _config_with_signal_exclusions(
                self.config,
                signals,
                'overlap_conflict_excluded_signals',
                'overlap_conflict_excluded_device_ids',
            )
            conflict_checks['overlap_conflicts'] = (
                overlap_conflict_sql(overlap_conflict_config), conflicts_frame)
        if self.config['same_movement_color_conflicts_enabled']:
            conflict_checks['same_movement_color_conflicts'] = (
                same_movement_color_conflict_sql(self.config), conflicts_frame)

        for alert_type in ('overlap_dual_indications', 'general_phase_conflicts',
                           'overlap_conflicts', 'same_movement_color_conflicts'):
            new_alerts[alert_type] = pd.DataFrame()
        if conflict_checks:
            log_message(f"Processing {', '.join(conflict_checks)}...", 1, verbosity)
            queries = {name: sql for name, (sql, _) in conflict_checks.items() if sql is not None}
            context = ' for overlap dual indications' if 'overlap_dual_indications' in queries else ''
            checked_timeline = validate_timeline(timeline_latest, context) if queries else None
            results = run_conflict_queries(checked_timeline, queries) if checked_timeline is not None else {}
            for name, (_, to_alerts) in conflict_checks.items():
                new_alerts[name] = to_alerts(results.get(name, pd.DataFrame()))
                log_message(f'Processed {name}. Shape: {new_alerts[name].shape}', 1, verbosity)

        # Filter new alerts to only recent ones (alert_flagging_days)
        log_message(f"Filtering newly generated alerts to the last {self.config['alert_flagging_days']} days...", 1, verbosity)
        # Counted from the report day, not the clock, so a re-run of an old day flags what its own run flagged.
        flagging_cutoff_date_naive = (today - pd.Timedelta(days=self.config['alert_flagging_days'])).to_pydatetime()
        
        recent_new_alerts = {}
        for alert_type, df in new_alerts.items():
            if not df.empty and 'Date' in df.columns:
                recent_new_alerts[alert_type] = df[df['Date'] >= flagging_cutoff_date_naive].copy()
            else:
                recent_new_alerts[alert_type] = df

        # Apply the recency rule to the checks that do not go through alert(),
        # which already enforces it. An issue that occurred earlier in the window
        # and has not recurred since is not something to report today.
        recency_days = self.config['alert_recency_days']
        for alert_type in ('phase_skips', 'clearance_intervals', 'general_phase_conflicts',
                           'overlap_conflicts', 'same_movement_color_conflicts',
                           'overlap_dual_indications', 'preempts', ATSPM_DETECTOR_HEALTH):
            df = recent_new_alerts.get(alert_type)
            if df is None or df.empty or 'Date' not in df.columns:
                continue
            dates = pd.to_datetime(df['Date'])
            cutoff = dates.max().normalize() - timedelta(days=recency_days)
            recent_new_alerts[alert_type] = df[dates >= cutoff].copy()

        # Apply suppression if enabled
        suppressed_alerts = {alert_type: pd.DataFrame() for alert_type in ALERT_CONFIG}
        if self.config['suppress_repeated_alerts']:
            log_message("Applying alert suppression...", 1, verbosity)
            final_alerts = {}
            for alert_type in ALERT_CONFIG:
                if alert_type in UNSUPPRESSED_ALERT_TYPES:
                    final_alerts[alert_type] = recent_new_alerts.get(alert_type, pd.DataFrame())
                elif alert_type == ATSPM_DETECTOR_HEALTH and not recent_new_alerts.get(
                        alert_type, pd.DataFrame()).empty:
                    # Lane closures are information, listed every run (the window
                    # already keeps a closure an earlier report showed out); only
                    # faults, timing plans and ped set-up issues go through suppression.
                    frame = recent_new_alerts[alert_type]
                    closures = frame[frame['Issue'] == 'lane_closure']
                    kept, suppressed_alerts[alert_type] = self._suppress_alerts(
                        frame[frame['Issue'] != 'lane_closure'],
                        past_alerts.get(alert_type, pd.DataFrame()),
                        self.config['alert_suppression_days'],
                        ALERT_CONFIG[alert_type]['id_cols'],
                        verbosity,
                        today,
                    )
                    final_alerts[alert_type] = pd.concat(
                        [f for f in (kept, closures) if not f.empty] or [closures], ignore_index=True)
                elif alert_type == 'system_outages' and not recent_new_alerts.get(alert_type, pd.DataFrame()).empty:
                    # An outage is judged as a whole, not by its region alone.
                    final_alerts[alert_type], suppressed_alerts[alert_type] = _split_system_outages(
                        new_alerts[alert_type],
                        recent_new_alerts[alert_type],
                        past_alerts.get(alert_type, pd.DataFrame()),
                    )
                elif alert_type in recent_new_alerts and not recent_new_alerts[alert_type].empty:
                    final_alerts[alert_type], suppressed_alerts[alert_type] = self._suppress_alerts(
                        recent_new_alerts[alert_type],
                        past_alerts.get(alert_type, pd.DataFrame()),
                        self.config['alert_suppression_days'],
                        ALERT_CONFIG[alert_type]['id_cols'],
                        verbosity,
                        today,
                    )
                else:
                    final_alerts[alert_type] = recent_new_alerts.get(alert_type, pd.DataFrame())
        else:
            final_alerts = recent_new_alerts
            log_message("Alert suppression skipped (disabled in config)", 1, verbosity)

        # Suppressed alerts still count as reported - they are the Ongoing list -
        # so they are tracked separately from whether the report displays them.
        # History is written from both, never from the wider working frames.
        if self.config['include_ongoing_issues']:
            ongoing_alerts = dict(suppressed_alerts)
            # A coordination alert stays on the Ongoing list only at high confidence; the others were told once.
            held = ongoing_alerts.get(COORDINATION)
            if held is not None and not held.empty and 'Confidence' in held.columns:
                ongoing_alerts[COORDINATION] = listed_alerts(held[held['Confidence'] == CONFIDENCE_HIGH], signals,
                                                             self.config['coordination_figures'])
        else:
            ongoing_alerts = {alert_type: pd.DataFrame() for alert_type in ALERT_CONFIG}

        # Coordination alerts: a few per region each run, every one with its chart. The ones left over are not in
        # final_alerts, so they are not filed as told and come up on the next run.
        if final_alerts.get(COORDINATION) is not None and not final_alerts[COORDINATION].empty:
            final_alerts[COORDINATION] = listed_alerts(final_alerts[COORDINATION], signals,
                                                       self.config['coordination_figures'])

        # Timing Insights: each table lists a few rows per report; the ones left over are not filed as told and come up later.
        insight_waiting = final_alerts.get(timing_insights.ALERT_TYPE)    # every new one, for the tables' totals
        final_alerts[timing_insights.ALERT_TYPE] = timing_insights.listed_alerts(
            final_alerts.get(timing_insights.ALERT_TYPE), signals, self.config[timing_insights.ROWS_CONFIG])

        # Signal Focus: each report's worst signals across the checks, which then leave its other tables and charts.
        signal_focus = {}
        if self.config['signal_focus_signals'] > 0:
            # Counts of the signals with vehicle faults, so 'all detectors down' can tell spare channels from dead detectors.
            focus_counts = None
            fault_frames = [frame for frame in (final_alerts.get(ATSPM_DETECTOR_HEALTH), suppressed_alerts.get(ATSPM_DETECTOR_HEALTH))
                            if frame is not None and not frame.empty and 'DetectorType' in frame]
            if fault_frames and atspm_window is not None and not _is_empty(detector_counts):
                fault_devices = sorted(pd.concat([frame.loc[frame['DetectorType'] == ATSPM_VEHICLE, 'DeviceId']
                                                  for frame in fault_frames]).astype(str).unique())
                if fault_devices:
                    focus_counts = _window_counts(detector_counts, fault_devices, *atspm_window)
            # The phases each signal with a fault ran, so a rise on every one of them reads 'all phases'.
            runs = {}
            if fault_frames and atspm_window is not None and not _is_empty(terminations):
                runs = signal_phases(terminations, sorted(pd.concat([frame['DeviceId'] for frame in fault_frames]).astype(str).unique()),
                                     *atspm_window)
            problems, problem_information = collect_problems(
                final_alerts, suppressed_alerts, alarm_alerts, atspm_info, self.config['phase_skip_retention_days'],
                detector_counts=focus_counts, signal_phases=runs)
            signal_focus = build_signal_focus(problems, signals, problem_information, self.config['signal_focus_signals'],
                                              related_only=self.config['signal_focus_related_only'])
        focus_hidden = hidden_signals(signal_focus)

        # Report length (table_limits). The Ongoing tables show each region the signals waiting longest since they were
        # last shown, worse ones sooner; every ongoing alert is still filed, as it was told before. New phase skips: only
        # the ones the tables have room for are filed (and charted), so the rest come up on a later run.
        updated_ongoing_last_shown = table_limits.normalize_state(ongoing_last_shown)
        ongoing_rotated = bool(self.config['include_ongoing_issues'] and self.config['ongoing_rotation_enabled'])
        ongoing_waiting = ongoing_alerts    # every ongoing alert, for the tables' totals ('Showing 15 of 40')
        if ongoing_rotated:
            ongoing_alerts, updated_ongoing_last_shown = table_limits.rotate_ongoing(
                ongoing_alerts, signals, ongoing_last_shown, self._ongoing_limits(),
                {alert_type: config['id_cols'] for alert_type, config in ALERT_CONFIG.items()}, datetime.now(),
                hidden=focus_hidden, retention_days=7 * self.config['alert_retention_weeks'])
        listed_phase_skips = table_limits.listed_phase_skips(
            final_alerts['phase_skips'], self.phase_skip_all_rows, signals, self.config['max_table_rows'],
            self.config['phase_skip_table_signals'], self.config['phase_skip_alert_threshold'], hidden=focus_hidden)

        # Generate visualizations
        log_message("Creating visualization plots...", 1, verbosity)
        num_figures = self.config['figures_per_device']

        phase_figures = create_device_plots(final_alerts['maxout'], signals, num_figures,
                                           hourly_data.get('maxout_hourly', pd.DataFrame()), exclude=focus_hidden)
        missing_hourly = hourly_data.get('missing_hourly')
        missing_data_figures = create_device_plots(final_alerts['missing_data'], signals, num_figures, missing_hourly, exclude=focus_hidden)

        # Ongoing issues get their own charts, drawn the same way and capped the same.
        # Ongoing Issues draw fewer charts (ongoing_figures), the first rows of each table.
        ongoing_charts = self.config['ongoing_figures'] or num_figures
        ongoing_phase_figures = create_device_plots(ongoing_alerts['maxout'], signals, ongoing_charts,
                                                   hourly_data.get('maxout_hourly', pd.DataFrame()), exclude=focus_hidden)
        ongoing_missing_data_figures = create_device_plots(ongoing_alerts['missing_data'], signals, ongoing_charts,
                                                          missing_hourly, exclude=focus_hidden)

        # Phase skip visualizations
        # A new skip is news about one day; an ongoing one is about persistence.
        phase_skip_figures = self._create_phase_skip_figures(
            listed_phase_skips, signals, num_figures,
            days_plotted=self.config['phase_skip_new_chart_days'], exclude=focus_hidden,
        )
        ongoing_phase_skip_figures = self._create_phase_skip_figures(
            ongoing_alerts['phase_skips'], signals, ongoing_charts,
            days_plotted=self.config['phase_skip_ongoing_chart_days'], exclude=focus_hidden,
        )
        
        preempt_figures = self._create_preempt_figures(
            final_alerts['preempts'], updated_preempt_calls, updated_preempt_history, signals, exclude=focus_hidden
        )
        ongoing_preempt_figures = self._create_preempt_figures(
            ongoing_alerts['preempts'], updated_preempt_calls, updated_preempt_history, signals, exclude=focus_hidden,
            num_figures=min(self.config['preempt_figures'], ongoing_charts),
        )
        # Coordination: each listed group's map and charts (coordination_check.create_coordination_plots), no table.
        coordination_figures = create_coordination_plots(final_alerts.get(COORDINATION), signals,
                                                         self.config['coordination_figures'])
        ongoing_coordination_figures = create_coordination_plots(ongoing_alerts.get(COORDINATION), signals,
                                                                 self.config['coordination_figures'])
        insight_rows = self.config[timing_insights.ROWS_CONFIG]
        insight_figures = timing_insights.create_insight_plots(final_alerts.get(timing_insights.ALERT_TYPE), signals, insight_rows)
        ongoing_insight_figures = timing_insights.create_insight_plots(ongoing_alerts.get(timing_insights.ALERT_TYPE), signals,
                                                                       min(ongoing_charts, self._ongoing_limits().get(timing_insights.ALERT_TYPE) or insight_rows))

        # Detector health (atspm-detector): one chart per signal phase for the faults, the lane
        # closures and the timing plans, from the 15-minute actuations in
        # detector_counts over the days this report covers; ped buttons one chart per
        # signal, from their presses.
        def charted(alerts, statuses, kind=ATSPM_VEHICLE):
            if alerts is None or alerts.empty or 'Status' not in alerts.columns or atspm_window is None:
                return None
            chosen = alerts[alerts['Status'].isin(statuses) & (alerts['DetectorType'] == kind)]
            return None if chosen.empty else chosen

        atspm_charted = {
            'fault': charted(final_alerts[ATSPM_DETECTOR_HEALTH], ATSPM_FAULT_STATUSES),
            'closure': charted(final_alerts[ATSPM_DETECTOR_HEALTH], ('lane_closure',)),
            'plan': charted(final_alerts[ATSPM_DETECTOR_HEALTH], ('timing_plan',)),
            'ongoing': charted(ongoing_alerts.get(ATSPM_DETECTOR_HEALTH), ATSPM_FAULT_STATUSES),
        }
        ped_charted = {
            'ped_fault': charted(final_alerts[ATSPM_DETECTOR_HEALTH], ATSPM_FAULT_STATUSES, ATSPM_PED),
            'ped_ongoing': charted(ongoing_alerts.get(ATSPM_DETECTOR_HEALTH), ATSPM_FAULT_STATUSES, ATSPM_PED),
        }
        # The counts of every charted signal, read once for all the chart sets.
        chart_counts = None
        charted_frames = [frame for frame in atspm_charted.values() if frame is not None]
        if charted_frames and not _is_empty(detector_counts):
            devices = pd.concat([frame['DeviceId'] for frame in charted_frames]).astype(str).unique().tolist()
            devices = sorted(set(devices).union(*focus_hidden.values()))
            chart_counts = _window_counts(detector_counts, devices, *atspm_window)
        atspm_detector_hourly = _hourly_actuations(chart_counts, *atspm_window) if chart_counts is not None else {}
        atspm_detector_expected = _usual_by_hour(
            _to_pandas(detector_usual_counts) if not _is_empty(detector_usual_counts) else None,
            sorted({device for device, _ in atspm_detector_hourly}), *(atspm_window or (None, None)), stop=data_stopped_at)

        # Ped charts and sparklines read the presses and walks of the signals with a flagged or set-up button.
        ped_rows = [frame for frame in (final_alerts[ATSPM_DETECTOR_HEALTH], ongoing_alerts.get(ATSPM_DETECTOR_HEALTH))
                    if frame is not None and not frame.empty]
        ped_rows = pd.concat(ped_rows, ignore_index=True) if ped_rows else pd.DataFrame()
        if not ped_rows.empty:
            ped_rows = ped_rows[ped_rows['DetectorType'] == ATSPM_PED]
        atspm_ped_hourly, ped_services = {}, None
        ped_inputs_table = _to_pandas(atspm_ped_inputs)
        # Greens by hour of the Signal Focus signals and the signals with a flagged button, from far enough back for the
        # overview's normal range: the overview's max outs and greens, and the ped charts' cycles.
        ped_devices = sorted(set(ped_rows['DeviceId'].astype(str)).union(*focus_hidden.values())) if not ped_rows.empty else []
        overview_devices = sorted(set(ped_devices).union(*focus_hidden.values()))
        term_hourly, ped_cycles = None, None
        if overview_devices and not _is_empty(terminations):
            reach_end = atspm_window[1] if atspm_window is not None else today
            reach_start = (atspm_window[0] if atspm_window is not None else reach_end - pd.Timedelta(days=1)) - pd.Timedelta(days=OVERVIEW_BASELINE_DAYS)
            term_hourly = hourly_terminations(terminations, overview_devices, reach_start, reach_end)
            if atspm_window is not None:
                ped_cycles = hourly_cycles(term_hourly, *atspm_window)
        if not ped_rows.empty and atspm_window is not None:
            if not _is_empty(ped_press_counts):
                atspm_ped_hourly = _hourly_actuations(_window_counts(ped_press_counts, ped_devices, *atspm_window), *atspm_window, fill=0.0)
            hours = len(pd.date_range(*atspm_window, freq='h', inclusive='left'))
            for key in zip(ped_rows['DeviceId'].astype(str), ped_rows['Detector'].astype(int)):
                atspm_ped_hourly.setdefault(key, [0.0] * hours)    # no row for a button nobody pressed
            if not _is_empty(pedestrian):
                ped_services = _window_services(pedestrian, ped_devices, *atspm_window)

        def operational(chosen):
            """With the urgency tiers, only the faults that affect operations are charted: the others are data only."""
            if chosen is None or 'Tier' not in chosen.columns:
                return chosen
            kept = chosen[atspm_affects_operations(chosen)]
            return None if kept.empty else kept

        def atspm_figures(key, kind, figures=None):
            chosen = operational(atspm_charted[key]) if kind == 'fault' else atspm_charted[key]
            if chosen is None or chart_counts is None:
                return []
            devices = chosen['DeviceId'].astype(str).unique().tolist()
            counts = chart_counts[chart_counts['DeviceId'].isin(devices)]
            return create_atspm_detector_health_plots(chosen, signals, figures or num_figures, counts, atspm_info,
                                                      window=atspm_window, kind=kind, exclude=focus_hidden)
        def ped_figures_for(key, figures=None):
            chosen = operational(ped_charted[key])
            if chosen is None or _is_empty(ped_press_counts):
                return []
            devices = chosen['DeviceId'].astype(str).unique().tolist()
            presses = _window_counts(ped_press_counts, devices, *atspm_window)
            # Fewer than the detectors': one chart per signal, and two panels each.
            return create_atspm_detector_health_plots(chosen, signals, figures or max(num_figures - 1, 1), presses, window=atspm_window,
                                                      kind='ped', exclude=focus_hidden, ped_services=ped_services,
                                                      ped_inputs=ped_inputs_table, ped_cycles=ped_cycles)
        atspm_detector_health_figures = {
            'fault': atspm_figures('fault', 'fault'),
            # Lane closures and timing plans are information: fewer charts.
            'closure': atspm_figures('closure', 'closure', max(num_figures - 1, 1)),
            'plan': atspm_figures('plan', 'closure', max(num_figures - 1, 1)),
            'ped_fault': ped_figures_for('ped_fault'),
        }
        ongoing_atspm_detector_health_figures = atspm_figures('ongoing', 'fault', ongoing_charts)
        ongoing_atspm_ped_figures = ped_figures_for('ped_ongoing', ongoing_charts)

        signal_focus_figures = self._signal_focus_figures(
            signal_focus, final_alerts, suppressed_alerts, signals, hourly_data, updated_preempt_calls,
            updated_preempt_history, chart_counts, atspm_info, atspm_window,
            ped_press_counts if not _is_empty(ped_press_counts) else None, ped_services, ped_inputs_table,
            term_hourly=term_hourly, ped_cycles=ped_cycles)

        log_message("Plots created successfully", 1, verbosity)
        
        # Generate PDF reports
        log_message("Generating PDF reports...", 1, verbosity)
        reports = generate_pdf_report(
            report_today=today,
            filtered_df_maxouts=final_alerts['maxout'],
            filtered_df_missing_data=final_alerts['missing_data'],
            system_outages_df=final_alerts['system_outages'],
            phase_figures=phase_figures,
            missing_data_figures=missing_data_figures,
            signals_df=signals,
            verbosity=verbosity,
            phase_skip_rows=self.phase_skip_all_rows,
            phase_skip_figures=phase_skip_figures,
            phase_skip_alerts_df=final_alerts['phase_skips'],
            phase_skip_threshold=self.config['phase_skip_alert_threshold'],
            clearance_alerts_df=final_alerts['clearance_intervals'],
            overlap_dual_indications_df=final_alerts['overlap_dual_indications'],
            general_phase_conflicts_df=final_alerts['general_phase_conflicts'],
            overlap_conflicts_df=final_alerts['overlap_conflicts'],
            same_movement_color_conflicts_df=final_alerts['same_movement_color_conflicts'],
            max_table_rows=self.config['max_table_rows'],
            joke_index=self.config['joke_index'],
            custom_logo_path=self.config['custom_logo_path'],
            alarms_df=alarm_alerts,
            preempt_alerts_df=final_alerts['preempts'],
            atspm_detector_health_df=final_alerts[ATSPM_DETECTOR_HEALTH],
            atspm_detector_hourly=atspm_detector_hourly,
            atspm_detector_expected=atspm_detector_expected,
            atspm_ped_hourly=atspm_ped_hourly,
            table_row_limits=self.config['table_row_limits'],
            atspm_detector_health_figures=atspm_detector_health_figures,
            ongoing_atspm_detector_health_figures=ongoing_atspm_detector_health_figures,
            ongoing_atspm_ped_figures=ongoing_atspm_ped_figures,
            ongoing_alerts=ongoing_alerts,
            ongoing_phase_figures=ongoing_phase_figures,
            ongoing_missing_data_figures=ongoing_missing_data_figures,
            ongoing_phase_skip_figures=ongoing_phase_skip_figures,
            preempt_figures=preempt_figures,
            ongoing_preempt_figures=ongoing_preempt_figures,
            signal_focus=signal_focus,
            signal_focus_figures=signal_focus_figures,
            data_stopped_at=data_stopped_at,
            missing_data_hourly=missing_hourly,
            system_outages_hourly=hourly_data.get('outage_hourly'),
            coordination_alerts_df=final_alerts.get(COORDINATION),
            coordination_figures=coordination_figures,
            ongoing_coordination_figures=ongoing_coordination_figures,
            measure_alerts={alert_type: final_alerts.get(alert_type) for alert_type in
                            (DELAY_RISE, SPLIT_FAIL_TREND, PLATOON_TREND)},
            insight_alerts=final_alerts.get(timing_insights.ALERT_TYPE),
            insight_waiting=insight_waiting,
            insight_figures=insight_figures,
            ongoing_insight_figures=ongoing_insight_figures,
            insight_rows=insight_rows,
            table_signal_limits={'data_only': self.config['data_only_table_signals'],
                                 'phase_skips': self.config['phase_skip_table_signals'],
                                 'alarms': self.config['alarm_table_signals']},
            ongoing_rotated=ongoing_rotated,
            ongoing_waiting=ongoing_waiting if ongoing_rotated else None,
        )

        # Update and save past alerts with retention
        log_message("Updating past alerts history...", 1, verbosity)
        updated_past_alerts = {}
        for alert_type in ALERT_CONFIG:
            # Record exactly what the report accounted for: the new alerts plus the
            # repeats suppression moved to Ongoing. Writing the wider working frame
            # would file alerts that no report ever showed, and suppression would
            # then hide them for the next three weeks - silently, and forever.
            new_reported = final_alerts.get(alert_type, pd.DataFrame())
            if alert_type == ATSPM_DETECTOR_HEALTH and new_reported is not None and not new_reported.empty:
                # Only what the tables listed: an alert cut by the row limit was never
                # seen, so it must not be suppressed as a repeat on the next run.
                new_reported = atspm_listed_alerts(new_reported, signals, self.config['max_table_rows'], hidden=focus_hidden,
                                                   limits=self.config['table_row_limits'],
                                                   data_only_signals=self.config['data_only_table_signals'])
            if alert_type == 'phase_skips':
                new_reported = listed_phase_skips    # the same: only the signals the tables had room for
            reported = [
                frame for frame in (
                    new_reported,
                    suppressed_alerts.get(alert_type, pd.DataFrame()),
                ) if frame is not None and not frame.empty
            ]
            reported_alerts = (
                pd.concat(reported, ignore_index=True) if reported else pd.DataFrame()
            )
            if alert_type == timing_insights.ALERT_TYPE:
                # charts are drawn from each run's own rows, never from history: keep the history small
                reported_alerts = reported_alerts.drop(columns=['ChartData'], errors='ignore')
            updated_past_alerts[alert_type] = self._update_alert_history(
                reported_alerts,
                past_alerts.get(alert_type, pd.DataFrame()),
                alert_type,
                self.config['alert_retention_weeks'],
                verbosity
            )
        
        log_message("Report generation complete.", 1, verbosity)
        
        return {
            'reports': reports,
            'alerts': final_alerts,
            'ongoing_alerts': ongoing_alerts,
            'updated_past_alerts': updated_past_alerts,
            'updated_ongoing_last_shown': updated_ongoing_last_shown,
            'updated_alarm_history': updated_alarm_history,
            'updated_preempt_history': updated_preempt_history,
            'updated_preempt_calls': updated_preempt_calls,
            'updated_atspm_detector_minutes': updated_atspm_minutes,
            'preempt_cascade_links': preempt_cascade_links,
            'overruled_preempt_alerts': overruled_preempt_alerts,
            'alarms': alarm_alerts,
            'signal_focus': signal_focus,
            'data_stopped_at': data_stopped_at,
            'silent_ped_phases': silent_phases,
            'hourly_data': hourly_data
        }
    
    def _summarize_phase_skip_alerts(self, alert_rows_all: pd.DataFrame, threshold: int) -> tuple:
        """Aggregate alert rows by device/phase and flag those exceeding the skip threshold."""
        PHASE_SKIP_SUMMARY_COLUMNS = ['DeviceId', 'Phase', 'AggregatedSkips', 'LatestDate']
        PHASE_SKIP_ALERT_CANDIDATE_COLUMNS = ['DeviceId', 'Phase', 'Date', 'AggregatedSkips']
        
        if alert_rows_all is None or alert_rows_all.empty:
            return (
                pd.DataFrame(columns=PHASE_SKIP_SUMMARY_COLUMNS),
                pd.DataFrame(columns=PHASE_SKIP_ALERT_CANDIDATE_COLUMNS)
            )

        grouped = (
            alert_rows_all.groupby(['DeviceId', 'Phase'], as_index=False)
            .agg(
                AggregatedSkips=('TotalSkips', 'sum'),
                LatestDate=('Date', 'max')
            )
        )
        grouped['LatestDate'] = pd.to_datetime(grouped['LatestDate']).dt.normalize()

        alerts = grouped[grouped['AggregatedSkips'] > threshold].copy()
        alerts = alerts.rename(columns={'LatestDate': 'Date'})

        return grouped, alerts.reindex(columns=PHASE_SKIP_ALERT_CANDIDATE_COLUMNS)

    def _ongoing_limits(self) -> dict:
        """Signals each region's Ongoing tables show (table_limits.rotate_ongoing): the 'ongoing' limit, or a table's own
        where lower; 0 is no limit. Coordination lists its own (coordination_check.listed_alerts) and the system outages
        are by region, so neither rotates."""
        rows, default = self.config['table_row_limits'], self.config['max_table_rows']

        def lowest(*limits):
            positive = [limit for limit in limits if limit and limit > 0]
            return min(positive) if positive else 0

        ongoing = rows.get('ongoing', default)
        faults = rows.get('detector_faults', default)
        limits = {alert_type: ongoing for alert_type in table_limits.SEVERITY}
        limits['phase_skips'] = lowest(ongoing, self.config['phase_skip_table_signals'])
        limits[ATSPM_DETECTOR_HEALTH] = lowest(ongoing, faults)
        limits[(ATSPM_DETECTOR_HEALTH, table_limits.DATA_ONLY_TABLE)] = lowest(ongoing, faults, self.config['data_only_table_signals'])
        limits[(ATSPM_DETECTOR_HEALTH, ATSPM_PED)] = lowest(ongoing, rows.get('ped_faults', default))
        limits[timing_insights.ALERT_TYPE] = lowest(ongoing, self.config[timing_insights.ROWS_CONFIG])
        return limits

    def _maxout_thresholds(self) -> dict:
        """Phase termination thresholds taken from config, defaults where unset."""
        return {
            key: self.config[key]
            for key in MAXOUT_ALERT_DEFAULTS
            if self.config.get(key) is not None
        }

    def _signal_focus_figures(self, focus, new_alerts, held_back, signals, hourly_data, preempt_calls, preempt_history,
                              detector_counts, detector_table, window, ped_presses, ped_services=None, ped_inputs=None,
                              term_hourly=None, ped_cycles=None) -> dict:
        """{signal: (figure, width, height, chart kinds) charts} for the signals of every report's Signal Focus, in the order of its table.

        First one overview (focus_overview): the faults of its detectors and buttons over the max outs, waits and greens of the
        phases its table names, against their normal range, with its skips, preempt calls and hours without data, all on the days
        its detector charts cover. Then the detector charts, the phases hurt first (none when every detector was down at once: the
        overview shows that), the preempt section's charts of its preempt alerts, and its ped buttons' chart. Conflicts,
        clearance and alarms have no chart.
        """
        per_signal = self.config['signal_focus_charts_per_signal']
        wanted = {}
        for rows in focus.values():
            for _, row in rows.iterrows():
                wanted[row['DeviceId']] = row
        if window is None:
            end = getattr(self, 'report_today', None) or pd.Timestamp(datetime.now().date())
            window = (end - pd.Timedelta(days=1), end)
        start, end = pd.Timestamp(window[0]), pd.Timestamp(window[1])
        waits = self.phase_skip_waits if self.phase_skip_waits is not None else pd.DataFrame()

        def rows_of(key, device):
            frames = [source.get(key) for source in (new_alerts, held_back)]
            frames = [frame[frame['DeviceId'].astype(str) == device] for frame in frames if frame is not None and not frame.empty]
            frames = [frame for frame in frames if not frame.empty]
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        def unique(created):
            return list(dict.fromkeys(figure for figure, _ in created))

        def of_device(frame, device):
            return frame[frame['DeviceId'].astype(str) == device] if frame is not None and not frame.empty else pd.DataFrame()

        figures = {}
        for device, row in wanted.items():
            one = signals[signals['DeviceId'] == device]
            health = rows_of(ATSPM_DETECTOR_HEALTH, device)
            types = {told['type'] for told in row['Rows']}
            phases = row['Charts']
            charts = []

            def add(created, kind, width=6.5 * inch, height=2.8 * inch):
                charts.extend((figure, width, height, kind) for figure in created)

            def detector_charts(faults):
                if faults.empty or detector_counts is None:
                    return []
                counts = detector_counts[detector_counts['DeviceId'] == device]
                return unique(create_atspm_detector_health_plots(faults, one, faults['Detector'].nunique(), counts, detector_table, window=window, max_drawn=12))

            faults = health[(health['DetectorType'] == ATSPM_VEHICLE) & health['Status'].isin(ATSPM_FAULT_STATUSES)] if not health.empty else health
            buttons = health[(health['DetectorType'] == ATSPM_PED) & health['Status'].isin(ATSPM_FAULT_STATUSES)] if not health.empty else health
            harm_faults, other_faults = faults, faults.iloc[0:0]
            if not faults.empty:
                placed = faults.drop(columns=['Phase'], errors='ignore').merge(
                    detector_table[['DeviceId', 'Detector', 'Phase']].assign(DeviceId=detector_table['DeviceId'].astype(str)), on=['DeviceId', 'Detector'], how='left')
                placed['Phase'] = placed['Phase'].fillna(-1)
                hurting = placed['Tier'].isin(FOCUS_HARM_TIERS) if 'Tier' in placed else pd.Series(False, index=placed.index)
                first = placed['Phase'].isin(placed.loc[hurting, 'Phase']).to_numpy()
                harm_faults, other_faults = faults[first], faults[~first]
            # Every detector down at once: one row of the overview for them all, and no chart per phase that all say the same.
            device_counts = detector_counts[detector_counts['DeviceId'] == device] if detector_counts is not None else None
            known = active_detectors(known_detectors(detector_table, device), device_counts)
            together = detectors_down_together(faults, known, device_counts) if not faults.empty else []

            # The overview.
            strip = []
            if not (faults.empty and buttons.empty):
                strip = fault_rows(pd.concat([harm_faults, other_faults, buttons], ignore_index=True), start, end, together, len(known))
            covered = {FOCUS_CHART_DETECTOR} if strip else set()
            mine = of_device(term_hourly, device)
            stamps = pd.to_datetime(mine['TimeStamp']) if not mine.empty else None
            in_window = mine[(stamps >= start) & (stamps < end)] if not mine.empty else mine
            all_phases = sorted(in_window.loc[in_window['Total'] > 0, 'Phase'].astype(int).unique()) if not in_window.empty else []
            panels = []
            for kind, chart in ((OVERVIEW_MAX_OUT, FOCUS_CHART_MAX_OUT), (OVERVIEW_WAIT, FOCUS_CHART_WAIT), (OVERVIEW_GREEN, FOCUS_CHART_GREEN)):
                wanted_phases = phases.get(chart)
                if not wanted_phases:
                    continue
                data = of_device(waits, device) if kind == OVERVIEW_WAIT else mine
                series = overview_measure(kind, data, wanted_phases)
                if series.empty:
                    continue
                # Wait is measured from a call to its green: with no calls logged (every detector down) there is none to show.
                panels.append({'kind': kind, 'series': series, 'band': normal_range(series, start, end),
                               'title': f"{OVERVIEW_TITLES[kind]} ({overview_phase_words(wanted_phases, all_phases)})",
                               'blank': OVERVIEW_NO_WAIT if kind == OVERVIEW_WAIT else None})
                covered.add(chart)
            ticks = []
            if FOCUS_SKIPS in types:
                skipped = sorted(rows_of('phase_skips', device)['Phase'].dropna().astype(int).unique())
                bins = of_device(waits, device)
                bins = bins[bins['TotalSkips'] > 0] if not bins.empty else bins
                ticks.append({'title': 'Skips', 'empty': 'none in these days',
                              'rows': [(f"Ph{phase}", bins.loc[bins['Phase'] == phase, 'TimeStamp'].tolist() if not bins.empty else [])
                                       for phase in skipped]})
                covered.add(FOCUS_CHART_SKIPS)
            if FOCUS_PREEMPT in types:
                alerted_rows = rows_of('preempts', device)
                alerted = sorted(alerted_rows['Preempt'].dropna().astype(int).unique())
                tsp_alerted = set(alerted_rows.loc[alerted_rows.get('Class', pd.Series(dtype=object)) == SECTION_TSP, 'Preempt']
                                  .dropna().astype(int)) if 'Class' in alerted_rows.columns else set()
                calls = of_device(preempt_calls, device)
                days = of_device(preempt_history, device)
                rail = {int(p) for p in (self.config['preempt_rail_numbers'] or [])}
                rows = []
                for number in alerted:
                    mine = calls[calls['Preempt'].astype(int) == number] if not calls.empty else calls
                    classes = mine['Class'] if 'Class' in mine.columns else pd.Series(None, index=mine.index, dtype=object)
                    if number in rail:
                        times = mine['StartTime'].tolist() if not mine.empty else []
                        rows.append((f"Rail P{number}", times, _no_calls_note(number, times, days, start)))
                        continue
                    # Emergency vehicle calls and bus calls on their own rows, so the bus pulses are not read as vehicles.
                    ev = mine[~classes.isin(TSP_CLASSES)] if not mine.empty else mine
                    times = ev['StartTime'].tolist() if not ev.empty else []
                    in_window = (mine['StartTime'] >= start) & (mine['StartTime'] < end) if not mine.empty else None
                    ev_alerted = number in set(alerted_rows.loc[alerted_rows.get('Class') != SECTION_TSP, 'Preempt']
                                               .dropna().astype(int)) if 'Class' in alerted_rows.columns else True
                    if ev_alerted or (not ev.empty and bool(in_window[~classes.isin(TSP_CLASSES)].any())):
                        # The last call before these days counts emergency vehicle calls only, not bus pulses.
                        ev_days = days
                        if not days.empty and 'Class' in days.columns:
                            ev_days = days[~days['Class'].isin(TSP_CLASSES)]
                        rows.append((f"P{number} emergency", times, _no_calls_note(number, times, ev_days, start)))
                    for cls, label in ((CLASS_BUS_PREEMPT, 'bus, ran preempt'), (CLASS_TSP, 'bus, no preempt')):
                        bus = mine[classes == cls] if not mine.empty else mine
                        if number in tsp_alerted or (not bus.empty and bool(in_window[classes == cls].any())):
                            rows.append((f"P{number} {label}", bus['StartTime'].tolist() if not bus.empty else [],
                                         'none in these days'))
                ticks.append({'title': 'Preempt Calls', 'empty': 'no calls in these days', 'rows': rows})
                covered.add(FOCUS_CHART_PREEMPT)
            gaps = no_data_hours(hourly_data.get('missing_hourly'), device, start, end)
            if gaps:
                covered.add(FOCUS_CHART_MISSING)
            overview = create_focus_overview(row['Signal'], (start, end), strip, panels, ticks, gaps)
            if overview is not None:
                add([overview], frozenset(covered), 7.5 * inch, None)

            if together:
                harm_faults = other_faults = faults.iloc[0:0]
            add(detector_charts(harm_faults), FOCUS_CHART_DETECTOR, 7.5 * inch, None)
            if FOCUS_PREEMPT in types:
                # The preempt section's own charts (calls a day over the call lengths), which the signal has left.
                drawn = self._create_preempt_figures(rows_of('preempts', device), preempt_calls, preempt_history, one)
                add(unique(created for section in PREEMPT_SECTIONS for created in drawn.get(section, [])), FOCUS_CHART_PREEMPT,
                    6.5 * inch, None)
            if not buttons.empty and ped_presses is not None:
                presses = _window_counts(ped_presses, [device], *window)
                add(unique(create_atspm_detector_health_plots(buttons, one, 1, presses, window=window, kind='ped', max_drawn=12,
                                                              ped_services=ped_services, ped_inputs=ped_inputs,
                                                              ped_cycles=ped_cycles))[:1], FOCUS_CHART_PED, 7.5 * inch, None)
            other_charts = detector_charts(other_faults)
            add(other_charts, FOCUS_CHART_DETECTOR, 7.5 * inch, None)
            figures[device] = charts[:per_signal]
            # Both detector rows share a kind of chart, so past the limit the second has to say so itself.
            kept = {id(chart[0]) for chart in figures[device]}
            if other_charts and not any(id(figure) in kept for figure in other_charts) and FOCUS_CHART_DETECTOR not in covered:
                for told in row['Rows']:
                    if told['type'] == FOCUS_OTHER_DETECTORS_ROW:
                        told['no_chart'] = True
        return figures

    def _create_preempt_figures(self, alerts_df, calls, history, signals, exclude=None, num_figures=None) -> dict:
        """{section: (figure, region) charts} for preempt alerts: stacked daily-count and call-duration charts, each section
        drawing only its own calls. The TSP charts count bus calls (bursts of pulses), split into those that ran as
        preempts and normal TSP requests, over the length of each pulse."""
        if alerts_df is None or alerts_df.empty:
            return {}
        rail = self.config['preempt_rail_numbers']
        alerts_df = _preempt_alert_classes(alerts_df, rail)
        calls = calls if calls is not None else pd.DataFrame()
        hist = history if history is not None else pd.DataFrame()
        call_sections = _history_sections(calls, rail)
        hist_sections = _history_sections(hist, rail)
        presence = hist[hist['Preempt'] == PRESENCE_PREEMPT] if not hist.empty else hist
        figures = {}
        for section in PREEMPT_SECTIONS:
            alerts = alerts_df[alerts_df['Class'] == section]
            if alerts.empty:
                continue
            section_calls = calls[call_sections == section] if not calls.empty else calls
            extra = {}
            if section == SECTION_TSP:
                days = preempt_episode_days(section_calls)
                section_history = pd.concat([df for df in (presence, days) if not df.empty]) if not days.empty else presence
                extra = dict(class_labels={CLASS_BUS_PREEMPT: 'ran as preempt', CLASS_TSP: 'no preempt'},
                             chatter_seconds=PULSE_MAX_SECONDS, stuck_lines=False, count_label='Bus calls/day')
            elif not hist.empty:
                section_history = pd.concat([presence, hist[(hist_sections == section) & (hist['Preempt'] != PRESENCE_PREEMPT)]])
            else:
                section_history = hist
            if section == SECTION_EV and self.config['preempt_false_calls_enabled'] and not section_calls.empty:
                lengths = pd.to_numeric(section_calls.loc[section_calls['IsValid'].fillna(False).astype(bool), 'Duration'], errors='coerce')
                if lengths.notna().any():
                    extra['usual_lengths'] = tuple(float(lengths.quantile(q)) for q in USUAL_LENGTH_QUANTILES)
            figures[section] = create_preempt_plots(
                alerts,
                section_calls,
                section_history,
                signals,
                num_figures=num_figures or self.config['preempt_figures'],
                rail_preempts=rail,
                exclude=exclude,
                **extra,
            )
        return figures

    def _create_phase_skip_figures(self, alerts_df: pd.DataFrame, signals: pd.DataFrame,
                                   num_figures: int, days_plotted: int = None, exclude: dict = None) -> list:
        """Build phase skip charts for one set of alerts, ranking devices by total skips."""
        if alerts_df is None or alerts_df.empty or self.phase_skip_summary.empty:
            return []

        alert_pairs = alerts_df[['DeviceId', 'Phase']].drop_duplicates()
        if alert_pairs.empty or self.phase_skip_waits.empty:
            return []

        ranking_source = self.phase_skip_summary.merge(alert_pairs, on=['DeviceId', 'Phase'], how='inner')
        rankings = pd.DataFrame()
        if not ranking_source.empty:
            rankings = (
                ranking_source.groupby('DeviceId', as_index=False)['AggregatedSkips']
                .sum()
                .rename(columns={'AggregatedSkips': 'TotalSkips'})
            )

        # Only the alerting signals are drawn, so only their rows are annotated.
        alert_devices = alert_pairs['DeviceId'].unique()
        alert_device_waits = self.phase_skip_waits[self.phase_skip_waits['DeviceId'].isin(alert_devices)]
        plot_phase_skip_waits = alert_device_waits.merge(
            alert_pairs.assign(AlertPhase=True),
            on=['DeviceId', 'Phase'],
            how='left'
        )
        plot_phase_skip_waits['AlertPhase'] = plot_phase_skip_waits['AlertPhase'].fillna(False).astype(bool)

        return create_phase_skip_plots(
            plot_phase_skip_waits,
            signals,
            rankings,
            num_figures,
            self.cycle_length_data,
            days_plotted=days_plotted,
            exclude=exclude,
        )

    def _suppress_alerts(self, new_alerts_df: pd.DataFrame, past_alerts_df: pd.DataFrame,
                         suppression_days: int, id_cols: list, verbosity: int, today=None) -> tuple:
        """Filters new alerts based on recent past alerts.

        Returns (surviving alerts, suppressed alerts). The suppressed frame carries
        an OngoingSince column so the report can say how long each repeat has been
        running; it is empty when nothing was suppressed.
        """
        if past_alerts_df.empty:
            return new_alerts_df, new_alerts_df.head(0)

        # The suppression_days days before today (_report_today: the day after the report day), as a daily run reads them.
        today = pd.Timestamp(datetime.now().date()) if today is None else pd.Timestamp(today).normalize()
        past_dates_naive = pd.to_datetime(past_alerts_df['Date']).dt.tz_localize(None).dt.normalize()
        recent_past_alerts = past_alerts_df[past_dates_naive > today - pd.Timedelta(days=suppression_days)]

        if recent_past_alerts.empty:
            return new_alerts_df, new_alerts_df.head(0)

        # Get unique keys from recent alerts
        suppression_keys = recent_past_alerts[id_cols].drop_duplicates()
        log_message(f"Found {len(suppression_keys)} unique items for suppression based on the last {suppression_days} days.", 2, verbosity)

        # Perform suppression using merge
        merged = new_alerts_df.merge(suppression_keys, on=id_cols, how='left', indicator=True)
        surviving_alerts_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
        suppressed_alerts_df = merged[merged['_merge'] == 'both'].drop(columns=['_merge'])

        num_suppressed = len(new_alerts_df) - len(surviving_alerts_df)
        log_message(f"Suppressed {num_suppressed} new alerts.", 1, verbosity)

        if not suppressed_alerts_df.empty:
            suppressed_alerts_df = suppressed_alerts_df.merge(
                self._ongoing_since(past_alerts_df, id_cols, suppression_days),
                on=id_cols,
                how='left',
            )

        return surviving_alerts_df, suppressed_alerts_df

    def _ongoing_since(self, past_alerts_df: pd.DataFrame, id_cols: list,
                       suppression_days: int) -> pd.DataFrame:
        """Return the start date of each key's current run of repeat alerts.

        An issue counts as one continuous run for as long as it keeps reappearing
        inside the suppression window; a longer quiet gap means the problem went
        away and later came back, so the run restarts there rather than reaching
        all the way back through the retained history.

        History holds one row per alert day, so the date is the day the issue was
        genuinely first reported.
        """
        history = past_alerts_df[id_cols + ['Date']].copy()
        history['Date'] = pd.to_datetime(history['Date']).dt.tz_localize(None).dt.normalize()
        history = history.dropna(subset=['Date']).drop_duplicates()
        if history.empty:
            return pd.DataFrame(columns=id_cols + [ONGOING_SINCE_COLUMN])

        history = history.sort_values(id_cols + ['Date'])
        gaps = history.groupby(id_cols, dropna=False)['Date'].diff()
        run_starts = gaps.isna() | (gaps > pd.Timedelta(days=suppression_days))
        history['RunId'] = run_starts.groupby([history[col] for col in id_cols], dropna=False).cumsum()

        latest_run = history.groupby(id_cols, dropna=False)['RunId'].transform('max')
        current_run = history[history['RunId'] == latest_run]

        return (
            current_run.groupby(id_cols, dropna=False, as_index=False)['Date']
            .min()
            .rename(columns={'Date': ONGOING_SINCE_COLUMN})
        )

    def _update_alert_history(self, new_alerts_df: pd.DataFrame, past_alerts_df: pd.DataFrame,
                               alert_type: str, retention_weeks: int, verbosity: int) -> pd.DataFrame:
        """Combines new and past alerts, applies retention, and returns updated history."""
        config = ALERT_CONFIG[alert_type]
        id_cols = config['id_cols']
        required_cols = id_cols + ['Date']

        # Prepare new alerts. The CUSUM checks carry every day of their window so
        # the charts have context, but only the days that actually alerted belong
        # in the history: filing the quiet days too would make a single alert look
        # like a week-long run, suppress it as a repeat on the next run, and date
        # its 'ongoing since' to before the problem started.
        if not new_alerts_df.empty:
            alert_rows = new_alerts_df
            if 'Alert' in alert_rows.columns:
                alert_rows = alert_rows[alert_rows['Alert'] == 1]
            new_alerts_to_save = alert_rows[required_cols].copy()
            new_alerts_to_save['Date'] = pd.to_datetime(new_alerts_to_save['Date'])
        else:
            new_alerts_to_save = pd.DataFrame(columns=required_cols)

        # Prepare past alerts
        if not past_alerts_df.empty:
            past_alerts_df = past_alerts_df[required_cols].copy()
        else:
            past_alerts_df = pd.DataFrame(columns=required_cols)

        # Combine past and new alerts
        to_concat = [df for df in [past_alerts_df, new_alerts_to_save] if not df.empty]
        if to_concat:
            combined_alerts = pd.concat(to_concat, ignore_index=True)
        else:
            combined_alerts = pd.DataFrame(columns=required_cols)

        # Drop duplicates
        if not combined_alerts.empty:
            combined_alerts = combined_alerts.drop_duplicates(subset=required_cols)

            # Apply retention policy
            if retention_weeks > 0:
                retention_cutoff = datetime.now() - timedelta(weeks=retention_weeks)
                combined_dates_naive = pd.to_datetime(combined_alerts['Date']).dt.tz_localize(None)
                retention_cutoff_naive = retention_cutoff.replace(tzinfo=None)
                
                retained_alerts = combined_alerts[combined_dates_naive >= retention_cutoff_naive]
                num_dropped = len(combined_alerts) - len(retained_alerts)
                if num_dropped > 0:
                    log_message(f"Dropped {num_dropped} '{alert_type}' alerts due to retention policy ({retention_weeks} weeks).", 1, verbosity)
            else:
                retained_alerts = combined_alerts
        else:
            retained_alerts = combined_alerts
            
        log_message(f"Updated {len(retained_alerts)} '{alert_type}' alerts in history", 1, verbosity)
        
        return retained_alerts
