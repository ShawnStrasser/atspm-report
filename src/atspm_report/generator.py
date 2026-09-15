"""Main ReportGenerator class for ATSPM anomaly detection."""

from typing import Optional, Dict, Union
import pandas as pd
from io import BytesIO

from datetime import datetime, timedelta
from pathlib import Path
import ibis
import ibis.expr.types as ir

from .data_processing import (
    process_maxout_data,
    process_actuations_data,
    process_missing_data,
    process_ped
)
from .statistical_analysis import (
    cusum, alert, MAXOUT_ALERT_DEFAULTS, DEFAULT_ALERT_RECENCY_DAYS,
)
from .visualization import create_device_plots, create_phase_skip_plots
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
from .preempt_processing import (
    summarize_daily_preempts,
    update_preempt_history,
    build_preempt_alerts,
)
from .overlap_dual_indication_processing import process_overlap_dual_indications
from .phase_conflict_processing import (
    process_general_phase_conflicts,
    process_overlap_conflicts,
    process_same_movement_color_conflicts,
)
from .utils import log_message


def _is_empty(data: Union[pd.DataFrame, ir.Table, None]) -> bool:
    """Check if data is None or empty, works for both pandas and Ibis."""
    if data is None:
        return True
    if isinstance(data, pd.DataFrame):
        return data.empty
    if isinstance(data, ir.Table):
        # For Ibis tables, check if there are any rows
        return data.count().execute() == 0
    return True


def _to_pandas(data: Union[pd.DataFrame, ir.Table]) -> pd.DataFrame:
    """Convert Ibis table to pandas DataFrame if needed."""
    if isinstance(data, ir.Table):
        return data.execute()
    return data


def _get_shape_str(data: Union[pd.DataFrame, ir.Table]) -> str:
    """Get a shape string for logging, works for both pandas and Ibis."""
    if isinstance(data, pd.DataFrame):
        return str(data.shape)
    if isinstance(data, ir.Table):
        return f"({data.count().execute()}, {len(data.columns)})"
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


def _normalize_deviceid(data: Union[pd.DataFrame, ir.Table, None]) -> Union[pd.DataFrame, ir.Table, None]:
    """Convert DeviceId column to string type if present, works for both pandas and Ibis."""
    if data is None:
        return None
    
    if isinstance(data, pd.DataFrame):
        if 'DeviceId' in data.columns:
            data = data.copy()
            data['DeviceId'] = data['DeviceId'].astype(str)
        return data
    
    if isinstance(data, ir.Table):
        if 'DeviceId' in data.columns:
            # For Ibis, cast the DeviceId column to string
            data = data.mutate(DeviceId=data.DeviceId.cast('string'))
        return data
    
    return data


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
    'actuations': {'id_cols': ['DeviceId', 'Detector'], 'file_suffix': 'actuations_alerts'},
    'missing_data': {'id_cols': ['DeviceId'], 'file_suffix': 'missing_data_alerts'},
    'pedestrian': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'pedestrian_alerts'},
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
    'preempts': {'id_cols': ['DeviceId', 'Preempt', 'Direction'], 'file_suffix': 'preempt_alerts'},
}

# Safety-critical signal conflicts must be reported every time they are observed.
UNSUPPRESSED_ALERT_TYPES = {'overlap_dual_indications'}


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
            - alert_flagging_days (int): Max age for new alerts. Default: 7
            - suppress_repeated_alerts (bool): Enable alert suppression. Default: True
            - alert_suppression_days (int): Days to suppress repeat alerts. Default: 21
            - include_ongoing_issues (bool): Add an 'Ongoing Issues' subsection under each
              section listing the repeat alerts suppression removed. Default: False
            - maxout_cusum_threshold / maxout_zscore_threshold / maxout_percent_threshold /
              maxout_min_services: phase termination alerting thresholds. A day is flagged
              only when all four are exceeded, so raising any one reduces sensitivity.
              Defaults: 0.25, 4.0, 0.2, 30
            - alert_retention_weeks (int): Weeks to retain alert history. Default: 104
            - figures_per_device (int): Plots per device in report. Default: 3
            - verbosity (int): 0=silent, 1=info, 2=debug. Default: 1
            - phase_skip_alert_threshold (int): Min skips to trigger alert. Default: 1
            - phase_skip_retention_days (int): Days to retain phase skip data. Default: 14
            - max_table_rows (int): Maximum rows to show in each report table. Default: 10
            - clearance_yellow_min_seconds (float): Minimum yellow clearance time. Default: 3.5
            - clearance_red_min_seconds (float): Minimum red clearance time. Default: 0.5
            - clearance_tolerance_seconds (float): Clearance timing tolerance. Default: 0.1
            - clearance_invalid_event_cushion_seconds (float): Seconds around invalid timeline events to exclude. Default: 30
            - filter_stoptime (bool): Filter irregular clearance intervals overlapping stop-time events. Default: True
            - clearance_stop_event_classes (list[str]): Timeline event classes used by filter_stoptime. Default: Stop Time Input, Preempt
            - clearance_flash_event_classes (list[str]): Flash classes whose surroundings are excluded. Default: every flash class (see alarm_processing.FLASH_EVENT_CLASSES)
            - clearance_flash_cushion_seconds (float): Seconds either side of a flash alarm to exclude. Default: 600
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
            'alert_retention_weeks': 104,
            'figures_per_device': 3,
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
            'joke_index': None,
            'custom_logo_path': None,
        }
        return {**defaults, **config}
    
    def generate(
        self,
        signals: Union[pd.DataFrame, ir.Table],  # REQUIRED
        terminations: Optional[Union[pd.DataFrame, ir.Table]] = None,
        detector_health: Optional[Union[pd.DataFrame, ir.Table]] = None,
        has_data: Optional[Union[pd.DataFrame, ir.Table]] = None,
        pedestrian: Optional[Union[pd.DataFrame, ir.Table]] = None,
        phase_wait: Optional[Union[pd.DataFrame, ir.Table]] = None,
        coordination_agg: Optional[Union[pd.DataFrame, ir.Table]] = None,
        timeline: Optional[Union[pd.DataFrame, ir.Table]] = None,
        past_alerts: Optional[Dict[str, pd.DataFrame]] = None,
        alarm_history: Optional[pd.DataFrame] = None,
        preempt_history: Optional[pd.DataFrame] = None,
        device_days: Optional[Union[pd.DataFrame, ir.Table]] = None,
    ) -> dict:
        """
        Generate reports from provided DataFrames or Ibis tables.
        
        Args:
            signals: REQUIRED. Signal metadata with columns: DeviceId, Name, Region.
                Can be pandas DataFrame or Ibis table.
            terminations: Phase termination data with columns:
                TimeStamp, DeviceId, Phase, Total, PerformanceMeasure
            detector_health: Detector actuation data with columns:
                TimeStamp, DeviceId, Detector, Total, anomaly, prediction
            has_data: Data availability records with columns:
                TimeStamp, DeviceId
            pedestrian: Pedestrian phase data with columns:
                TimeStamp, DeviceId, Phase, PedServices, PedActuation
            phase_wait: Phase wait data with columns:
                TimeStamp, DeviceId, Phase, AvgPhaseWait, MaxPhaseWait, TotalSkips
            coordination_agg: Coordination aggregation data with columns:
                TimeStamp, DeviceId, ActualCycleLength
                (15-minute bin aggregated data for cycle length plotting)
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
            past_alerts: Dict of alert_type -> DataFrame for suppression.
                Keys: 'maxout', 'actuations', 'missing_data', 'pedestrian',
                      'phase_skips', 'clearance_intervals', 'overlap_dual_indications',
                      'general_phase_conflicts', 'overlap_conflicts', 'system_outages',
                      'preempts'

        Returns:
            dict with keys:
                - 'reports': Dict[str, BytesIO] - region name -> PDF bytes (empty if no alerts)
                - 'alerts': Dict[str, pd.DataFrame] - alert type -> alert DataFrame
                    Keys: 'maxout', 'actuations', 'missing_data', 'pedestrian',
                          'phase_skips', 'clearance_intervals', 'overlap_dual_indications',
                          'general_phase_conflicts', 'overlap_conflicts', 'system_outages',
                          'preempts'
                - 'ongoing_alerts': Dict[str, pd.DataFrame] - repeat alerts suppression
                    removed, carrying an OngoingSince column; empty unless
                    include_ongoing_issues is set
                - 'updated_past_alerts': Dict[str, pd.DataFrame] - for next run's suppression
                - 'updated_alarm_history': pd.DataFrame - for next run's alarm totals
                - 'updated_preempt_history': pd.DataFrame - for next run's preempt baseline
                - 'alarms': pd.DataFrame - controller alarms listed this run
                - 'hourly_data': Dict[str, pd.DataFrame] - intermediate hourly aggregates
                    Keys: 'maxout_hourly', 'detector_hourly', 'ped_hourly'
        """
        # Validate required input
        if _is_empty(signals):
            raise ValueError("'signals' is required and cannot be None or empty")
        
        # Normalize DeviceId to string in all inputs
        signals = _normalize_deviceid(signals)
        terminations = _normalize_deviceid(terminations)
        detector_health = _normalize_deviceid(detector_health)
        has_data = _normalize_deviceid(has_data)
        pedestrian = _normalize_deviceid(pedestrian)
        phase_wait = _normalize_deviceid(phase_wait)
        coordination_agg = _normalize_deviceid(coordination_agg)
        timeline = _normalize_deviceid(timeline)
        device_days = _normalize_deviceid(device_days)
        
        # Convert signals to pandas (needed for downstream operations)
        signals = _to_pandas(signals)

        # Latest day = report day for the clearance and conflict checks; every day
        # feeds the alarm and preempt counters (see _split_timeline_days).
        timeline_latest, report_day = None, None
        if not _is_empty(timeline):
            timeline, timeline_latest, report_day = _split_timeline_days(_to_pandas(timeline))
        device_days = _to_pandas(device_days) if not _is_empty(device_days) else None
        
        verbosity = self.config['verbosity']
        log_message("Starting signal analysis...", 1, verbosity)
        
        # Initialize past alerts if not provided
        if past_alerts is None:
            past_alerts = {}
        for alert_type in ALERT_CONFIG:
            if alert_type not in past_alerts:
                past_alerts[alert_type] = pd.DataFrame()
        
        # Initialize result containers
        new_alerts = {}
        hourly_data = {}
        
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
            ).execute()
        else:
            new_alerts['maxout'] = pd.DataFrame()
            hourly_data['maxout_hourly'] = pd.DataFrame()
        
        # Process actuations data if provided
        if not _is_empty(detector_health):
            log_message("Processing actuations data...", 1, verbosity)
            detector_daily, detector_hourly = process_actuations_data(detector_health)
            hourly_data['detector_hourly'] = _to_pandas(detector_hourly)
            log_message(f"Processed actuations data. Shape: {_get_shape_str(detector_daily)}", 1, verbosity)
            
            log_message("Calculating CUSUM statistics for actuations...", 1, verbosity)
            t_actuations = cusum(detector_daily, k_value=1)
            new_alerts['actuations'] = alert(
                t_actuations, recency_days=self.config['alert_recency_days']
            ).execute()
        else:
            new_alerts['actuations'] = pd.DataFrame()
            hourly_data['detector_hourly'] = pd.DataFrame()
        
        # Process missing data if provided
        if not _is_empty(has_data):
            log_message("Processing missing data...", 1, verbosity)
            missing_data = process_missing_data(has_data)
            log_message(f"Processed missing data. Shape: {_get_shape_str(missing_data)}", 1, verbosity)
            
            # Filter out dates with system-wide missing data
            ibis.options.interactive = True
            signal_count = len(signals) * 0.30
            
            # Convert to ibis tables
            missing_data_tbl = ibis.memtable(missing_data)
            signals_tbl = ibis.memtable(signals)
            
            # Join and filter
            md_with_region = missing_data_tbl.join(
                signals_tbl,
                missing_data_tbl.DeviceId == signals_tbl.DeviceId
            )
            
            # Find dates/regions where average missing data < 0.3
            valid_date_regions = md_with_region.group_by(['Date', 'Region']).aggregate(
                avg_missing=md_with_region.MissingData.mean()
            ).filter(lambda t: t.avg_missing < 0.3)
            
            # Get all devices for those valid date/regions
            valid_combos = valid_date_regions.join(
                signals_tbl,
                valid_date_regions.Region == signals_tbl.Region
            ).select(
                Date=valid_date_regions.Date,
                DeviceId=signals_tbl.DeviceId
            )
            
            # Filter missing_data to keep only valid combinations
            missing_data_filtered = missing_data_tbl.join(
                valid_combos,
                [missing_data_tbl.DeviceId == valid_combos.DeviceId,
                 missing_data_tbl.Date == valid_combos.Date]
            ).select(
                DeviceId=missing_data_tbl.DeviceId,
                Date=missing_data_tbl.Date,
                MissingData=missing_data_tbl.MissingData
            ).order_by(['Date', 'DeviceId']).execute()

            # Get system outages (dates/regions where avg missing data >= 0.3)
            system_outages = md_with_region.group_by(['Date', 'Region']).aggregate(
                MissingData=md_with_region.MissingData.mean()
            ).filter(lambda t: t.MissingData >= 0.3).order_by(['Date', 'Region']).execute()
            
            log_message("Calculating CUSUM statistics for missing data...", 1, verbosity)
            t_missing_data = cusum(missing_data_filtered, k_value=1)
            new_alerts['missing_data'] = alert(
                t_missing_data, recency_days=self.config['alert_recency_days']
            ).execute()
            new_alerts['system_outages'] = system_outages
        else:
            new_alerts['missing_data'] = pd.DataFrame()
            new_alerts['system_outages'] = pd.DataFrame()
        
        # Process pedestrian data if provided
        if not _is_empty(pedestrian) and not _is_empty(terminations):
            log_message("Processing pedestrian data...", 1, verbosity)
            ped_alerts, ped_hourly = process_ped(df_ped=pedestrian, df_maxout=maxout_daily, df_intersections=signals)
            new_alerts['pedestrian'] = _to_pandas(ped_alerts)
            hourly_data['ped_hourly'] = _to_pandas(ped_hourly)
            log_message(f"Processed pedestrian data. Shape: {_get_shape_str(ped_alerts)}", 1, verbosity)
        else:
            new_alerts['pedestrian'] = pd.DataFrame()
            hourly_data['ped_hourly'] = pd.DataFrame()
        
        # Process phase wait data if provided
        if not _is_empty(phase_wait):
            log_message("Processing phase wait data...", 1, verbosity)
            
            phase_skip_waits, phase_skip_alert_rows, cycle_length_data = process_phase_wait_data(
                phase_wait,
                coordination_agg
            )
            
            # Convert to pandas for downstream processing
            phase_skip_waits_pd = _to_pandas(phase_skip_waits)
            phase_skip_alert_rows_pd = _to_pandas(phase_skip_alert_rows)
            cycle_length_data_pd = _to_pandas(cycle_length_data)
            
            # Apply retention to phase skip alert rows
            if self.config['phase_skip_retention_days'] > 0 and 'Date' in phase_skip_alert_rows_pd.columns:
                cutoff_datetime = (
                    datetime.now() - timedelta(days=self.config['phase_skip_retention_days'])
                ).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
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
        daily_preempts = summarize_daily_preempts(timeline, device_days=device_days)
        updated_preempt_history = update_preempt_history(
            daily_preempts,
            preempt_history if preempt_history is not None else pd.DataFrame(),
            verbosity=verbosity,
        )
        preempt_alerts = build_preempt_alerts(updated_preempt_history, report_date=report_day)
        new_alerts['preempts'] = preempt_alerts
        log_message(
            f"Processed preempt frequency. Shape: {preempt_alerts.shape}",
            1,
            verbosity,
        )

        # This check is intentionally opt-in because not every agency uses
        # same-numbered phase/overlap relationships.
        if self.config['overlap_dual_indications_enabled']:
            log_message('Processing overlap dual indications...', 1, verbosity)
            overlap_dual_indications = process_overlap_dual_indications(timeline_latest, self.config)
            new_alerts['overlap_dual_indications'] = overlap_dual_indications
            log_message(
                f'Processed overlap dual indications. Shape: {overlap_dual_indications.shape}',
                1,
                verbosity,
            )
        else:
            new_alerts['overlap_dual_indications'] = pd.DataFrame()

        if self.config['general_phase_conflicts_enabled']:
            log_message('Processing general phase conflicts...', 1, verbosity)
            general_phase_config = _config_with_signal_exclusions(
                self.config,
                signals,
                'general_phase_conflict_excluded_signals',
                'general_phase_conflict_excluded_device_ids',
            )
            general_phase_conflicts = process_general_phase_conflicts(
                timeline_latest,
                general_phase_config,
            )
            new_alerts['general_phase_conflicts'] = general_phase_conflicts
            log_message(
                f'Processed general phase conflicts. Shape: {general_phase_conflicts.shape}',
                1,
                verbosity,
            )
        else:
            new_alerts['general_phase_conflicts'] = pd.DataFrame()

        if self.config['overlap_conflicts_enabled']:
            log_message('Processing overlap conflicts...', 1, verbosity)
            overlap_conflict_config = _config_with_signal_exclusions(
                self.config,
                signals,
                'overlap_conflict_excluded_signals',
                'overlap_conflict_excluded_device_ids',
            )
            overlap_conflicts = process_overlap_conflicts(
                timeline_latest,
                overlap_conflict_config,
            )
            new_alerts['overlap_conflicts'] = overlap_conflicts
            log_message(
                f'Processed overlap conflicts. Shape: {overlap_conflicts.shape}',
                1,
                verbosity,
            )
        else:
            new_alerts['overlap_conflicts'] = pd.DataFrame()

        if self.config['same_movement_color_conflicts_enabled']:
            log_message('Processing same movement color conflicts...', 1, verbosity)
            same_movement_color_conflicts = process_same_movement_color_conflicts(
                timeline_latest,
                self.config,
            )
            new_alerts['same_movement_color_conflicts'] = same_movement_color_conflicts
            log_message(
                f'Processed same movement color conflicts. Shape: {same_movement_color_conflicts.shape}',
                1,
                verbosity,
            )
        else:
            new_alerts['same_movement_color_conflicts'] = pd.DataFrame()
        
        # Filter new alerts to only recent ones (alert_flagging_days)
        log_message(f"Filtering newly generated alerts to the last {self.config['alert_flagging_days']} days...", 1, verbosity)
        flagging_cutoff_date = datetime.now() - timedelta(days=self.config['alert_flagging_days'])
        # Normalize to beginning of day for proper comparison with date-only columns
        flagging_cutoff_date_naive = flagging_cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        
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
                           'overlap_dual_indications', 'preempts'):
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
                elif alert_type in recent_new_alerts and not recent_new_alerts[alert_type].empty:
                    final_alerts[alert_type], suppressed_alerts[alert_type] = self._suppress_alerts(
                        recent_new_alerts[alert_type],
                        past_alerts.get(alert_type, pd.DataFrame()),
                        self.config['alert_suppression_days'],
                        ALERT_CONFIG[alert_type]['id_cols'],
                        verbosity
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
            ongoing_alerts = suppressed_alerts
        else:
            ongoing_alerts = {alert_type: pd.DataFrame() for alert_type in ALERT_CONFIG}

        # Generate visualizations
        log_message("Creating visualization plots...", 1, verbosity)
        num_figures = self.config['figures_per_device']
        
        phase_figures = create_device_plots(final_alerts['maxout'], signals, num_figures,
                                           hourly_data.get('maxout_hourly', pd.DataFrame()))
        detector_figures = create_device_plots(final_alerts['actuations'], signals, num_figures,
                                              hourly_data.get('detector_hourly', pd.DataFrame()))
        missing_data_figures = create_device_plots(final_alerts['missing_data'], signals, num_figures)
        ped_figures = create_device_plots(final_alerts['pedestrian'], signals, num_figures,
                                         hourly_data.get('ped_hourly', pd.DataFrame()))

        # Ongoing issues get their own charts, drawn the same way and capped the same.
        ongoing_phase_figures = create_device_plots(ongoing_alerts['maxout'], signals, num_figures,
                                                   hourly_data.get('maxout_hourly', pd.DataFrame()))
        ongoing_detector_figures = create_device_plots(ongoing_alerts['actuations'], signals, num_figures,
                                                       hourly_data.get('detector_hourly', pd.DataFrame()))
        ongoing_missing_data_figures = create_device_plots(ongoing_alerts['missing_data'], signals, num_figures)
        ongoing_ped_figures = create_device_plots(ongoing_alerts['pedestrian'], signals, num_figures,
                                                 hourly_data.get('ped_hourly', pd.DataFrame()))

        # Phase skip visualizations
        # A new skip is news about one day; an ongoing one is about persistence.
        phase_skip_figures = self._create_phase_skip_figures(
            final_alerts['phase_skips'], signals, num_figures,
            days_plotted=self.config['phase_skip_new_chart_days'],
        )
        ongoing_phase_skip_figures = self._create_phase_skip_figures(
            ongoing_alerts['phase_skips'], signals, num_figures,
            days_plotted=self.config['phase_skip_ongoing_chart_days'],
        )
        
        log_message("Plots created successfully", 1, verbosity)
        
        # Generate PDF reports
        log_message("Generating PDF reports...", 1, verbosity)
        reports = generate_pdf_report(
            filtered_df_maxouts=final_alerts['maxout'],
            filtered_df_actuations=final_alerts['actuations'],
            filtered_df_ped=final_alerts['pedestrian'],
            ped_hourly_df=hourly_data.get('ped_hourly', pd.DataFrame()),
            detector_hourly_df=hourly_data.get('detector_hourly', pd.DataFrame()),
            filtered_df_missing_data=final_alerts['missing_data'],
            system_outages_df=final_alerts['system_outages'],
            phase_figures=phase_figures,
            detector_figures=detector_figures,
            ped_figures=ped_figures,
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
            clearance_yellow_min_seconds=self.config['clearance_yellow_min_seconds'],
            clearance_red_min_seconds=self.config['clearance_red_min_seconds'],
            max_table_rows=self.config['max_table_rows'],
            joke_index=self.config['joke_index'],
            custom_logo_path=self.config['custom_logo_path'],
            alarms_df=alarm_alerts,
            preempt_alerts_df=final_alerts['preempts'],
            ongoing_alerts=ongoing_alerts,
            ongoing_phase_figures=ongoing_phase_figures,
            ongoing_detector_figures=ongoing_detector_figures,
            ongoing_ped_figures=ongoing_ped_figures,
            ongoing_missing_data_figures=ongoing_missing_data_figures,
            ongoing_phase_skip_figures=ongoing_phase_skip_figures,
        )
        
        # Update and save past alerts with retention
        log_message("Updating past alerts history...", 1, verbosity)
        updated_past_alerts = {}
        for alert_type in ALERT_CONFIG:
            # Record exactly what the report accounted for: the new alerts plus the
            # repeats suppression moved to Ongoing. Writing the wider working frame
            # would file alerts that no report ever showed, and suppression would
            # then hide them for the next three weeks - silently, and forever.
            reported = [
                frame for frame in (
                    final_alerts.get(alert_type, pd.DataFrame()),
                    suppressed_alerts.get(alert_type, pd.DataFrame()),
                ) if frame is not None and not frame.empty
            ]
            reported_alerts = (
                pd.concat(reported, ignore_index=True) if reported else pd.DataFrame()
            )
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
            'updated_alarm_history': updated_alarm_history,
            'updated_preempt_history': updated_preempt_history,
            'alarms': alarm_alerts,
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
    
    def _maxout_thresholds(self) -> dict:
        """Phase termination thresholds taken from config, defaults where unset."""
        return {
            key: self.config[key]
            for key in MAXOUT_ALERT_DEFAULTS
            if self.config.get(key) is not None
        }

    def _create_phase_skip_figures(self, alerts_df: pd.DataFrame, signals: pd.DataFrame,
                                   num_figures: int, days_plotted: int = None) -> list:
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

        annotated_phase_waits = self.phase_skip_waits.merge(
            alert_pairs.assign(AlertPhase=True),
            on=['DeviceId', 'Phase'],
            how='left'
        )
        annotated_phase_waits['AlertPhase'] = annotated_phase_waits['AlertPhase'].fillna(False).astype(bool)
        alert_devices = alert_pairs['DeviceId'].unique()
        plot_phase_skip_waits = annotated_phase_waits[annotated_phase_waits['DeviceId'].isin(alert_devices)]

        return create_phase_skip_plots(
            plot_phase_skip_waits,
            signals,
            rankings,
            num_figures,
            self.cycle_length_data,
            days_plotted=days_plotted,
        )

    def _suppress_alerts(self, new_alerts_df: pd.DataFrame, past_alerts_df: pd.DataFrame,
                         suppression_days: int, id_cols: list, verbosity: int) -> tuple:
        """Filters new alerts based on recent past alerts.

        Returns (surviving alerts, suppressed alerts). The suppressed frame carries
        an OngoingSince column so the report can say how long each repeat has been
        running; it is empty when nothing was suppressed.
        """
        if past_alerts_df.empty:
            return new_alerts_df, new_alerts_df.head(0)

        cutoff_date = datetime.now() - timedelta(days=suppression_days)

        # Ensure dates are comparable (naive)
        past_dates_naive = pd.to_datetime(past_alerts_df['Date']).dt.tz_localize(None)
        cutoff_date_naive = cutoff_date.replace(tzinfo=None)

        # Filter past alerts to find recent ones
        recent_past_alerts = past_alerts_df[past_dates_naive >= cutoff_date_naive]

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
            combined_alerts.drop_duplicates(subset=required_cols, inplace=True)

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
