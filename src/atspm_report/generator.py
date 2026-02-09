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
from .statistical_analysis import cusum, alert
from .visualization import create_device_plots, create_phase_skip_plots
from .report_generation import generate_pdf_report
from .phase_skip_processing import process_phase_wait_data
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


# Alert configuration
ALERT_CONFIG = {
    'maxout': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'maxout_alerts'},
    'actuations': {'id_cols': ['DeviceId', 'Detector'], 'file_suffix': 'actuations_alerts'},
    'missing_data': {'id_cols': ['DeviceId'], 'file_suffix': 'missing_data_alerts'},
    'pedestrian': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'pedestrian_alerts'},
    'phase_skips': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'phase_skips_alerts'},
    'system_outages': {'id_cols': ['Region'], 'file_suffix': 'system_outages_alerts'}
}


class ReportGenerator:
    """
    Generate ATSPM anomaly detection reports.
    
    Accepts DataFrames as input and returns reports as BytesIO objects along with
    alert DataFrames. All inputs are optional except 'signals' (required for regional grouping).
    """
    
    def __init__(self, config: dict):
        """
        Initialize with configuration dict.
        
        Config keys (all optional with defaults):
            - historical_window_days (int): Days of data to analyze. Default: 21
            - alert_flagging_days (int): Max age for new alerts. Default: 7
            - suppress_repeated_alerts (bool): Enable alert suppression. Default: True
            - alert_suppression_days (int): Days to suppress repeat alerts. Default: 21
            - alert_retention_weeks (int): Weeks to retain alert history. Default: 104
            - figures_per_device (int): Plots per device in report. Default: 3
            - verbosity (int): 0=silent, 1=info, 2=debug. Default: 1
            - phase_skip_alert_threshold (int): Min skips to trigger alert. Default: 1
            - phase_skip_retention_days (int): Days to retain phase skip data. Default: 14
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
            'alert_retention_weeks': 104,
            'figures_per_device': 3,
            'verbosity': 1,
            'phase_skip_alert_threshold': 1,
            'phase_skip_retention_days': 14,
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
        past_alerts: Optional[Dict[str, pd.DataFrame]] = None,
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
            past_alerts: Dict of alert_type -> DataFrame for suppression.
                Keys: 'maxout', 'actuations', 'missing_data', 'pedestrian', 
                      'phase_skips', 'system_outages'
        
        Returns:
            dict with keys:
                - 'reports': Dict[str, BytesIO] - region name -> PDF bytes (empty if no alerts)
                - 'alerts': Dict[str, pd.DataFrame] - alert type -> alert DataFrame
                    Keys: 'maxout', 'actuations', 'missing_data', 'pedestrian',
                          'phase_skips', 'system_outages'
                - 'updated_past_alerts': Dict[str, pd.DataFrame] - for next run's suppression
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
        
        # Convert signals to pandas (needed for downstream operations)
        signals = _to_pandas(signals)
        
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
            new_alerts['maxout'] = alert(t).execute()
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
            new_alerts['actuations'] = alert(t_actuations).execute()
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
            new_alerts['missing_data'] = alert(t_missing_data).execute()
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
        
        # Apply suppression if enabled
        if self.config['suppress_repeated_alerts']:
            log_message("Applying alert suppression...", 1, verbosity)
            final_alerts = {}
            for alert_type in ALERT_CONFIG:
                if alert_type in recent_new_alerts and not recent_new_alerts[alert_type].empty:
                    final_alerts[alert_type] = self._suppress_alerts(
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
        
        # Phase skip visualizations
        phase_skip_figures = []
        phase_skip_rankings = pd.DataFrame()
        if not final_alerts['phase_skips'].empty and not self.phase_skip_summary.empty:
            active_pairs = final_alerts['phase_skips'][['DeviceId', 'Phase']].drop_duplicates()
            ranking_source = self.phase_skip_summary.merge(active_pairs, on=['DeviceId', 'Phase'], how='inner')
            if not ranking_source.empty:
                phase_skip_rankings = (
                    ranking_source.groupby('DeviceId', as_index=False)['AggregatedSkips']
                    .sum()
                    .rename(columns={'AggregatedSkips': 'TotalSkips'})
                )
            
            # Prepare data for plotting
            phase_skip_alert_pairs = final_alerts['phase_skips'][['DeviceId', 'Phase']].drop_duplicates()
            if not phase_skip_alert_pairs.empty and not self.phase_skip_waits.empty:
                annotated_phase_waits = self.phase_skip_waits.merge(
                    phase_skip_alert_pairs.assign(AlertPhase=True),
                    on=['DeviceId', 'Phase'],
                    how='left'
                )
                annotated_phase_waits['AlertPhase'] = annotated_phase_waits['AlertPhase'].fillna(False).astype(bool)
                alert_devices = phase_skip_alert_pairs['DeviceId'].unique()
                plot_phase_skip_waits = annotated_phase_waits[annotated_phase_waits['DeviceId'].isin(alert_devices)]
                phase_skip_figures = create_phase_skip_plots(
                    plot_phase_skip_waits, 
                    signals, 
                    phase_skip_rankings, 
                    num_figures,
                    self.cycle_length_data
                )
        
        log_message("Plots created successfully", 1, verbosity)
        
        # Generate PDF reports
        log_message("Generating PDF reports...", 1, verbosity)
        reports = generate_pdf_report(
            filtered_df_maxouts=final_alerts['maxout'],
            filtered_df_actuations=final_alerts['actuations'],
            filtered_df_ped=final_alerts['pedestrian'],
            ped_hourly_df=hourly_data.get('ped_hourly', pd.DataFrame()),
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
            joke_index=self.config['joke_index'],
            custom_logo_path=self.config['custom_logo_path']
        )
        
        # Update and save past alerts with retention
        log_message("Updating past alerts history...", 1, verbosity)
        updated_past_alerts = {}
        for alert_type in ALERT_CONFIG:
            updated_past_alerts[alert_type] = self._update_alert_history(
                recent_new_alerts.get(alert_type, pd.DataFrame()),
                past_alerts.get(alert_type, pd.DataFrame()),
                alert_type,
                self.config['alert_retention_weeks'],
                verbosity
            )
        
        log_message("Report generation complete.", 1, verbosity)
        
        return {
            'reports': reports,
            'alerts': final_alerts,
            'updated_past_alerts': updated_past_alerts,
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
    
    def _suppress_alerts(self, new_alerts_df: pd.DataFrame, past_alerts_df: pd.DataFrame, 
                         suppression_days: int, id_cols: list, verbosity: int) -> pd.DataFrame:
        """Filters new alerts based on recent past alerts."""
        if past_alerts_df.empty:
            return new_alerts_df

        cutoff_date = datetime.now() - timedelta(days=suppression_days)
        
        # Ensure dates are comparable (naive)
        past_dates_naive = pd.to_datetime(past_alerts_df['Date']).dt.tz_localize(None)
        cutoff_date_naive = cutoff_date.replace(tzinfo=None)

        # Filter past alerts to find recent ones
        recent_past_alerts = past_alerts_df[past_dates_naive >= cutoff_date_naive]
        
        if recent_past_alerts.empty:
            return new_alerts_df

        # Get unique keys from recent alerts
        suppression_keys = recent_past_alerts[id_cols].drop_duplicates()
        log_message(f"Found {len(suppression_keys)} unique items for suppression based on the last {suppression_days} days.", 2, verbosity)

        # Perform suppression using merge
        merged = new_alerts_df.merge(suppression_keys, on=id_cols, how='left', indicator=True)
        suppressed_alerts_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        num_suppressed = len(new_alerts_df) - len(suppressed_alerts_df)
        log_message(f"Suppressed {num_suppressed} new alerts.", 1, verbosity)
        
        return suppressed_alerts_df
    
    def _update_alert_history(self, new_alerts_df: pd.DataFrame, past_alerts_df: pd.DataFrame,
                               alert_type: str, retention_weeks: int, verbosity: int) -> pd.DataFrame:
        """Combines new and past alerts, applies retention, and returns updated history."""
        config = ALERT_CONFIG[alert_type]
        id_cols = config['id_cols']
        required_cols = id_cols + ['Date']

        # Prepare new alerts
        if not new_alerts_df.empty:
            new_alerts_to_save = new_alerts_df[required_cols].copy()
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
