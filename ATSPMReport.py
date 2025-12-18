import os
import json
import argparse
import pandas as pd # Add pandas import
import duckdb
from datetime import datetime, timedelta # Add timedelta
from pathlib import Path # Add pathlib
from data_access import get_data
from data_processing import (
    process_maxout_data,
    process_actuations_data,
    process_missing_data,
    cusum,
    alert,
    process_ped
)
from visualization import create_device_plots, create_phase_skip_plots
from report_generation import generate_pdf_report
from email_module import email_reports
from utils import log_message # Import the utility function

# Define alert types and their key columns
ALERT_CONFIG = {
    'maxout': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'maxout_alerts'},
    'actuations': {'id_cols': ['DeviceId', 'Detector'], 'file_suffix': 'actuations_alerts'},
    'missing_data': {'id_cols': ['DeviceId'], 'file_suffix': 'missing_data_alerts'},
    'pedestrian': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'pedestrian_alerts'},
    'phase_skips': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'phase_skips_alerts'},
    'system_outages': {'id_cols': ['Region'], 'file_suffix': 'system_outages_alerts'}
}

PHASE_SKIP_PHASE_WAITS_COLUMNS = ['DeviceId', 'Timestamp', 'Phase', 'PhaseWaitTime', 'PreemptFlag', 'MaxCycleLength']
PHASE_SKIP_ALERT_HISTORY_COLUMNS = ['DeviceId', 'Phase', 'Date', 'MaxCycleLength', 'MaxWaitTime', 'TotalSkips']
PHASE_SKIP_SUMMARY_COLUMNS = ['DeviceId', 'Phase', 'AggregatedSkips', 'LatestDate']
PHASE_SKIP_ALERT_CANDIDATE_COLUMNS = ['DeviceId', 'Phase', 'Date', 'AggregatedSkips']

def load_past_alerts(folder: str, file_format: str, verbosity: int, signals_df: pd.DataFrame) -> dict:
    """Loads past alerts from files. Simplified version."""
    past_alerts = {}
    folder_path = Path(folder)
    signal_count = len(signals_df) * 0.30  # Calculate 30% of total signals
    log_message(f"Loading past alerts from {folder_path}...", 1, verbosity)
    for alert_type, config in ALERT_CONFIG.items():
        file_path = folder_path / f"past_{config['file_suffix']}.{file_format}"
        
        if file_path.exists():           
            if alert_type == 'missing_data':
                # Filter out past alert dates with more than 30% signals included
                sql = f"""
                with d as (
                select Date from '{file_path}'
                group by Date
                having count(*) < {signal_count}
                )
                select * from '{file_path}'
                natural join d
                """
            else:
                sql = f"select * from '{file_path}'"

            past_alerts[alert_type] = duckdb.query(sql).df()
            log_message(f"Loaded {len(past_alerts[alert_type])} past '{alert_type}' alerts from {file_path}.", 1, verbosity)
            
        else:
            log_message(f"Past alerts file not found: {file_path}.", 2, verbosity)
            past_alerts[alert_type] = pd.DataFrame()
            
    return past_alerts


def load_phase_skip_phase_waits(file_path: str, verbosity: int) -> pd.DataFrame:
    """Load phase waits data generated during raw data extraction."""
    if not file_path:
        return pd.DataFrame(columns=PHASE_SKIP_PHASE_WAITS_COLUMNS)

    path = Path(file_path)
    if not path.exists():
        log_message(f"Phase Skip phase waits file not found at {path}", 1, verbosity)
        return pd.DataFrame(columns=PHASE_SKIP_PHASE_WAITS_COLUMNS)

    try:
        df = pd.read_parquet(path)
        if df.empty:
            return pd.DataFrame(columns=PHASE_SKIP_PHASE_WAITS_COLUMNS)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        df['DeviceId'] = df['DeviceId'].astype(str)
        df['Phase'] = df['Phase'].astype(int)
        return df
    except Exception as e:
        log_message(f"Error reading phase waits file {path}: {e}", 1, verbosity)
        return pd.DataFrame(columns=PHASE_SKIP_PHASE_WAITS_COLUMNS)


def load_phase_skip_history(folder: str, retention_days: int, verbosity: int) -> pd.DataFrame:
    """Load historical alert rows and optionally prune files beyond retention."""
    folder_path = Path(folder)
    if not folder_path.exists():
        log_message(f"Phase Skip alert folder not found: {folder_path}", 1, verbosity)
        return pd.DataFrame(columns=PHASE_SKIP_ALERT_HISTORY_COLUMNS)

    if retention_days and retention_days > 0:
        cutoff_date = datetime.now().date() - timedelta(days=retention_days)
        for file_path in folder_path.glob("*.parquet"):
            try:
                file_date = datetime.strptime(file_path.stem, "%Y-%m-%d").date()
            except ValueError:
                continue
            if file_date < cutoff_date:
                file_path.unlink(missing_ok=True)

    frames = []
    for file_path in sorted(folder_path.glob("*.parquet")):
        try:
            frames.append(pd.read_parquet(file_path))
        except Exception as e:
            log_message(f"Warning: Could not read Phase Skip file {file_path}: {e}", 1, verbosity)

    if not frames:
        return pd.DataFrame(columns=PHASE_SKIP_ALERT_HISTORY_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=PHASE_SKIP_ALERT_HISTORY_COLUMNS)

    combined['Date'] = pd.to_datetime(combined['Date']).dt.normalize()
    combined['DeviceId'] = combined['DeviceId'].astype(str)
    combined['Phase'] = combined['Phase'].astype(int)
    return combined


def summarize_phase_skip_alerts(alert_rows_all: pd.DataFrame, threshold: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate alert rows by device/phase and flag those exceeding the skip threshold."""
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

def suppress_alerts(new_alerts_df: pd.DataFrame, past_alerts_df: pd.DataFrame, suppression_days: int, id_cols: list, verbosity: int) -> pd.DataFrame:
    """Filters new alerts based on recent past alerts."""
    print("\n", "*"*50)
    print("inside suppress_alerts")
    print(f"New alerts DataFrame shape: {new_alerts_df.shape}")
    print(f"New alerts DataFrame columns: {new_alerts_df.columns.tolist()}")
    print(f"Past alerts DataFrame shape: {past_alerts_df.shape}")
    if past_alerts_df.empty:
        print("?"*200)
        return new_alerts_df

    cutoff_date = datetime.now() - timedelta(days=suppression_days)
    print(f'Cutoff date for suppression: {cutoff_date}')
    
    # Ensure dates are comparable (naive)
    past_dates_naive = pd.to_datetime(past_alerts_df['Date']).dt.tz_localize(None)
    print(f"Past dates (naive): {past_dates_naive.head()}")  # Debugging line
    cutoff_date_naive = cutoff_date.replace(tzinfo=None)
    print(f"Cutoff date (naive): {cutoff_date_naive}")  # Debugging line

    # Filter past alerts to find recent ones
    recent_past_alerts = past_alerts_df[past_dates_naive >= cutoff_date_naive]
    print(f"Recent past alerts shape: {recent_past_alerts.shape}")
    
    if recent_past_alerts.empty:
        print("IT's EMPTY!")
        return new_alerts_df

    # Get unique keys (DeviceId, Phase/Detector) from recent alerts
    suppression_keys = recent_past_alerts[id_cols].drop_duplicates()
    log_message(f"Found {len(suppression_keys)} unique items for suppression based on the last {suppression_days} days.", 2, verbosity)

    # Perform suppression using merge
    print("mergin now on id_cols")
    print(f"ID columns for suppression: {id_cols}")
    merged = new_alerts_df.merge(suppression_keys, on=id_cols, how='left', indicator=True)
    print(f"Merged DataFrame shape: {merged.shape}")
    print(merged.head())  # Debugging line
    suppressed_alerts_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    print(f"Suppressed alerts DataFrame shape: {suppressed_alerts_df.shape}")
    print(suppressed_alerts_df.head())  # Debugging line
    
    num_suppressed = len(new_alerts_df) - len(suppressed_alerts_df)
    log_message(f"Suppressed {num_suppressed} new alerts.", 1, verbosity)
    
    return suppressed_alerts_df



def update_and_save_alerts(new_alerts_df: pd.DataFrame, past_alerts_df: pd.DataFrame, folder: str, file_format: str, alert_type: str, retention_weeks: int, verbosity: int):
    """Combines new and past alerts, applies retention, and saves. Simplified version."""
    folder_path = Path(folder)
    config = ALERT_CONFIG[alert_type]
    id_cols = config['id_cols']
    file_path = folder_path / f"past_{config['file_suffix']}.{file_format}"
    required_cols = id_cols + ['Date']

    # Prepare new alerts: select columns and ensure Date is datetime
    new_alerts_to_save = new_alerts_df[required_cols].copy()
    new_alerts_to_save['Date'] = pd.to_datetime(new_alerts_to_save['Date'])

    # Prepare past alerts: ensure only required columns exist
    # This helps prevent the FutureWarning in pd.concat
    if not past_alerts_df.empty:
        past_alerts_df = past_alerts_df[required_cols].copy()
    # else: past_alerts_df is already an empty DF with correct columns from load_past_alerts

    # Combine past and new alerts
    combined_alerts = pd.concat([past_alerts_df, new_alerts_to_save], ignore_index=True)

    # Drop duplicates based on all key columns including Date
    combined_alerts.drop_duplicates(subset=required_cols, inplace=True)

    # Apply retention policy
    if retention_weeks > 0:
        retention_cutoff = datetime.now() - timedelta(weeks=retention_weeks)
        # Compare using naive datetimes
        combined_dates_naive = pd.to_datetime(combined_alerts['Date']).dt.tz_localize(None)
        retention_cutoff_naive = retention_cutoff.replace(tzinfo=None)
        
        retained_alerts = combined_alerts[combined_dates_naive >= retention_cutoff_naive]
        num_dropped = len(combined_alerts) - len(retained_alerts)
        if num_dropped > 0:
            log_message(f"Dropped {num_dropped} '{alert_type}' alerts due to retention policy ({retention_weeks} weeks).", 1, verbosity)
    else:
        retained_alerts = combined_alerts # Keep all if retention_weeks <= 0

    # Save the updated alerts
    if file_format == 'parquet':
        retained_alerts.to_parquet(file_path, index=False)
    elif file_format == 'csv':
        retained_alerts.to_csv(file_path, index=False)
        
    log_message(f"Saved {len(retained_alerts)} '{alert_type}' alerts to {file_path}", 1, verbosity)

def load_config(config_path=None, config_dict=None):
    """
    Load configuration from a file or dictionary
    
    Args:
        config_path (str): Path to JSON config file
        config_dict (dict): Configuration dictionary
        
    Returns:
        dict: Configuration parameters
    """
    if config_dict is not None:
        return config_dict
    
    cfg_file = config_path or 'config.json'
    if not os.path.exists(cfg_file):
        raise FileNotFoundError(f"Config file not found: {cfg_file}")
    
    try:
        with open(cfg_file, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file: {e}")


def main(use_parquet=None, connection_params=None, num_figures=None, 
         should_email_reports=None, config_path=None, config_dict=None):
    """
    Main function to run the signal analysis and generate report
    
    Args:
        use_parquet (bool): If True, read from parquet files, otherwise query database
        connection_params (dict): Database connection parameters if use_parquet is False
        num_figures (int): Number of figures to generate for each device
        should_email_reports (bool): If True, email reports instead of saving to disk
        config_path (str): Path to JSON config file
        config_dict (dict): Directly provided config dictionary
        
    Returns:
        tuple or None: If should_email_reports is True, returns (success, report_buffers, region_names)
                      If should_email_reports is False, returns (pdf_paths)
                      Returns None if data loading fails
    """
    # Load configuration
    cfg = load_config(config_path, config_dict)
    
    # Explicit parameters override config file
    use_parquet = use_parquet if use_parquet is not None else cfg.get('use_parquet', True)
    connection_params = connection_params or cfg.get('connection_params')
    num_figures = num_figures if num_figures is not None else cfg.get('num_figures', 1)
    should_email_reports = should_email_reports if should_email_reports is not None else cfg.get('email_reports', False)
    delete_sent_emails = cfg.get('delete_sent_emails', False)
    signals_query = cfg.get('signals_query')
    verbosity = cfg.get('verbosity', 1) # Get verbosity from config, default to 1
    days_back = cfg.get('days_back', 21) # Get days_back from config, default to 21
    
    # Load new config parameters
    output_format = cfg.get('output_format', 'parquet')
    alert_suppression_days = cfg.get('alert_suppression_days', 21)
    alert_retention_weeks = cfg.get('alert_retention_weeks', 104)
    past_alerts_folder = cfg.get('past_alerts_folder', 'Past_Alerts')
    alert_flagging_days = cfg.get('alert_flagging_days', 7) # Load the new parameter
    use_past_alerts = cfg.get('use_past_alerts', True)  # Default to True for backwards compatibility
    phase_skip_alert_rows_folder = cfg.get('phase_skip_alert_rows_folder', 'alert_rows')
    phase_skip_phase_waits_file = cfg.get('phase_skip_phase_waits_file', 'raw_data/phase_skip_phase_waits.parquet')
    phase_skip_retention_days = cfg.get('phase_skip_retention_days', 14)
    phase_skip_alert_threshold = cfg.get('phase_skip_alert_threshold', 2)

    # Validate connection params if using database
    if not use_parquet and (not connection_params or 
                           not all(k in connection_params for k in ['server', 'database', 'username'])):
        raise ValueError("Database connection requires 'server', 'database', and 'username' parameters")

    if not use_parquet and not signals_query:
        raise ValueError("Custom signals_query required in config.json when using database")

    log_message("Starting signal analysis...", 1, verbosity)

    # Get data
    log_message("Reading data...", 1, verbosity)
    maxout_df, actuations_df, signals_df, has_data_df, ped_df = get_data(
        use_parquet=use_parquet, 
        connection_params=connection_params,
        signals_query=signals_query,
        verbosity=verbosity, # Pass verbosity
        days_back=days_back # Pass days_back
    )
    if maxout_df is None or actuations_df is None or signals_df is None or has_data_df is None:
        log_message("Failed to get data", 1, verbosity)
        return None
    log_message(f"Successfully read data. MaxOut shape: {maxout_df.shape}, Actuations shape: {actuations_df.shape}, Signals shape: {signals_df.shape}, Has Data shape: {has_data_df.shape}", 1, verbosity)

    phase_skip_waits = load_phase_skip_phase_waits(phase_skip_phase_waits_file, verbosity)
    phase_skip_all_rows = load_phase_skip_history(phase_skip_alert_rows_folder, phase_skip_retention_days, verbosity)
    phase_skip_summary, phase_skip_alert_candidates = summarize_phase_skip_alerts(
        phase_skip_all_rows,
        phase_skip_alert_threshold
    )

    # Process max out data
    log_message("Processing max out data...", 1, verbosity)
    maxout_daily, maxout_hourly = process_maxout_data(maxout_df)
    log_message(f"Processed max out data. Shape: {maxout_daily.shape}", 1, verbosity)
    log_message(f"Maxout Hourly Data: {maxout_hourly.shape}", 1, verbosity)
    
    # Process actuations data
    log_message("Processing actuations data...", 1, verbosity)
    detector_daily, detector_hourly = process_actuations_data(actuations_df)
    log_message(f"Processed actuations data. Shape: {detector_daily.shape}", 1, verbosity)
    log_message(f"Detector Hourly Data: {detector_hourly.shape}", 1, verbosity)
    
    # Process missing data
    log_message("Processing missing data...", 1, verbosity)
    missing_data = process_missing_data(has_data_df)
    log_message(f"Processed missing data. Shape: {missing_data.shape}", 1, verbosity)
    # Filter out dates with system-wide missing data
    print(signals_df.head())
    missing_data_filtered = duckdb.sql("""
        with a as (
        select Date, Region
        from missing_data
        natural join signals_df
        group by all
        having avg(MissingData) < 0.3
        ),
        b as (
        select a.Date, signals_df.DeviceId
        from a
        natural join signals_df
        )
        select * from missing_data
        natural join b
        order by Date, DeviceId                               
        """
    ).df()

    system_outages = duckdb.sql("""
        select Date, Region, avg(MissingData) as MissingData
        from missing_data
        natural join signals_df
        group by all
        having avg(MissingData) >= 0.3
        order by all
        """
    ).df()
    
    # Process ped data
    log_message("Processing pedestrian data...", 1, verbosity)
    ped_alerts, ped_hourly = process_ped(df_ped=ped_df, df_maxout=maxout_daily, df_intersections=signals_df)
    log_message(f"Processed pedestrian data. Shape: {ped_alerts.shape}", 1, verbosity)
    print(f"Pedestrian Hourly Data: {ped_hourly.shape}", 1, verbosity)
    print(ped_hourly.head())
    print(ped_hourly.tail())

    # Calculate CUSUM and generate alerts
    log_message("Calculating CUSUM statistics...", 1, verbosity)
    t = cusum(maxout_daily, k_value=1)
    t_actuations = cusum(detector_daily, k_value=1)
    t_missing_data = cusum(missing_data_filtered, k_value=1)
    log_message("CUSUM calculation complete", 1, verbosity)

    log_message("Generating alerts...", 1, verbosity)
    new_maxout_alerts = alert(t).execute()
    new_actuations_alerts = alert(t_actuations).execute()
    new_missing_data_alerts = alert(t_missing_data).execute()
    log_message(f"Generated alerts. Found {len(new_maxout_alerts)} phase alerts, {len(new_actuations_alerts)} detector alerts, and {len(new_missing_data_alerts)} missing data alerts", 1, verbosity)

    # Add debug logging for missing data alerts
    if verbosity >= 2:
        log_message("Missing data alerts before filtering:", 2, verbosity)
        log_message(f"Total rows in missing data CUSUM: {len(t_missing_data.execute())}", 2, verbosity)
        log_message(f"Rows with Alert=1: {len(new_missing_data_alerts[new_missing_data_alerts['Alert'] == 1])}", 2, verbosity)
        if len(new_missing_data_alerts[new_missing_data_alerts['Alert'] == 1]) > 0:
            log_message("Sample missing data alerts:", 2, verbosity)
            log_message(f"{new_missing_data_alerts[new_missing_data_alerts['Alert'] == 1].head()}", 2, verbosity)
    
    # Filter NEW alerts based on alert_flagging_days BEFORE suppression and saving
    log_message(f"Filtering newly generated alerts to the last {alert_flagging_days} days...", 1, verbosity)
    flagging_cutoff_date = datetime.now() - timedelta(days=alert_flagging_days)
    flagging_cutoff_date_naive = flagging_cutoff_date.replace(tzinfo=None)

    # Use .copy() to avoid SettingWithCopyWarning   
    recent_new_maxout_alerts = new_maxout_alerts[new_maxout_alerts['Date'] >= flagging_cutoff_date_naive].copy()
    recent_new_actuations_alerts = new_actuations_alerts[new_actuations_alerts['Date'] >= flagging_cutoff_date_naive].copy()
    recent_new_missing_data_alerts = new_missing_data_alerts[new_missing_data_alerts['Date'] >= flagging_cutoff_date_naive].copy()
    recent_new_ped_alerts = ped_alerts[ped_alerts['Date'] >= flagging_cutoff_date_naive].copy()
    recent_phase_skip_alerts = phase_skip_alert_candidates[phase_skip_alert_candidates['Date'] >= flagging_cutoff_date_naive].copy()
    
    log_message(
        f"Filtered new alerts. Keeping {len(recent_new_maxout_alerts)} phase, "
        f"{len(recent_new_actuations_alerts)} detector, {len(recent_new_missing_data_alerts)} missing data, "
        f"{len(recent_phase_skip_alerts)} phase skip alerts for suppression and saving.",
        2,
        verbosity
    )

    # Add debug logging for date filtering impact
    if verbosity >= 2:
        log_message(f"Missing data alerts before date filtering: {len(new_missing_data_alerts)}", 2, verbosity)
        log_message(f"Missing data alerts after date filtering: {len(recent_new_missing_data_alerts)}", 2, verbosity)
        if len(recent_new_missing_data_alerts) > 0:
            log_message("Recent missing data alerts sample:", 2, verbosity)
            log_message(f"{recent_new_missing_data_alerts.head()}", 2, verbosity)
    
    # Suppress alerts using the RECENT NEW alerts
    log_message("Applying alert suppression...", 1, verbosity)
    if use_past_alerts:
        # Create past alerts folder if it doesn't exist
        past_alerts = {}
        if use_past_alerts:
            try:
                Path(past_alerts_folder).mkdir(parents=True, exist_ok=True)
                log_message(f"Ensured past alerts directory exists: {past_alerts_folder}", 1, verbosity)
                past_alerts = load_past_alerts(past_alerts_folder, output_format, verbosity, signals_df=signals_df)
                print(past_alerts.keys())
                print(past_alerts['maxout'].head())
            except Exception as e:
                log_message(f"Error handling past alerts: {e}", 1, verbosity)
                raise e
        else:
            log_message("Past alerts handling is disabled", 1, verbosity)            # Initialize empty DataFrames for past alerts
            for alert_type in ALERT_CONFIG:
                past_alerts[alert_type] = pd.DataFrame(columns=ALERT_CONFIG[alert_type]['id_cols'] + ['Date'])
        final_maxout_alerts = suppress_alerts(
            recent_new_maxout_alerts, # Use date-filtered new alerts
            past_alerts.get('maxout', pd.DataFrame()), 
            alert_suppression_days, 
            ALERT_CONFIG['maxout']['id_cols'],
            verbosity
        )
        final_actuations_alerts = suppress_alerts(
            recent_new_actuations_alerts, # Use date-filtered new alerts
            past_alerts.get('actuations', pd.DataFrame()), 
            alert_suppression_days, 
            ALERT_CONFIG['actuations']['id_cols'],
            verbosity
        )
        final_missing_data_alerts = suppress_alerts(
            recent_new_missing_data_alerts, # Use date-filtered new alerts
            past_alerts.get('missing_data', pd.DataFrame()), 
            alert_suppression_days, 
            ALERT_CONFIG['missing_data']['id_cols'],
            verbosity
        )
        final_ped_alerts = suppress_alerts(
            recent_new_ped_alerts, # Use date-filtered new alerts
            past_alerts.get('pedestrian', pd.DataFrame()), 
            alert_suppression_days, 
            ALERT_CONFIG['pedestrian']['id_cols'],
            verbosity
        )
        final_phase_skip_alerts = suppress_alerts(
            recent_phase_skip_alerts,
            past_alerts.get('phase_skips', pd.DataFrame()),
            alert_suppression_days,
            ALERT_CONFIG['phase_skips']['id_cols'],
            verbosity
        )
        final_system_outages = suppress_alerts(
            system_outages, # Use system outages
            past_alerts.get('system_outages', pd.DataFrame()), 
            alert_suppression_days, 
            ALERT_CONFIG['system_outages']['id_cols'],
            verbosity
        )
    else:
        final_maxout_alerts = recent_new_maxout_alerts
        final_actuations_alerts = recent_new_actuations_alerts
        final_missing_data_alerts = recent_new_missing_data_alerts
        final_ped_alerts = recent_new_ped_alerts
        final_phase_skip_alerts = recent_phase_skip_alerts
        final_system_outages = system_outages
        log_message("Alert suppression skipped (past alerts disabled)", 1, verbosity)

    log_message(
        f"Suppression complete. Reporting {len(final_maxout_alerts)} phase alerts, "
        f"{len(final_actuations_alerts)} detector alerts, {len(final_missing_data_alerts)} missing data alerts, "
        f"{len(final_phase_skip_alerts)} phase skip alerts, {len(final_system_outages)} system outages.",
        1,
        verbosity
    )

    phase_skip_rankings = pd.DataFrame()
    if not final_phase_skip_alerts.empty and not phase_skip_summary.empty:
        active_pairs = final_phase_skip_alerts[['DeviceId', 'Phase']].drop_duplicates()
        ranking_source = phase_skip_summary.merge(active_pairs, on=['DeviceId', 'Phase'], how='inner')
        if not ranking_source.empty:
            phase_skip_rankings = (
                ranking_source.groupby('DeviceId', as_index=False)['AggregatedSkips']
                .sum()
                .rename(columns={'AggregatedSkips': 'TotalSkips'})
            )

    # Create plots using FINAL (date-filtered and suppressed) alerts
    log_message("Creating visualization plots...", 1, verbosity)
    phase_figures = create_device_plots(final_maxout_alerts, signals_df, num_figures, df_hourly=maxout_hourly)
    detector_figures = create_device_plots(final_actuations_alerts, signals_df, num_figures, df_hourly=detector_hourly)
    missing_data_figures = create_device_plots(final_missing_data_alerts, signals_df, num_figures)
    ped_figures = create_device_plots(final_ped_alerts, signals_df, num_figures, ped_hourly)
    phase_skip_alert_pairs = None
    if not final_phase_skip_alerts.empty:
        phase_skip_alert_pairs = final_phase_skip_alerts[['DeviceId', 'Phase']].drop_duplicates()
    plot_phase_skip_waits = pd.DataFrame()
    if (
        phase_skip_alert_pairs is not None
        and not phase_skip_alert_pairs.empty
        and not phase_skip_waits.empty
    ):
        annotated_phase_waits = phase_skip_waits.merge(
            phase_skip_alert_pairs.assign(AlertPhase=True),
            on=['DeviceId', 'Phase'],
            how='left'
        )
        annotated_phase_waits['AlertPhase'] = annotated_phase_waits['AlertPhase'].fillna(False)
        alert_devices = phase_skip_alert_pairs['DeviceId'].unique()
        plot_phase_skip_waits = annotated_phase_waits[annotated_phase_waits['DeviceId'].isin(alert_devices)]
    phase_skip_figures = create_phase_skip_plots(plot_phase_skip_waits, signals_df, phase_skip_rankings, num_figures)
    log_message("Plots created successfully", 1, verbosity)

    # Generate PDF reports using FINAL (date-filtered and suppressed) alerts
    log_message("Generating PDF reports...", 1, verbosity)
    report_result = None
    if should_email_reports:        # Generate reports in memory and email them        log_message("Generating reports for email delivery...", 1, verbosity)
        report_buffers, region_names = generate_pdf_report(
            filtered_df_maxouts=final_maxout_alerts, # Use final
            filtered_df_actuations=final_actuations_alerts, # Use final
            filtered_df_ped=final_ped_alerts, # Include pedestrian alerts
            ped_hourly_df=ped_hourly, # Include pedestrian hourly data
            filtered_df_missing_data=final_missing_data_alerts, # Use final
            system_outages_df=final_system_outages, # Include final system outages data
            phase_figures=phase_figures,
            detector_figures=detector_figures,
            ped_figures=ped_figures, # Include pedestrian figures
            missing_data_figures=missing_data_figures,
            signals_df=signals_df,
            save_to_disk=False,
            verbosity=verbosity, # Pass verbosity
            phase_skip_rows=phase_skip_all_rows,
            phase_skip_figures=phase_skip_figures,
            phase_skip_alerts_df=final_phase_skip_alerts,
            phase_skip_threshold=phase_skip_alert_threshold
        )
          # Email the reports
        log_message("Emailing reports...", 1, verbosity)
        
        # Determine which regions have alerts by checking the generated reports
        regions_with_alerts = region_names.copy() if region_names else []
        
        success = email_reports(
            region_reports=report_buffers,
            regions=region_names,
            report_in_memory=True,
            verbosity=verbosity, # Pass verbosity
            regions_with_alerts=regions_with_alerts,
            delete_sent_emails=delete_sent_emails
        )
        
        if success:
            log_message("All reports were successfully emailed.", 1, verbosity)
        else:
            log_message("There were issues emailing some reports. Check the logs above for details.", 1, verbosity)
        
        report_result = success, report_buffers, region_names
    else:        # Generate and save PDF reports to disk using final alerts
        pdf_paths = generate_pdf_report(
            filtered_df_maxouts=final_maxout_alerts, # Use final
            filtered_df_actuations=final_actuations_alerts, # Use final
            filtered_df_ped=final_ped_alerts, # Include pedestrian alerts
            ped_hourly_df=ped_hourly, # Include pedestrian hourly data
            filtered_df_missing_data=final_missing_data_alerts, # Use final
            system_outages_df=final_system_outages, # Include final system outages data
            phase_figures=phase_figures,
            detector_figures=detector_figures,
            ped_figures=ped_figures, # Include pedestrian figures
            missing_data_figures=missing_data_figures,
            signals_df=signals_df,
            save_to_disk=True,
            verbosity=verbosity, # Pass verbosity
            phase_skip_rows=phase_skip_all_rows,
            phase_skip_figures=phase_skip_figures,
            phase_skip_alerts_df=final_phase_skip_alerts,
            phase_skip_threshold=phase_skip_alert_threshold
        )
        log_message(f"Generated {len(pdf_paths)} PDF reports:", 1, verbosity)
        for path in pdf_paths:
            log_message(f"- {path}", 1, verbosity)
        
        report_result = pdf_paths

    # Update and save past alerts using RECENT NEW alerts (date-filtered, pre-suppression)
    if use_past_alerts:
        log_message("Updating and saving past alerts history...", 1, verbosity)
        update_and_save_alerts(
            recent_new_maxout_alerts, # Use date-filtered new alerts
            past_alerts.get('maxout', pd.DataFrame()), 
            past_alerts_folder, 
            output_format, 
            'maxout', 
            alert_retention_weeks,
            verbosity
        )
        update_and_save_alerts(
            recent_new_actuations_alerts, # Use date-filtered new alerts
            past_alerts.get('actuations', pd.DataFrame()), 
            past_alerts_folder, 
            output_format, 
            'actuations', 
            alert_retention_weeks,
            verbosity
        )
        update_and_save_alerts(
            recent_new_missing_data_alerts, # Use date-filtered new alerts
            past_alerts.get('missing_data', pd.DataFrame()), 
            past_alerts_folder, 
            output_format, 
            'missing_data',            alert_retention_weeks,
            verbosity
        )
        update_and_save_alerts(
            recent_new_ped_alerts, # Use date-filtered new pedestrian alerts
            past_alerts.get('pedestrian', pd.DataFrame()),
            past_alerts_folder,
            output_format,
            'pedestrian',
            alert_retention_weeks,
            verbosity
        )
        update_and_save_alerts(
            recent_phase_skip_alerts,
            past_alerts.get('phase_skips', pd.DataFrame()),
            past_alerts_folder,
            output_format,
            'phase_skips',
            alert_retention_weeks,
            verbosity
        )
        update_and_save_alerts(
            system_outages, # Use system outages 
            past_alerts.get('system_outages', pd.DataFrame()),
            past_alerts_folder,
            output_format,
            'system_outages',
            alert_retention_weeks,
            verbosity
        )
        log_message("Past alerts history updated.", 1, verbosity)
    else:
        log_message("Skipped updating past alerts (disabled in config)", 1, verbosity)

    return report_result


def run_from_command_line():
    """Entry point for command line execution"""
    parser = argparse.ArgumentParser(description='ATSPM Report Generator')
    parser.add_argument('--config', type=str, default=None,
                       help='Path to JSON config file (default: config.json in current directory)')
    parser.add_argument('--use-database', action='store_true',
                       help='Override config to use database instead of parquet files')
    parser.add_argument('--email', action='store_true',
                       help='Override config to email reports instead of saving to disk') # Help text still relevant
    parser.add_argument('--figures', type=int,
                       help='Override config to set number of figures per device')
    
    args = parser.parse_args() # Add this line to parse arguments
    
    try:
        # Load any explicit overrides from command line
        use_parquet = not args.use_database if args.use_database else None
        email_reports_override = args.email if args.email else None # Use a temp variable for clarity
        num_figures = args.figures if args.figures is not None else None
        
        main(
            config_path=args.config,
            use_parquet=use_parquet,
            should_email_reports=email_reports_override, # Pass the override value
            num_figures=num_figures
            # Verbosity will be loaded inside main
        )
    except Exception as e:
        print(f"Error running script: {e}")
        raise  # Re-raise the exception to see the full traceback


if __name__ == "__main__":
    run_from_command_line()
