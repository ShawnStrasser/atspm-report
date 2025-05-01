import os
import json
import argparse
import pandas as pd # Add pandas import
from datetime import datetime, timedelta # Add timedelta
from pathlib import Path # Add pathlib
from data_access import get_data
from data_processing import (
    process_maxout_data,
    process_actuations_data,
    process_missing_data,
    cusum,
    alert
)
from visualization import create_device_plots
from report_generation import generate_pdf_report
from email_module import email_reports
from utils import log_message # Import the utility function

# Define alert types and their key columns
ALERT_CONFIG = {
    'maxout': {'id_cols': ['DeviceId', 'Phase'], 'file_suffix': 'maxout_alerts'},
    'actuations': {'id_cols': ['DeviceId', 'Detector'], 'file_suffix': 'actuations_alerts'},
    'missing_data': {'id_cols': ['DeviceId'], 'file_suffix': 'missing_data_alerts'}
}

def load_past_alerts(folder: str, file_format: str, verbosity: int) -> dict:
    """Loads past alerts from files. Simplified version."""
    past_alerts = {}
    folder_path = Path(folder)
    log_message(f"Loading past alerts from {folder_path}...", 1, verbosity)
    for alert_type, config in ALERT_CONFIG.items():
        expected_cols = config['id_cols'] + ['Date']
        file_path = folder_path / f"past_{config['file_suffix']}.{file_format}"
        
        if file_path.exists():
            if file_format == 'parquet':
                df = pd.read_parquet(file_path)
            elif file_format == 'csv':
                df = pd.read_csv(file_path, parse_dates=['Date'])
            else: # Should not happen based on config validation, but default to empty
                df = pd.DataFrame(columns=expected_cols)
            
            # Ensure Date column is datetime and select expected columns
            if 'Date' in df.columns:
                 df['Date'] = pd.to_datetime(df['Date'])
                 # Ensure all expected columns are present, fill missing if necessary (though unlikely)
                 for col in expected_cols:
                     if col not in df.columns:
                         df[col] = pd.NA # Or appropriate default
                 past_alerts[alert_type] = df[expected_cols].copy()
                 log_message(f"Loaded {len(df)} past '{alert_type}' alerts from {file_path}", 2, verbosity)
            else:
                 log_message(f"Warning: 'Date' column missing in {file_path}. Creating empty DataFrame.", 1, verbosity)
                 past_alerts[alert_type] = pd.DataFrame(columns=expected_cols)
        else:
            log_message(f"Past alerts file not found: {file_path}. Creating empty DataFrame.", 2, verbosity)
            past_alerts[alert_type] = pd.DataFrame(columns=expected_cols)
            
    return past_alerts

def suppress_alerts(new_alerts_df: pd.DataFrame, past_alerts_df: pd.DataFrame, suppression_days: int, id_cols: list, verbosity: int) -> pd.DataFrame:
    """Filters new alerts based on recent past alerts. Simplified version."""
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

    # Get unique keys (DeviceId, Phase/Detector) from recent alerts
    suppression_keys = recent_past_alerts[id_cols].drop_duplicates()
    log_message(f"Found {len(suppression_keys)} unique items for suppression based on the last {suppression_days} days.", 2, verbosity)

    # Perform suppression using merge
    merged = new_alerts_df.merge(suppression_keys, on=id_cols, how='left', indicator=True)
    suppressed_alerts_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    
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
         email_reports=None, config_path=None, config_dict=None): # Renamed parameter
    """
    Main function to run the signal analysis and generate report
    
    Args:
        use_parquet (bool): If True, read from parquet files, otherwise query database
        connection_params (dict): Database connection parameters if use_parquet is False
        num_figures (int): Number of figures to generate for each device
        email_reports (bool): If True, email reports instead of saving to disk # Renamed parameter
        config_path (str): Path to JSON config file
        config_dict (dict): Directly provided config dictionary
        
    Returns:
        tuple or None: If email_reports is True, returns (success, report_buffers, region_names)
                      If email_reports is False, returns (pdf_paths)
                      Returns None if data loading fails
    """
    # Load configuration
    cfg = load_config(config_path, config_dict)
    
    # Explicit parameters override config file
    use_parquet = use_parquet if use_parquet is not None else cfg.get('use_parquet', True)
    connection_params = connection_params or cfg.get('connection_params')
    num_figures = num_figures if num_figures is not None else cfg.get('num_figures', 1)
    email_reports = email_reports if email_reports is not None else cfg.get('email_reports', False) # Updated key and variable name
    signals_query = cfg.get('signals_query')
    verbosity = cfg.get('verbosity', 1) # Get verbosity from config, default to 1
    days_back = cfg.get('days_back', 21) # Get days_back from config, default to 21
    
    # Load new config parameters
    output_format = cfg.get('output_format', 'parquet')
    alert_suppression_days = cfg.get('alert_suppression_days', 21)
    alert_retention_weeks = cfg.get('alert_retention_weeks', 104)
    past_alerts_folder = cfg.get('past_alerts_folder', 'Past_Alerts')
    alert_flagging_days = cfg.get('alert_flagging_days', 7) # Load the new parameter

    # Validate connection params if using database
    if not use_parquet and (not connection_params or 
                           not all(k in connection_params for k in ['server', 'database', 'username'])):
        raise ValueError("Database connection requires 'server', 'database', and 'username' parameters")

    if not use_parquet and not signals_query:
        raise ValueError("Custom signals_query required in config.json when using database")

    log_message("Starting signal analysis...", 1, verbosity)

    # Create past alerts folder if it doesn't exist
    try:
        Path(past_alerts_folder).mkdir(parents=True, exist_ok=True)
        log_message(f"Ensured past alerts directory exists: {past_alerts_folder}", 2, verbosity)
    except Exception as e:
        log_message(f"Error creating past alerts directory {past_alerts_folder}: {e}", 1, verbosity)
        # Decide if we should stop or continue without saving/loading past alerts
        # For now, continue, but suppression/history won't work

    # Load past alerts
    past_alerts = load_past_alerts(past_alerts_folder, output_format, verbosity)
    
    # Get data
    log_message("Reading data...", 1, verbosity)
    maxout_df, actuations_df, signals_df, has_data_df = get_data(
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

    # Process max out data
    log_message("Processing max out data...", 1, verbosity)
    df2 = process_maxout_data(maxout_df)
    log_message(f"Processed max out data. Shape: {df2.shape}", 1, verbosity)
    
    # Process actuations data
    log_message("Processing actuations data...", 1, verbosity)
    detector_health = process_actuations_data(actuations_df)
    log_message(f"Processed actuations data. Shape: {detector_health.shape}", 1, verbosity)
    
    # Process missing data
    log_message("Processing missing data...", 1, verbosity)
    missing_data = process_missing_data(has_data_df)
    log_message(f"Processed missing data. Shape: {missing_data.shape}", 1, verbosity)

    # Calculate CUSUM and generate alerts
    log_message("Calculating CUSUM statistics...", 1, verbosity)
    t = cusum(df2, k_value=1)
    t_actuations = cusum(detector_health, k_value=1)
    t_missing_data = cusum(missing_data, k_value=1)
    log_message("CUSUM calculation complete", 1, verbosity)

    log_message("Generating alerts...", 1, verbosity)
    new_maxout_alerts = alert(t).execute()
    new_actuations_alerts = alert(t_actuations).execute()
    new_missing_data_alerts = alert(t_missing_data).execute()
    log_message(f"Generated alerts. Found {len(new_maxout_alerts)} phase alerts, {len(new_actuations_alerts)} detector alerts, and {len(new_missing_data_alerts)} missing data alerts", 1, verbosity)

    # Filter NEW alerts based on alert_flagging_days BEFORE suppression and saving
    log_message(f"Filtering newly generated alerts to the last {alert_flagging_days} days...", 1, verbosity)
    flagging_cutoff_date = datetime.now() - timedelta(days=alert_flagging_days)
    flagging_cutoff_date_naive = flagging_cutoff_date.replace(tzinfo=None)

    # Ensure 'Date' column is datetime before filtering
    # Use .copy() to avoid SettingWithCopyWarning
    new_maxout_alerts['Date'] = pd.to_datetime(new_maxout_alerts['Date']).dt.tz_localize(None)
    new_actuations_alerts['Date'] = pd.to_datetime(new_actuations_alerts['Date']).dt.tz_localize(None)
    new_missing_data_alerts['Date'] = pd.to_datetime(new_missing_data_alerts['Date']).dt.tz_localize(None)
    
    recent_new_maxout_alerts = new_maxout_alerts[new_maxout_alerts['Date'] >= flagging_cutoff_date_naive].copy()
    recent_new_actuations_alerts = new_actuations_alerts[new_actuations_alerts['Date'] >= flagging_cutoff_date_naive].copy()
    recent_new_missing_data_alerts = new_missing_data_alerts[new_missing_data_alerts['Date'] >= flagging_cutoff_date_naive].copy()
    
    log_message(f"Filtered new alerts. Keeping {len(recent_new_maxout_alerts)} phase, {len(recent_new_actuations_alerts)} detector, {len(recent_new_missing_data_alerts)} missing data alerts for suppression and saving.", 2, verbosity)

    # Suppress alerts using the RECENT NEW alerts
    log_message("Applying alert suppression...", 1, verbosity)
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
    log_message(f"Suppression complete. Reporting {len(final_maxout_alerts)} phase alerts, {len(final_actuations_alerts)} detector alerts, {len(final_missing_data_alerts)} missing data alerts.", 1, verbosity) # Updated log message

    # Create plots using FINAL (date-filtered and suppressed) alerts
    log_message("Creating visualization plots...", 1, verbosity)
    phase_figures = create_device_plots(final_maxout_alerts, signals_df, num_figures)
    detector_figures = create_device_plots(final_actuations_alerts, signals_df, num_figures)
    missing_data_figures = create_device_plots(final_missing_data_alerts, signals_df, num_figures)
    log_message("Plots created successfully", 1, verbosity)

    # Generate PDF reports using FINAL (date-filtered and suppressed) alerts
    log_message("Generating PDF reports...", 1, verbosity)
    report_result = None
    if email_reports: # Updated variable name
        # Generate reports in memory and email them
        log_message("Generating reports for email delivery...", 1, verbosity)
        report_buffers, region_names = generate_pdf_report(
            filtered_df=final_maxout_alerts, # Use final
            filtered_df_actuations=final_actuations_alerts, # Use final
            filtered_df_missing_data=final_missing_data_alerts, # Use final
            phase_figures=phase_figures,
            detector_figures=detector_figures,
            missing_data_figures=missing_data_figures,
            signals_df=signals_df,
            save_to_disk=False,
            verbosity=verbosity # Pass verbosity
        )
        
        # Email the reports
        log_message("Emailing reports...", 1, verbosity)
        success = email_reports(
            region_reports=report_buffers,
            regions=region_names,
            report_in_memory=True,
            verbosity=verbosity # Pass verbosity
        )
        
        if success:
            log_message("All reports were successfully emailed.", 1, verbosity)
        else:
            log_message("There were issues emailing some reports. Check the logs above for details.", 1, verbosity)
        
        report_result = success, report_buffers, region_names
    else:
        # Generate and save PDF reports to disk using final alerts
        pdf_paths = generate_pdf_report(
            filtered_df=final_maxout_alerts, # Use final
            filtered_df_actuations=final_actuations_alerts, # Use final
            filtered_df_missing_data=final_missing_data_alerts, # Use final
            phase_figures=phase_figures,
            detector_figures=detector_figures,
            missing_data_figures=missing_data_figures,
            signals_df=signals_df,
            save_to_disk=True,
            verbosity=verbosity # Pass verbosity
        )
        log_message(f"Generated {len(pdf_paths)} PDF reports:", 1, verbosity)
        for path in pdf_paths:
            log_message(f"- {path}", 1, verbosity)
        
        report_result = pdf_paths

    # Update and save past alerts using RECENT NEW alerts (date-filtered, pre-suppression)
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
        'missing_data', 
        alert_retention_weeks,
        verbosity
    )
    log_message("Past alerts history updated.", 1, verbosity)

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
            email_reports=email_reports_override, # Pass the override value
            num_figures=num_figures
            # Verbosity will be loaded inside main
        )
    except Exception as e:
        print(f"Error running script: {e}")
        raise  # Re-raise the exception to see the full traceback


if __name__ == "__main__":
    run_from_command_line()