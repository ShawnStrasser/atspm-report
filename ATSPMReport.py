from datetime import datetime
import argparse
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


def main(use_parquet=True, connection_params=None, num_figures=1, email_reports_flag=False):
    """Main function to run the signal analysis and generate report
    Args:
        use_parquet (bool): If True, read from parquet files, otherwise query database
        connection_params (dict): Database connection parameters if use_parquet is False
        num_figures (int): Number of figures to generate for each device
        email_reports_flag (bool): If True, email reports instead of saving to disk
    """
    print("Starting signal analysis...")
    
    # Get data
    print("Reading data...")
    maxout_df, actuations_df, signals_df, has_data_df = get_data(use_parquet, connection_params)
    if maxout_df is None or actuations_df is None or signals_df is None or has_data_df is None:
        print("Failed to get data")
        return
    print(f"Successfully read data. MaxOut shape: {maxout_df.shape}, Actuations shape: {actuations_df.shape}, Signals shape: {signals_df.shape}, Has Data shape: {has_data_df.shape}")

    # Process max out data
    print("Processing max out data...")
    df2 = process_maxout_data(maxout_df)
    print(f"Processed max out data. Shape: {df2.shape}")
    
    # Process actuations data
    print("Processing actuations data...")
    detector_health = process_actuations_data(actuations_df)
    print(f"Processed actuations data. Shape: {detector_health.shape}")
    
    # Process missing data
    print("Processing missing data...")
    missing_data = process_missing_data(has_data_df)
    print(f"Processed missing data. Shape: {missing_data.shape}")

    # Calculate CUSUM and generate alerts
    print("Calculating CUSUM statistics...")
    t = cusum(df2, k_value=1)
    t_actuations = cusum(detector_health, k_value=1)
    t_missing_data = cusum(missing_data, k_value=1)
    print("CUSUM calculation complete")

    print("Generating alerts...")
    filtered_df = alert(t).execute()
    filtered_df_actuations = alert(t_actuations).execute()
    filtered_df_missing_data = alert(t_missing_data).execute()
    print(f"Alert generation complete. Found {len(filtered_df)} phase alerts, {len(filtered_df_actuations)} detector alerts, and {len(filtered_df_missing_data)} missing data alerts")

    # Create plots
    print("Creating visualization plots...")
    phase_figures = create_device_plots(filtered_df, signals_df, num_figures)
    detector_figures = create_device_plots(filtered_df_actuations, signals_df, num_figures)
    missing_data_figures = create_device_plots(filtered_df_missing_data, signals_df, num_figures)
    print("Plots created successfully")

    # Generate PDF reports for each region
    print("Generating PDF reports...")
    
    if email_reports_flag:
        # Generate reports in memory and email them
        print("Generating reports for email delivery...")
        report_buffers, region_names = generate_pdf_report(
            filtered_df=filtered_df,
            filtered_df_actuations=filtered_df_actuations,
            filtered_df_missing_data=filtered_df_missing_data,
            phase_figures=phase_figures,
            detector_figures=detector_figures,
            missing_data_figures=missing_data_figures,
            signals_df=signals_df,
            save_to_disk=False
        )
        
        # Email the reports
        print("Emailing reports...")
        success = email_reports(
            region_reports=report_buffers,
            regions=region_names,
            report_in_memory=True
        )
        
        if success:
            print("All reports were successfully emailed.")
        else:
            print("There were issues emailing some reports. Check the logs above for details.")
    else:
        # Generate and save PDF reports to disk
        pdf_paths = generate_pdf_report(
            filtered_df=filtered_df,
            filtered_df_actuations=filtered_df_actuations,
            filtered_df_missing_data=filtered_df_missing_data,
            phase_figures=phase_figures,
            detector_figures=detector_figures,
            missing_data_figures=missing_data_figures,
            signals_df=signals_df,
            save_to_disk=True
        )
        print(f"Generated {len(pdf_paths)} PDF reports:")
        for path in pdf_paths:
            print(f"- {path}")


if __name__ == "__main__":
    try:
        # Set up argument parser
        parser = argparse.ArgumentParser(description='ATSPM Report Generator')
        parser.add_argument('--email', action='store_true', 
                           help='Email reports instead of saving them to disk')
        parser.add_argument('--figures', type=int, default=1,
                           help='Number of figures to generate per device (default: 1)')
        parser.add_argument('--use-database', action='store_true',
                           help='Query database instead of using parquet files')
        
        args = parser.parse_args()
        
        # Example usage with command line arguments
        main(
            use_parquet=not args.use_database,
            num_figures=args.figures,
            email_reports_flag=args.email
        )
    except Exception as e:
        print(f"Error running script: {e}")
        raise  # Re-raise the exception to see the full traceback