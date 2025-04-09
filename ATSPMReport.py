from datetime import datetime
from data_access import get_data
from data_processing import (
    process_maxout_data,
    process_actuations_data,
    cusum,
    alert
)
from visualization import create_device_plots
from report_generation import generate_pdf_report


def main(use_parquet=True, connection_params=None):
    """Main function to run the signal analysis and generate report"""
    print("Starting signal analysis...")
    
    # Get data
    print("Reading data...")
    maxout_df, actuations_df = get_data(use_parquet, connection_params)
    if maxout_df is None or actuations_df is None:
        print("Failed to get data")
        return
    print(f"Successfully read data. MaxOut shape: {maxout_df.shape}, Actuations shape: {actuations_df.shape}")

    # Process max out data
    print("Processing max out data...")
    df2 = process_maxout_data(maxout_df)
    print(f"Processed max out data. Shape: {df2.shape}")
    
    # Process actuations data
    print("Processing actuations data...")
    detector_health = process_actuations_data(actuations_df)
    print(f"Processed actuations data. Shape: {detector_health.shape}")

    # Calculate CUSUM and generate alerts
    print("Calculating CUSUM statistics...")
    t = cusum(df2, k_value=1)
    t_actuations = cusum(detector_health, k_value=1)
    print("CUSUM calculation complete")

    print("Generating alerts...")
    filtered_df = alert(t).execute()
    filtered_df_actuations = alert(t_actuations).execute()
    print(f"Alert generation complete. Found {len(filtered_df)} phase alerts and {len(filtered_df_actuations)} detector alerts")

    # Create plots
    print("Creating visualization plots...")
    phase_figures = create_device_plots(filtered_df)
    detector_figures = create_device_plots(filtered_df_actuations)
    print("Plots created successfully")

    # Generate PDF report
    print("Generating PDF report...")
    pdf_path = generate_pdf_report(
        filtered_df=filtered_df,
        filtered_df_actuations=filtered_df_actuations,
        phase_figures=phase_figures,
        detector_figures=detector_figures
    )
    print(f"PDF report generated at: {pdf_path}")


if __name__ == "__main__":
    try:
        # Example usage with parquet files (default)
        main()
    except Exception as e:
        print(f"Error running script: {e}")
        raise  # Re-raise the exception to see the full traceback

    # Example usage with database connection
    # connection_params = {
    #     'server': 'kinsigsynapseprod-ondemand.sql.azuresynapse.net',
    #     'database': 'PerformanceMetrics',
    #     'username': 'user@domain.gov'
    # }
    # main(use_parquet=False, connection_params=connection_params)