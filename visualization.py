import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from typing import List
import pandas as pd

def create_device_plots(df: 'pd.DataFrame', signals_df: 'pd.DataFrame', num_figures: int) -> List['plt.Figure']:
    """Generate plots for each device's data
    
    Args:
        df: DataFrame containing either phase termination, detector data, or missing data
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        num_figures: Number of top devices to generate plots for PER REGION
        
    Returns:
        List of tuples containing (matplotlib figure, region)
    """
    # Set larger font sizes for better readability
    plt.rcParams.update({
        'font.size': 12,
        'axes.titlesize': 16,
        'axes.labelsize': 14,
        'xtick.labelsize': 12,
        'ytick.labelsize': 12,
        'legend.fontsize': 12
    })
    
    # Determine the type of data and set appropriate columns
    if 'Percent MaxOut' in df.columns:
        group_column = 'Phase'
        value_column = 'Percent MaxOut'
        ranking_column = 'CUSUM_Percent MaxOut'
        plot_title = 'Phase Termination'
        y_label = 'Percent MaxOut'
        use_percent_format = True
        fixed_y_limits = (0, 1)
    elif 'PercentAnomalous' in df.columns:
        group_column = 'Detector'
        value_column = 'Total'  # Changed from PercentAnomalous to Total
        ranking_column = 'CUSUM_PercentAnomalous'
        plot_title = 'Detector Health'
        y_label = 'Daily Total Actuations'
        use_percent_format = False
        fixed_y_limits = None
    elif 'MissingData' in df.columns:
        group_column = None
        value_column = 'MissingData'
        ranking_column = 'CUSUM_MissingData'
        plot_title = 'Missing Data'
        y_label = 'Missing Data'
        use_percent_format = True
        fixed_y_limits = (0, 1)
    else:
        raise ValueError("Unknown data format in DataFrame")
    
    # Colors for different phases/detectors - using more distinct colors
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
    figures = []  # Store figures for PDF generation

    # Process each region separately
    for region in signals_df['Region'].unique():
        # Get devices for this region
        region_signals = signals_df[signals_df['Region'] == region]
        region_devices = region_signals['DeviceId'].unique()
        
        # Filter data for this region's devices
        region_data = df[df['DeviceId'].isin(region_devices)]
        
        # Find top N devices for this region based on maximum ranking values
        device_rankings = region_data.groupby('DeviceId')[ranking_column].max().sort_values(ascending=False)
        top_devices = device_rankings.head(num_figures).index
        devices_info = region_signals[region_signals['DeviceId'].isin(top_devices)]
        
        # Create a plot for each DeviceId in this region
        for _, device_info in devices_info.iterrows():
            device = device_info['DeviceId']
            name = device_info['Name']
            
            # Filter data for this device
            device_data = df[df['DeviceId'] == device]
            
            # Create the plot with a bigger figure size for better readability
            fig, ax = plt.subplots(figsize=(10, 5))
            
            if group_column:
                # For data with group columns (Phase or Detector)
                numbers = device_data[group_column].unique()
                
                # Plot each phase/detector
                for i, number in enumerate(sorted(numbers)):
                    # Filter data for this phase/detector
                    data = device_data[device_data[group_column] == number]
                    
                    # Plot values with thicker lines
                    ax.plot(data['Date'], data[value_column], 
                            color=colors[i % len(colors)], linewidth=2.5, 
                            label=f'{group_column} {number}')
                    
                    # Add markers where alerts occur
                    alerts = data[data['Alert'] == 1]
                    if not alerts.empty:
                        ax.scatter(alerts['Date'], alerts[value_column], 
                                  color=colors[i % len(colors)], s=80, zorder=10)
            else:
                # For Missing Data (no group column)
                ax.plot(device_data['Date'], device_data[value_column], 
                        color='#1f77b4', linewidth=2.5)
                
                # Add markers where alerts occur
                alerts = device_data[device_data['Alert'] == 1]
                if not alerts.empty:
                    ax.scatter(alerts['Date'], alerts[value_column], 
                              color='#1f77b4', s=80, zorder=10)
            
            # Customize the plot with improved styling
            ax.set_title(f'{plot_title} Over Time\n{name}',
                        pad=20, fontweight='bold')
            
            # REMOVED date label from x-axis
            # ax.set_xlabel('Date', fontweight='bold')
            
            ax.set_ylabel(y_label, fontweight='bold')
            ax.grid(True, alpha=0.3, linestyle='--')
            
            # Always show legend for phase termination and detector health charts
            # Moved legend to left side
            if group_column:
                ax.legend(frameon=True, fancybox=True, framealpha=0.9, 
                         loc='upper left', bbox_to_anchor=(0.01, 1))
            
            # Set fixed y-axis limits if specified
            if fixed_y_limits:
                ax.set_ylim(fixed_y_limits)
            
            # Format y-axis as percentage if needed
            if use_percent_format:
                ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
            
            # Rotate x-axis labels for better readability and add padding
            plt.xticks(rotation=45, ha='right')
            
            # Add light gray background grid
            ax.set_axisbelow(True)
            ax.set_facecolor('#f8f8f8')  # Very light gray background
            
            # Add border around plot
            for spine in ax.spines.values():
                spine.set_linewidth(1.2)
            
            # Adjust layout to prevent label cutoff with more padding
            plt.tight_layout(pad=2.0)
            
            figures.append((fig, region))
    
    return figures