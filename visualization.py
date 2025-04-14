import matplotlib.pyplot as plt
from typing import List
import pandas as pd

def create_device_plots(df: 'pd.DataFrame', signals_df: 'pd.DataFrame', num_figures: int) -> List['plt.Figure']:
    """Generate plots for each device's data
    
    Args:
        df: DataFrame containing either phase termination or detector data
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        num_figures: Number of top devices to generate plots for PER REGION
        
    Returns:
        List of tuples containing (matplotlib figure, region)
    """
    # Check if Percent MaxOut column exists
    if 'Percent MaxOut' in df.columns:
        group_column = 'Phase'
        value_column = 'Percent MaxOut'
        ranking_column = 'CUSUM_Percent MaxOut'
    else:
        group_column = 'Detector'
        value_column = 'Total'
        ranking_column = 'CUSUM_PercentAnomalous'
    
    # Colors for different phases/detectors
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']  # blue, orange, green
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
            numbers = device_data[group_column].unique()
            
            # Create the plot
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Plot each phase/detector
            for number, color in zip(sorted(numbers), colors):
                # Filter data for this phase/detector
                data = device_data[device_data[group_column] == number]
                
                # Plot values
                ax.plot(data['Date'], data[value_column], 
                        color=color, linewidth=2, label=f'{group_column} {number}')
                
                # Add alert points if they exist
                alerts = data[data['Alert'] == 1]
                if len(alerts) > 0:
                    ax.scatter(alerts['Date'], alerts[value_column], 
                              color=color, s=100, marker='.', 
                              label=f'{group_column} {number} Alerts')
            
            # Customize the plot
            ax.set_title(f'{group_column} Over Time\n{name}',
                        pad=20)
            ax.set_xlabel('Date')
            ax.set_ylabel(value_column)
            ax.grid(True, alpha=0.3, linestyle='--')
            ax.legend()
            
            # Rotate x-axis labels for better readability
            plt.xticks(rotation=45)
            
            # Add light gray background grid
            ax.set_axisbelow(True)
            
            # Adjust layout to prevent label cutoff
            plt.tight_layout()
            
            figures.append((fig, region))
    
    return figures