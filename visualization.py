import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from typing import List, Optional, Tuple
import pandas as pd

def create_device_plots(df_daily: 'pd.DataFrame', signals_df: 'pd.DataFrame', num_figures: int, 
                        df_hourly: Optional['pd.DataFrame'] = None) -> List[Tuple['plt.Figure', str]]:
    """Generate plots for each device's data
    
    Args:
        df_daily: DataFrame containing daily aggregated data for either phase termination, detector data, or missing data
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        num_figures: Number of top devices to generate plots for PER REGION
        df_hourly: Optional DataFrame containing hourly aggregated data. If provided, this will be used for 
                  plotting instead of daily data. The datetime column for this DataFrame is 'TimeStamp'.
        
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
    if 'PedActuation' in df_hourly.columns if df_hourly is not None else False:
        # Pedestrian data handling
        group_column = 'Phase'
        left_value_column = 'PedServices'
        right_value_column = 'PedActuation'
        plot_title = 'Pedestrian Services and Actuations'
        y_label_left = 'Ped Services'
        y_label_right = 'Ped Actuations'
        use_percent_format = False
        fixed_y_limits = None
        is_ped_data = True
    elif 'Percent MaxOut' in df_daily.columns:
        group_column = 'Phase'
        value_column = 'Percent MaxOut'
        ranking_column = 'CUSUM_Percent MaxOut'
        plot_title = 'Phase Termination'
        y_label = 'Percent MaxOut'
        use_percent_format = True
        fixed_y_limits = (-0.02, 1.02)
        is_ped_data = False
    elif 'PercentAnomalous' in df_daily.columns:
        group_column = 'Detector'
        value_column = 'Total'  # Changed from PercentAnomalous to Total
        ranking_column = 'CUSUM_PercentAnomalous'
        plot_title = 'Detector Actuations'
        # Adjust y_label based on hourly or daily data - will be set later
        y_label_daily = 'Daily Total'
        y_label_hourly = 'Hourly Total'
        use_percent_format = False
        fixed_y_limits = None
        is_ped_data = False
    elif 'MissingData' in df_daily.columns:
        group_column = None
        value_column = 'MissingData'
        ranking_column = 'CUSUM_MissingData'
        plot_title = 'Missing Data'
        y_label = 'Missing Data'
        use_percent_format = True
        fixed_y_limits = (-0.02, 1.02)
        is_ped_data = False
    else:
        raise ValueError("Unknown data format in DataFrame")
    
    # Colors for different phases/detectors - using more distinct colors
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
    figures = []  # Store figures for PDF generation
    figures_with_rank = []  # Store figures with ranking information for sorting

    # Process each region separately
    regions = list(signals_df['Region'].unique())
    regions.append('All Regions')  # Append "All" as an additional region
    print(f'Making visuals for {len(regions)} regions: {regions}')
    
    for region in regions:
        if region == "All Regions":
            # For "All" region, include all devices
            region_signals = signals_df
            region_devices = signals_df['DeviceId'].unique()
        else:
            # For specific regions, filter as before
            region_signals = signals_df[signals_df['Region'] == region]
            region_devices = region_signals['DeviceId'].unique()
        
        # Filter data for this region's devices
        region_data = df_daily[df_daily['DeviceId'].isin(region_devices)]
        
        # For missing data, handle differently - create one chart with multiple devices
        if 'MissingData' in df_daily.columns:
            # Find devices with alerts or high ranking values
            if 'Alert' in region_data.columns:
                devices_with_alerts = region_data[region_data['Alert'] == 1]['DeviceId'].unique()
            else:
                # If no Alert column, use ranking values
                device_rankings = region_data.groupby('DeviceId')[ranking_column].max().sort_values(ascending=False)
                devices_with_alerts = device_rankings.head(10).index
            
            # If there are devices with alerts, create a single chart for all of them (up to 10)
            if len(devices_with_alerts) > 0:
                devices_to_plot = devices_with_alerts[:10]  # Limit to 10 devices
                devices_info = region_signals[region_signals['DeviceId'].isin(devices_to_plot)]
                
                # Create a single plot for all devices
                fig, ax = plt.subplots(figsize=(12, 6))
                
                # Determine which dataset to use for plotting
                if df_hourly is not None:
                    plot_data = df_hourly[df_hourly['DeviceId'].isin(devices_to_plot)]
                    time_column = 'TimeStamp'
                else:
                    plot_data = region_data[region_data['DeviceId'].isin(devices_to_plot)]
                    time_column = 'Date'
                
                # Get the min and max dates for the device data to set x-axis limits
                min_date = plot_data[time_column].min()
                max_date = plot_data[time_column].max()
                
                # Plot each device
                for i, device_id in enumerate(devices_to_plot):
                    device_name = region_signals[region_signals['DeviceId'] == device_id]['Name'].iloc[0]
                    device_data = plot_data[plot_data['DeviceId'] == device_id]
                    
                    # Plot with device name in legend
                    ax.plot(device_data[time_column], device_data[value_column], 
                            color=colors[i % len(colors)], linewidth=2.5, 
                            label=f'{device_name}')
                    
                    # Add markers where alerts occur (only if using daily data)
                    if df_hourly is None and 'Alert' in device_data.columns:
                        alerts = device_data[device_data['Alert'] == 1]
                        if not alerts.empty:
                            ax.scatter(alerts[time_column], alerts[value_column], 
                                      color=colors[i % len(colors)], s=80, zorder=10)
                
                # Set the x-axis limits to match the data range exactly
                ax.set_xlim(min_date, max_date)
                
                # Determine time granularity for the title
                time_granularity = "Hourly" if df_hourly is not None else "Daily"
                
                # Customize the plot with improved styling
                ax.set_title(f'{time_granularity} {plot_title} - {region}',
                            pad=20, fontweight='bold')
                
                ax.set_ylabel(y_label, fontweight='bold')
                ax.grid(True, alpha=0.3, linestyle='--')
                
                # Add legend
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
            
            continue  # Skip the rest of the loop for missing data charts
            
        # For pedestrian data, process differently
        if is_ped_data:
            # Use the first num_figures devices from df_daily for this region
            region_data = df_daily[df_daily['DeviceId'].isin(region_devices)]
            unique_devices = region_data['DeviceId'].unique()
            top_devices = unique_devices[:min(num_figures, len(unique_devices))]
            devices_info = region_signals[region_signals['DeviceId'].isin(top_devices)]
            
            # Create a plot for each DeviceId
            for device_id in top_devices:
                device_info = devices_info[devices_info['DeviceId'] == device_id].iloc[0]
                device = device_info['DeviceId']
                name = device_info['Name']
                
                # Filter hourly data for this device
                plot_data = df_hourly[df_hourly['DeviceId'] == device]
                time_column = 'TimeStamp'
                
                # Skip if no data
                if plot_data.empty:
                    continue
                
                # Create the plot with a bigger figure size for better readability
                fig, ax1 = plt.subplots(figsize=(10, 5))
                ax2 = ax1.twinx()  # Create a second y-axis
                
                # Get the min and max dates for the device data to set x-axis limits
                min_date = plot_data[time_column].min()
                max_date = plot_data[time_column].max()
                
                # Get all phases for this device
                phases = sorted(plot_data[group_column].unique())
                
                # Plot each phase with both services and actuations
                for i, phase in enumerate(phases):
                    # Filter data for this phase
                    phase_data = plot_data[plot_data[group_column] == phase]
                    color = colors[i % len(colors)]
                    
                    # Plot PedServices on left y-axis with solid line
                    ax1.plot(phase_data[time_column], phase_data[left_value_column], 
                             color=color, linewidth=2.5, 
                             label=f'Phase {phase} - Services',
                             linestyle='-')
                    
                    # Plot PedActuation on right y-axis with dashed line
                    ax2.plot(phase_data[time_column], phase_data[right_value_column], 
                             color=color, linewidth=2.0, 
                             label=f'Phase {phase} - Actuations',
                             linestyle='--', 
                             dashes=(4, 2))  # Custom dash pattern for better visibility
                
                # Set axes labels
                ax1.set_ylabel(y_label_left, fontweight='bold', color='black')
                ax2.set_ylabel(y_label_right, fontweight='bold', color='black')
                
                # Set the x-axis limits to match the data range exactly
                ax1.set_xlim(min_date, max_date)
                
                # Customize the plot with improved styling
                ax1.set_title(f'Hourly {plot_title}\n{name}',
                              pad=20, fontweight='bold')
                
                ax1.grid(True, alpha=0.3, linestyle='--')
                
                # Combine legends from both axes
                lines1, labels1 = ax1.get_legend_handles_labels()
                lines2, labels2 = ax2.get_legend_handles_labels()
                
                # Create custom legend entries to show solid/dashed line styles for services/actuations
                from matplotlib.lines import Line2D
                legend_elements = []
                for i, phase in enumerate(phases):
                    color = colors[i % len(colors)]
                    # Add services line (solid)
                    legend_elements.append(Line2D([0], [0], color=color, linewidth=2.5,
                                                  label=f'Phase {phase} - Services'))
                    # Add actuations line (dashed)
                    legend_elements.append(Line2D([0], [0], color=color, linewidth=2.0,
                                                  linestyle='--', dashes=(4, 2),
                                                  label=f'Phase {phase} - Actuations'))
                
                # Add the combined legend
                ax1.legend(handles=legend_elements, frameon=True, fancybox=True, framealpha=0.9,
                           loc='upper left', bbox_to_anchor=(0.01, 1))
                
                # Rotate x-axis labels for better readability and add padding
                plt.xticks(rotation=45, ha='right')
                
                # Add light gray background
                ax1.set_facecolor('#f8f8f8')  # Very light gray background
                
                # Add border around plot
                for spine in ax1.spines.values():
                    spine.set_linewidth(1.2)
                
                # Format ticks on both y-axes to show integers
                ax1.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
                ax2.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
                
                # Adjust layout to prevent label cutoff with more padding
                plt.tight_layout(pad=2.0)
                
                # Add to figures list
                figures.append((fig, region))
                
            # Continue to the next region after processing pedestrian data
            continue
            
        # For other chart types, find top N devices for this region based on maximum ranking values
        device_rankings = region_data.groupby('DeviceId')[ranking_column].max().sort_values(ascending=False)
        
        # Filter devices to only include those with alerts if possible
        devices_with_alerts = []
        if 'Alert' in region_data.columns:
            devices_with_alerts = region_data[region_data['Alert'] == 1]['DeviceId'].unique()
            # Sort the devices with alerts by their ranking
            if len(devices_with_alerts) > 0:
                sorted_devices = device_rankings[device_rankings.index.isin(devices_with_alerts)]
                devices_with_alerts = sorted_devices.index.tolist()
        
        # Use devices with alerts first, then add other devices if needed up to num_figures
        if len(devices_with_alerts) > 0:
            top_devices = devices_with_alerts[:num_figures]  # Only use up to num_figures
        else:
            # If no alerts, use the top devices by ranking
            top_devices = device_rankings.head(num_figures).index.tolist()
        
        devices_info = region_signals[region_signals['DeviceId'].isin(top_devices)]
        
        # Create a plot for each DeviceId in this region, in order of severity
        for device_id in top_devices:
            device_info = devices_info[devices_info['DeviceId'] == device_id].iloc[0]
            device = device_info['DeviceId']
            name = device_info['Name']
            
            # Determine which dataset to use for plotting
            if df_hourly is not None:
                # Use hourly data for plotting
                plot_data = df_hourly[df_hourly['DeviceId'] == device]
                time_column = 'TimeStamp'
                # Set the correct y-label for hourly data if it's detector health
                if 'PercentAnomalous' in df_daily.columns:
                    y_label = y_label_hourly
            else:
                # Use daily data for plotting
                plot_data = df_daily[df_daily['DeviceId'] == device]
                time_column = 'Date'
                # Set the correct y-label for daily data if it's detector health
                if 'PercentAnomalous' in df_daily.columns:
                    y_label = y_label_daily
            
            # Create the plot with a bigger figure size for better readability
            fig, ax = plt.subplots(figsize=(10, 5))
            
            # Get the min and max dates for the device data to set x-axis limits
            min_date = plot_data[time_column].min()
            max_date = plot_data[time_column].max()
            
            if group_column:
                # For detector health, filter detectors differently based on if using hourly data
                if df_hourly is not None and 'PercentAnomalous' in df_daily.columns:
                    # For hourly detector health charts, identify detectors in daily data
                    daily_data = df_daily[df_daily['DeviceId'] == device]
                    detectors_in_daily = set(daily_data[group_column].unique())
                    
                    # Get all detectors in hourly data
                    all_detectors = sorted(plot_data[group_column].unique())
                    
                    # First plot all 'other' detectors with low zorder to ensure they're below important detectors
                    other_detector_lines = []
                    for number in all_detectors:
                        if number not in detectors_in_daily:
                            # Plot other detectors in light gray, thinner line, with low zorder
                            data = plot_data[plot_data[group_column] == number]
                            line = ax.plot(data[time_column], data[value_column], 
                                    color='#cccccc', linewidth=1.5, alpha=0.7, zorder=1)
                            other_detector_lines.append(line[0])
                    
                    # Create a list to store all legend handles and labels
                    handles, labels = [], []
                    
                    # Then plot important detectors with higher zorder to ensure they're on top
                    for i, number in enumerate(sorted(list(detectors_in_daily))):
                        # Filter data for this detector
                        data = plot_data[plot_data[group_column] == number]
                        color = colors[i % len(colors)]
                        
                        # Plot actual values with solid line
                        line = ax.plot(data[time_column], data[value_column], 
                                color=color, linewidth=2.5, zorder=5,
                                label=f'{group_column} {number}')
                        handles.append(line[0])
                        labels.append(f'{group_column} {number}')
                        
                        # Plot forecast values with dotted line of same color if Forecast column exists
                        if 'Forecast' in data.columns:
                            ax.plot(data[time_column], data['Forecast'], 
                                    color=color, linewidth=1.5, linestyle='dotted', zorder=4)
                    
                    # Add "Other Detectors" to the legend
                    if other_detector_lines:
                        other_detector_lines[0].set_label("Other Detectors")
                    
                    # Add the legend
                    ax.legend(frameon=True, fancybox=True, framealpha=0.9, 
                              loc='upper left', bbox_to_anchor=(0.01, 1))
                    
                    # Add a note about forecasts below the plot
                    if 'Forecast' in plot_data.columns:
                        fig.text(0.5, 0.01, 'Note: Dotted lines represent forecasted values from historical data', 
                                 ha='center', fontsize=10, color='gray')
                else:
                    # For all other chart types, plot normally
                    numbers = plot_data[group_column].unique()
                    
                    # Plot each phase/detector
                    for i, number in enumerate(sorted(numbers)):
                        # Filter data for this phase/detector
                        data = plot_data[plot_data[group_column] == number]
                        
                        # Plot values with thicker lines
                        ax.plot(data[time_column], data[value_column], 
                                color=colors[i % len(colors)], linewidth=2.5, 
                                label=f'{group_column} {number}')
                        
                        # Add markers where alerts occur (only if using daily data)
                        if df_hourly is None and 'Alert' in data.columns:
                            alerts = data[data['Alert'] == 1]
                            if not alerts.empty:
                                ax.scatter(alerts[time_column], alerts[value_column], 
                                          color=colors[i % len(colors)], s=80, zorder=10)
            else:
                # For Missing Data (no group column)
                ax.plot(plot_data[time_column], plot_data[value_column], 
                        color='#1f77b4', linewidth=2.5)
                
                # Add markers where alerts occur (only if using daily data)
                if df_hourly is None and 'Alert' in plot_data.columns:
                    alerts = plot_data[plot_data['Alert'] == 1]
                    if not alerts.empty:
                        ax.scatter(alerts[time_column], alerts[value_column], 
                                  color='#1f77b4', s=80, zorder=10)
            
            # Set the x-axis limits to match the data range exactly
            ax.set_xlim(min_date, max_date)
            
            # Determine time granularity for the title
            time_granularity = "Hourly" if df_hourly is not None else "Daily"
            
            # Customize the plot with improved styling
            ax.set_title(f'{time_granularity} {plot_title}\n{name}',
                        pad=20, fontweight='bold')
            
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
            
            # Store device ranking with figure for later sorting
            device_rank = device_rankings.get(device, 0)
            figures_with_rank.append((fig, region, device_rank))
    
    # Sort figures by device ranking (highest/worst first)
    sorted_figures_with_rank = sorted(figures_with_rank, key=lambda x: x[2], reverse=True)
    
    # Add the sorted ranked figures to the main figures list
    for fig, region, _ in sorted_figures_with_rank:
        figures.append((fig, region))
    
    return figures
