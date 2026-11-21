import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter
from typing import List, Optional, Tuple
import numpy as np
import pandas as pd
import warnings
import math
import textwrap
import re

from .focus_overview import _centred, day_time_ticks, walk_share
from .preempt_cascade import DIRECTION_FALSE_CALLS, bus_calls_text

warnings.filterwarnings('ignore', message='More than.*figures have been opened') # default is 20 and thats too low



# Vertical room a one-row legend needs between the axes and the title, in points,
# plus what each extra wrapped row adds.
_LEGEND_TITLE_PAD = 34
_LEGEND_ROW_HEIGHT = 16
_LEGEND_MAX_COLUMNS = 4


def _legend_above(ax, handles=None, labels=None, ncol=None):
    """Put the legend above the plot, under the title, rather than over the data.

    Anchored inside the axes it routinely sat over the first hours of the series,
    which on these charts is exactly where the interesting peaks tend to be. The
    title is pushed up to make room, since otherwise the two land on each other.
    """
    if handles is None:
        handles, labels = ax.get_legend_handles_labels()
    if not handles:
        return None

    # Fit the legend to the axes width. A legend wider than the axes makes
    # tight_layout (and bbox_inches='tight' at save time) squeeze the plot into
    # the left part of the image, so drop columns until it fits, and wrap labels
    # if even a single column is too wide.
    fig = ax.figure
    renderer = fig.canvas.get_renderer()
    axes_width = ax.get_window_extent(renderer).width
    labels = list(labels)
    column_options = [ncol] if ncol else range(min(len(handles), _LEGEND_MAX_COLUMNS), 0, -1)
    wrap_widths = [None, 40, 30, 20]
    legend = None
    for wrap in wrap_widths:
        shown = labels if wrap is None else [textwrap.fill(str(l), wrap) for l in labels]
        for n in column_options:
            if legend is not None:
                legend.remove()
            legend = ax.legend(
                handles, shown,
                frameon=False,
                loc='lower left',
                bbox_to_anchor=(0, 1.01),
                borderaxespad=0,
                ncol=n,
                fontsize=11,
                handletextpad=0.5,
                columnspacing=1.4,
            )
            if legend.get_window_extent(renderer).width <= axes_width:
                break
        else:
            continue
        break
    # Vertical room is reserved through the title pad below, so keep the legend
    # out of the layout engine's width calculation.
    legend.set_in_layout(False)

    # The title has to clear however many rows the legend ended up using.
    legend_height_pts = legend.get_window_extent(renderer).height * 72 / fig.dpi
    title_text = ax.get_title()
    if title_text:
        ax.set_title(
            title_text,
            pad=max(_LEGEND_TITLE_PAD, legend_height_pts + _LEGEND_TITLE_PAD - _LEGEND_ROW_HEIGHT),
            fontweight=ax.title.get_fontweight(),
            fontsize=ax.title.get_fontsize(),
        )
    return legend

def _hidden_in(exclude: Optional[dict], region) -> set:
    """The signals a report does not chart: exclude holds {report: signals}, those its Signal Focus lists."""
    return (exclude or {}).get(region, set())


def new_figure(figsize, dpi=None) -> Figure:
    """A new figure on an Agg canvas, made without pyplot.

    The report only saves its charts as images. A pyplot figure also gets a window
    from an interactive backend (Tk on Windows), which costs more than the drawing.
    """
    figure = Figure(figsize=figsize, dpi=dpi)
    FigureCanvasAgg(figure)
    return figure


def _rotate_x_labels(ax: 'plt.Axes', **kwargs) -> None:
    """Rotate the x tick labels 45 degrees, right aligned (plt.xticks without pyplot)."""
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', **kwargs)


def _format_time_axis(ax: 'plt.Axes', timestamps: 'pd.Series') -> None:
    """Format time axes consistently across daily, intraday, and multi-day charts."""
    valid_timestamps = pd.Series(pd.to_datetime(timestamps, errors='coerce')).dropna()
    if valid_timestamps.empty:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        return

    normalized_timestamps = valid_timestamps.dt.normalize()
    spans_multiple_days = normalized_timestamps.nunique() > 1
    date_only_values = (valid_timestamps == normalized_timestamps).all()

    if spans_multiple_days and date_only_values:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b-%d'))
    elif spans_multiple_days:
        def format_tick(value, position):
            tick_datetime = mdates.num2date(value)
            if tick_datetime.hour == 0 and tick_datetime.minute == 0 and tick_datetime.second == 0:
                return tick_datetime.strftime('%b-%d')
            return tick_datetime.strftime('%b-%d\n%H:%M')

        ax.xaxis.set_major_formatter(FuncFormatter(format_tick))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())


def create_device_plots(df_daily: 'pd.DataFrame', signals_df: 'pd.DataFrame', num_figures: int, 
                        df_hourly: Optional['pd.DataFrame'] = None,
                        exclude: Optional[dict] = None) -> List[Tuple['plt.Figure', str]]:
    """Generate plots for each device's data
    
    Args:
        df_daily: DataFrame containing daily aggregated data for either phase termination, pedestrian, or missing data
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        num_figures: Number of top devices to generate plots for PER REGION
        df_hourly: Optional DataFrame containing hourly aggregated data. If provided, this will be used for 
                  plotting instead of daily data. The datetime column for this DataFrame is 'TimeStamp'.
        exclude: Optional {report: signals} that report leaves out (its Signal Focus lists them)
        
    Returns:
        List of tuples containing (matplotlib figure, region)
    """
    # No daily alert rows means there is nothing to visualize for this alert type.
    if df_daily is None or df_daily.empty:
        return []

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
        plot_title = 'Pedestrian Services and Actuations per Service'
        y_label_left = 'Ped Services'
        y_label_right = 'Act/Service'
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
    # Colorblind-friendly palette, avoiding yellow for better contrast on white
    colors = ['#E41A1C', '#377EB8', '#4DAF4A', '#984EA3', '#FF7F00', '#A65628', '#F781BF', '#17BECF']
    # Create a consistent color mapping for phases
    if group_column == 'Phase':
        daily_phases = set(df_daily[group_column].unique())
        hourly_phases = set(df_hourly[group_column].unique()) if df_hourly is not None and group_column in df_hourly.columns else set()
        all_phases = sorted(daily_phases.union(hourly_phases))
        phase_color_map = {phase: colors[i % len(colors)] for i, phase in enumerate(all_phases)}
    figures = []  # Store figures for PDF generation
    figures_with_rank = []  # Store figures with ranking information for sorting
    # A device's chart is the same in its region's report and in All Regions,
    # so it is drawn once and listed for both.
    device_figures = {}

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
        
        region_devices = [device for device in region_devices if device not in _hidden_in(exclude, region)]

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
                fig = new_figure((12, 6))
                ax = fig.subplots()
                
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
                            pad=_LEGEND_TITLE_PAD, fontweight='bold')
                
                ax.set_ylabel(y_label, fontweight='bold')
                ax.grid(True, alpha=0.3, linestyle='--')
                
                # Add legend
                _legend_above(ax)

                # Set fixed y-axis limits if specified
                if fixed_y_limits:
                    ax.set_ylim(fixed_y_limits)
                
                # Format y-axis as percentage if needed
                if use_percent_format:
                    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
                
                _format_time_axis(ax, plot_data[time_column])
                
                # Rotate x-axis labels for better readability and add padding
                _rotate_x_labels(ax)
                
                # Add light gray background grid
                ax.set_axisbelow(True)
                ax.set_facecolor('#f8f8f8')  # Very light gray background
                
                # Add border around plot
                for spine in ax.spines.values():
                    spine.set_linewidth(1.2)
                
                # Adjust layout to prevent label cutoff with more padding
                fig.tight_layout(pad=2.0)
                
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
                if device in device_figures:
                    figures.append((device_figures[device], region))
                    continue
                
                # Filter hourly data for this device
                plot_data = df_hourly[df_hourly['DeviceId'] == device].copy()
                time_column = 'TimeStamp'
                
                # Skip if no data
                if plot_data.empty:
                    continue
                
                # Calculate Actuations per Service (avoid division by zero)
                plot_data['ActPerService'] = plot_data.apply(
                    lambda row: row[right_value_column] / row[left_value_column] 
                    if row[left_value_column] > 0 else 0, axis=1
                )
                
                # Identify phases with alerts from daily data
                device_daily = region_data[region_data['DeviceId'] == device]
                alert_phases = set(device_daily['Phase'].unique())  # All phases in alerts df have alerts
                
                # Create the plot with a bigger figure size for better readability
                # Single y-axis for both metrics
                fig = new_figure((10, 5))
                ax1 = fig.subplots()
                
                # Get the min and max dates for the device data to set x-axis limits
                min_date = plot_data[time_column].min()
                max_date = plot_data[time_column].max()
                
                # Get all phases for this device
                all_phases = sorted(plot_data[group_column].unique())
                other_phases = sorted(set(all_phases) - alert_phases)
                
                # First plot "other" phases (non-alert) in grey with lower zorder
                from matplotlib.lines import Line2D
                other_phase_lines = []
                for phase in other_phases:
                    phase_data = plot_data[plot_data[group_column] == phase]
                    # Plot PedServices in grey
                    line = ax1.plot(phase_data[time_column], phase_data[left_value_column], 
                             color='#cccccc', linewidth=1.5, alpha=0.7,
                             linestyle='-', zorder=1)
                    other_phase_lines.append(line[0])
                    # Plot Actuations per Service in grey dashed
                    ax1.plot(phase_data[time_column], phase_data['ActPerService'], 
                             color='#cccccc', linewidth=1.0, alpha=0.7,
                             linestyle='--', dashes=(4, 2), zorder=1)
                if other_phase_lines:
                    other_phase_lines[0].set_label("Other Phases")
                
                # Then plot alert phases with colors and higher zorder
                legend_elements = []
                for phase in sorted(alert_phases):
                    phase_data = plot_data[plot_data[group_column] == phase]
                    color = phase_color_map[phase]
                    
                    # Plot PedServices on left y-axis with solid line
                    ax1.plot(phase_data[time_column], phase_data[left_value_column], 
                             color=color, linewidth=2.5, 
                             label=f'Phase {phase} - Services',
                             linestyle='-', zorder=5)
                    
                    # Plot Actuations per Service on same y-axis with dashed line
                    ax1.plot(phase_data[time_column], phase_data['ActPerService'], 
                             color=color, linewidth=2.0, 
                             label=f'Phase {phase} - Act/Svc',
                             linestyle='--', 
                             dashes=(4, 2), zorder=5)
                    
                    # Add to legend
                    legend_elements.append(Line2D([0], [0], color=color, linewidth=2.5,
                                                  label=f'Phase {phase} - Services'))
                    legend_elements.append(Line2D([0], [0], color=color, linewidth=2.0,
                                                  linestyle='--', dashes=(4, 2),
                                                  label=f'Phase {phase} - Act/Svc'))
                
                # Set axis label
                ax1.set_ylabel(f"{y_label_left} / {y_label_right}", fontweight='bold', color='black')
                
                # Set the x-axis limits to match the data range exactly
                ax1.set_xlim(min_date, max_date)
                
                # Customize the plot with improved styling
                ax1.set_title(f'Hourly {plot_title}\n{name}',
                              pad=_LEGEND_TITLE_PAD, fontweight='bold')
                
                ax1.grid(True, alpha=0.3, linestyle='--')
                
                # Add the combined legend
                _legend_above(ax1, handles=legend_elements,
                              labels=[h.get_label() for h in legend_elements])
                
                # Rotate x-axis labels for better readability and add padding
                _rotate_x_labels(ax1)
                
                # Add light gray background
                ax1.set_facecolor('#f8f8f8')  # Very light gray background
                
                # Add border around plot
                for spine in ax1.spines.values():
                    spine.set_linewidth(1.2)
                
                # Format ticks on y-axis to show integers
                ax1.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
                
                _format_time_axis(ax1, plot_data[time_column])
                
                # Adjust layout to prevent label cutoff with more padding
                fig.tight_layout(pad=2.0)
                
                # Add to figures list
                figures.append((fig, region))
                device_figures[device] = fig
                
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
            device_rank = device_rankings.get(device, 0)
            if device in device_figures:
                figures_with_rank.append((device_figures[device], region, device_rank))
                continue
            
            # Determine which dataset to use for plotting
            if df_hourly is not None:
                # Use hourly data for plotting
                plot_data = df_hourly[df_hourly['DeviceId'] == device]
                time_column = 'TimeStamp'
            else:
                # Use daily data for plotting
                plot_data = df_daily[df_daily['DeviceId'] == device]
                time_column = 'Date'
            
            # Create the plot with a bigger figure size for better readability
            fig = new_figure((10, 5))
            ax = fig.subplots()
            
            # Get the min and max dates for the device data to set x-axis limits
            min_date = plot_data[time_column].min()
            max_date = plot_data[time_column].max()
            
            # Only phase terminations get here (pedestrian and missing data charts are
            # drawn above): phases without an alert are drawn in grey.
            daily_data = df_daily[df_daily['DeviceId'] == device]
            phases_with_alerts = set(daily_data[daily_data['Alert'] == 1][group_column].unique())
            
            # Get all phases in hourly/plot data
            all_phases = sorted(plot_data[group_column].unique())
            
            # First plot all 'other' phases with low zorder to ensure they're below alert phases
            other_phase_lines = []
            for number in all_phases:
                if number not in phases_with_alerts:
                    # Plot other phases in light gray, thinner line, with low zorder
                    data = plot_data[plot_data[group_column] == number]
                    line = ax.plot(data[time_column], data[value_column], 
                            color='#cccccc', linewidth=1.5, alpha=0.7, zorder=1)
                    other_phase_lines.append(line[0])
            
            # Then plot phases with alerts with higher zorder to ensure they're on top
            for i, number in enumerate(sorted(list(phases_with_alerts))):
                # Filter data for this phase
                data = plot_data[plot_data[group_column] == number]
                color = phase_color_map[number]
                
                # Plot actual values with solid line
                ax.plot(data[time_column], data[value_column],
                        color=color, linewidth=2.5, zorder=5,
                        label=f'{group_column} {number}')
                
                # Add markers where alerts occur (only if using daily data)
                if df_hourly is None and 'Alert' in data.columns:
                    alerts = data[data['Alert'] == 1]
                    if not alerts.empty:
                        ax.scatter(alerts[time_column], alerts[value_column], 
                                  color=color, s=80, zorder=10)
            
            # Add "Other Phases" to the legend
            if other_phase_lines:
                other_phase_lines[0].set_label("Other Phases")
            
            # Set the x-axis limits to match the data range exactly
            ax.set_xlim(min_date, max_date)
            
            # Determine time granularity for the title
            time_granularity = "Hourly" if df_hourly is not None else "Daily"
            
            # Customize the plot with improved styling
            ax.set_title(f'{time_granularity} {plot_title}\n{name}',
                        pad=_LEGEND_TITLE_PAD, fontweight='bold')
            
            ax.set_ylabel(y_label, fontweight='bold')
            ax.grid(True, alpha=0.3, linestyle='--')
            
            _legend_above(ax)

            # Set fixed y-axis limits if specified
            if fixed_y_limits:
                ax.set_ylim(fixed_y_limits)
            
            # Format y-axis as percentage if needed
            if use_percent_format:
                ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
            
            _format_time_axis(ax, plot_data[time_column])
            
            # Rotate x-axis labels for better readability and add padding
            _rotate_x_labels(ax)
            
            # Add light gray background grid
            ax.set_axisbelow(True)
            ax.set_facecolor('#f8f8f8')  # Very light gray background
            
            # Add border around plot
            for spine in ax.spines.values():
                spine.set_linewidth(1.2)
            
            # Adjust layout to prevent label cutoff with more padding
            fig.tight_layout(pad=2.0)
            
            # Store device ranking with figure for later sorting
            figures_with_rank.append((fig, region, device_rank))
            device_figures[device] = fig
    
    # Sort figures by device ranking (highest/worst first)
    sorted_figures_with_rank = sorted(figures_with_rank, key=lambda x: x[2], reverse=True)
    
    # Add the sorted ranked figures to the main figures list
    for fig, region, _ in sorted_figures_with_rank:
        figures.append((fig, region))
    
    return figures


def _wait_line(data: 'pd.DataFrame') -> Tuple['pd.Series', 'pd.Series']:
    """A phase's times and max waits to draw, empty for an hour or more without a wait, so the line breaks there instead of
    sloping across a stretch with no data to a value it never reached."""
    times = data['TimeStamp']
    holes = times[times.diff() >= pd.Timedelta(hours=1)]
    if holes.empty:
        return times, data['MaxPhaseWait']
    line = pd.concat([data[['TimeStamp', 'MaxPhaseWait']], pd.DataFrame({'TimeStamp': holes - pd.Timedelta(minutes=15)})])
    line = line.sort_values('TimeStamp', kind='stable')
    return line['TimeStamp'], line['MaxPhaseWait']


def create_phase_skip_plots(
    phase_waits_df: 'pd.DataFrame',
    signals_df: 'pd.DataFrame',
    device_rankings_df: 'pd.DataFrame',
    num_figures: int,
    cycle_length_df: Optional['pd.DataFrame'] = None,
    days_plotted: Optional[int] = None,
    exclude: Optional[dict] = None,
) -> List[Tuple['plt.Figure', str]]:
    """Generate per-device Phase Skip (Phase Wait Time) plots with styling identical to the main device charts.

    Args:
        phase_waits_df: DataFrame with columns DeviceId, TimeStamp, Phase, AvgPhaseWait, MaxPhaseWait, TotalSkips
        signals_df: DataFrame with columns DeviceId, Name, Region
        device_rankings_df: DataFrame with columns DeviceId, TotalSkips for ranking
        num_figures: Number of figures to generate per region
        cycle_length_df: Optional DataFrame with columns DeviceId, TimeStamp, CycleLength
                        and optionally IsAssumed, from coordination_agg
        days_plotted: Trailing days of history to draw; None keeps everything supplied
        exclude: Optional {report: signals} that report leaves out (its Signal Focus lists them)

    Returns:
        List of tuples containing (matplotlib figure, region)
    """
    if (
        phase_waits_df is None or phase_waits_df.empty or
        device_rankings_df is None or device_rankings_df.empty or
        signals_df is None or signals_df.empty
    ):
        return []

    # Ensure consistent font settings with the rest of the charts
    plt.rcParams.update({
        'font.size': 12,
        'axes.titlesize': 16,
        'axes.labelsize': 14,
        'xtick.labelsize': 12,
        'ytick.labelsize': 12,
        'legend.fontsize': 12
    })

    # A new alert is about what just happened, so its chart shows the day it
    # happened on. An ongoing one is about persistence, so it gets the week.
    if days_plotted is not None and not phase_waits_df.empty:
        phase_waits_df = phase_waits_df.copy()
        phase_waits_df['TimeStamp'] = pd.to_datetime(phase_waits_df['TimeStamp'])
        latest = phase_waits_df['TimeStamp'].max()
        if pd.notna(latest):
            window_start = latest.normalize() - pd.Timedelta(days=days_plotted - 1)
            phase_waits_df = phase_waits_df[phase_waits_df['TimeStamp'] >= window_start]
            if cycle_length_df is not None and not cycle_length_df.empty:
                cycle_length_df = cycle_length_df.copy()
                cycle_length_df['TimeStamp'] = pd.to_datetime(cycle_length_df['TimeStamp'])
                cycle_length_df = cycle_length_df[cycle_length_df['TimeStamp'] >= window_start]
        if phase_waits_df.empty:
            return []

    rankings = device_rankings_df.copy()
    rankings['DeviceId'] = rankings['DeviceId'].astype(str)

    signals = signals_df[['DeviceId', 'Name', 'Region']].copy()
    signals['DeviceId'] = signals['DeviceId'].astype(str)

    rankings = rankings.merge(signals, on='DeviceId', how='left')
    rankings = rankings.dropna(subset=['Region'])

    if rankings.empty:
        return []

    regions = rankings['Region'].dropna().unique().tolist()
    if 'All Regions' not in regions:
        regions.append('All Regions')

    figures: List[Tuple['plt.Figure', str]] = []

    phase_waits_df = phase_waits_df.copy()
    phase_waits_df['DeviceId'] = phase_waits_df['DeviceId'].astype(str)
    phase_waits_df['TimeStamp'] = pd.to_datetime(phase_waits_df['TimeStamp'])
    phase_waits_df = phase_waits_df.sort_values('TimeStamp')
    alert_column = 'AlertPhase'
    if alert_column not in phase_waits_df.columns:
        phase_waits_df[alert_column] = False
    phase_waits_df[alert_column] = phase_waits_df[alert_column].fillna(False).astype(bool)

    # Same color palette as the other charts
    colors = ['#E41A1C', '#377EB8', '#4DAF4A', '#984EA3', '#FF7F00', '#A65628', '#F781BF', '#17BECF']
    # Drawn once per device and listed for its region and for All Regions.
    device_figures = {}

    for region in regions:
        if region == 'All Regions':
            region_rankings = rankings
        else:
            region_rankings = rankings[rankings['Region'] == region]
        region_rankings = region_rankings[~region_rankings['DeviceId'].isin(_hidden_in(exclude, region))]

        if region_rankings.empty:
            continue

        top_devices = (
            region_rankings
            .sort_values(by='TotalSkips', ascending=False)
            .head(num_figures if num_figures is not None else 3)
        )

        for _, device_row in top_devices.iterrows():
            device_id = device_row['DeviceId']
            if device_id in device_figures:
                figures.append((device_figures[device_id], region))
                continue
            device_data = phase_waits_df[phase_waits_df['DeviceId'] == device_id]

            if device_data.empty:
                continue

            # Match figure size with other individual device charts
            fig = new_figure((10, 5))
            ax = fig.subplots()

            unique_phases = sorted(device_data['Phase'].astype(int).unique())
            phase_colors = {phase: colors[i % len(colors)] for i, phase in enumerate(unique_phases)}
            alert_phases = sorted(device_data.loc[device_data[alert_column], 'Phase'].astype(int).unique())
            if not alert_phases:
                alert_phases = unique_phases
            other_phases = sorted(set(unique_phases) - set(alert_phases))

            other_phase_lines = []
            for phase_value in other_phases:
                data = device_data[device_data['Phase'] == phase_value]
                if data.empty:
                    continue
                line = ax.plot(
                    *_wait_line(data),
                    color='#cccccc',
                    linewidth=1.5,
                    alpha=0.7,
                    zorder=1
                )
                other_phase_lines.append(line[0])
            if other_phase_lines:
                other_phase_lines[0].set_label("Other Phases")

            for phase_value in alert_phases:
                data = device_data[device_data['Phase'] == phase_value]
                if data.empty:
                    continue
                ax.plot(
                    *_wait_line(data),
                    label=f"Phase {phase_value}",
                    color=phase_colors.get(phase_value, colors[0]),
                    linewidth=2.5,
                    zorder=5
                )

            # Cycle length runs on top of the phase traces so it stays readable,
            # and every wait that crossed the skip threshold gets a marker. The
            # threshold itself is not drawn: the markers say which points crossed
            # it without adding another line to an already busy chart.
            if cycle_length_df is not None and not cycle_length_df.empty:
                device_cycle_data = cycle_length_df[cycle_length_df['DeviceId'] == device_id].copy()
                if not device_cycle_data.empty:
                    device_cycle_data = device_cycle_data.sort_values('TimeStamp')
                    ax.step(
                        device_cycle_data['TimeStamp'],
                        device_cycle_data['CycleLength'],
                        where='post',
                        color='#4d4d4d',
                        linestyle='--',
                        linewidth=1.5,
                        zorder=10,
                        label='Cycle Length'
                    )

            # Mark the bins the detector actually counted a skip in. Recomputing
            # the threshold here would only approximate it: the detector tests each
            # wait event against the cycle lengths at its own start and end, while
            # MaxPhaseWait is a bin aggregate, so the two disagree on exactly the
            # marginal cases that make up most single-skip alerts. TotalSkips is
            # the detector's own answer, so the markers cannot drift from it.
            if 'TotalSkips' in device_data.columns:
                for phase_value in alert_phases:
                    data = device_data[device_data['Phase'] == phase_value]
                    if data.empty:
                        continue
                    data = data.sort_values('TimeStamp')
                    skipped = pd.to_numeric(data['TotalSkips'], errors='coerce').fillna(0) > 0
                    if not skipped.any():
                        continue
                    ax.scatter(
                        pd.to_datetime(data.loc[skipped, 'TimeStamp']).values,
                        data.loc[skipped, 'MaxPhaseWait'].values,
                        s=14,
                        color=phase_colors.get(phase_value, colors[0]),
                        zorder=11,
                        label='_nolegend_',
                    )

            signal_label = device_row.get('Name') or device_id
            
            title_timestamps = device_data['TimeStamp'].dropna().sort_values()
            start_timestamp = title_timestamps.iloc[0]
            end_timestamp = title_timestamps.iloc[-1]
            is_multi_day = start_timestamp.normalize() != end_timestamp.normalize()
            if is_multi_day:
                date_str = (
                    f"{start_timestamp.strftime('%Y-%m-%d')} to "
                    f"{end_timestamp.strftime('%Y-%m-%d')}"
                )
                title_fontsize = 14
            else:
                date_str = start_timestamp.strftime('%Y-%m-%d')
                title_fontsize = 16

            # Title and labels with bold, same padding and font sizes
            ax.set_title(
                f"Max Phase Wait Time (15-minute bins)\n{signal_label} - {date_str}",
                pad=_LEGEND_TITLE_PAD,
                fontweight='bold',
                fontsize=title_fontsize
            )
            ax.set_ylabel("Max Phase Wait Time (s)", fontweight='bold', fontsize=14)

            # Visual styling identical to the main charts
            ax.set_facecolor('#f8f8f8')                    # light gray background
            ax.grid(True, alpha=0.3, linestyle='--')
            ax.set_axisbelow(True)

            _format_time_axis(ax, device_data['TimeStamp'])
            _rotate_x_labels(ax, fontsize=12)
            ax.tick_params(axis='both', labelsize=12)

            # Legend placed exactly like the other charts
            _legend_above(ax)

            # Thicker spines for consistent look
            for spine in ax.spines.values():
                spine.set_linewidth(1.2)

            # Tight layout with extra padding to prevent cutoff (same as others)
            fig.tight_layout(pad=2.0)

            figures.append((fig, region))
            device_figures[device_id] = fig

    return figures


# Preempt alert charts: daily calls on top, every call's duration below, on
# one date axis, compact enough for three to a page.
PREEMPT_COLORS = ['#E41A1C', '#377EB8', '#4DAF4A', '#984EA3', '#FF7F00']
_DURATION_TICKS = [(0.1, '0.1s'), (1, '1s'), (10, '10s'), (60, '1m'), (600, '10m'),
                   (3600, '1h'), (36000, '10h'), (360000, '100h')]


def _preempt_groups(alerts: 'pd.DataFrame') -> list:
    """Group alerts into charts, highest score first.

    An alert whose preempt is already drawn on an earlier chart (a linked
    neighbour, or a second input at the same signal) joins that chart instead
    of getting its own.
    """
    groups = []
    for row in alerts.sort_values('Score', ascending=False).itertuples():
        node = (str(row.DeviceId), int(row.Preempt))
        chart_series = row.ChartSeries if isinstance(row.ChartSeries, (list, tuple)) else []
        series = [(str(d), int(p)) for d, p in chart_series] or [node]
        home = next((g for g in groups if node in g['series']), None)
        if home is None:
            groups.append({'series': series, 'alerts': [row]})
        else:
            home['alerts'].append(row)
    return groups


def _problem_text(alert, lead_device, names) -> str:
    """One alert as 'P1 Chatter: 155 of 155 calls under 3 s'."""
    prefix = '' if str(alert.DeviceId) == lead_device else names.get(str(alert.DeviceId), '').split('-')[0] + ' '
    note = alert.Note if isinstance(getattr(alert, 'Note', None), str) else ''
    ran, total = getattr(alert, 'RanAsPreempt', None), getattr(alert, 'BusCalls', None)
    if pd.notna(ran) and pd.notna(total):
        return f'{prefix}P{int(alert.Preempt)}: {bus_calls_text(alert.Direction, ran, total)}'
    if alert.Direction == DIRECTION_FALSE_CALLS:
        note = note.split('. ')[0]   # the evidence; the table gives the rest
    if alert.Direction in ('Increase', 'Decrease'):
        rates = f'{float(alert.BaselinePerDay):.1f} calls/day before, {float(alert.RecentPerDay):.1f} in the last 7 days'
        note = rates if not note or note.startswith('no linked') else f'{rates}; {note}'
    return f'{prefix}P{int(alert.Preempt)} {alert.Direction}: {note}'.rstrip(': ')


def _draw_preempt_group(group, calls, history, names, report_day, recent_days,
                        chatter_seconds, stuck_limits, class_labels=None, count_label='Calls/day', usual_lengths=None):
    series = group['series']
    lead = group['alerts'][0]
    lead_device = str(lead.DeviceId)
    alerting = {(str(a.DeviceId), int(a.Preempt)) for a in group['alerts']}

    # One line per alert under the title, at most two; a chart's only alert may run onto the second line.
    problems = [textwrap.shorten(_problem_text(a, lead_device, names), 100, placeholder=' ...')
                for a in group['alerts']]
    if len(group['alerts']) == 1:
        problems = textwrap.wrap(textwrap.shorten(_problem_text(lead, lead_device, names), 200, placeholder=' ...'), 100)
    if len(problems) > 2:
        problems = problems[:1] + [f'and {len(problems) - 1} more alerts on this chart']

    # Laid out in inches at close to the width it is printed (6.5 in), so the
    # text keeps its size on the page.
    width = 7.2
    left, right = 0.75, 0.5
    header = 0.34 + 0.22 * len(problems) + 0.32
    count_h, gap, dur_h, bottom = 1.0, 0.12, 1.3, 0.38
    height = header + count_h + gap + dur_h + bottom
    fig = new_figure((width, height))
    plot_w = (width - left - right) / width
    ax_dur = fig.add_axes([left / width, bottom / height, plot_w, dur_h / height])
    ax_count = fig.add_axes([left / width, (bottom + dur_h + gap) / height, plot_w, count_h / height],
                            sharex=ax_dur)

    end = report_day + pd.Timedelta(days=1)
    start = history['Date'].min()
    recent_start = end - pd.Timedelta(days=recent_days)
    for ax in (ax_count, ax_dur):
        ax.axvspan(recent_start, end, color='#eef1f6', zorder=0, lw=0)
        ax.grid(True, color='#e6e6e6', lw=0.7, zorder=1)
        ax.set_axisbelow(True)
        ax.tick_params(labelsize=9, length=3)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)
    ax_count.tick_params(labelbottom=False)

    # With class_labels ({Class: label}) each preempt is drawn once per class, so
    # one input's two kinds of call (bus calls that ran as preempts and normal
    # TSP) can be told apart.
    drawn = [(d, p, None, '') for d, p in series]
    if class_labels:
        drawn = [(d, p, cls, f' {label}') for d, p in series for cls, label in class_labels.items()]
    handles, durations = [], []
    for i, (d, p, cls, suffix) in enumerate(drawn):
        color = PREEMPT_COLORS[i % len(PREEMPT_COLORS)]
        is_alert = (d, p) in alerting
        # Every day the signal reported counts, zero where the preempt was quiet.
        days = history.loc[history['DeviceId'] == d, 'Date'].drop_duplicates().sort_values()
        mine = history[(history['DeviceId'] == d) & (history['Preempt'] == p)]
        c = calls[(calls['DeviceId'] == d) & (calls['Preempt'] == p) & calls['IsValid']]
        if cls is not None:
            mine = mine[mine['Class'] == cls] if 'Class' in mine.columns else mine.head(0)
            c = c[c['Class'] == cls] if 'Class' in c.columns else c.head(0)
            if mine.empty and c.empty:
                continue
        counts = mine.groupby('Date')['Count'].sum()
        y = [int(counts.get(day, 0)) for day in days]
        label = f"{names.get(d, d).split('-')[0]} P{p}{suffix}"
        line, = ax_count.plot(days + pd.Timedelta(hours=12), y, color=color, lw=2.0 if is_alert else 1.1,
                              marker='o', ms=2.5, label=label, zorder=4 if is_alert else 3)
        handles.append(line)
        durations.append(c['Duration'])
        ax_dur.scatter(c['StartTime'], c['Duration'].clip(lower=0.1), s=10 if is_alert else 6, color=color,
                       alpha=0.8, edgecolors='none', zorder=4 if is_alert else 3)

    ax_count.set_ylabel(count_label, fontsize=9.5)
    ax_count.set_ylim(bottom=0)
    ax_count.yaxis.set_major_locator(mtick.MaxNLocator(integer=True, nbins=3))
    ax_count.text(recent_start, 1.02, 'last 7 days', transform=ax_count.get_xaxis_transform(),
                  color='#5b6b80', fontsize=8.5, va='bottom', ha='left')

    # Only the range of call lengths present, so the panel is not mostly empty.
    all_durations = pd.concat(durations).dropna() if durations else pd.Series(dtype=float)
    low = max(0.1, all_durations.min()) if not all_durations.empty else 1.0
    high = all_durations.max() if not all_durations.empty else 60.0
    low = 10 ** np.floor(np.log10(low * 0.9))
    high = max(10 ** np.ceil(np.log10(high * 1.1)), 100.0)
    ax_dur.set_yscale('log')
    ax_dur.set_ylim(low * 0.7, high * 1.4)
    ticks = [(v, t) for v, t in _DURATION_TICKS if low <= v <= high]
    ax_dur.set_yticks([v for v, _ in ticks])
    ax_dur.set_yticklabels([t for _, t in ticks])
    ax_dur.yaxis.set_minor_locator(mtick.NullLocator())
    ax_dur.set_ylabel('Call length', fontsize=9.5)
    if usual_lengths:
        # Where vehicle calls usually fall, so calls outside it stand out.
        usual_low, usual_high = usual_lengths
        ax_dur.axhspan(usual_low, usual_high, color='#dfeedd', zorder=0.5, lw=0)
        ax_dur.text(end, (usual_low * usual_high) ** 0.5, ' usual', color='#3f7a3a', fontsize=8.5, va='center', ha='left')
    for limit, text, style in [(chatter_seconds, f'{chatter_seconds:g} s', '--')] + [(l, t, ':') for l, t in stuck_limits]:
        if low <= limit <= high:
            ax_dur.axhline(limit, color='#888888', ls=style, lw=0.9, zorder=2)
            ax_dur.text(end, limit, f' {text}', color='#555555', fontsize=8.5, va='center', ha='left')
    ax_dur.set_xlim(start, end)
    ax_dur.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    ax_dur.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))

    # Title names the signal; the lines under it state the problem.
    x0 = left / width
    fig.text(x0, 1 - 0.08 / height, names.get(lead_device, lead_device), fontsize=12, fontweight='bold', va='top')
    fig.text(x0, 1 - 0.36 / height, '\n'.join(problems), fontsize=9.5, color='#333333', va='top', linespacing=1.45)
    fig.legend(handles, [h.get_label() for h in handles], loc='lower left',
               bbox_to_anchor=(x0 - 0.01, (bottom + dur_h + gap + count_h + 0.1) / height),
               ncol=len(handles), frameon=False, fontsize=9, handlelength=1.5, columnspacing=1.2,
               borderaxespad=0, borderpad=0)
    return fig


def create_preempt_plots(
    alerts_df: 'pd.DataFrame',
    calls_df: 'pd.DataFrame',
    history_df: 'pd.DataFrame',
    signals_df: 'pd.DataFrame',
    num_figures: int = 6,
    recent_days: int = 7,
    rail_preempts=None,
    chatter_seconds: float = 3.0,
    stuck_seconds_ev: float = 600.0,
    stuck_seconds_rail: float = 3600.0,
    exclude: Optional[dict] = None,
    class_labels: Optional[dict] = None,
    stuck_lines: bool = True,
    count_label: str = 'Calls/day',
    usual_lengths: Optional[tuple] = None,
) -> List[Tuple['plt.Figure', str]]:
    """Charts for preempt alerts as (figure, region) tuples.

    Each chart stacks the daily call count over the retained history above
    every stored call's length (log scale) on one date axis, for the alert's
    preempt and its linked or companion preempts (``ChartSeries``), with the
    problem stated under the signal name. Up to ``num_figures`` per region.
    ``class_labels`` ({Class: legend words}) draws each preempt once per call
    class (history and calls then need Class); ``stuck_lines`` False leaves out
    the stuck-on limits; ``count_label`` names the count axis; ``usual_lengths``
    (low, high seconds) shades where vehicle calls usually fall.
    """
    if alerts_df is None or alerts_df.empty or signals_df is None or signals_df.empty:
        return []
    if calls_df is None or history_df is None or history_df.empty:
        return []
    alerts = alerts_df.copy()
    alerts['DeviceId'] = alerts['DeviceId'].astype(str)
    for col in ('ChartSeries', 'Note'):
        if col not in alerts.columns:
            alerts[col] = None
    alerts['Score'] = pd.to_numeric(alerts.get('Score'), errors='coerce').fillna(0)
    signals = signals_df[['DeviceId', 'Name', 'Region']].copy()
    signals['DeviceId'] = signals['DeviceId'].astype(str)
    names = dict(zip(signals['DeviceId'], signals['Name'].astype(str)))
    region_of = dict(zip(signals['DeviceId'], signals['Region']))
    alerts = alerts[alerts['DeviceId'].isin(region_of)]
    if alerts.empty:
        return []

    calls = calls_df.copy()
    calls['DeviceId'] = calls['DeviceId'].astype(str)
    calls['StartTime'] = pd.to_datetime(calls['StartTime'])
    calls['Duration'] = pd.to_numeric(calls['Duration'], errors='coerce')
    calls['IsValid'] = calls['IsValid'].fillna(False).astype(bool)
    history = history_df.copy()
    history['DeviceId'] = history['DeviceId'].astype(str)
    history['Date'] = pd.to_datetime(history['Date'])
    report_day = pd.to_datetime(alerts['Date']).max().normalize() if 'Date' in alerts.columns else history['Date'].max()
    history = history[history['Date'] <= report_day]
    if history.empty:
        return []
    calls = calls[calls['StartTime'] < report_day + pd.Timedelta(days=1)]
    rail = None if rail_preempts is None else {int(p) for p in rail_preempts}

    figures = []
    # A chart with the same preempts and alerts is the same in a region's report
    # and in All Regions, so it is drawn once.
    drawn = {}
    regions = list(dict.fromkeys(region_of[d] for d in alerts['DeviceId'])) + ['All Regions']
    for region in regions:
        scope = alerts if region == 'All Regions' else alerts[alerts['DeviceId'].map(region_of) == region]
        scope = scope[~scope['DeviceId'].isin(_hidden_in(exclude, region))]
        for group in _preempt_groups(scope)[:num_figures]:
            key = (tuple(group['series']), tuple(alert.Index for alert in group['alerts']))
            if key in drawn:
                figures.append((drawn[key], region))
                continue
            preempts = {p for _, p in group['series']}
            if not stuck_lines:
                limits = []
            elif rail is None:
                limits = [(stuck_seconds_rail, 'stuck')]
            else:
                limits = []
                if preempts - rail:
                    limits.append((stuck_seconds_ev, 'EV stuck'))
                if preempts & rail:
                    limits.append((stuck_seconds_rail, 'rail stuck'))
            fig = drawn[key] = _draw_preempt_group(group, calls, history, names, report_day, recent_days,
                                                   chatter_seconds, limits, class_labels, count_label, usual_lengths)
            figures.append((fig, region))
    return figures


_ATSPM_DETECTOR_COLORS = ['#D62728', '#1F77B4', '#2CA02C', '#9467BD', '#FF7F0E', '#8C564B', '#E377C2', '#17BECF']


# Health charts: characters of a line under the title, inches per such line and per row of the period strip,
# and the grey of the detectors that are not flagged.
_ATSPM_LINE_CHARS = 95
_ATSPM_LEGEND_KEY_CHARS = 7
_ATSPM_LINE_H = 0.18
_ATSPM_STRIP_ROW_H = 0.15
_ATSPM_OTHER_COLOR = '#c6c6c6'


def _atspm_phase_groups(alerts: 'pd.DataFrame', info: 'pd.DataFrame') -> 'pd.DataFrame':
    """The alerts with their detector's phase and function (from info); Phase -1 = no known phase."""
    merged = alerts.merge(info, on=['DeviceId', 'Detector'], how='left')
    merged['Phase'] = pd.to_numeric(merged['Phase'], errors='coerce').fillna(-1).astype(int)
    return merged


def create_atspm_detector_health_plots(alerts: 'pd.DataFrame', signals_df: 'pd.DataFrame', num_figures: int,
                                       counts: Optional['pd.DataFrame'] = None,
                                       detector_info: Optional['pd.DataFrame'] = None,
                                       window: Optional[Tuple] = None,
                                       kind: str = 'fault', max_drawn: int = 4,
                                       exclude: Optional[dict] = None,
                                       ped_services: Optional['pd.DataFrame'] = None,
                                       ped_inputs: Optional['pd.DataFrame'] = None,
                                       ped_cycles: Optional['pd.DataFrame'] = None) -> List[Tuple['plt.Figure', str]]:
    """Charts for the detector health section (atspm-detector): one chart per signal phase, worst first.

    A chart shows the actuations per 15 minutes of the phase's flagged detectors (each in its own colour,
    named in the legend and, with its finding, in the title lines) over the report window, the phase's
    other detectors in grey behind them, and under the chart a strip with one labelled row per flagged
    detector showing its periods as bars in its colour.
    kind 'fault' charts the bad / suspect detectors; kind 'closure' charts the lane closures, with the
    detectors that took the closed lane's traffic drawn in colour too. kind 'ped' charts ped buttons the same
    way, one chart per signal: their presses per 15 minutes (counts holds Detector = the input), the signal's other
    buttons in grey, the share of each hour's cycles with a walk in a panel below (with ped_cycles; else walks per 15 minutes),
    and their fault periods below that. A share tells a phase on ped recall (every cycle) from one walking on presses, where a
    count depends on the cycle length.

    Args:
        alerts: atspm-detector health alerts (DeviceId, Detector, Date, Status, Score, Reason,
            Periods as a list of {start, end, what} dicts), already limited to the report window
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        num_figures: Number of signals per region; up to twice as many phase charts are drawn
        counts: Actuations per 15 minutes (TimeStamp, DeviceId, Detector, Total)
        detector_info: Phase and Function of every detector (DeviceId, Detector, Phase, Function)
        window: (start, end) of the days the report covers; default the three days up to the report day
        kind: 'fault', 'closure' or 'ped'
        max_drawn: Flagged detectors drawn per chart (worst first); the others are named in a line
        exclude: Optional {report: signals} that report leaves out (its Signal Focus lists them)
        ped_services: Kind 'ped': walks per 15 minutes (TimeStamp, DeviceId, Phase, PedServices), which adds the walks panel
        ped_inputs: Kind 'ped': the phase each button calls (DeviceId, Input, Phase), which colours its phase's walks as the button
        ped_cycles: Kind 'ped': cycles per hour (DeviceId, TimeStamp, Cycles; focus_overview.hourly_cycles), which turns the walks
            into the share of cycles with a walk

    Returns:
        List of tuples containing (matplotlib figure, region)
    """
    if alerts is None or alerts.empty or counts is None or counts.empty:
        return []

    alerts = alerts.copy()
    alerts['DeviceId'] = alerts['DeviceId'].astype(str)
    counts = counts.copy()
    counts['DeviceId'] = counts['DeviceId'].astype(str)
    counts['TimeStamp'] = pd.to_datetime(counts['TimeStamp'])
    info = detector_info.copy() if detector_info is not None else pd.DataFrame(
        columns=['DeviceId', 'Detector', 'Phase', 'Function'])
    info['DeviceId'] = info['DeviceId'].astype(str)
    alerts = _atspm_phase_groups(alerts, info)

    if window is None:
        end = pd.Timestamp(alerts['Date'].max()).normalize() + pd.Timedelta(days=1)
        window = (end - pd.Timedelta(days=3), end)
    window_start, window_end = pd.Timestamp(window[0]), pd.Timestamp(window[1])
    counts = counts[(counts['TimeStamp'] >= window_start) & (counts['TimeStamp'] < window_end)]

    # Worst phase first: faults by the number rated bad, then count and lowest score, or by the detector triage when
    # the alerts carry it, so the charts follow the table; closures by the vehicles they moved.
    alerts['_triage'] = pd.to_numeric(alerts['TriageRank'], errors='coerce') if 'TriageRank' in alerts.columns else 0.0
    if kind == 'closure':
        alerts['_moved'] = pd.to_numeric(alerts['Reason'].str.extract(r'~([\d,]+) of its vehicles')[0]
                                         .str.replace(',', ''), errors='coerce').fillna(0)
        ranking = (alerts.groupby(['DeviceId', 'Phase'])['_moved'].sum().sort_values(ascending=False))
    else:
        alerts['_bad'] = (alerts['Status'] == 'bad').astype(int)
        ranking = (alerts.groupby(['DeviceId', 'Phase'])
                   .agg(triage=('_triage', 'min'), n_bad=('_bad', 'sum'), n=('Detector', 'size'), score=('Score', 'min'))
                   .sort_values(['triage', 'n_bad', 'n', 'score'], ascending=[True, False, False, True]))
    charts_per_region = 2 * num_figures if kind in ('fault', 'ped') else num_figures
    item = 'Ped' if kind == 'ped' else 'Det'

    signals = signals_df.copy()
    signals['DeviceId'] = signals['DeviceId'].astype(str)
    names = signals.drop_duplicates('DeviceId').set_index('DeviceId')['Name']
    regions = list(signals['Region'].unique()) + ['All Regions']

    one_day = window_end - window_start <= pd.Timedelta(days=1)

    figures = []
    # A phase's chart is the same in its region's report and in All Regions.
    drawn = {}
    for region in regions:
        region_ids = set(signals['DeviceId'] if region == 'All Regions'
                         else signals.loc[signals['Region'] == region, 'DeviceId']) - _hidden_in(exclude, region)
        groups = [key for key in ranking.index if key[0] in region_ids][:charts_per_region]
        for device, phase in groups:
            if (device, phase) in drawn:
                figures.append((drawn[(device, phase)], region))
                continue
            group = alerts[(alerts['DeviceId'] == device) & (alerts['Phase'] == phase)]
            if kind != 'closure':
                group = group.sort_values(['_triage', '_bad', 'Score'], ascending=[True, False, True])
            device_counts = counts[counts['DeviceId'] == device]
            if device_counts.empty:
                continue
            device_info = info[info['DeviceId'] == device]
            bins15 = pd.date_range(window_start, window_end, freq='15min', inclusive='left')

            def series(number):
                # On the full 15-minute grid, so a gap in the data breaks the line instead of bridging it.
                one = device_counts[device_counts['Detector'] == number].groupby('TimeStamp')['Total'].sum()
                one = one.reindex(bins15).rename_axis('TimeStamp')
                # A button has no row for a quarter hour nobody pressed it.
                return (one if kind != 'ped' else one.fillna(0)).reset_index()

            # Lines in colour: the flagged detectors, and for a closure the detectors that took its traffic.
            # At most MAX_DRAWN flagged detectors are drawn (worst first); the rest of the phase's flagged
            # detectors are named on one line and left to the table.
            shown_alerts = group[group['Detector'].isin(group['Detector'].drop_duplicates().head(max_drawn))]
            extra = group['Detector'].drop_duplicates().iloc[max_drawn:].astype(int).tolist()
            # Title lines: one per kind of finding, naming its detectors once ("Stuck on: det 42, 48"),
            # for every flagged detector of the phase (drawn or not).
            colored = []
            for number in shown_alerts['Detector'].astype(int):
                if number not in colored:
                    colored.append(number)
            by_kind = {}
            for _, alert in group.iterrows():
                reason = re.sub(r'^det \d+: ', '', str(alert['Reason'])).split(' - ', 1)[-1]
                if kind == 'ped':
                    what = str(alert['Label']).capitalize()
                elif kind == 'fault':
                    what = reason.split('; ')[0].split(':')[0].strip()
                else:   # a lane closure / timing plan: one line per detector, its time and where the traffic went
                    first = reason.split('; ')[0]
                    span = re.search(r'\w{3} \d{2}:\d{2}-\w{3} \d{2}:\d{2}', first)
                    span = span.group(0) if span else ''
                    went = [n for n in re.findall(r'det (\d+)', first.split('moved to')[-1])] if 'moved to' in first else []
                    pattern = re.search(r'pattern (\d+)', first)
                    if alert['Status'] == 'lane_closure':
                        what = f"Det {int(alert['Detector'])} lane closed {span}" + (f", traffic moved to det {', '.join(went)}" if went else '')
                    else:
                        what = f"Det {int(alert['Detector'])} silent {span}" + (f" while coordination pattern {pattern.group(1)} ran" if pattern else '')
                    by_kind[what] = []
                    continue
                by_kind.setdefault(what, []).append(int(alert['Detector']))
            lines = [(f"{what}: {item.lower()} {', '.join(str(n) for n in dict.fromkeys(dets))}" if dets else what, '#333333')
                     for what, dets in by_kind.items()]
            if extra:
                lines.append((f"Not drawn (see the table): {item.lower()} {', '.join(str(n) for n in extra)}", '#777777'))
            partners = []
            if kind == 'closure':
                for reason in shown_alerts['Reason']:
                    for part in re.findall(r'moved to ([^;]+)', str(reason)):
                        partners += [int(n) for n in re.findall(r'det (\d+)', part)]
                partners = [p for p in dict.fromkeys(partners) if p not in colored]
            colored += partners

            wrapped = [(part, color) for text, color in lines for part in textwrap.wrap(text, width=_ATSPM_LINE_CHARS)]

            # Rows of the period strip under the chart: each drawn detector that has periods.
            spans = {}
            for number in colored:
                rows = []
                for _, alert in shown_alerts[shown_alerts['Detector'] == number].iterrows():
                    for period in alert['Periods'] or []:
                        start = pd.to_datetime(period.get('start'), errors='coerce')
                        end = pd.to_datetime(period.get('end'), errors='coerce')
                        if pd.notna(start) and pd.notna(end) and end > window_start and start < window_end:
                            rows.append((max(start, window_start), min(end, window_end)))
                if rows:
                    spans[number] = rows

            # The phase's other detectors (all other detectors when the phase is unknown) in grey; the
            # flagged ones not drawn stay out of it.
            if phase >= 0:
                others = device_info[(device_info['Phase'] == phase)]['Detector'].astype(int).tolist()
            else:
                others = device_counts['Detector'].astype(int).unique().tolist()
            others = sorted(d for d in others if d not in colored and d not in extra)
            other_label = ('Other ped buttons' if kind == 'ped'
                           else 'Other ' + (f'Ph{phase} ' if phase >= 0 else '') + 'detectors')
            line_labels = {number: f'{item} {number}' + (' (took its traffic)' if number in partners else '')
                           for number in colored}

            # The legend goes above the plot, in as many rows as its labels need on the chart's width.
            legend_labels = ([other_label] if others else []) + list(line_labels.values())
            legend_rows = math.ceil(sum(len(text) + _ATSPM_LEGEND_KEY_CHARS for text in legend_labels) / _ATSPM_LINE_CHARS)
            title_h = 0.30 + _ATSPM_LINE_H * (len(wrapped) + legend_rows)
            # Ped: the walks in a panel of their own, under the presses, with their legend above them.
            walks = {}
            cycles = ped_cycles[ped_cycles['DeviceId'].astype(str) == device] if ped_cycles is not None and not ped_cycles.empty else None
            share = cycles is not None and not cycles.empty
            if kind == 'ped' and ped_services is not None:
                services = ped_services[ped_services['DeviceId'].astype(str) == device]
                if share:
                    walks = walk_share(services, cycles, window_start, window_end)
                else:
                    for one_phase, rows in services.groupby('Phase'):
                        series_walks = rows.groupby(pd.to_datetime(rows['TimeStamp']))['PedServices'].sum().reindex(bins15).fillna(0)
                        if series_walks.sum() > 0:
                            walks[int(one_phase)] = series_walks
            phase_color = {}
            if walks and ped_inputs is not None and not ped_inputs.empty:
                button_phases = ped_inputs[ped_inputs['DeviceId'].astype(str) == device]
                for k, number in enumerate(colored):
                    for one_phase in button_phases.loc[button_phases['Input'] == number, 'Phase'].dropna():
                        phase_color.setdefault(int(one_phase), _ATSPM_DETECTOR_COLORS[k % len(_ATSPM_DETECTOR_COLORS)])
            walks_h, walks_gap = (0.85, 0.26) if walks else (0.0, 0.0)
            chart_h, strip_h, axis_h = (1.25 if walks else 2.0), _ATSPM_STRIP_ROW_H * len(spans) + 0.04, 0.32
            height = title_h + chart_h + walks_gap + walks_h + 0.06 + strip_h + axis_h
            fig = new_figure((7.5, height))
            left_edge, right_edge = 0.085, 0.975
            lower = axis_h / height
            if walks:
                # Top to bottom: presses, the gap with the walks legend, walks, the period strip.
                strip = fig.add_axes([left_edge, lower, right_edge - left_edge, strip_h / height])
                ax_walks = fig.add_axes([left_edge, lower + (strip_h + 0.06) / height, right_edge - left_edge, walks_h / height], sharex=strip)
                ax = fig.add_axes([left_edge, lower + (strip_h + 0.06 + walks_h + walks_gap) / height, right_edge - left_edge, chart_h / height], sharex=strip)
            else:
                ax_walks = None
                grid = fig.add_gridspec(2, 1, height_ratios=[chart_h, strip_h], hspace=0.06 / chart_h,
                                        left=left_edge, right=right_edge, bottom=lower, top=1 - title_h / height)
                ax = fig.add_subplot(grid[0])
                strip = fig.add_subplot(grid[1], sharex=ax)

            phase_name = 'ped buttons' if kind == 'ped' else f'Ph{phase}' if phase >= 0 else 'detectors without a known phase'
            fig.text(0.01, 1 - 0.08 / height, f"{names.get(device, device)}  ·  {phase_name}", fontsize=11,
                     fontweight='bold', va='top')
            for k, (text, color) in enumerate(wrapped):
                fig.text(0.01, 1 - (0.30 + _ATSPM_LINE_H * k) / height, text, fontsize=9.5, color=color, va='top')

            for k, number in enumerate(others):
                data = series(number)
                # Above the axis line and not clipped at it, so a line at zero shows. The grey is solid for
                # the same reason: see-through, it would turn black on the axis line.
                ax.plot(data['TimeStamp'], data['Total'], color=_ATSPM_OTHER_COLOR, linewidth=0.8, zorder=3, clip_on=False,
                        label=other_label if k == 0 else None)

            for k, number in enumerate(colored):
                color = _ATSPM_DETECTOR_COLORS[k % len(_ATSPM_DETECTOR_COLORS)]
                data = series(number)
                ax.plot(data['TimeStamp'], data['Total'], color=color, linewidth=1.4, zorder=5, clip_on=False,
                        label=line_labels[number])

            # Period strip: one labelled row per detector, its periods as bars in its colour (at least 0.4 % of
            # the window wide, so a short one shows).
            min_width = (window_end - window_start) * 0.004
            for row, (number, rows) in enumerate(spans.items()):
                color = _ATSPM_DETECTOR_COLORS[colored.index(number) % len(_ATSPM_DETECTOR_COLORS)]
                for start, end in rows:
                    if end - start < min_width:
                        middle = start + (end - start) / 2
                        start, end = middle - min_width / 2, middle + min_width / 2
                    strip.barh(row, mdates.date2num(end) - mdates.date2num(start), left=mdates.date2num(start),
                               height=0.75, color=color, linewidth=0)
            strip.set_yticks(range(len(spans)))
            strip.set_yticklabels([f'{item} {n}' for n in spans], fontsize=8.5)
            for label, number in zip(strip.get_yticklabels(), spans):
                label.set_color(_ATSPM_DETECTOR_COLORS[colored.index(number) % len(_ATSPM_DETECTOR_COLORS)])
            strip.set_ylim(len(spans) - 0.5, -0.5)
            strip.tick_params(axis='y', length=0)
            strip.set_facecolor('#f4f4f4')
            strip.grid(True, axis='x', alpha=0.3, linestyle='--')
            for spine in ('top', 'right'):
                strip.spines[spine].set_visible(False)
            plt.setp(ax.get_xticklabels(), visible=False)

            if ax_walks is not None:
                # Colour for the phases of the flagged buttons, grey behind them for the signal's other phases.
                flagged_count = sum(phase in phase_color for phase in walks)
                for k, (one_phase, series_walks) in enumerate(sorted(walks.items(), key=lambda item: item[0] in phase_color)):
                    color = phase_color.get(one_phase, _ATSPM_OTHER_COLOR)
                    flagged = one_phase in phase_color
                    label = f'Ph{one_phase}' if share else f'Ph{one_phase} walks'
                    # Phases walking every cycle sit on one line: each drawn wider than the next, so all their colours show.
                    width = 1.4 + 1.5 * (flagged_count - 1 - (k - (len(walks) - flagged_count))) if flagged else 0.8
                    if share:   # a line through the middle of each hour, like the Signal Focus overview
                        x, y = _centred(series_walks, window_start, window_end)
                        ax_walks.plot(x, y, color=color, linewidth=width, zorder=5 + k if flagged else 3,
                                      clip_on=False, label=label if flagged or len(walks) <= 1 else None)
                    else:
                        ax_walks.plot(series_walks.index, series_walks.values, color=color, linewidth=width, zorder=5 + k if flagged else 3,
                                      clip_on=False, label=label if flagged or len(walks) <= 1 else None)
                if any(phase not in phase_color for phase in walks) and len(walks) > 1:
                    ax_walks.plot([], [], color=_ATSPM_OTHER_COLOR, linewidth=0.8, label='Other phases')
                if share:
                    ax_walks.set_ylim(0, 1.05)
                    ax_walks.yaxis.set_major_locator(mtick.FixedLocator([0, 0.5, 1]))
                    ax_walks.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
                    ax_walks.set_ylabel('Cycles with\na walk', fontsize=10)
                else:
                    ax_walks.set_ylim(bottom=0)
                    ax_walks.yaxis.set_major_locator(mtick.MaxNLocator(nbins=4, integer=True))
                    ax_walks.set_ylabel('Walks / 15 min', fontsize=10)
                ax_walks.tick_params(axis='both', labelsize=9.5)
                ax_walks.grid(True, alpha=0.3, linestyle='--')
                ax_walks.set_axisbelow(True)
                ax_walks.set_facecolor('#fafafa')
                plt.setp(ax_walks.get_xticklabels(), visible=False)
                handles, labels = ax_walks.get_legend_handles_labels()
                ax_walks.legend(handles, labels, loc='lower left', bbox_to_anchor=(0, 1), borderaxespad=0.2, fontsize=9, frameon=False,
                                ncol=max(len(labels), 1))

            ax.set_xlim(window_start, window_end)
            ax.set_ylim(bottom=0)
            ax.set_ylabel('Presses / 15 min' if kind == 'ped' else 'Actuations / 15 min', fontsize=10)
            ax.tick_params(axis='both', labelsize=9.5)
            ax.grid(True, alpha=0.3, linestyle='--')
            ax.set_axisbelow(True)
            ax.set_facecolor('#fafafa')
            strip.tick_params(axis='x', labelsize=9.5)
            days = pd.date_range(window_start, window_end, freq='D')[1:-1]
            for axis in (ax, strip) if ax_walks is None else (ax, ax_walks, strip):
                for day in days:
                    axis.axvline(day, color='#999999', linewidth=0.8, zorder=1)
            day_time_ticks(strip, window_start, window_end, 9.5)
            if one_day:
                strip.set_xlabel(window_start.strftime('%A %b %d'), fontsize=10)
            ax.legend(loc='lower left', bbox_to_anchor=(0, 1), borderaxespad=0.2, fontsize=9, frameon=False,
                      ncol=math.ceil(len(legend_labels) / legend_rows))
            figures.append((fig, region))
            drawn[(device, phase)] = fig
    return figures
