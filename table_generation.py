import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
import base64
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Table, TableStyle, Paragraph, Image, Spacer
from reportlab.lib.units import inch

def prepare_phase_termination_alerts_table(filtered_df, signals_df, max_rows=10):
    """
    Prepare a sorted table of phase termination alerts with signal name, phase, and date
    
    Args:
        filtered_df: DataFrame containing phase termination alerts
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        max_rows: Maximum number of rows to include in the table
        
    Returns:
        Tuple of (Sorted DataFrame with Signal Name, Phase, Date and Alert columns, total_alerts_count)
    """
    if filtered_df.empty:
        return pd.DataFrame(), 0
        
    # Join with signals data to get signal names
    alerts_df = filtered_df.copy()  # Use all data for sparklines, not just alerts
    
    # Filter alerts for table display
    alert_rows = filtered_df[filtered_df['Alert'] == 1].copy()
    if alert_rows.empty:
        return pd.DataFrame(), 0
        
    # Merge with signals dataframe to get names
    result = pd.merge(
        alert_rows[['DeviceId', 'Phase', 'Date', 'Alert', 'Percent MaxOut']],
        signals_df[['DeviceId', 'Name']],
        on='DeviceId',
        how='left'
    )
    
    # Filter out rows where Signal is NaN
    result = result.dropna(subset=['Name'])
    
    if result.empty:
        return pd.DataFrame(), 0
    
    # Get the total count of alerts after filtering but before limiting rows
    total_alerts_count = len(result)
    
    # Rename and select columns
    result = result.rename(columns={'Name': 'Signal', 'Percent MaxOut': 'MaxOut %'})
    
    # Add sparkline column - Group by Signal and Phase and collect time series data
    sparkline_data = {}
    
    # Get all DeviceId and Phase pairs that have alerts
    device_phase_pairs = result[['DeviceId', 'Phase']].drop_duplicates().values.tolist()
    
    # For each device/phase pair with alerts, collect all data for sparklines
    for device_id, phase in device_phase_pairs:
        # Get all data for this device/phase pair, not just alerts
        device_data = alerts_df[(alerts_df['DeviceId'] == device_id) & 
                                (alerts_df['Phase'] == phase)]
        
        if not device_data.empty:
            # Sort by date to ensure correct time series
            device_data = device_data.sort_values('Date')
            # Store the full time series data
            sparkline_data[(device_id, phase)] = device_data['Percent MaxOut'].tolist()
    
    # Add the sparkline data to the result dataframe
    result['Sparkline_Data'] = result.apply(
        lambda row: sparkline_data.get((row['DeviceId'], row['Phase']), []), 
        axis=1
    )
    
    # Select and order columns
    result = result[['Signal', 'Phase', 'Date', 'MaxOut %', 'Sparkline_Data']]
    
    # Sort by MaxOut % in descending order, then by Signal, Phase
    result = result.sort_values(by=['MaxOut %', 'Signal', 'Phase'], ascending=[False, True, True])
    
    # Limit the number of rows
    if max_rows > 0 and len(result) > max_rows:
        result = result.head(max_rows)
    
    return result, total_alerts_count

def prepare_detector_health_alerts_table(filtered_df_actuations, signals_df, max_rows=10):
    """
    Prepare a sorted table of detector health alerts with signal name, detector, and date
    
    Args:
        filtered_df_actuations: DataFrame containing detector health alerts
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        max_rows: Maximum number of rows to include in the table
        
    Returns:
        Tuple of (Sorted DataFrame with Signal Name, Detector, Date and Alert columns, total_alerts_count)
    """
    if filtered_df_actuations.empty:
        return pd.DataFrame(), 0
        
    # Use all data for sparklines, not just alerts
    detector_df = filtered_df_actuations.copy()
    
    # Filter alerts for table display
    alert_rows = filtered_df_actuations[filtered_df_actuations['Alert'] == 1].copy()
    if alert_rows.empty:
        return pd.DataFrame(), 0
        
    # Merge with signals dataframe to get names
    result = pd.merge(
        alert_rows[['DeviceId', 'Detector', 'Date', 'Alert', 'PercentAnomalous', 'Total']],
        signals_df[['DeviceId', 'Name']],
        on='DeviceId',
        how='left'
    )
    
    # Filter out rows where Signal is NaN
    result = result.dropna(subset=['Name'])
    
    if result.empty:
        return pd.DataFrame(), 0
    
    # Get the total count of alerts after filtering but before limiting rows
    total_alerts_count = len(result)
    
    # Rename and select columns
    result = result.rename(columns={'Name': 'Signal', 'PercentAnomalous': 'Anomalous %'})
    
    # Add sparkline column - Group by Signal and Detector and collect time series data
    sparkline_data = {}
    
    # Get all DeviceId and Detector pairs that have alerts
    device_detector_pairs = result[['DeviceId', 'Detector']].drop_duplicates().values.tolist()
    
    # For each device/detector pair with alerts, collect all data for sparklines
    for device_id, detector in device_detector_pairs:
        # Get all data for this device/detector pair, not just alerts
        device_data = detector_df[(detector_df['DeviceId'] == device_id) & 
                                  (detector_df['Detector'] == detector)]
        
        if not device_data.empty:
            # Sort by date to ensure correct time series
            device_data = device_data.sort_values('Date')
            # Store the Total values for sparklines instead of PercentAnomalous
            sparkline_data[(device_id, detector)] = device_data['Total'].tolist()
    
    # Add the sparkline data to the result dataframe
    result['Sparkline_Data'] = result.apply(
        lambda row: sparkline_data.get((row['DeviceId'], row['Detector']), []), 
        axis=1
    )
    
    # Select and order columns
    result = result[['Signal', 'Detector', 'Date', 'Anomalous %', 'Sparkline_Data']]
    
    # Sort by Anomalous % in descending order, then by Signal, Detector
    result = result.sort_values(by=['Anomalous %', 'Signal', 'Detector'], ascending=[False, True, True])
    
    # Limit the number of rows
    if max_rows > 0 and len(result) > max_rows:
        result = result.head(max_rows)
    
    return result, total_alerts_count

def create_sparkline(data, width=1.0, height=0.25, color='#1f77b4'):
    """
    Create a sparkline image from a list of values
    
    Args:
        data: List of values to plot
        width: Width of the image in inches
        height: Height of the image in inches
        color: Color of the sparkline
        
    Returns:
        ReportLab Image object
    """
    if not data or len(data) < 2:
        # Create an empty image if no data
        fig, ax = plt.subplots(figsize=(width, height), dpi=150)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        buf = BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', pad_inches=0, transparent=True)
        plt.close(fig)
        buf.seek(0)
        
        return Image(buf, width=width*inch, height=height*inch)
    
    # Create the sparkline
    fig, ax = plt.subplots(figsize=(width, height), dpi=150)  # Increased DPI for better quality
    
    # Plot data points as a line only - no markers
    x = list(range(len(data)))
    ax.plot(x, data, color=color, linewidth=1.0)
    
    # No endpoint marker - removing this line
    # ax.scatter(x[-1], data[-1], color=color, s=15, zorder=3)
    
    # Set limits with a bit of padding
    y_min = min(data) * 0.9 if min(data) > 0 else min(data) * 1.1
    y_max = max(data) * 1.1
    ax.set_xlim(-0.5, len(data) - 0.5)
    ax.set_ylim(y_min, y_max)
    
    # Remove axes and borders
    ax.axis('off')
    fig.patch.set_alpha(0)
    
    # Tighter layout to remove excess whitespace
    plt.tight_layout(pad=0)
    
    # Convert to Image
    buf = BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', pad_inches=0, transparent=True)
    plt.close(fig)
    buf.seek(0)
    
    return Image(buf, width=width*inch, height=height*inch)

def create_reportlab_table(df, title, styles, total_count=None, max_rows=10):
    """
    Create a ReportLab table from a pandas DataFrame
    
    Args:
        df: DataFrame containing table data
        title: Title for the table
        styles: ReportLab styles object
        total_count: Total number of alerts (if different from the number of rows)
        max_rows: Maximum number of rows displayed in the table
        
    Returns:
        List of ReportLab flowables (Title paragraph and Table)
    """
    if df.empty:
        return [Paragraph(f"{title} - No alerts found", styles['Normal'])]
    
    # Create a copy to avoid modifying the original
    df_display = df.copy()
    
    # Show message about total alerts vs. displayed alerts
    if total_count is not None:
        table_notice = f"Showing top {len(df)} of {total_count} total alerts"
    else:
        table_notice = f"Showing {len(df)} alerts"
    
    # Get sparkline data and remove it from display DataFrame
    sparkline_data = None
    if 'Sparkline_Data' in df_display.columns:
        sparkline_data = df_display['Sparkline_Data'].tolist()
        df_display = df_display.drop(columns=['Sparkline_Data'])
    
    # Format percentage columns if they exist
    if 'MaxOut %' in df_display.columns:
        df_display['MaxOut %'] = df_display['MaxOut %'].apply(lambda x: f"{x:.1%}")
    if 'Anomalous %' in df_display.columns:
        df_display['Anomalous %'] = df_display['Anomalous %'].apply(lambda x: f"{x:.1%}")
    if 'Missing Data %' in df_display.columns:
        df_display['Missing Data %'] = df_display['Missing Data %'].apply(lambda x: f"{x:.1%}")
    
    # Format date column
    df_display['Date'] = df_display['Date'].dt.strftime('%Y-%m-%d')
    
    # Add Trend column header
    df_display['Trend'] = ""
    
    # Create header and data for the table
    header = df_display.columns.tolist()
    data = [header]
    
    # Add rows 
    for _, row in df_display.iterrows():
        data.append(row.tolist())
    
    # Create the table
    colWidths = [None] * (len(header) - 1) + [1.2*inch]  # Make the Trend column wider
    table = Table(data, colWidths=colWidths)
    
    # Style the table
    table_style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  # Vertically center all content
    ])
    
    # Add alternating row colors
    for i in range(1, len(data), 2):
        table_style.add('BACKGROUND', (0, i), (-1, i), colors.lightgrey)
    
    # Apply the table style
    table.setStyle(table_style)
    
    # If we have sparklines, add them to the last column after the table is created
    if sparkline_data is not None:
        for i, data_points in enumerate(sparkline_data):
            row_index = i + 1  # +1 because row 0 is the header
            
            # Only create sparkline if we have data points
            if data_points and len(data_points) >= 2:
                # Use consistent color for all sparklines
                sparkline = create_sparkline(data_points, width=1.0, height=0.25, color='#1f77b4')
                
                # Replace the content of the last column with the sparkline image
                table._cellvalues[row_index][-1] = sparkline
    
    result_elements = [
        Paragraph(title, styles['SectionHeading'])
    ]
    
    # Always add notice about total alerts
    result_elements.append(Paragraph(table_notice, styles['Normal']))
    result_elements.append(Spacer(1, 0.05*inch))
    
    result_elements.append(table)
    
    return result_elements

def prepare_missing_data_alerts_table(filtered_df_missing_data, signals_df, max_rows=10):
    """
    Prepare a sorted table of missing data alerts with signal name and date
    
    Args:
        filtered_df_missing_data: DataFrame containing missing data alerts
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        max_rows: Maximum number of rows to include in the table
        
    Returns:
        Tuple of (Sorted DataFrame with Signal Name, Date and Alert columns, total_alerts_count)
    """
    if filtered_df_missing_data.empty:
        return pd.DataFrame(), 0
        
    # Use all data for sparklines, not just alerts
    missing_data_df = filtered_df_missing_data.copy()
    
    # Filter alerts for table display
    alert_rows = filtered_df_missing_data[filtered_df_missing_data['Alert'] == 1].copy()
    if alert_rows.empty:
        return pd.DataFrame(), 0
    
    # Get the total count of unique devices with alerts before limiting rows
    unique_devices_with_alerts = alert_rows['DeviceId'].nunique()
        
    # Merge with signals dataframe to get names
    result = pd.merge(
        alert_rows[['DeviceId', 'Date', 'Alert', 'MissingData']],
        signals_df[['DeviceId', 'Name']],
        on='DeviceId',
        how='left'
    )
    
    # Filter out rows where Signal is NaN
    result = result.dropna(subset=['Name'])
    
    if result.empty:
        return pd.DataFrame(), 0
    
    # Rename and select columns
    result = result.rename(columns={'Name': 'Signal', 'MissingData': 'Missing Data %'})
    
    # For missing data, only show one row per device (the worst one)
    # First, find the index of the maximum MissingData value for each device
    idx = result.groupby('DeviceId')['Missing Data %'].idxmax()
    
    # Use these indices to filter the dataframe to get just one row per device
    result = result.loc[idx]
    
    # Add sparkline column - Group by Signal and collect time series data
    sparkline_data = {}
    
    # Get all DeviceIds that have alerts
    device_ids = result['DeviceId'].unique()
    
    # For each device with alerts, collect all data for sparklines
    for device_id in device_ids:
        # Get all data for this device, not just alerts
        device_data = missing_data_df[missing_data_df['DeviceId'] == device_id]
        
        if not device_data.empty:
            # Sort by date to ensure correct time series
            device_data = device_data.sort_values('Date')
            # Store the MissingData values for sparklines
            sparkline_data[device_id] = device_data['MissingData'].tolist()
    
    # Add the sparkline data to the result dataframe
    result['Sparkline_Data'] = result.apply(
        lambda row: sparkline_data.get(row['DeviceId'], []), 
        axis=1
    )
    
    # Select and order columns
    result = result[['Signal', 'Date', 'Missing Data %', 'Sparkline_Data']]
    
    # Sort by Missing Data % in descending order, then by Signal
    result = result.sort_values(by=['Missing Data %', 'Signal'], ascending=[False, True])
    
    # Limit the number of rows
    if max_rows > 0 and len(result) > max_rows:
        result = result.head(max_rows)
    
    return result, unique_devices_with_alerts