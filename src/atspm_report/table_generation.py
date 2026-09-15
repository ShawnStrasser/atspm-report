import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
import base64
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Table, TableStyle, Paragraph, Image, Spacer
from reportlab.lib.units import inch
from reportlab.pdfbase.pdfmetrics import stringWidth

# Ongoing-issue rows arrive carrying the date their current run of repeats started.
# Every prepare_* function passes that column through untouched and renders it as
# the display column below, so a table looks exactly as it does today unless the
# caller supplied ongoing rows.
ONGOING_SINCE_COLUMN = 'OngoingSince'
ONGOING_DISPLAY_COLUMN = 'Ongoing'

# Hours per point in the pedestrian sparkline. The underlying data is hourly,
# which is 168 points across the seven days the column covers - far too fine to
# read at that size.
PED_SPARKLINE_BUCKET_HOURS = 4


def _format_ongoing_since(value):
    """Render an ongoing-since date as 'M/D/YY (Nd)'."""
    timestamp = pd.to_datetime(value, errors='coerce')
    if pd.isna(timestamp):
        return ''
    days = (pd.Timestamp.today().normalize() - timestamp.normalize()).days
    return f"{timestamp.month}/{timestamp.day}/{timestamp:%y} ({days}d)"


def _add_ongoing_column(result, output_columns):
    """Append the formatted ongoing column when the rows carry an ongoing date."""
    if ONGOING_SINCE_COLUMN not in result.columns:
        return result, output_columns
    result = result.copy()
    result[ONGOING_DISPLAY_COLUMN] = result[ONGOING_SINCE_COLUMN].apply(_format_ongoing_since)
    return result, list(output_columns) + [ONGOING_DISPLAY_COLUMN]


def _carry_ongoing_column(source_columns, columns):
    """Add the ongoing date to a column subset when the source frame has one."""
    if ONGOING_SINCE_COLUMN in source_columns:
        return list(columns) + [ONGOING_SINCE_COLUMN]
    return list(columns)


def _ongoing_aggregation(source_columns):
    """Aggregation kwargs that keep the longest-running ongoing date for a group."""
    if ONGOING_SINCE_COLUMN in source_columns:
        return {ONGOING_SINCE_COLUMN: (ONGOING_SINCE_COLUMN, 'min')}
    return {}


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
    
    # Ensure DeviceId is string type to avoid merge issues
    signals_df = signals_df.copy()
    signals_df['DeviceId'] = signals_df['DeviceId'].astype(str)
    
    # Filter alerts for table display
    alert_rows = filtered_df[filtered_df['Alert'] == 1].copy()
    if alert_rows.empty:
        return pd.DataFrame(), 0
        
    # Merge with signals dataframe to get names
    result = pd.merge(
        alert_rows[_carry_ongoing_column(
            alert_rows.columns, ['DeviceId', 'Phase', 'Date', 'Alert', 'Percent MaxOut']
        )],
        signals_df[['DeviceId', 'Name', 'Region']], # Added Region for consistency
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
    result, output_columns = _add_ongoing_column(
        result, ['Signal', 'Phase', 'Date', 'MaxOut %', 'Sparkline_Data']
    )
    result = result[output_columns]

    # Sort by MaxOut % in descending order, then by Signal, Phase
    result = result.sort_values(by=['MaxOut %', 'Signal', 'Phase'], ascending=[False, True, True])
    
    # Limit the number of rows
    if max_rows > 0 and len(result) > max_rows:
        result = result.head(max_rows)
    
    return result, total_alerts_count

def prepare_phase_skip_alerts_table(phase_skip_rows, signals_df, region=None, allowed_pairs=None, min_total_skips=0, max_rows=10):
    """
    Prepare the Phase Skip table showing one row per signal per date with aggregated phases.

    Args:
        phase_skip_rows: DataFrame with DeviceId, Phase, Date, TotalSkips
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        region: Optional region filter. When provided (and not "All Regions") the table is limited to that region.
        allowed_pairs: Optional DataFrame with DeviceId and Phase pairs to filter
        min_total_skips: Minimum total skips threshold (applied after aggregation)
        max_rows: Maximum number of rows to include in the table

    Returns:
        Tuple of (Sorted DataFrame with Signal, Date, Phases, Total Skips columns, total_alerts_count)
    """
    if phase_skip_rows is None or phase_skip_rows.empty:
        return pd.DataFrame(), 0

    df = phase_skip_rows.copy()
    df['DeviceId'] = df['DeviceId'].astype(str)

    # Filter by allowed pairs if provided
    if allowed_pairs is not None and not allowed_pairs.empty:
        allowed = allowed_pairs[['DeviceId', 'Phase']].drop_duplicates().copy()
        allowed['DeviceId'] = allowed['DeviceId'].astype(str)
        allowed['Phase'] = allowed['Phase'].astype(int)
        df = df.merge(allowed, on=['DeviceId', 'Phase'], how='inner')

    if df.empty:
        return pd.DataFrame(), 0

    signals_df = signals_df.copy()
    signals_df['DeviceId'] = signals_df['DeviceId'].astype(str)

    # Merge with signals to get names and regions
    result = df.merge(
        signals_df[['DeviceId', 'Name', 'Region']],
        on='DeviceId',
        how='left'
    )

    result = result.dropna(subset=['Name'])
    if result.empty:
        return pd.DataFrame(), 0

    # Filter by region if specified
    if region and region != "All Regions":
        result = result[result['Region'] == region]
        if result.empty:
            return pd.DataFrame(), 0

    # Aggregate by Signal and Date to combine phases
    result['Date'] = pd.to_datetime(result['Date']).dt.date
    result = result.rename(columns={'Name': 'Signal'})
    
    # Group by Signal and Date, aggregating phases and summing skips
    # A Signal/Date row can cover several phases, so the group inherits the
    # earliest of their ongoing dates - the longest-running of the bunch.
    aggregated = (
        result.groupby(['Signal', 'Date'], as_index=False)
        .agg(
            Phases=('Phase', lambda x: ', '.join(sorted(set(str(p) for p in x)))),
            TotalSkips=('TotalSkips', 'sum'),
            **_ongoing_aggregation(result.columns)
        )
    )
    
    # Apply min_total_skips filter after aggregation
    if min_total_skips and min_total_skips > 0:
        aggregated = aggregated[aggregated['TotalSkips'] >= min_total_skips]
    
    if aggregated.empty:
        return pd.DataFrame(), 0
    
    # Get the total count before limiting rows
    total_alerts_count = len(aggregated)
    
    # Rename and select final columns
    aggregated = aggregated.rename(columns={'TotalSkips': 'Total Skips'})
    aggregated, output_columns = _add_ongoing_column(
        aggregated, ['Signal', 'Date', 'Phases', 'Total Skips']
    )
    aggregated = aggregated[output_columns]
    aggregated = aggregated.sort_values(by=['Total Skips', 'Signal', 'Date'], ascending=[False, True, True])
    
    # Limit the number of rows
    if max_rows > 0 and len(aggregated) > max_rows:
        aggregated = aggregated.head(max_rows)

    return aggregated, total_alerts_count


def prepare_detector_health_alerts_table(filtered_df_actuations, signals_df, max_rows=10,
                                         detector_hourly_df=None):
    """
    Prepare a sorted table of detector health alerts with signal name, detector, and date
    
    Args:
        filtered_df_actuations: DataFrame containing detector health alerts
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        max_rows: Maximum number of rows to include in the table
        detector_hourly_df: Optional hourly actuation counts, used for the sparkline
            so the trend shows the shape of a day rather than a handful of daily totals
        
    Returns:
        Tuple of (Sorted DataFrame with Signal Name, Detector, Date and Alert columns, total_alerts_count)
    """
    if filtered_df_actuations.empty:
        return pd.DataFrame(), 0
        
    # Use all data for sparklines, not just alerts
    detector_df = filtered_df_actuations.copy()
    
    # Ensure DeviceId is string type to avoid merge issues
    signals_df = signals_df.copy()
    signals_df['DeviceId'] = signals_df['DeviceId'].astype(str)
    
    # Filter alerts for table display
    alert_rows = filtered_df_actuations[filtered_df_actuations['Alert'] == 1].copy()
    if alert_rows.empty:
        return pd.DataFrame(), 0
        
    # Merge with signals dataframe to get names
    result = pd.merge(
        alert_rows[_carry_ongoing_column(
            alert_rows.columns,
            ['DeviceId', 'Detector', 'Date', 'Alert', 'PercentAnomalous', 'Total'],
        )],
        signals_df[['DeviceId', 'Name', 'Region']],  # Updated column names to match signals_df
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

    # Hourly counts make a far more legible trend than a dozen daily totals: a
    # detector that dies mid-morning, or only fails at night, is invisible once a
    # day is collapsed to one point.
    if detector_hourly_df is not None and not detector_hourly_df.empty:
        hourly = detector_hourly_df.copy()
        hourly['DeviceId'] = hourly['DeviceId'].astype(str)
        hourly['TimeStamp'] = pd.to_datetime(hourly['TimeStamp'])
        for device_id, detector in device_detector_pairs:
            pair_data = hourly[(hourly['DeviceId'] == str(device_id)) &
                               (hourly['Detector'] == detector)]
            if pair_data.empty:
                continue
            pair_data = pair_data.sort_values('TimeStamp')
            sparkline_data[(device_id, detector)] = pair_data['Total'].tolist()
    
    # Add the sparkline data to the result dataframe
    result['Sparkline_Data'] = result.apply(
        lambda row: sparkline_data.get((row['DeviceId'], row['Detector']), []), 
        axis=1
    )
    
    # Select and order columns
    result, output_columns = _add_ongoing_column(
        result, ['Signal', 'Detector', 'Date', 'Anomalous %', 'Sparkline_Data']
    )
    result = result[output_columns]

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

# The space a table actually gets on the page: letter paper with the half-inch
# side margins and 1.2in/0.5in top and bottom margins the report uses, less the
# 6pt of padding a frame keeps on every side.
TABLE_AVAILABLE_WIDTH = 528.0
TABLE_AVAILABLE_HEIGHT = 657.6

# Reportlab's default cell padding, 6pt on each side.
_CELL_PADDING = 12.0
_HEADER_FONT = ('Helvetica-Bold', 10)
_BODY_FONT = ('Helvetica', 9)

# Columns holding a sentence rather than a value. Left to size themselves these
# end up as wide as their longest word, which wraps the sentence into six or
# seven lines and makes the row taller than it has any need to be, so they take
# whatever width the value columns do not need instead.
FLEX_COLUMNS = ('Details',)
MIN_FLEX_WIDTH = 1.75 * inch


def _natural_width(header_text, values):
    """Width that fits the header and every value in a column without wrapping."""
    width = stringWidth(str(header_text), *_HEADER_FONT)
    for value in values:
        width = max(width, stringWidth(str(value), *_BODY_FONT))
    return width + _CELL_PADDING


def _column_widths(df_display, header, trend_width=None):
    """Size the value columns to their content and give the rest to the sentence.

    Returns None when there is no sentence column, which leaves reportlab to size
    everything itself exactly as it did before.
    """
    if not any(name in FLEX_COLUMNS for name in header):
        return None

    widths = []
    for index, name in enumerate(header):
        if trend_width is not None and index == len(header) - 1:
            widths.append(trend_width)
        elif name in FLEX_COLUMNS:
            widths.append(None)
        else:
            values = df_display[name] if name in df_display.columns else []
            widths.append(_natural_width(name, values))

    flex = [i for i, width in enumerate(widths) if width is None]
    fixed = sum(width for width in widths if width is not None)
    slack = TABLE_AVAILABLE_WIDTH - fixed
    if slack < MIN_FLEX_WIDTH * len(flex):
        # The value columns alone want more room than the page has. Shrink them
        # proportionally rather than running off the edge.
        keep = TABLE_AVAILABLE_WIDTH - MIN_FLEX_WIDTH * len(flex)
        scale = keep / fixed if fixed else 1.0
        widths = [MIN_FLEX_WIDTH if width is None else width * scale for width in widths]
    else:
        share = slack / len(flex)
        widths = [share if width is None else width for width in widths]
    return widths


def _blocks_that_fit(first_row, length, row_heights, max_height):
    """Break one group's rows into blocks no taller than the page.

    A merged cell cannot be split across pages, so a group taller than the frame
    leaves reportlab with nothing it can place anywhere and it gives up with a
    LayoutError. Splitting the merge into page-sized blocks keeps the grouping
    visible and lets the table flow.
    """
    blocks = []
    block_start = first_row
    block_height = 0.0
    for row in range(first_row, first_row + length):
        height = row_heights[row] if row < len(row_heights) and row_heights[row] else 0.0
        if block_height and block_height + height > max_height:
            blocks.append((block_start, row - block_start))
            block_start, block_height = row, height
        else:
            block_height += height
    blocks.append((block_start, first_row + length - block_start))
    return blocks


# Tables all lead with the signal name. Repeated names are merged into one
# centered cell so the rows for a signal read as a visual group.
GROUP_COLUMN = 'Signal'


def _cluster_rows_by_group(values):
    """Order row positions so equal values are contiguous.

    Groups keep the order in which they first appeared, and rows keep their
    order within a group, so any ranking the caller applied still holds.
    """
    first_seen = {}
    for position, value in enumerate(values):
        first_seen.setdefault(value, position)
    return sorted(range(len(values)), key=lambda i: (first_seen[values[i]], i))


def _contiguous_runs(values):
    """Return (start, length) for each run of consecutive equal values."""
    runs = []
    start = 0
    while start < len(values):
        end = start
        while end + 1 < len(values) and values[end + 1] == values[start]:
            end += 1
        runs.append((start, end - start + 1))
        start = end + 1
    return runs


def create_reportlab_table(df, title, styles, total_count=None, max_rows=10, include_trend=True, trend_header='Trend'):
    """Create a ReportLab table from a pandas DataFrame
    
    Args:
        trend_header: Custom header name for the trend column (default: 'Trend')
    """
    if df.empty:
        return [Paragraph("No alerts found", styles['Normal'])]
    
    # Create a copy to avoid modifying the original
    df_display = df.copy()
    
    # Show message about total alerts vs. displayed alerts
    if total_count is not None:
        table_notice = f"Showing top {len(df)} of {total_count} total alerts"
    else:
        table_notice = f"Showing {len(df)} alerts"
    
    # Get sparkline data and remove it from display DataFrame
    sparkline_data = None
    sparkline_columns = ['Sparkline_Data', 'Hourly Services Trend', 'Services (7d)']
    for col in sparkline_columns:
        if col in df_display.columns:
            sparkline_data = df_display[col].tolist()
            df_display = df_display.drop(columns=[col])
            break
    
    # Format percentage columns if they exist
    if 'MaxOut %' in df_display.columns:
        df_display['MaxOut %'] = df_display['MaxOut %'].apply(lambda x: f"{x:.1%}")
    if 'Anomalous %' in df_display.columns:
        df_display['Anomalous %'] = df_display['Anomalous %'].apply(lambda x: f"{x:.1%}")
    if 'Missing Data %' in df_display.columns:
        df_display['Missing Data %'] = df_display['Missing Data %'].apply(lambda x: f"{x:.1%}")
      # Convert non-sparkline columns to strings
    for col in df_display.columns:
        df_display[col] = df_display[col].astype(str)
    
    # Group the rows for each signal together and blank the repeated labels; the
    # blanked cells are merged away by SPAN commands added to the table style.
    group_runs = None
    if len(df_display.columns) and df_display.columns[0] == GROUP_COLUMN:
        order = _cluster_rows_by_group(df_display[GROUP_COLUMN].tolist())
        df_display = df_display.iloc[order].reset_index(drop=True)
        if sparkline_data is not None:
            sparkline_data = [sparkline_data[i] for i in order]
        group_runs = _contiguous_runs(df_display[GROUP_COLUMN].tolist())
        # The repeated labels are blanked further down, once the row heights are
        # known and the merges have been placed: a label that ends up starting a
        # continued block has to stay.

    # Add Trend column header only if requested
    if include_trend:
        df_display[trend_header] = ""
    
    # Create header and data for the table
    header = df_display.columns.tolist()
    data = [header]
    
    # Add rows
    for _, row in df_display.iterrows():
        values = row.tolist()
        if 'Details' in header:
            details_index = header.index('Details')
            values[details_index] = Paragraph(str(values[details_index]), styles['Normal'])
        data.append(values)
      # Create the table
    trend_width = 1.2*inch if include_trend else None  # Make the Trend column wider
    colWidths = _column_widths(df_display, header, trend_width)
    if colWidths is None:
        # No sentence column to lay out around, so leave the sizing to reportlab.
        colWidths = [None] * len(header)
        if include_trend:
            colWidths[-1] = trend_width
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
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ])
    
    if group_runs is not None:
        # The merges themselves are added after the table has been measured, so
        # that none of them ends up taller than a page.
        table_style.add('ALIGN', (0, 1), (0, -1), 'CENTER')
        table_style.add('VALIGN', (0, 1), (0, -1), 'MIDDLE')
        # Shade whole groups rather than single rows, so the banding reinforces
        # the grouping instead of cutting across it.
        for index, (start, length) in enumerate(group_runs):
            if index % 2:
                table_style.add(
                    'BACKGROUND', (0, start + 1), (-1, start + length), colors.lightgrey
                )
    else:
        # Add alternating row colors
        for i in range(1, len(data), 2):
            table_style.add('BACKGROUND', (0, i), (-1, i), colors.lightgrey)
    
    # Apply the table style
    table.setStyle(table_style)
      # If we have sparklines and trend column is included, add them to the last column after the table is created
    if sparkline_data is not None and include_trend:
        for i, data_points in enumerate(sparkline_data):
            row_index = i + 1  # +1 because row 0 is the header
            
            # Only create sparkline if we have data points
            if data_points and len(data_points) >= 2:
                # Use consistent color for all sparklines
                sparkline = create_sparkline(data_points, width=1.0, height=0.25, color='#1f77b4')
                
                # Replace the content of the last column with the sparkline image
                table._cellvalues[row_index][-1] = sparkline

    if group_runs is not None:
        # Merge each signal's repeated cells into one, centered across its rows.
        # Measuring first, with every label still in place, gives an upper bound
        # on the row heights, so a merge placed here is certain to fit its page.
        table.wrap(TABLE_AVAILABLE_WIDTH, TABLE_AVAILABLE_HEIGHT)
        row_heights = list(table._rowHeights or [])
        header_height = row_heights[0] if row_heights else 0.0
        span_budget = TABLE_AVAILABLE_HEIGHT - header_height
        span_style = TableStyle([])
        for start, length in group_runs:
            for block_start, block_length in _blocks_that_fit(
                start + 1, length, row_heights, span_budget
            ):
                if block_length > 1:
                    span_style.add(
                        'SPAN', (0, block_start), (0, block_start + block_length - 1)
                    )
                # Only the row a merge begins on keeps the label; the rest are
                # covered by it. A group split across pages repeats its label,
                # which is what the reader needs anyway.
                for offset in range(1, block_length):
                    table._cellvalues[block_start + offset][0] = ""
        table.setStyle(span_style)

    result_elements = [
        Paragraph(table_notice, styles['Normal']),
        Spacer(1, 0.05*inch),
        table
    ]
    
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
    
    # Ensure DeviceId is string type to avoid merge issues
    signals_df = signals_df.copy()
    signals_df['DeviceId'] = signals_df['DeviceId'].astype(str)
    
    # Filter alerts for table display
    alert_rows = filtered_df_missing_data[filtered_df_missing_data['Alert'] == 1].copy()
    if alert_rows.empty:
        return pd.DataFrame(), 0
      # Merge with signals dataframe to get names
    result = pd.merge(
        alert_rows[['DeviceId', 'Date', 'Alert', 'MissingData']],
        signals_df[['DeviceId', 'Name', 'Region']],  # Updated to include Region for consistency
        on='DeviceId',
        how='left'
    )
    
    # Filter out rows where Signal is NaN
    result = result.dropna(subset=['Name'])
    
    if result.empty:
        return pd.DataFrame(), 0
    
    # Get the total count of unique devices with alerts after region filtering
    unique_devices_with_alerts = result['DeviceId'].nunique()
    
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
    result, output_columns = _add_ongoing_column(
        result, ['Signal', 'Date', 'Missing Data %', 'Sparkline_Data']
    )
    result = result[output_columns]

    # Sort by Missing Data % in descending order, then by Signal
    result = result.sort_values(by=['Missing Data %', 'Signal'], ascending=[False, True])
    
    # Limit the number of rows
    if max_rows > 0 and len(result) > max_rows:
        result = result.head(max_rows)
    
    return result, unique_devices_with_alerts

def prepare_ped_alerts_table(filtered_df_ped, ped_hourly_df, signals_df, max_rows=10):
    """
    Prepare a sorted table of pedestrian alerts with signal name, phase, and hourly ped services
    
    Args:
        filtered_df_ped: DataFrame containing pedestrian alerts (with DeviceId, Phase, Date)
        ped_hourly_df: DataFrame containing hourly pedestrian data (with DeviceId, Phase, TimeStamp, PedServices)
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        max_rows: Maximum number of rows to include in the table
        
    Returns:
        Tuple of (Sorted DataFrame with Signal Name, Phase, Alert Dates and Sparkline columns, total_alerts_count)
    """
    if filtered_df_ped.empty:
        return pd.DataFrame(), 0
    
    # Ensure DeviceId is string type to avoid merge issues
    signals_df = signals_df.copy()
    signals_df['DeviceId'] = signals_df['DeviceId'].astype(str)
    filtered_df_ped = filtered_df_ped.copy()
    filtered_df_ped['DeviceId'] = filtered_df_ped['DeviceId'].astype(str)
    ped_hourly_df = ped_hourly_df.copy()
    ped_hourly_df['DeviceId'] = ped_hourly_df['DeviceId'].astype(str)
      # Group all dates for each DeviceId/Phase combination
    dates_grouped = filtered_df_ped.groupby(['DeviceId', 'Phase'], as_index=False).agg(
        Date=('Date', list),
        **_ongoing_aggregation(filtered_df_ped.columns)
    )
    
    # Join with signals data to get signal names
    result = pd.merge(
        dates_grouped,
        signals_df[['DeviceId', 'Name', 'Region']], # Added Region for consistency
        on='DeviceId',
        how='left'
    )
    
    # Filter out rows where Signal is NaN
    result = result.dropna(subset=['Name'])
    
    if result.empty:
        return pd.DataFrame(), 0
    
    # Get the total count of unique ped detectors with alerts after region filtering (distinct DeviceId/Phase combinations)
    total_alerts_count = result[['DeviceId', 'Phase']].drop_duplicates().shape[0]
    
    # Format dates as strings and join with newlines for stacked appearance
    result['Date'] = result['Date'].apply(
        lambda dates: '\n'.join(d.strftime('%Y-%m-%d') for d in sorted(dates))
    )
    
    # Rename columns
    result = result.rename(columns={
        'Name': 'Signal',
        'Date': 'Alert Dates'
    })
    
    # Add sparkline column using ped_hourly_df data
    sparkline_data = {}
    
    # Get all DeviceId and Phase pairs
    device_phase_pairs = result[['DeviceId', 'Phase']].drop_duplicates().values.tolist()
    
    # For each device/phase pair, collect hourly ped services data for sparklines
    for device_id, phase in device_phase_pairs:
        # Get hourly data for this device/phase pair
        hourly_data = ped_hourly_df[
            (ped_hourly_df['DeviceId'] == device_id) & 
            (ped_hourly_df['Phase'] == phase)
        ]
        
        if not hourly_data.empty:
            # Sort by timestamp to ensure correct time series
            hourly_data = hourly_data.sort_values('TimeStamp')
            # A week of hourly points is far more detail than a one-inch sparkline
            # can show, so roll them up into wider buckets before drawing.
            bucketed = hourly_data.groupby(
                hourly_data['TimeStamp'].dt.floor(f'{PED_SPARKLINE_BUCKET_HOURS}h')
            )['PedServices'].sum()
            sparkline_data[(device_id, phase)] = bucketed.tolist()
    
    # Add the sparkline data to the result dataframe
    result['Services (7d)'] = result.apply(
        lambda row: sparkline_data.get((row['DeviceId'], row['Phase']), []), 
        axis=1
    )
    
    # Select and order columns
    result, output_columns = _add_ongoing_column(
        result, ['Signal', 'Phase', 'Alert Dates', 'Services (7d)']
    )
    result = result[output_columns]

    # Sort by Signal and Phase
    result = result.sort_values(by=['Signal', 'Phase'])
    
    # Limit the number of rows if needed
    if len(result) > max_rows:
        result = result.head(max_rows)
    
    return result, total_alerts_count

def prepare_system_outages_table(system_outages_df, max_rows=10):
    """
    Prepare a sorted table of system outages showing dates and regions with >30% missing data
    
    Args:
        system_outages_df: DataFrame containing system outages (Date, Region, MissingData)
        max_rows: Maximum number of rows to include in the table
        
    Returns:
        Tuple of (Sorted DataFrame with Date, Region, Missing Data % columns, total_outages_count)
    """
    if system_outages_df.empty:
        return pd.DataFrame(), 0
    
    # Make a copy to avoid modifying the original
    result = system_outages_df.copy()
    
    # Get the total count before limiting rows
    total_outages_count = len(result)
    
    # Convert MissingData to percentage and rename columns
    result['Missing Data %'] = result['MissingData']
    result, output_columns = _add_ongoing_column(result, ['Date', 'Region', 'Missing Data %'])
    result = result[output_columns]


    # Sort by Date descending (most recent first), then by Region
    result = result.sort_values(by=['Date', 'Region'], ascending=[False, True])
    
    # Limit the number of rows if needed
    if max_rows > 0 and len(result) > max_rows:
        result = result.head(max_rows)
    
    return result, total_outages_count


def prepare_clearance_interval_alerts_table(clearance_alerts_df, signals_df, region=None, max_rows=10, include_region=False):
    """
    Prepare clearance interval alerts with compact details text.

    Rows are filtered to the top N largest max deltas, then displayed by signal and
    movement order so the table is stable and easy to scan.
    """
    if clearance_alerts_df is None or clearance_alerts_df.empty:
        return pd.DataFrame(), 0

    df = clearance_alerts_df.copy()
    df['DeviceId'] = df['DeviceId'].astype(str)

    signals_df = signals_df.copy()
    signals_df['DeviceId'] = signals_df['DeviceId'].astype(str)

    result = df.merge(
        signals_df[['DeviceId', 'Name', 'Region']],
        on='DeviceId',
        how='left'
    )
    result = result.dropna(subset=['Name'])
    if result.empty:
        return pd.DataFrame(), 0

    if region and region != "All Regions":
        result = result[result['Region'] == region]
        if result.empty:
            return pd.DataFrame(), 0

    total_alerts_count = len(result)

    result['Signal'] = result['Name']
    result['Movement'] = result.apply(_format_clearance_movement, axis=1)
    result['Median'] = result['MedianDuration'].apply(lambda value: f"{float(value):.1f}s")
    result['Details'] = result.apply(_format_clearance_details, axis=1)
    result['MovementTypeSort'] = result['EventClass'].apply(lambda value: 1 if str(value).startswith('Overlap') else 0)
    result['MovementNumberSort'] = pd.to_numeric(result['EventValue'], errors='coerce').fillna(0).astype(int)
    result['MovementStateSort'] = result['EventClass'].apply(lambda value: 0 if 'Yellow' in str(value) else 1)
    result['MaxAbsDelta'] = pd.to_numeric(result['MaxAbsDelta'], errors='coerce').fillna(0)

    if max_rows > 0 and len(result) > max_rows:
        phase_rows = result[result['MovementTypeSort'] == 0].sort_values('MaxAbsDelta', ascending=False)
        overlap_rows = result[result['MovementTypeSort'] == 1].sort_values('MaxAbsDelta', ascending=False)

        selected_parts = []
        if not phase_rows.empty:
            selected_parts.append(phase_rows.head(max_rows))
        selected_count = sum(len(part) for part in selected_parts)
        remaining_rows = max_rows - selected_count
        if remaining_rows > 0 and not overlap_rows.empty:
            selected_parts.append(overlap_rows.head(remaining_rows))

        result = pd.concat(selected_parts, ignore_index=True) if selected_parts else result.head(0)

    result = result.sort_values(
        by=['Signal', 'MovementTypeSort', 'MovementNumberSort', 'MovementStateSort'],
        ascending=[True, True, True, True]
    )

    output_columns = ['Signal', 'Movement', 'Median', 'Details']
    if include_region:
        output_columns = ['Region'] + output_columns
    result, output_columns = _add_ongoing_column(result, output_columns)

    return result[output_columns], total_alerts_count


def _format_clearance_movement(row):
    event_class = str(row['EventClass'])
    prefix = 'Ovlp' if event_class.startswith('Overlap') else 'Ph'
    state = 'Yellow' if 'Yellow' in event_class else 'Red'
    return f"{prefix} {int(row['EventValue'])} {state}"


def _format_clearance_details(row):
    sample_count = int(row['SampleCount'])
    details = [f"Of {sample_count} samples"]

    short_count = int(row.get('ShortCount', 0) or 0)
    irregular_count = int(row.get('IrregularCount', 0) or 0)
    long_count = int(row.get('LongCount', 0) or 0)
    clauses = []
    if short_count > 0:
        clauses.append(
            f"{short_count} {_were(short_count)} short "
            f"({_format_seconds(row.get('RepresentativeShortDuration'))} at "
            f"{_format_timestamp(row.get('RepresentativeShortTime'))})"
        )
    if irregular_count > 0:
        irregular_duration = _format_seconds(
            row.get('RepresentativeIrregularDuration')
        )
        irregular_time = _format_timestamp(
            row.get('RepresentativeIrregularTime')
        )
        clauses.append(
            f'{irregular_count} {_were(irregular_count)} irregular '
            f'({irregular_duration} at {irregular_time})'
        )
    if long_count > 0:
        clauses.append(
            f"{long_count} {_were(long_count)} long "
            f"({_format_seconds(row.get('RepresentativeLongDuration'))} at "
            f"{_format_timestamp(row.get('RepresentativeLongTime'))})"
        )

    if clauses:
        return f"{details[0]}, " + ", ".join(clauses)
    return details[0]


def _were(count):
    return "was" if int(count) == 1 else "were"


def _format_seconds(value):
    if pd.isna(value):
        return "n/a"
    return f"{float(value):.1f}s"


def prepare_signal_conflicts_table(
    conflicts_df,
    signals_df,
    region=None,
    max_rows=10,
):
    '''Prepare grouped phase/overlap interval conflicts for a report table.'''
    if conflicts_df is None or conflicts_df.empty:
        return pd.DataFrame(), 0

    conflicts = conflicts_df.copy()
    conflicts['DeviceId'] = conflicts['DeviceId'].astype(str)
    conflicts['ConflictStart'] = pd.to_datetime(
        conflicts['ConflictStart'], errors='coerce'
    )
    signals = signals_df.copy()
    signals['DeviceId'] = signals['DeviceId'].astype(str)
    result = conflicts.merge(
        signals[['DeviceId', 'Name', 'Region']],
        on='DeviceId',
        how='left',
    ).dropna(subset=['Name', 'ConflictStart'])
    result['DurationSeconds'] = pd.to_numeric(
        result['DurationSeconds'], errors='coerce'
    )
    result = result.dropna(subset=['DurationSeconds'])

    if region and region != 'All Regions':
        result = result[result['Region'] == region]
    if result.empty:
        return pd.DataFrame(), 0

    group_cols = [
        'Name',
        'Movement1Type',
        'Movement1Number',
        'Movement2Type',
        'Movement2Number',
    ]
    grouped_counts = (
        result
        .groupby(group_cols, dropna=False)
        .size()
        .rename('Conflicts')
        .reset_index()
    )
    max_duration_indices = result.groupby(group_cols, dropna=False)['DurationSeconds'].idxmax()
    max_events = result.loc[
        max_duration_indices,
        _carry_ongoing_column(result.columns, group_cols + ['ConflictStart', 'DurationSeconds']),
    ]
    summary = grouped_counts.merge(max_events, on=group_cols, how='left')

    total_alerts_count = len(summary)
    summary = summary.sort_values(
        ['Name', 'Movement1Type', 'Movement1Number', 'Movement2Type', 'Movement2Number'],
        ascending=True,
    )
    if max_rows > 0:
        summary = summary.head(max_rows)

    summary['Signal'] = summary['Name']
    summary['Pair'] = summary.apply(
        _format_conflict_pair,
        axis=1,
    )
    summary['Max Event'] = summary['ConflictStart'].apply(_format_timestamp)
    summary['Max Duration'] = summary['DurationSeconds'].apply(
        lambda value: f'{float(value):.1f}s'
    )
    summary, output_columns = _add_ongoing_column(
        summary, ['Signal', 'Pair', 'Conflicts', 'Max Event', 'Max Duration']
    )
    return summary[output_columns], total_alerts_count


def _format_conflict_pair(row):
    movement_1_type = 'Ovlp' if str(row['Movement1Type']) == 'Overlap' else 'Ph'
    movement_2_type = 'Ovlp' if str(row['Movement2Type']) == 'Overlap' else 'Ph'
    movement_1_number = int(row['Movement1Number'])
    movement_2_number = int(row['Movement2Number'])
    return f'{movement_1_type} {movement_1_number} / {movement_2_type} {movement_2_number}'


def _format_conflict_movement(row, movement_number):
    movement_type = str(row[f'Movement{movement_number}Type'])
    number = int(row[f'Movement{movement_number}Number'])
    indication = str(row[f'Movement{movement_number}Indication'])
    indication = indication.replace('Overlap ', '')
    prefix = 'Ovlp' if movement_type == 'Overlap' else 'Ph'
    return f'{prefix} {number} {indication}'


def prepare_overlap_dual_indications_table(
    conflicts_df,
    signals_df,
    region=None,
    max_rows=10,
):
    '''Prepare phase-green/same-numbered-overlap conflict rows for a report.'''
    if conflicts_df is None or conflicts_df.empty:
        return pd.DataFrame(), 0

    conflicts = conflicts_df.copy()
    conflicts['DeviceId'] = conflicts['DeviceId'].astype(str)
    conflicts['ConflictStart'] = pd.to_datetime(conflicts['ConflictStart'], errors='coerce')

    signals = signals_df.copy()
    signals['DeviceId'] = signals['DeviceId'].astype(str)
    result = conflicts.merge(
        signals[['DeviceId', 'Name', 'Region']],
        on='DeviceId',
        how='left',
    ).dropna(subset=['Name', 'ConflictStart'])

    if region and region != 'All Regions':
        result = result[result['Region'] == region]
    if result.empty:
        return pd.DataFrame(), 0

    total_alerts_count = len(result)
    result = result.sort_values('ConflictStart', ascending=False)
    if max_rows > 0:
        result = result.head(max_rows)

    result['Signal'] = result['Name']
    result['Phase'] = pd.to_numeric(result['Phase'], errors='coerce').astype('Int64')
    result['Overlap'] = result['OverlapIndication'].str.replace('Overlap ', '', regex=False)
    result['Start'] = result['ConflictStart'].apply(_format_timestamp)
    result['Duration'] = result['DurationSeconds'].apply(lambda value: f'{float(value):.1f}s')

    return result[['Signal', 'Phase', 'Overlap', 'Start', 'Duration']], total_alerts_count


def _format_timestamp(value):
    timestamp = pd.to_datetime(value, errors='coerce')
    if pd.isna(timestamp):
        return "n/a"
    hour = timestamp.hour % 12 or 12
    am_pm = "AM" if timestamp.hour < 12 else "PM"
    tenths = int(timestamp.microsecond / 100000)
    return (
        f"{timestamp.month}/{timestamp.day}/{timestamp.year % 100:02d} "
        f"{hour}:{timestamp.minute:02d}:{timestamp.second:02d}.{tenths} {am_pm}"
    )


def prepare_alarms_table(
    alarms_df,
    signals_df,
    region=None,
    max_rows=10,
):
    '''Prepare controller alarm rows for a report.

    One row per signal and alarm type. Rows are present only for pairs that
    alarmed again on the report day; the count shown covers the trailing six
    weeks. Sorted by signal, then alarm type.
    '''
    if alarms_df is None or alarms_df.empty:
        return pd.DataFrame(), 0

    alarms = alarms_df.copy()
    alarms['DeviceId'] = alarms['DeviceId'].astype(str)
    alarms['LatestAlarm'] = pd.to_datetime(alarms['LatestAlarm'], errors='coerce')

    signals = signals_df.copy()
    signals['DeviceId'] = signals['DeviceId'].astype(str)
    result = alarms.merge(
        signals[['DeviceId', 'Name', 'Region']],
        on='DeviceId',
        how='left',
    ).dropna(subset=['Name'])

    if region and region != 'All Regions':
        result = result[result['Region'] == region]
    if result.empty:
        return pd.DataFrame(), 0

    total_alerts_count = len(result)
    result = result.sort_values(['Name', 'AlarmType'])
    if max_rows > 0:
        result = result.head(max_rows)

    result['Signal'] = result['Name']
    result['Alarm'] = result['AlarmType']
    result['6-Week Total'] = result['TotalCount'].astype(int)
    result['Most Recent'] = result['LatestAlarm'].apply(_format_timestamp)

    return (
        result[['Signal', 'Alarm', '6-Week Total', 'Most Recent']],
        total_alerts_count,
    )


def prepare_preempt_alerts_table(
    preempt_alerts_df,
    signals_df,
    region=None,
    max_rows=10,
):
    '''Prepare preempt frequency alert rows for a report.

    One row per signal and preempt number, sorted by CUSUM score so the
    largest shifts come first. Sparkline_Data carries the pair's daily call
    counts over the retained history.
    '''
    if preempt_alerts_df is None or preempt_alerts_df.empty:
        return pd.DataFrame(), 0

    alerts = preempt_alerts_df.copy()
    alerts['DeviceId'] = alerts['DeviceId'].astype(str)

    signals = signals_df.copy()
    signals['DeviceId'] = signals['DeviceId'].astype(str)
    result = alerts.merge(
        signals[['DeviceId', 'Name', 'Region']],
        on='DeviceId',
        how='left',
    ).dropna(subset=['Name'])

    if region and region != 'All Regions':
        result = result[result['Region'] == region]
    if result.empty:
        return pd.DataFrame(), 0

    total_alerts_count = len(result)
    result = result.sort_values(['CusumScore', 'Name', 'Preempt'], ascending=[False, True, True])
    if max_rows > 0:
        result = result.head(max_rows)

    result['Signal'] = result['Name']
    result['Preempt'] = pd.to_numeric(result['Preempt'], errors='coerce').astype('Int64')
    result['Change'] = result['Direction']
    result['Baseline/Day'] = result['BaselinePerDay'].apply(lambda value: f'{float(value):.1f}')
    result['Recent/Day'] = result['RecentPerDay'].apply(lambda value: f'{float(value):.1f}')
    result['Sparkline_Data'] = result['DailyCounts']

    result, output_columns = _add_ongoing_column(
        result, ['Signal', 'Preempt', 'Change', 'Baseline/Day', 'Recent/Day', 'Sparkline_Data']
    )
    return result[output_columns], total_alerts_count
