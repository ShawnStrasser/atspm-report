import pandas as pd
from .visualization import new_figure
from io import BytesIO
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import Table, TableStyle, Paragraph, Image, Spacer
from reportlab.lib.units import inch
from reportlab.pdfbase.pdfmetrics import stringWidth

from .atspm_detector_health import PED, VEHICLE, affects_operations, health_table_order, signal_wide_rows
from .preempt_cascade import bus_calls_text
from .health_story import detector_label, health_story

# Ongoing-issue rows arrive carrying the date their current run of repeats started.
# Every prepare_* function passes that column through untouched and renders it as
# the display column below, so a table looks exactly as it does today unless the
# caller supplied ongoing rows.
ONGOING_SINCE_COLUMN = 'OngoingSince'
ONGOING_DISPLAY_COLUMN = 'Ongoing'

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


def head_by_signal(result, max_rows, key='Signal'):
    """The first max_rows rows of an already-sorted table, with every row of a signal kept together.

    Signals keep the order of their best-ranked row and each signal's rows stay in
    their order; whole signals are taken until max_rows is reached, so the last
    signal can take the table past max_rows rather than leave some of its rows out.
    A table without the key column (not per signal) is cut at max_rows as before.
    """
    if max_rows <= 0 or result.empty:
        return result
    if key not in result.columns:
        return result.head(max_rows)
    first = {}
    for position, signal in enumerate(result[key]):
        first.setdefault(signal, position)
    ordered = result.assign(_signal_rank=result[key].map(first), _row=range(len(result)))
    ordered = ordered.sort_values(['_signal_rank', '_row'], kind='stable')
    sizes = ordered.groupby('_signal_rank', sort=True).size()
    keep = sizes.index[(sizes.cumsum() - sizes) < max_rows]
    return ordered[ordered['_signal_rank'].isin(keep)].drop(columns=['_signal_rank', '_row'])


def head_signals(result, max_signals, key='Signal'):
    """The rows of the first max_signals signals of an already-sorted table (a signal's place is that of its first row),
    in their order. 0 or None: no limit. A limit that counts signals, where head_by_signal counts rows."""
    if not max_signals or max_signals <= 0 or result.empty or key not in result.columns:
        return result
    return result[result[key].isin(pd.unique(result[key])[:max_signals])]


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
        result = head_by_signal(result, max_rows)
    
    return result, total_alerts_count

def prepare_phase_skip_alerts_table(phase_skip_rows, signals_df, region=None, allowed_pairs=None, min_total_skips=0, max_rows=10,
                                    max_signals=0):
    """
    Prepare the Phase Skip table showing one row per signal per date with aggregated phases.

    Args:
        phase_skip_rows: DataFrame with DeviceId, Phase, Date, TotalSkips
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        region: Optional region filter. When provided (and not "All Regions") the table is limited to that region.
        allowed_pairs: Optional DataFrame with DeviceId and Phase pairs to filter
        min_total_skips: Minimum total skips threshold (applied after aggregation)
        max_rows: Maximum number of rows to include in the table
        max_signals: At most this many signals, in the table's order (0: no limit)

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
    aggregated = head_signals(aggregated, max_signals)

    # Limit the number of rows
    if max_rows > 0 and len(aggregated) > max_rows:
        aggregated = head_by_signal(aggregated, max_rows)

    return aggregated, total_alerts_count


# Sparkline PNGs by their data: a row shows in its region's report and in All
# Regions, and the same series often shows in several tables.
_SPARKLINE_PNGS = {}
_SPARKLINE_CACHE_SIZE = 4096
# Share of the value range kept free above and below a sparkline's line.
SPARKLINE_MARGIN = 0.1
# The usual counts behind a health sparkline's actual ones: seen on white and on the grey bands, and no rival to the blue.
SPARKLINE_EXPECTED_COLOR = '#808080'
SPARKLINE_EXPECTED_WIDTH = 0.6
SPARKLINE_EXPECTED_NOTE = "The grey line in a sparkline is the detector's usual count for that hour."


def create_sparkline(data, width=1.0, height=0.25, color='#1f77b4', expected=None):
    """
    Create a sparkline image from a list of values

    Args:
        data: List of values to plot
        width: Width of the image in inches
        height: Height of the image in inches
        color: Color of the sparkline
        expected: Optional usual values of the same points, a thin muted line behind the actual ones

    Returns:
        ReportLab Image object
    """
    # repr tells 1 from 1.0 and an int from a numpy int, so a hit is the same drawing.
    key = (repr(list(data) if data is not None else data), width, height, color, repr(list(expected)) if expected is not None else None)
    png = _SPARKLINE_PNGS.get(key)
    if png is None:
        if len(_SPARKLINE_PNGS) >= _SPARKLINE_CACHE_SIZE:
            _SPARKLINE_PNGS.clear()
        png = _SPARKLINE_PNGS[key] = _sparkline_png(data, width, height, color, expected)
    return Image(BytesIO(png), width=width*inch, height=height*inch)


def _sparkline_png(data, width, height, color, expected=None) -> bytes:
    values = [value for value in (data or []) if not pd.isna(value)]
    if len(values) < 2:
        # Create an empty image if no data
        fig = new_figure((width, height), dpi=150)
        ax = fig.subplots()
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        buf = BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', pad_inches=0, transparent=True)
        return buf.getvalue()
    
    # Create the sparkline. The axes fill the image exactly, so it is drawn at the size it is shown.
    fig = new_figure((width, height), dpi=150)  # Increased DPI for better quality
    ax = fig.add_axes((0, 0, 1, 1))
    
    # Plot data points as a line only - no markers
    x = list(range(len(data)))
    usual = [value for value in (expected or []) if not pd.isna(value)]
    if usual and len(expected) == len(data):
        ax.plot(x, expected, color=SPARKLINE_EXPECTED_COLOR, linewidth=SPARKLINE_EXPECTED_WIDTH, zorder=1)
        values = values + usual
    ax.plot(x, data, color=color, linewidth=1.0, zorder=2)
    
    # No endpoint marker - removing this line
    # ax.scatter(x[-1], data[-1], color=color, s=15, zorder=3)
    
    # Room above and below the line, so the lowest and highest values keep their full thickness.
    low, high = min(values), max(values)
    span = (high - low) or max(abs(high), 1)
    ax.set_xlim(-0.5, len(data) - 0.5)
    ax.set_ylim(low - SPARKLINE_MARGIN * span, high + SPARKLINE_MARGIN * span)
    
    # Remove axes and borders
    ax.axis('off')
    fig.patch.set_alpha(0)

    buf = BytesIO()
    fig.savefig(buf, format='png', transparent=True)
    return buf.getvalue()

# The space a table actually gets on the page: letter paper with the half-inch
# side margins and 1.2in/0.5in top and bottom margins the report uses, less the
# 6pt of padding a frame keeps on every side.
TABLE_AVAILABLE_WIDTH = 528.0
TABLE_AVAILABLE_HEIGHT = 657.6
# The tallest a signal's merged cell gets in the ranked health tables, whose signals with the most detectors come first. It cannot
# split, so a taller one moves whole to the next page and can leave a heading alone on this one; a signal with more rows repeats
# its name.
MAX_MERGED_HEIGHT = 0.45 * TABLE_AVAILABLE_HEIGHT

# Reportlab's default cell padding, 6pt on each side, and the tighter one of a table
# with more columns than the page has room for.
_CELL_PADDING = 12.0
_COMPACT_PADDING = 6.0
_HEADER_FONT = ('Helvetica-Bold', 10)
_BODY_FONT = ('Helvetica', 9)

# Columns holding a sentence rather than a value. Left to size themselves these
# end up as wide as their longest word, which wraps the sentence into six or
# seven lines and makes the row taller than it has any need to be, so they take
# whatever width the value columns do not need instead.
FLEX_COLUMNS = ('Details', 'What Is Wrong', 'What Happened')
MIN_FLEX_WIDTH = 1.75 * inch
# The health tables' narrative is the point of those tables, so it gets more room:
# a line less per row is pages less.
STORY_COLUMNS = ('What Is Wrong', 'What Happened')
MIN_STORY_WIDTH = 4.1 * inch
# A signal name wraps before anything else does, down to this.
MIN_SIGNAL_WIDTH = 1.45 * inch
# The sparkline's width when its column has room for it.
SPARKLINE_WIDTH = 1.0 * inch
# The detector triage's short label wraps inside a column of this width.
IMPACT_COLUMN = 'Impact'
IMPACT_WIDTH = 1.55 * inch


def _natural_width(header_text, values):
    """Width that fits the header and every value in a column without wrapping."""
    width = stringWidth(str(header_text), *_HEADER_FONT)
    for value in values:
        width = max(width, stringWidth(str(value), *_BODY_FONT))
    return width + _CELL_PADDING


def _word_width(header_text, values, padding=_CELL_PADDING):
    """Width of the longest single word in a column: the narrowest it can wrap to."""
    width = max((stringWidth(w, *_HEADER_FONT) for w in str(header_text).split()), default=0)
    for value in values:
        for word in str(value).split():
            width = max(width, stringWidth(word, *_BODY_FONT))
    # A couple of points spare: a word exactly as wide as its cell still breaks.
    return width + padding + 2


def _column_widths(df_display, header, trend_width=None):
    """Size the value columns to their content and give the rest to the sentence.

    Returns (widths, compact). widths is None when there is no sentence column,
    which leaves reportlab to size everything itself exactly as it did before.
    compact says the table only fits with tighter cell padding. The trend column
    keeps its width throughout, so its sparkline always fits its cell.
    """
    flex_names = FLEX_COLUMNS
    if not any(name in FLEX_COLUMNS for name in header):
        # Without a sentence column, leave it to reportlab unless the row would
        # run off the page. Then the signal name, usually the longest value,
        # wraps to take whatever the other columns leave.
        natural = sum(
            trend_width if trend_width is not None and i == len(header) - 1
            else _natural_width(name, df_display[name] if name in df_display.columns else [])
            for i, name in enumerate(header)
        )
        if natural <= TABLE_AVAILABLE_WIDTH or not header or header[0] != GROUP_COLUMN:
            return None, False
        flex_names = (GROUP_COLUMN,)

    def values(index):
        return df_display[header[index]] if header[index] in df_display.columns else []

    is_trend = [trend_width is not None and index == len(header) - 1 for index in range(len(header))]
    widths = []
    for index, name in enumerate(header):
        if is_trend[index]:
            widths.append(trend_width)
        elif name == IMPACT_COLUMN:
            widths.append(IMPACT_WIDTH)
        elif name in flex_names:
            widths.append(None)
        else:
            widths.append(_natural_width(name, values(index)))

    flex = [i for i, width in enumerate(widths) if width is None]
    fixed = sum(width for width in widths if width is not None)
    min_flex = MIN_STORY_WIDTH if any(name in STORY_COLUMNS for name in header) else MIN_FLEX_WIDTH
    if TABLE_AVAILABLE_WIDTH - fixed >= min_flex * len(flex):
        share = (TABLE_AVAILABLE_WIDTH - fixed) / len(flex)
        return [share if width is None else width for width in widths], False

    # The value columns alone want more room than the page has. The signal name
    # gives first; the others shrink in proportion, wrapping but never narrower
    # than their longest word. If even that leaves the sentence too little, the
    # cells get tighter padding.
    trend_total = (trend_width if trend_width is not None else 0.0) + (IMPACT_WIDTH if IMPACT_COLUMN in header else 0.0)
    keep = TABLE_AVAILABLE_WIDTH - min_flex * len(flex) - trend_total
    squeezable = [i for i, width in enumerate(widths) if width is not None and not is_trend[i] and header[i] != IMPACT_COLUMN]
    for padding, compact in ((_CELL_PADDING, False), (_COMPACT_PADDING, True)):
        floors = {i: _word_width(header[i], values(i), padding) for i in squeezable}
        wanted = {i: widths[i] for i in squeezable}
        if header and header[0] == GROUP_COLUMN and 0 in wanted:
            floors[0] = max(floors[0], min(wanted[0], MIN_SIGNAL_WIDTH))
            wanted[0] = max(floors[0], wanted[0] - max(sum(wanted.values()) - keep, 0.0))
        total = sum(wanted.values())
        scale = keep / total if total > keep and total else 1.0
        squeezed = {i: max(wanted[i] * scale, floors[i]) for i in squeezable}
        remaining = TABLE_AVAILABLE_WIDTH - trend_total - sum(squeezed.values())
        if remaining >= MIN_FLEX_WIDTH * len(flex) or compact:
            break
    result = [squeezed.get(i, width) for i, width in enumerate(widths)]
    return [max(remaining / len(flex), MIN_FLEX_WIDTH * 0.6) if width is None else width for width in result], compact


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
    
    expected_data = None
    if 'Sparkline_Expected' in df_display.columns:
        expected_data = df_display['Sparkline_Expected'].tolist()
        df_display = df_display.drop(columns=['Sparkline_Expected'])
        if any(values for values in expected_data):
            table_notice += f". {SPARKLINE_EXPECTED_NOTE}"

    # Format percentage columns if they exist
    if 'MaxOut %' in df_display.columns:
        df_display['MaxOut %'] = df_display['MaxOut %'].apply(lambda x: f"{x:.1%}")
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
        if expected_data is not None:
            expected_data = [expected_data[i] for i in order]
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
    story_style = ParagraphStyle('StoryCell', parent=styles['Normal'], fontSize=9, leading=11)

    # Add rows
    for _, row in df_display.iterrows():
        values = row.tolist()
        for flex_name in FLEX_COLUMNS:
            if flex_name in header:
                flex_index = header.index(flex_name)
                flex_style = story_style if flex_name in STORY_COLUMNS else styles['Normal']
                values[flex_index] = Paragraph(str(values[flex_index]), flex_style)
        if IMPACT_COLUMN in header:
            impact_index = header.index(IMPACT_COLUMN)
            values[impact_index] = Paragraph(str(values[impact_index]), story_style)
        data.append(values)
      # Create the table
    trend_width = 1.2*inch if include_trend else None  # Make the Trend column wider
    colWidths, compact = _column_widths(df_display, header, trend_width)
    padding = _COMPACT_PADDING if compact else _CELL_PADDING
    if colWidths is not None:
        # A column squeezed below its natural width would run into the next
        # one, so wrap its text instead.
        body = ParagraphStyle('SqueezedCell', parent=styles['Normal'], fontSize=9, leading=11)
        head = ParagraphStyle('SqueezedHeader', parent=body, fontName='Helvetica-Bold', fontSize=10,
                              leading=12, alignment=1)
        for j, name in enumerate(header):
            if include_trend and j == len(header) - 1:
                # The trend column has no text of its own, only a header to wrap.
                if stringWidth(str(name), *_HEADER_FONT) + padding > colWidths[j]:
                    data[0][j] = Paragraph(str(name), head)
                continue
            if colWidths[j] is None or name in FLEX_COLUMNS or name == IMPACT_COLUMN:
                continue
            if colWidths[j] >= _natural_width(name, df_display[name]) - _CELL_PADDING + padding:
                continue
            style = ParagraphStyle(f'SqueezedCell{j}', parent=body, alignment=1) if j == 0 and group_runs else body
            data[0][j] = Paragraph(str(name), head)
            for row in data[1:]:
                if not isinstance(row[j], Paragraph):
                    # A wrapped cell keeps the lines its text was given (one run of hours a line).
                    row[j] = Paragraph(str(row[j]).replace(chr(10), '<br/>'), style)
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
    if compact:
        table_style.add('LEFTPADDING', (0, 0), (-1, -1), _COMPACT_PADDING / 2)
        table_style.add('RIGHTPADDING', (0, 0), (-1, -1), _COMPACT_PADDING / 2)

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
                # Use consistent color for all sparklines, sized to stay inside the cell.
                room = (colWidths[-1] - padding) if colWidths[-1] is not None else SPARKLINE_WIDTH
                sparkline = create_sparkline(data_points, width=min(room, SPARKLINE_WIDTH) / inch, height=0.25,
                                             color='#1f77b4', expected=expected_data[i] if expected_data is not None else None)
                
                # Replace the content of the last column with the sparkline image
                table._cellvalues[row_index][-1] = sparkline

    if group_runs is not None:
        # Merge each signal's name into one cell, centered across its rows. Only the name:
        # a detector's details are its own, so every other cell stays on its row.
        # Measuring first, with every label still in place, gives an upper bound
        # on the row heights, so a merge placed here is certain to fit its page.
        table.wrap(TABLE_AVAILABLE_WIDTH, TABLE_AVAILABLE_HEIGHT)
        row_heights = list(table._rowHeights or [])
        header_height = row_heights[0] if row_heights else 0.0
        span_budget = TABLE_AVAILABLE_HEIGHT - header_height
        if IMPACT_COLUMN in df_display.columns:
            span_budget = min(span_budget, MAX_MERGED_HEIGHT)
        # What must fit under a heading for the table to start on this page, for a caller who wants to keep them together.
        first_rows = _blocks_that_fit(1, group_runs[0][1], row_heights, span_budget)[0][1]
        table.start_height = header_height + sum(row_heights[1:1 + first_rows])
        span_style = TableStyle([])
        cells = [list(row) for row in table._cellvalues]
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
                    cells[block_start + offset][0] = ""
        # Fresh, since a measured table keeps the heights of what it held.
        start_height = table.start_height
        table = Table(cells, colWidths=colWidths)
        table.setStyle(table_style)
        table.setStyle(span_style)
        table.start_height = start_height
        table.wrap(TABLE_AVAILABLE_WIDTH, TABLE_AVAILABLE_HEIGHT)

    result_elements = [
        Paragraph(table_notice, styles['Normal']),
        Spacer(1, 0.05*inch),
        table
    ]
    
    return result_elements

# Missing hours told by time in a table cell; the rest are counted.
MISSING_SPANS_SHOWN = 3
MISSING_HOURLY_HEADER = 'Missing by Hour (7d)'
# An hour a system outage was out: its regions averaged this share missing or more, the daily check's limit.
OUTAGE_HOUR_SHARE = 0.3


def _hour_spans(hours):
    """[(start, end)] of runs of consecutive hours (the hours' start times)."""
    spans = []
    for hour in sorted(pd.to_datetime(pd.Series(list(hours))).dt.floor('h').unique()):
        hour = pd.Timestamp(hour)
        if spans and hour <= spans[-1][1]:
            spans[-1] = (spans[-1][0], max(spans[-1][1], hour + pd.Timedelta(hours=1)))
        else:
            spans.append((hour, hour + pd.Timedelta(hours=1)))
    return spans


def _format_hour_span(start, end):
    """'Sun 11/8 02:00-06:00', or with both days when it crosses midnight: 'Sat 11/7 22:00 to Sun 11/8 06:00'."""
    last = end - pd.Timedelta(seconds=1)
    if last.normalize() == start.normalize():
        until = '24:00' if end == end.normalize() else f"{end:%H:%M}"
        return f"{start:%a} {start.month}/{start.day} {start:%H:%M}-{until}"
    return f"{start:%a} {start.month}/{start.day} {start:%H:%M} to {end:%a} {end.month}/{end.day} {end:%H:%M}"


def hours_text(hours, shown=MISSING_SPANS_SHOWN):
    """The hours as their runs, newest first, the newest `shown` by time and the others counted: one a line."""
    spans = _hour_spans(hours)[::-1]
    lines = [_format_hour_span(start, end) for start, end in spans[:shown]]
    if len(spans) > shown:
        rest = len(spans) - shown
        lines.append(f"and {rest} more {'time' if rest == 1 else 'times'}")
    return '\n'.join(lines)


def _format_hours(value):
    """Hours of missing data, to the quarter hour a 15-minute record is."""
    return f"{round(float(value) * 4) / 4:g}"


def prepare_missing_data_alerts_table(filtered_df_missing_data, signals_df, max_rows=10, hourly=None):
    """
    Prepare a sorted table of missing data alerts with signal name and date

    With hourly (DeviceId, TimeStamp = the hour, MissingData; data_processing.missing_data_by_hour) the table places the
    loss in time instead of naming the worst day: When Missing gives the runs of hours with any record missing, newest
    first, Hours Missing the hours of records lost, and the sparkline the share missing hour by hour. Which signals are
    listed does not change: the alert is still decided by day.

    Args:
        filtered_df_missing_data: DataFrame containing missing data alerts
        signals_df: DataFrame containing signal metadata (DeviceId, Name, Region)
        max_rows: Maximum number of rows to include in the table
        hourly: Optional share of missing records per device and hour over the days shown

    Returns:
        Tuple of (Sorted DataFrame with Signal Name, Date and Alert columns, total_alerts_count)
    """
    if filtered_df_missing_data.empty:
        return pd.DataFrame(), 0
    if hourly is not None and not hourly.empty:
        return _missing_data_hourly_table(filtered_df_missing_data, signals_df, max_rows, hourly)
        
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
        result = head_by_signal(result, max_rows)
    
    return result, unique_devices_with_alerts

def _missing_data_hourly_table(alerts, signals_df, max_rows, hourly):
    """The missing data table by hour: see prepare_missing_data_alerts_table."""
    alerts = alerts.assign(DeviceId=alerts['DeviceId'].astype(str))
    alerted = alerts[alerts['Alert'] == 1]
    rows = alerted.groupby('DeviceId', as_index=False).agg(**_ongoing_aggregation(alerted.columns), _last=('Date', 'max'))
    names = signals_df[['DeviceId', 'Name']].assign(DeviceId=signals_df['DeviceId'].astype(str)).drop_duplicates('DeviceId')
    rows = rows.merge(names, on='DeviceId', how='inner')
    if rows.empty:
        return pd.DataFrame(), 0
    hourly = hourly.assign(DeviceId=hourly['DeviceId'].astype(str)).sort_values('TimeStamp')
    by_device = dict(tuple(hourly[hourly['DeviceId'].isin(rows['DeviceId'])].groupby('DeviceId')))
    empty = hourly.head(0)

    def device_hours(device):
        return by_device.get(device, empty)

    rows['When Missing'] = rows['DeviceId'].map(
        lambda device: hours_text(device_hours(device).loc[lambda t: t['MissingData'] > 0, 'TimeStamp']))
    rows['_hours'] = rows['DeviceId'].map(lambda device: float(device_hours(device)['MissingData'].sum()))
    rows['Hours Missing'] = rows['_hours'].map(_format_hours)
    rows['Sparkline_Data'] = rows['DeviceId'].map(lambda device: device_hours(device)['MissingData'].tolist())
    rows = rows.rename(columns={'Name': 'Signal'}).sort_values(['_hours', 'Signal'], ascending=[False, True])
    total = rows['DeviceId'].nunique()
    result, output_columns = _add_ongoing_column(rows, ['Signal', 'When Missing', 'Hours Missing', 'Sparkline_Data'])
    result = result[output_columns]
    if max_rows > 0 and len(result) > max_rows:
        result = head_by_signal(result, max_rows)
    return result, total


def _format_region_list(regions, all_regions=None):
    """'All 5 regions' when every region is out, else the regions by name."""
    regions = sorted(set(regions))
    if all_regions and set(regions) >= set(all_regions) and len(regions) > 1:
        return f"All {len(regions)} regions"
    return ', '.join(regions)


def _format_date_span(first, last):
    text = f"{first.month}/{first.day}/{first:%y}"
    return text if first == last else f"{text} - {last.month}/{last.day}/{last:%y}"


def prepare_system_outages_table(system_outages_df, max_rows=10, all_regions=None, hourly=None):
    """
    Prepare a table of system outages, one row per real outage.

    The input holds one row per region and day at 30% missing data or more. Days in a
    row are one outage, a day that recovered ends it, and regions out on the same days
    are one outage too. Missing Data % is the outage's worst day, the average over its
    regions. Ongoing says the day it began and how many days it has lasted while it is
    still out on the latest day of data (LatestDataDay), else the day it ended; rows
    without LatestDataDay have no Ongoing column.

    With hourly (Region, TimeStamp = the hour, MissingData; data_processing.region_missing_by_hour) the outage is placed in
    time instead: When gives the runs of hours its regions averaged OUTAGE_HOUR_SHARE missing or more, and Hours Out how
    many hours that was, in place of Days and its worst day's Missing Data %. Which days are an outage does not change.

    Args:
        system_outages_df: DataFrame of Date, Region, MissingData and optionally LatestDataDay
        max_rows: Maximum number of rows to include in the table
        all_regions: Every region of the report, to say 'All 5 regions'
        hourly: Optional share of missing records per region and hour

    Returns:
        Tuple of (DataFrame, newest outage first, total_outages_count)
    """
    if system_outages_df.empty:
        return pd.DataFrame(), 0

    days = system_outages_df.copy()
    days['Date'] = pd.to_datetime(days['Date']).dt.normalize()
    by_day = days.groupby('Date').agg(
        Regions=('Region', list), Worst=('MissingData', 'mean')
    ).reset_index().sort_values('Date')
    by_day['Outage'] = (by_day['Date'].diff() != pd.Timedelta(days=1)).cumsum()
    latest = pd.to_datetime(days['LatestDataDay']).max() if 'LatestDataDay' in days.columns else None

    by_hour = hourly is not None and not hourly.empty
    if by_hour:
        hourly = hourly.assign(_day=pd.to_datetime(hourly['TimeStamp']).dt.normalize())
    rows = []
    for _, outage in by_day.groupby('Outage'):
        first, last = outage['Date'].iloc[0], outage['Date'].iloc[-1]
        regions = [r for regions in outage['Regions'] for r in regions]
        row = {
            'Dates': _format_date_span(first, last),
            'Regions': _format_region_list(regions, all_regions),
        }
        if by_hour:
            # Each hour of its days, averaged over the regions out on that day.
            out = days[days['Date'].between(first, last)][['Date', 'Region']].rename(columns={'Date': '_day'})
            shares = hourly.merge(out, on=['_day', 'Region']).groupby('TimeStamp')['MissingData'].mean()
            out_hours = shares[shares >= OUTAGE_HOUR_SHARE].index
            row['When'] = hours_text(out_hours)
            row['Hours Out'] = len(out_hours)
        else:
            row['Days'] = len(outage)
            row['Missing Data %'] = outage['Worst'].max()
        row['_last'] = last
        if latest is not None:
            row[ONGOING_DISPLAY_COLUMN] = (
                f"{first.month}/{first.day}/{first:%y} ({len(outage)}d)" if last >= latest
                else f"Ended {last.month}/{last.day}/{last:%y}"
            )
        rows.append(row)

    result = pd.DataFrame(rows).sort_values('_last', ascending=False).drop(columns='_last')
    total_outages_count = len(result)
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
        # phases before overlaps, largest delta first; every row of a signal kept together
        ranked = result.sort_values(['MovementTypeSort', 'MaxAbsDelta'], ascending=[True, False])
        result = head_by_signal(ranked, max_rows)

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
    long_count = int(row.get('LongCount', 0) or 0)
    clauses = []
    if short_count > 0:
        clauses.append(
            f"{short_count} {_were(short_count)} short "
            f"({_format_seconds(row.get('RepresentativeShortDuration'))} at "
            f"{_format_timestamp(row.get('RepresentativeShortTime'))})"
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
        summary = head_by_signal(summary, max_rows, key='Name')

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
        result = head_by_signal(result, max_rows, key='Name')

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
    max_signals=0,
):
    '''Prepare controller alarm rows for a report.

    One row per signal and alarm type. Rows are present only for pairs that
    alarmed again on the report day; the count shown covers the trailing six
    weeks. Signals come by their six-week total (all their rows together), the
    most first, each signal's alarms the most first; max_signals keeps the first
    that many signals (0: all).
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
    result = result.assign(_signal_total=result.groupby('Name')['TotalCount'].transform('sum')).sort_values(
        ['_signal_total', 'Name', 'TotalCount', 'AlarmType'], ascending=[False, True, False, True], kind='stable')
    result = head_signals(result, max_signals, key='Name')
    if max_rows > 0:
        result = head_by_signal(result, max_rows, key='Name')

    result['Signal'] = result['Name']
    result['Alarm'] = result['AlarmType']
    result['6-Week Total'] = result['TotalCount'].astype(int)
    result['Most Recent'] = result['LatestAlarm'].apply(_format_timestamp)
    columns = ['Signal', 'Alarm', '6-Week Total', 'Most Recent']
    if 'DailyCounts' in result.columns:
        result['Sparkline_Data'] = result['DailyCounts']
        columns.append('Sparkline_Data')

    return result[columns], total_alerts_count


def prepare_preempt_alerts_table(
    preempt_alerts_df,
    signals_df,
    region=None,
    max_rows=10,
):
    '''Prepare preempt frequency alert rows for a report.

    One row per signal, preempt number and kind of alert (frequency change,
    stuck on, chatter, cascade break), sorted by score. Note holds the cascade
    or duration evidence when present. Sparkline_Data carries the pair's daily
    call counts over the retained history.
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
    result = result.sort_values(['Score', 'Name', 'Preempt'], ascending=[False, True, True])
    if max_rows > 0:
        result = head_by_signal(result, max_rows, key='Name')

    result['Signal'] = result['Name']
    result['Preempt'] = pd.to_numeric(result['Preempt'], errors='coerce').astype('Int64')
    result['Type'] = result['Direction']
    result['Base/Day'] = result['BaselinePerDay'].apply(lambda value: f'{float(value):.1f}')
    result['Last 7d/Day'] = result['RecentPerDay'].apply(lambda value: f'{float(value):.1f}')
    result['Sparkline_Data'] = result['DailyCounts']
    columns = ['Signal', 'Preempt', 'Type', 'Base/Day', 'Last 7d/Day']
    if 'Note' in result.columns and result['Note'].fillna('').astype(str).str.len().gt(0).any():
        result['Details'] = result['Note'].fillna('').astype(str)
        if {'RanAsPreempt', 'BusCalls'} <= set(result.columns):
            # Bus calls on a preempt input: the counts alone.
            bus = result['RanAsPreempt'].notna() & result['BusCalls'].notna()
            result.loc[bus, 'Details'] = [
                bus_calls_text(direction, ran, total)
                for direction, ran, total in zip(result.loc[bus, 'Direction'], result.loc[bus, 'RanAsPreempt'], result.loc[bus, 'BusCalls'])
            ]
        columns.append('Details')

    result, output_columns = _add_ongoing_column(result, columns + ['Sparkline_Data'])
    return result[output_columns], total_alerts_count


def prepare_atspm_detector_health_table(
    alerts_df,
    signals_df,
    statuses,
    region=None,
    max_rows=10,
    story_header='What Is Wrong',
    detector_type=VEHICLE,
    hourly_counts=None,
    ped_hourly_counts=None,
    expected_counts=None,
    impact=None,
    max_signals=0,
):
    '''Prepare atspm-detector health rows for a report (max_signals: at most that many signals, in the table's order).

    statuses picks the rows: the fault status (bad) for the alert table, or an
    information one (lane closure, timing plan, set-up issue) for the tables below it. Sorted by health_table_order. The story column says in one short narrative what is wrong, how bad
    and when (see health_story). detector_type picks vehicle detectors (column Det,
    'Det 22, Ph8 Count, 1 lane'), ped buttons (column Button, 'Button 4, Ph8') or, with None, both in one priority order
    (column Det). hourly_counts, {(DeviceId, Detector): counts per hour}, adds a sparkline for each row, and
    ped_hourly_counts the same for the ped buttons of a table with both. expected_counts, in the same form, adds a vehicle detector's
    usual counts behind its sparkline. Rows carrying the detector triage (see
    detector_triage) come in its order, service lost first, then harm to other phases, and get an Impact column. A signal's
    rows judged as one group, or signal-wide, tell one effect, so they read as one row.
    impact splits the faults in two tables: True keeps the faults measured to affect operations, with their Impact column;
    False keeps the others, which are data only, and has no Impact column. None keeps them all in one table.
    '''
    if alerts_df is None or alerts_df.empty:
        return pd.DataFrame(), 0

    wanted = alerts_df['Status'].isin(statuses)
    if detector_type is not None:
        wanted &= alerts_df['DetectorType'] == detector_type
    alerts = alerts_df[wanted].copy()
    if impact is not None:
        harm = affects_operations(alerts)
        alerts = alerts[harm if impact else ~harm]
    if alerts.empty:
        return pd.DataFrame(), 0
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

    result = health_table_order(result)
    joined_numbers = signal_wide_rows(result)
    result = result.loc[joined_numbers.index]
    total_alerts_count = len(result)
    result = head_signals(result, max_signals, key='Name')
    if max_rows > 0:
        result = head_by_signal(result, max_rows, key='Name')

    result['Signal'] = result['Name']
    number_header = 'Button' if detector_type == PED else 'Det'
    phases = result['TriagePhase'] if 'TriagePhase' in result.columns else [None] * len(result)
    labels = []
    for number, reason, kind, phase, others in zip(result['Detector'].astype(int), result['Reason'], result['DetectorType'], phases,
                                                   joined_numbers.reindex(result.index)):
        if kind == PED and others:
            labels.append(f"Button {', '.join(str(n) for n in sorted([number, *others]))}")
        elif kind == PED:
            label = detector_label(number, reason, 'Button')
            labels.append(f"{label}, Ph{int(phase)}" if pd.notna(phase) and 'Ph' not in label else label)
        elif others:
            labels.append(f"Det {', '.join(str(n) for n in sorted([number, *others]))}")
        else:
            labels.append(detector_label(number, reason, 'Det'))
    result[number_header] = labels
    evidence = result['Evidence'] if 'Evidence' in result.columns else [None] * len(result)
    result[story_header] = [
        health_story(reason, periods, proof, name_detector=False)
        for reason, periods, proof in zip(result['Reason'], result['Periods'], evidence)
    ]
    for index, others in joined_numbers.reindex(result.index).items():
        if others:
            count = len(others)
            item = 'Button' if result.loc[index, 'DetectorType'] == PED else 'Det'
            result.loc[index, story_header] += f" {item} {int(result.loc[index, 'Detector'])} shown; {count} more at fault with it."
    columns = ['Signal', number_header, story_header]
    if impact is not False and 'ImpactLabel' in result.columns and result['ImpactLabel'].notna().any():
        result[IMPACT_COLUMN] = result['ImpactLabel'].fillna('Not measured')
        columns.insert(2, IMPACT_COLUMN)

    result, output_columns = _add_ongoing_column(result, columns)
    if hourly_counts or ped_hourly_counts:
        result['Sparkline_Data'] = [
            (ped_hourly_counts if kind == PED and detector_type is None else hourly_counts or {}).get((device, int(number)), [])
            for device, number, kind in zip(result['DeviceId'], result['Detector'], result['DetectorType'])
        ]
        output_columns = output_columns + ['Sparkline_Data']
        if expected_counts:
            result['Sparkline_Expected'] = [
                expected_counts.get((device, int(number)), []) if kind != PED else []
                for device, number, kind in zip(result['DeviceId'], result['Detector'], result['DetectorType'])
            ]
            output_columns = output_columns + ['Sparkline_Expected']
    return result[output_columns], total_alerts_count
