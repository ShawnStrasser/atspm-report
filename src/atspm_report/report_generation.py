import pandas as pd
import importlib.resources
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, FrameBreak, KeepTogether
from reportlab.platypus.flowables import Flowable
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from io import BytesIO
import matplotlib.pyplot as plt
from datetime import datetime, date
import os
import calendar
from pathlib import Path
from typing import List, Tuple, Union, Dict, Any, Optional
from .table_generation import (
    prepare_phase_termination_alerts_table,
    prepare_phase_skip_alerts_table,
    prepare_detector_health_alerts_table,
    prepare_ped_alerts_table,
    prepare_missing_data_alerts_table,
    prepare_system_outages_table,
    prepare_clearance_interval_alerts_table,
    prepare_overlap_dual_indications_table,
    prepare_alarms_table,
    prepare_preempt_alerts_table,
    prepare_signal_conflicts_table,
    create_reportlab_table,
    ONGOING_SINCE_COLUMN,
)
from .utils import log_message

# Repeat alerts are normally held back so they do not crowd out new ones. When the
# caller passes them in, each check gets a second "Ongoing ..." section - its own
# table and charts - directly after the "New ..." section for that same check.
ONGOING_SECTION_EXPLANATION = (
    'These issues were reported previously and are still occurring, so they are held '
    'out of the new alerts above to keep genuinely new problems visible. Ongoing gives '
    'the date the current run of repeat alerts started, and how many days ago that was.'
)


def _has_rows(df: Optional[pd.DataFrame]) -> bool:
    """True when a DataFrame exists and holds at least one row."""
    return df is not None and not df.empty


def _chart_flowables(figures: list) -> list:
    """Lay out alert charts one per row, each kept whole on a single page."""
    elements = []
    for figure in figures:
        elements.append(KeepTogether([MatplotlibFigure(figure, width=6.5*inch, height=2.8*inch)]))
        elements.append(Spacer(1, 0.15*inch))
        plt.close(figure)
    return elements


def _ongoing_section(
        title: str,
        rows_df: Optional[pd.DataFrame],
        total_count: int,
        styles,
        max_rows: int,
        figures: Optional[list] = None,
        include_trend: bool = False,
        trend_header: str = 'Trend',
) -> list:
    """Build the 'Ongoing <title>' section, or nothing when there is nothing ongoing."""
    if not _has_rows(rows_df):
        return []

    elements = [
        Paragraph(f'Ongoing {title}', styles['SectionHeading']),
        Spacer(1, 0.1*inch),
        Paragraph(ONGOING_SECTION_EXPLANATION, styles['Normal']),
        Spacer(1, 0.2*inch),
    ]
    elements.extend(create_reportlab_table(
        rows_df,
        f'Ongoing {title}',
        styles,
        total_count=total_count,
        max_rows=max_rows,
        include_trend=include_trend,
        trend_header=trend_header,
    ))
    elements.append(Spacer(1, 0.3*inch))
    elements.extend(_chart_flowables(figures or []))
    return elements

# Load jokes from package data
def _load_jokes() -> list[str]:
    """Load jokes from package data."""
    try:
        with importlib.resources.files(__package__).joinpath('jokes.csv').open(encoding='utf-8') as f:
            df = pd.read_csv(f)
            return df['Joke'].tolist()
    except Exception as e:
        print(f"Warning: Could not load jokes.csv: {e}")
        return ["Why did the traffic engineer break up with the signal? The timing was off!"]

_JOKES = _load_jokes()

def get_joke(joke_index: int = None) -> str:
    """
    Get a joke for the report.
    
    Args:
        joke_index: Specific joke index (0-based). If None, auto-cycles based on today's date.
    
    Returns:
        Joke string
    """
    if not _JOKES:
        return "Why did the traffic engineer break up with the signal? The timing was off!"
    
    if joke_index is not None:
        # Use provided index (wrap around if out of range)
        idx = joke_index % len(_JOKES)
    else:
        # Auto-cycle based on today's date
        days_since_epoch = (date.today() - date(1970, 1, 1)).days
        idx = days_since_epoch % len(_JOKES)
    
    return _JOKES[idx]

def get_logo_path(custom_logo_path: str = None) -> str:
    """
    Get the logo path to use in reports.
    
    Args:
        custom_logo_path: User-provided logo file path. If None, uses default ODOT logo.
    
    Returns:
        Path to logo image file, or None if not found
    """
    if custom_logo_path:
        if Path(custom_logo_path).exists():
            return custom_logo_path
        else:
            print(f"Warning: Custom logo not found at {custom_logo_path}, using default")
    
    # Use default logo from package
    try:
        with importlib.resources.as_file(
            importlib.resources.files(__package__).joinpath('images/logo.png')
        ) as path:
            return str(path)
    except Exception as e:
        print(f"Warning: Could not load default logo: {e}")
        return None

def get_signal_head_path() -> str:
    """Get the signal head icon path from package data."""
    try:
        with importlib.resources.as_file(
            importlib.resources.files(__package__).joinpath('images/signal_head.png')
        ) as path:
            return str(path)
    except Exception as e:
        print(f"Warning: Could not load signal head icon: {e}")
        return None


class PageNumCanvas(canvas.Canvas):
    """Canvas that knows its page count for numbering"""
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []
        self._saved_footer_handler = None

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        """Add page info to each page (page x of y)"""
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            if self._saved_footer_handler:
                self._saved_footer_handler(self, page_num=self._pageNumber, num_pages=num_pages)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def set_footer_handler(self, handler):
        """Set the function that will draw the footer"""
        self._saved_footer_handler = handler


class HeaderFooter:
    """Handles header for the PDF report"""
    def __init__(self, logo_path: str, signal_head_path: str, region: str = None):
        self.logo_path = logo_path
        self.signal_head_path = signal_head_path
        self.region = region

    def draw_header(self, canvas, doc):
        """Draw the header on the first page"""
        # Logo on the left
        try:
            if os.path.exists(self.logo_path):
                canvas.drawImage(self.logo_path,
                               doc.leftMargin,
                               doc.height + doc.topMargin - 0.7*inch,
                               width=1.8*inch,
                               height=0.7*inch,
                               preserveAspectRatio=True)
            else:
                print(f"Warning: Logo file not found at {self.logo_path}")
        except Exception as e:
            print(f"Error loading logo: {e}")

        # Title 
        canvas.setFont('Helvetica-Bold', 24)
        canvas.setFillColor(colors.black)
        title_text = "ATSPM Report"
        title_width = canvas.stringWidth(title_text, "Helvetica-Bold", 24)
        title_x = doc.width + doc.leftMargin - title_width - 0.5*inch  # Move title left to make room for icon
        canvas.drawString(title_x,
                         doc.height + doc.topMargin - 0.3*inch, title_text)

        # Traffic light image - to the right of the title and higher up
        try:
            if os.path.exists(self.signal_head_path):
                canvas.drawImage(self.signal_head_path,
                               title_x + title_width + 0.1*inch,  # Position right after title text
                               doc.height + doc.topMargin - 0.35*inch,  # Moved higher
                               width=0.35*inch,  # Slightly smaller
                               height=0.35*inch,  # Slightly smaller
                               preserveAspectRatio=True)
            else:
                print(f"Warning: Signal image not found at {self.signal_head_path}")
        except Exception as e:
            print(f"Error loading signal image: {e}")

        # Subtitle with bold and italic style - right aligned
        canvas.setFont('Times-BoldItalic', 12)
        subtitle = "More Problems You Didn't Know You Had"
        subtitle_width = canvas.stringWidth(subtitle, "Times-BoldItalic", 12)
        canvas.drawString(doc.width + doc.leftMargin - subtitle_width,
                         doc.height + doc.topMargin - 0.55*inch, subtitle)

        # Draw horizontal line
        canvas.setStrokeColor(colors.black)
        canvas.setLineWidth(1)
        canvas.line(doc.leftMargin, doc.height + doc.topMargin - 0.8*inch,
                   doc.width + doc.leftMargin, doc.height + doc.topMargin - 0.8*inch)

    def firstPage(self, canvas, doc):
        """First page gets a header"""
        canvas.saveState()
        self.draw_header(canvas, doc)
        canvas.restoreState()

    def laterPages(self, canvas, doc):
        """Later pages get nothing - footer handled by PageNumCanvas"""
        pass


class MatplotlibFigure(Flowable):
    """A Flowable wrapper for matplotlib figures"""
    def __init__(self, figure: plt.Figure, width: float = 6.5*inch, height: float = 3*inch):
        Flowable.__init__(self)
        self.figure = figure
        self.width = width
        self.height = height

    def draw(self):
        try:
            # Create a BytesIO buffer and save the figure to it
            buf = BytesIO()
            self.figure.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            buf.seek(0)

            # Use ReportLab's canvas to draw the image
            img = Image(buf, width=self.width, height=self.height)
            img.drawOn(self.canv, 0, 0)
            buf.close()
        except Exception as e:
            # If there's an error, print a message in the PDF
            self.canv.setFont('Helvetica', 12)
            self.canv.setFillColor(colors.red)
            self.canv.drawString(inch, self.height/2, f"Error drawing plot: {str(e)}")
            print(f"Plot rendering error: {e}")


def draw_page_footer(canvas, page_num, num_pages, region=None):
    """Draw the footer with page numbers"""
    width = float(canvas._pagesize[0])
    left_margin = 0.5*inch
    
    canvas.saveState()
    canvas.setFont('Helvetica', 10)
    
    # Left side: Date
    today = datetime.today().strftime("%B %d, %Y")
    canvas.drawString(left_margin, 0.5*inch, today)
    
    # Center: Region
    if region:
        region_text = str(region)
        text_width = canvas.stringWidth(region_text, 'Helvetica', 10)
        canvas.drawString(
            width/2 - text_width/2,
            0.5*inch,
            region_text
        )
    
    # Right side: Page numbers
    page_text = f"Page {page_num} of {num_pages}"
    text_width = canvas.stringWidth(page_text, 'Helvetica', 10)
    canvas.drawString(
        width - text_width - left_margin,
        0.5*inch,
        page_text
    )
    canvas.restoreState()


def generate_pdf_report(
        filtered_df_maxouts: pd.DataFrame, 
        filtered_df_actuations: pd.DataFrame,
        filtered_df_ped: pd.DataFrame,
        ped_hourly_df: pd.DataFrame,
        filtered_df_missing_data: pd.DataFrame,
        system_outages_df: pd.DataFrame,
        phase_figures: List[tuple[plt.Figure, str]],
        detector_figures: List[tuple[plt.Figure, str]],
        ped_figures: List[tuple[plt.Figure, str]],
        missing_data_figures: List[tuple[plt.Figure, str]],
        signals_df: pd.DataFrame = None,
        detector_hourly_df: pd.DataFrame = None,
        save_to_disk: bool = False,
        max_table_rows: int = 10,
        verbosity: int = 1,
        phase_skip_rows: pd.DataFrame = None,
        phase_skip_figures: List[tuple[plt.Figure, str]] = None,
        phase_skip_alerts_df: Optional[pd.DataFrame] = None,
        phase_skip_threshold: Optional[float] = None,
        clearance_alerts_df: Optional[pd.DataFrame] = None,
        overlap_dual_indications_df: Optional[pd.DataFrame] = None,
        general_phase_conflicts_df: Optional[pd.DataFrame] = None,
        overlap_conflicts_df: Optional[pd.DataFrame] = None,
        same_movement_color_conflicts_df: Optional[pd.DataFrame] = None,
        alarms_df: Optional[pd.DataFrame] = None,
        preempt_alerts_df: Optional[pd.DataFrame] = None,
        clearance_yellow_min_seconds: float = 3.5,
        clearance_red_min_seconds: float = 0.5,
        joke_index: int = None,
        custom_logo_path: str = None,
        ongoing_alerts: Optional[Dict[str, pd.DataFrame]] = None,
        ongoing_phase_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        ongoing_detector_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        ongoing_ped_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        ongoing_missing_data_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        ongoing_phase_skip_figures: Optional[List[tuple[plt.Figure, str]]] = None
) -> Dict[str, BytesIO]:
    """Generate PDF reports for each region with the plots.
    
    Args:
        filtered_df_maxouts: DataFrame with phase termination alerts
        filtered_df_actuations: DataFrame with detector health alerts
        filtered_df_ped: DataFrame with pedestrian alerts
        ped_hourly_df: DataFrame with pedestrian hourly data
        detector_hourly_df: DataFrame with hourly detector actuation counts, used for
            the detector health sparklines
        filtered_df_missing_data: DataFrame with missing data alerts
        system_outages_df: DataFrame with system-wide outages (Date, Region, MissingData)
        phase_figures: List of (figure, region) tuples for phase termination
        detector_figures: List of (figure, region) tuples for detector health
        ped_figures: List of (figure, region) tuples for pedestrian alerts
        missing_data_figures: List of (figure, region) tuples for missing data
        signals_df: DataFrame with signal information
        save_to_disk: Must be False (legacy parameter, kept for compatibility)
        max_table_rows: Maximum number of rows to show in each alert table
        verbosity: Verbosity level (0=silent, 1=info, 2=debug)
        phase_skip_rows: DataFrame containing combined Phase Skip alert rows (for tables)
        phase_skip_figures: List of (figure, region) tuples for Phase Skip charts
        phase_skip_alerts_df: DataFrame with Phase Skip alerts after suppression
        phase_skip_threshold: Minimum per-row skips to display in the Phase Skip table
        clearance_alerts_df: DataFrame with clearance interval alerts after suppression
        overlap_dual_indications_df: Same-numbered phase/overlap conflicts after suppression
        general_phase_conflicts_df: Standard conflicting phase indications after suppression
        overlap_conflicts_df: Conflicting phase/overlap indications after suppression
        same_movement_color_conflicts_df: Same phase/overlap multi-color conflicts after suppression
        alarms_df: Controller alarms listed this run (never suppressed)
        preempt_alerts_df: Preempt frequency shift alerts after suppression
        clearance_yellow_min_seconds: Yellow clearance threshold used in report text
        clearance_red_min_seconds: Red clearance threshold used in report text
        joke_index: Specific joke index to use (0-based), None for date-based cycling
        custom_logo_path: Path to custom logo file, None for default ODOT logo
        ongoing_alerts: Alert type -> repeat alerts held back by suppression, carrying an
            OngoingSince column. Each type present adds an 'Ongoing ...' section after the
            matching 'New ...' one; pass None or empty frames for new alerts only
        ongoing_phase_figures: Charts for the ongoing phase termination section
        ongoing_detector_figures: Charts for the ongoing detector health section
        ongoing_ped_figures: Charts for the ongoing pedestrian detector section
        ongoing_missing_data_figures: Charts for the ongoing missing data section
        ongoing_phase_skip_figures: Charts for the ongoing phase skip section

    Returns:
        Dict mapping region name to BytesIO containing PDF bytes
    """
    # Get unique regions from figure collections and Phase Skip tables
    figure_collections = [
        phase_figures or [],
        detector_figures or [],
        ped_figures or [],
        missing_data_figures or [],
        phase_skip_figures or []
    ]
    regions = set()
    for collection in figure_collections:
        regions.update(region for _, region in collection)

    if phase_skip_rows is not None and not phase_skip_rows.empty and signals_df is not None:
        region_lookup = (
            phase_skip_rows[['DeviceId']]
            .drop_duplicates()
            .merge(signals_df[['DeviceId', 'Region']], on='DeviceId', how='left')
        )
        regions.update(region_lookup['Region'].dropna().tolist())

    if phase_skip_alerts_df is not None and not phase_skip_alerts_df.empty and signals_df is not None:
        alert_region_lookup = (
            phase_skip_alerts_df[['DeviceId']]
            .drop_duplicates()
            .merge(signals_df[['DeviceId', 'Region']], on='DeviceId', how='left')
        )
        regions.update(alert_region_lookup['Region'].dropna().tolist())

    if clearance_alerts_df is not None and not clearance_alerts_df.empty and signals_df is not None:
        clearance_region_lookup = (
            clearance_alerts_df[['DeviceId']]
            .drop_duplicates()
            .merge(signals_df[['DeviceId', 'Region']], on='DeviceId', how='left')
        )
        regions.update(clearance_region_lookup['Region'].dropna().tolist())

    if overlap_dual_indications_df is not None and not overlap_dual_indications_df.empty and signals_df is not None:
        overlap_region_lookup = (
            overlap_dual_indications_df[['DeviceId']]
            .drop_duplicates()
            .merge(signals_df[['DeviceId', 'Region']], on='DeviceId', how='left')
        )
        regions.update(overlap_region_lookup['Region'].dropna().tolist())

    for conflict_df in [
        general_phase_conflicts_df,
        overlap_conflicts_df,
        same_movement_color_conflicts_df,
        preempt_alerts_df,
    ]:
        if conflict_df is not None and not conflict_df.empty and signals_df is not None:
            conflict_region_lookup = (
                conflict_df[['DeviceId']]
                .drop_duplicates()
                .merge(signals_df[['DeviceId', 'Region']], on='DeviceId', how='left')
            )
            regions.update(conflict_region_lookup['Region'].dropna().tolist())

    # A region can have nothing new today yet still have ongoing issues worth a report.
    ongoing_alerts = ongoing_alerts or {}
    for ongoing_df in ongoing_alerts.values():
        if not _has_rows(ongoing_df):
            continue
        if 'Region' in ongoing_df.columns:
            regions.update(ongoing_df['Region'].dropna().unique().tolist())
        elif 'DeviceId' in ongoing_df.columns and signals_df is not None:
            ongoing_region_lookup = (
                ongoing_df[['DeviceId']]
                .drop_duplicates()
                .merge(signals_df[['DeviceId', 'Region']], on='DeviceId', how='left')
            )
            regions.update(ongoing_region_lookup['Region'].dropna().tolist())

    if not regions and signals_df is not None:
        regions.update(signals_df['Region'].unique().tolist())

    if regions:
        regions.add("All Regions")
        regions = sorted(regions)
    allowed_phase_skip_pairs = None
    if phase_skip_alerts_df is not None and not phase_skip_alerts_df.empty:
        allowed_phase_skip_pairs = phase_skip_alerts_df[['DeviceId', 'Phase']].drop_duplicates()

    # Phase skip tables are built from the full wait-time rows rather than the alert
    # frame, so the ongoing dates have to be carried onto those rows by device/phase.
    ongoing_phase_skip_pairs = None
    ongoing_phase_skip_rows = None
    ongoing_phase_skips_df = ongoing_alerts.get('phase_skips')
    if _has_rows(ongoing_phase_skips_df) and _has_rows(phase_skip_rows):
        ongoing_phase_skip_pairs = ongoing_phase_skips_df[['DeviceId', 'Phase']].drop_duplicates()
        ongoing_phase_skip_rows = phase_skip_rows.merge(
            ongoing_phase_skips_df[['DeviceId', 'Phase', ONGOING_SINCE_COLUMN]]
            .drop_duplicates(subset=['DeviceId', 'Phase']),
            on=['DeviceId', 'Phase'],
            how='inner',
        )

    buffer_objects = []

    # Get joke for this report
    joke_text = get_joke(joke_index)
    joke_title = "Joke of the Week"

    # Process each individual region first
    for region in regions:
        log_message(f"Generating report for {region}...", 1, verbosity)
        # Filter figures for this region
        region_phase_figures = [fig for fig, reg in phase_figures if reg == region]
        region_detector_figures = [fig for fig, reg in detector_figures if reg == region]
        region_ped_figures = [fig for fig, reg in ped_figures if reg == region]
        region_missing_data_figures = [fig for fig, reg in missing_data_figures if reg == region]
        region_ongoing_phase_figures = [fig for fig, reg in (ongoing_phase_figures or []) if reg == region]
        region_ongoing_detector_figures = [fig for fig, reg in (ongoing_detector_figures or []) if reg == region]
        region_ongoing_ped_figures = [fig for fig, reg in (ongoing_ped_figures or []) if reg == region]
        region_ongoing_missing_data_figures = [
            fig for fig, reg in (ongoing_missing_data_figures or []) if reg == region
        ]
        region_ongoing_phase_skip_figures = [
            fig for fig, reg in (ongoing_phase_skip_figures or []) if reg == region
        ]

        # Filter signals
        if region == "All Regions":
            region_signals_df = signals_df
        else:
            region_signals_df = signals_df[signals_df['Region'] == region] if signals_df is not None else None

        # Create header/footer handler
        logo_path = get_logo_path(custom_logo_path)
        signal_head_path = get_signal_head_path()
        header_footer = HeaderFooter(
            logo_path=logo_path if logo_path else "",
            signal_head_path=signal_head_path if signal_head_path else "",
            region=region
        )

        # Create document with custom canvas
        def make_canvas(*args, **kwargs):
            canvas = PageNumCanvas(*args, **kwargs)
            canvas.set_footer_handler(
                lambda c, page_num, num_pages: draw_page_footer(c, page_num, num_pages, region)
            )
            return canvas

        # Determine if we're writing to disk or memory
        # Create a BytesIO buffer for this report
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            leftMargin=0.5*inch,
            rightMargin=0.5*inch,
            topMargin=1.2*inch,
            bottomMargin=0.5*inch
        )

        # Content building
        content = []

        # Add report title and date
        styles = getSampleStyleSheet()
        styles['Title'].fontSize = 16
        styles['Title'].spaceAfter = 12
        styles['Title'].leading = 18
        
        styles.add(ParagraphStyle(
            name='SectionHeading',
            parent=styles['Heading2'],
            fontSize=14,
            spaceAfter=8,
            textColor=colors.navy
        ))
        
        styles.add(ParagraphStyle(
            name='SubsectionHeading',
            parent=styles['Heading3'],
            fontSize=12,
            spaceAfter=6,
            textColor=colors.navy
        ))

        # Add extra space after the header line
        content.append(Spacer(1, 0.3*inch))

        # Header: Report for this region
        content.append(Paragraph(f"{region}", styles['Title']))
        content.append(Spacer(1, 0.2*inch))

        # Introduction text
        any_ongoing = any(_has_rows(df) for df in ongoing_alerts.values())
        recurring_note = (
            """Each section lists new alerts first, followed by an Ongoing Issues table of
            problems that were reported before and are still occurring."""
            if any_ongoing else
            """These are new alerts only, recurring issues are not shown but will be added in a future update."""
        )
        intro_text = f"""This report for {region} includes alerts for phase skips, increased percent maxout, vehicle & pedestrian detector performance, and data completeness.
        {recurring_note}
        """
        content.append(Paragraph(intro_text, styles['Normal']))
        content.append(Spacer(1, 0.2*inch))

        # Joke section comes first in the report body.
        content.append(Paragraph(joke_title, styles['SectionHeading']))
        content.append(Paragraph(joke_text, styles['Normal']))
        content.append(Spacer(1, 0.3*inch))

        # Section: Controller Alarms
        if alarms_df is not None and not alarms_df.empty and signals_df is not None:
            region_alarm_rows, total_alarm_alerts = prepare_alarms_table(
                alarms_df,
                signals_df,
                region=region,
                max_rows=max_table_rows,
            )
        else:
            region_alarm_rows = pd.DataFrame()
            total_alarm_alerts = 0

        if not region_alarm_rows.empty:
            content.append(Paragraph('Controller Alarms', styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            explanation = (
                'Controller alarm events reported by the cabinet. A signal and alarm type '
                'is listed only when it alarmed again on the most recent day, so a pair '
                'drops off once the underlying problem is resolved. The count covers the '
                'trailing six weeks, which shows how long a recurring problem has persisted.'
            )
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
            content.extend(create_reportlab_table(
                region_alarm_rows,
                'Controller Alarms',
                styles,
                total_count=total_alarm_alerts,
                max_rows=max_table_rows,
                include_trend=False,
            ))
            content.append(Spacer(1, 0.3*inch))

        # Section: Preempt Monitoring
        if preempt_alerts_df is not None and not preempt_alerts_df.empty and signals_df is not None:
            region_preempt_rows, total_preempt_alerts = prepare_preempt_alerts_table(
                preempt_alerts_df,
                signals_df,
                region=region,
                max_rows=max_table_rows,
            )
        else:
            region_preempt_rows = pd.DataFrame()
            total_preempt_alerts = 0

        if _has_rows(ongoing_alerts.get('preempts')) and signals_df is not None:
            region_preempt_ongoing_rows, total_preempt_ongoing = prepare_preempt_alerts_table(
                ongoing_alerts['preempts'],
                signals_df,
                region=region,
                max_rows=max_table_rows,
            )
        else:
            region_preempt_ongoing_rows = pd.DataFrame()
            total_preempt_ongoing = 0

        if not region_preempt_rows.empty:
            content.append(Paragraph('New Preempt Monitoring Alerts', styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            explanation = (
                'Preempt call frequency is tracked for each signal and preempt number. '
                'Baseline/Day is the typical (median) number of calls per day over the '
                'trailing six weeks, excluding the most recent seven days of data, which '
                'are compared against it as Recent/Day. A pair is listed when its recent '
                'calls shifted well outside the day-to-day variation expected at its '
                'baseline, in either direction. A Decrease is only reported for preempts '
                'that normally fire at least once a day. Each signal and preempt is '
                'reported once per shift; the trend shows daily calls over the history.'
            )
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
            content.extend(create_reportlab_table(
                region_preempt_rows,
                'Preempt Monitoring',
                styles,
                total_count=total_preempt_alerts,
                max_rows=max_table_rows,
                trend_header='Calls/Day (6wk)',
            ))
            content.append(Spacer(1, 0.3*inch))

        content.extend(_ongoing_section(
            'Preempt Monitoring Alerts',
            region_preempt_ongoing_rows,
            total_preempt_ongoing,
            styles,
            max_table_rows,
            include_trend=True,
            trend_header='Calls/Day (6wk)',
        ))

        if clearance_alerts_df is not None and not clearance_alerts_df.empty and signals_df is not None:
            region_clearance_rows, total_clearance_alerts = prepare_clearance_interval_alerts_table(
                clearance_alerts_df,
                signals_df,
                region=region,
                max_rows=max_table_rows
            )
        else:
            region_clearance_rows = pd.DataFrame()
            total_clearance_alerts = 0

        if _has_rows(ongoing_alerts.get('clearance_intervals')) and signals_df is not None:
            region_clearance_ongoing_rows, total_clearance_ongoing = (
                prepare_clearance_interval_alerts_table(
                    ongoing_alerts['clearance_intervals'],
                    signals_df,
                    region=region,
                    max_rows=max_table_rows,
                )
            )
        else:
            region_clearance_ongoing_rows = pd.DataFrame()
            total_clearance_ongoing = 0

        if (
            overlap_dual_indications_df is not None
            and not overlap_dual_indications_df.empty
            and signals_df is not None
        ):
            region_overlap_dual_rows, total_overlap_dual_alerts = (
                prepare_overlap_dual_indications_table(
                    overlap_dual_indications_df,
                    signals_df,
                    region=region,
                    max_rows=max_table_rows,
                )
            )
        else:
            region_overlap_dual_rows = pd.DataFrame()
            total_overlap_dual_alerts = 0

        if not region_overlap_dual_rows.empty:
            content.append(Paragraph('Overlap Dual Indications', styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            explanation = (
                'These conflicts occur when a phase is green while the same-numbered '
                'overlap is yellow or red. Invalid timeline intervals are excluded.'
            )
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
            content.extend(create_reportlab_table(
                region_overlap_dual_rows,
                'Overlap Dual Indications',
                styles,
                total_count=total_overlap_dual_alerts,
                max_rows=max_table_rows,
                include_trend=False,
            ))
            content.append(Spacer(1, 0.3*inch))

        if (
            general_phase_conflicts_df is not None
            and not general_phase_conflicts_df.empty
            and signals_df is not None
        ):
            region_general_phase_rows, total_general_phase_conflicts = (
                prepare_signal_conflicts_table(
                    general_phase_conflicts_df,
                    signals_df,
                    region=region,
                    max_rows=max_table_rows,
                )
            )
        else:
            region_general_phase_rows = pd.DataFrame()
            total_general_phase_conflicts = 0

        if _has_rows(ongoing_alerts.get('general_phase_conflicts')) and signals_df is not None:
            region_general_phase_ongoing_rows, total_general_phase_ongoing = (
                prepare_signal_conflicts_table(
                    ongoing_alerts['general_phase_conflicts'],
                    signals_df,
                    region=region,
                    max_rows=max_table_rows,
                )
            )
        else:
            region_general_phase_ongoing_rows = pd.DataFrame()
            total_general_phase_ongoing = 0

        if not region_general_phase_rows.empty:
            content.append(Paragraph('New General Phase Conflicts', styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            explanation = (
                'These conflicts identify overlapping green, yellow, or red-clearance '
                'intervals for phase pairs prohibited by the standard dual-ring sequence. '
                'Intervals that only touch at a shared end/start timestamp are allowed. '
                'This check assumes default sequence operation and can raise false alarms '
                'for signals using non-standard phasing; configure those signals as excluded.'
            )
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
            content.extend(create_reportlab_table(
                region_general_phase_rows,
                'General Phase Conflicts',
                styles,
                total_count=total_general_phase_conflicts,
                max_rows=max_table_rows,
                include_trend=False,
            ))
            content.append(Spacer(1, 0.3*inch))

        content.extend(_ongoing_section(
            'General Phase Conflicts',
            region_general_phase_ongoing_rows,
            total_general_phase_ongoing,
            styles,
            max_table_rows,
        ))

        if (
            overlap_conflicts_df is not None
            and not overlap_conflicts_df.empty
            and signals_df is not None
        ):
            region_overlap_conflict_rows, total_overlap_conflicts = (
                prepare_signal_conflicts_table(
                    overlap_conflicts_df,
                    signals_df,
                    region=region,
                    max_rows=max_table_rows,
                )
            )
        else:
            region_overlap_conflict_rows = pd.DataFrame()
            total_overlap_conflicts = 0

        if _has_rows(ongoing_alerts.get('overlap_conflicts')) and signals_df is not None:
            region_overlap_conflict_ongoing_rows, total_overlap_conflict_ongoing = (
                prepare_signal_conflicts_table(
                    ongoing_alerts['overlap_conflicts'],
                    signals_df,
                    region=region,
                    max_rows=max_table_rows,
                )
            )
        else:
            region_overlap_conflict_ongoing_rows = pd.DataFrame()
            total_overlap_conflict_ongoing = 0

        if not region_overlap_conflict_rows.empty:
            content.append(Paragraph('New Overlap Conflicts', styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            explanation = (
                'These conflicts identify configured overlap green or yellow intervals '
                'running with a prohibited phase or overlap green/yellow interval. '
                'Red indications are not included in this check. This check assumes default '
                'sequence and overlap compatibility groups and can raise false alarms for '
                'signals using non-standard phasing; configure those signals as excluded.'
            )
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
            content.extend(create_reportlab_table(
                region_overlap_conflict_rows,
                'Overlap Conflicts',
                styles,
                total_count=total_overlap_conflicts,
                max_rows=max_table_rows,
                include_trend=False,
            ))
            content.append(Spacer(1, 0.3*inch))

        content.extend(_ongoing_section(
            'Overlap Conflicts',
            region_overlap_conflict_ongoing_rows,
            total_overlap_conflict_ongoing,
            styles,
            max_table_rows,
        ))

        if _has_rows(region_clearance_rows):
            content.append(Paragraph('New Clearance Interval Alerts', styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            explanation = (
                'Clearance interval alerts identify short, irregular, or long yellow '
                'intervals and red intervals below the configured global minimum. Each '
                'movement is judged against a single day of data - the most recent day '
                'available - so the median shown is that day\'s median for that movement.'
            )
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
            content.extend(create_reportlab_table(
                region_clearance_rows,
                'Clearance Interval Alerts',
                styles,
                total_count=total_clearance_alerts,
                max_rows=max_table_rows,
                include_trend=False,
            ))
            content.append(Spacer(1, 0.3*inch))

        content.extend(_ongoing_section(
            'Clearance Interval Alerts',
            region_clearance_ongoing_rows,
            total_clearance_ongoing,
            styles,
            max_table_rows,
        ))

        # Section: Phase Terminations - Changed to a single header
        if _has_rows(ongoing_alerts.get('maxout')) and region_signals_df is not None:
            region_maxout_ongoing_rows, total_maxout_ongoing = prepare_phase_termination_alerts_table(
                ongoing_alerts['maxout'],
                region_signals_df,
                max_rows=max_table_rows,
            )
        else:
            region_maxout_ongoing_rows = pd.DataFrame()
            total_maxout_ongoing = 0

        if len(filtered_df_maxouts) > 0 and region_phase_figures:
            content.append(Paragraph("New Phase Termination Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """The following tables and charts display phase termination patterns that have been flagged as anomalous.
            Points marked with dots in the charts indicate periods where the system detected unusual max-out or force-off behavior.
            A phase is listed when it was flagged within the last 7 days, measured against a 21-day baseline for that phase."""
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
            
            if region_signals_df is not None:
                # Create phase termination table with row limit
                phase_alerts_df, total_phase_alerts = prepare_phase_termination_alerts_table(
                    filtered_df_maxouts,
                    region_signals_df,
                    max_rows=max_table_rows
                )

                table_content = create_reportlab_table(
                    phase_alerts_df,
                    "Phase Termination Alerts",
                    styles,
                    total_count=total_phase_alerts,
                    max_rows=max_table_rows,
                    trend_header='MaxOut (7d)'
                )
                content.extend(table_content)
                content.append(Spacer(1, 0.3*inch))

            # Charts for the new alerts belong directly under their own table
            content.extend(_chart_flowables(region_phase_figures))

        content.extend(_ongoing_section(
            'Phase Termination Alerts',
            region_maxout_ongoing_rows,
            total_maxout_ongoing,
            styles,
            max_table_rows,
            figures=region_ongoing_phase_figures,
            include_trend=True,
            trend_header='MaxOut (7d)',
        ))

        region_phase_skip_figures = [fig for fig, reg in (phase_skip_figures or []) if reg == region]
        if (
            phase_skip_rows is not None and not phase_skip_rows.empty and
            signals_df is not None and
            allowed_phase_skip_pairs is not None and not allowed_phase_skip_pairs.empty
        ):
            region_phase_skip_rows, total_phase_skip_alerts = prepare_phase_skip_alerts_table(
                phase_skip_rows,
                signals_df,
                region=region,
                allowed_pairs=allowed_phase_skip_pairs,
                min_total_skips=phase_skip_threshold if phase_skip_threshold is not None else 0,
                max_rows=max_table_rows
            )
        else:
            region_phase_skip_rows = pd.DataFrame()
            total_phase_skip_alerts = 0

        if _has_rows(ongoing_phase_skip_rows) and signals_df is not None:
            region_phase_skip_ongoing_rows, total_phase_skip_ongoing = prepare_phase_skip_alerts_table(
                ongoing_phase_skip_rows,
                signals_df,
                region=region,
                allowed_pairs=ongoing_phase_skip_pairs,
                min_total_skips=phase_skip_threshold if phase_skip_threshold is not None else 0,
                max_rows=max_table_rows,
            )
        else:
            region_phase_skip_ongoing_rows = pd.DataFrame()
            total_phase_skip_ongoing = 0

        if (
            (region_phase_skip_rows is not None and not region_phase_skip_rows.empty)
            or region_phase_skip_figures
        ):
            content.append(Paragraph("New Phase Skip Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """Phase Skip alerts highlight phases where wait times exceeded 1.5x the cycle length without an active preempt window.
            Each table row represents a device/phase/day combination that met these conditions within the last two weeks."""
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))

            if region_phase_skip_rows is not None and not region_phase_skip_rows.empty:
                table_content = create_reportlab_table(
                    region_phase_skip_rows,
                    "Phase Skip Alerts",
                    styles,
                    total_count=total_phase_skip_alerts,
                    max_rows=max_table_rows,
                    include_trend=False
                )
                content.extend(table_content)
                content.append(Spacer(1, 0.3*inch))

            content.extend(_chart_flowables(region_phase_skip_figures))

        content.extend(_ongoing_section(
            'Phase Skip Alerts',
            region_phase_skip_ongoing_rows,
            total_phase_skip_ongoing,
            styles,
            max_table_rows,
            figures=region_ongoing_phase_skip_figures,
        ))

        # Section: Detector Health - Changed to a single header
        if _has_rows(ongoing_alerts.get('actuations')) and region_signals_df is not None:
            region_detector_ongoing_rows, total_detector_ongoing = prepare_detector_health_alerts_table(
                ongoing_alerts['actuations'],
                region_signals_df,
                max_rows=max_table_rows,
                detector_hourly_df=detector_hourly_df,
            )
        else:
            region_detector_ongoing_rows = pd.DataFrame()
            total_detector_ongoing = 0

        if len(filtered_df_actuations) > 0 and region_detector_figures:
            content.append(Paragraph("New Detector Health Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """The following tables and charts display detector health metrics that have been flagged as anomalous.
            Points marked with dots in the charts indicate periods where the system detected unusual detector behavior.
            A detector is listed when it was flagged within the last 7 days, measured against a 21-day baseline for that detector."""
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))

            if region_signals_df is not None:
                # Create detector health table with row limit
                detector_alerts_df, total_detector_alerts = prepare_detector_health_alerts_table(
                    filtered_df_actuations,
                    region_signals_df,
                    max_rows=max_table_rows,
                    detector_hourly_df=detector_hourly_df,
                )

                table_content = create_reportlab_table(
                    detector_alerts_df,
                    "Detector Health Alerts",
                    styles,
                    total_count=total_detector_alerts,
                    max_rows=max_table_rows,
                    trend_header='Count (7d)'
                )
                content.extend(table_content)
                content.append(Spacer(1, 0.3*inch))

            # Add detector health charts without additional header
            content.extend(_chart_flowables(region_detector_figures))

        content.extend(_ongoing_section(
            'Detector Health Alerts',
            region_detector_ongoing_rows,
            total_detector_ongoing,
            styles,
            max_table_rows,
            figures=region_ongoing_detector_figures,
            include_trend=True,
            trend_header='Count (7d)',
        ))


        # Section: Ped Detector Health
        if _has_rows(ongoing_alerts.get('pedestrian')) and region_signals_df is not None:
            region_ped_ongoing_rows, total_ped_ongoing = prepare_ped_alerts_table(
                ongoing_alerts['pedestrian'],
                ped_hourly_df,
                region_signals_df,
                max_rows=max_table_rows,
            )
        else:
            region_ped_ongoing_rows = pd.DataFrame()
            total_ped_ongoing = 0

        if len(filtered_df_ped) > 0 and region_ped_figures:
            content.append(Paragraph("New Pedestrian Detector Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """Pedestrian detector alerts are are generated when an anomaly in ped services and/or actuations is detected."""
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))

            if region_signals_df is not None:
                # Create detector health table with row limit
                detector_alerts_df, total_detector_alerts = prepare_ped_alerts_table(
                    filtered_df_ped,
                    ped_hourly_df,
                    region_signals_df,
                    max_rows=max_table_rows
                )

                table_content = create_reportlab_table(
                    detector_alerts_df,
                    "Ped Detector Alerts",
                    styles,
                    total_count=total_detector_alerts,
                    max_rows=max_table_rows,
                    trend_header='Svc (7d)'
                )
                content.extend(table_content)
                content.append(Spacer(1, 0.3*inch))

            # Add ped detector charts without additional header
            content.extend(_chart_flowables(region_ped_figures))

        content.extend(_ongoing_section(
            'Pedestrian Detector Alerts',
            region_ped_ongoing_rows,
            total_ped_ongoing,
            styles,
            max_table_rows,
            figures=region_ongoing_ped_figures,
            include_trend=True,
            trend_header='Svc (7d)',
        ))

        # Section: Missing Data - Changed to a single header
        if _has_rows(ongoing_alerts.get('missing_data')) and region_signals_df is not None:
            region_missing_data_ongoing_rows, total_missing_data_ongoing = prepare_missing_data_alerts_table(
                ongoing_alerts['missing_data'],
                region_signals_df,
                max_rows=max_table_rows,
            )
        else:
            region_missing_data_ongoing_rows = pd.DataFrame()
            total_missing_data_ongoing = 0

        if len(filtered_df_missing_data) > 0 and region_missing_data_figures:
            content.append(Paragraph("New Missing Data Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """The following tables and charts display missing data patterns that have been flagged as anomalous.
            Higher values indicate a greater percentage of missing data. Points marked with dots in the charts indicate periods
            where the system detected significant data loss which may affect signal operation analysis.
            A signal is listed when it was flagged within the last 7 days, measured against a 21-day baseline for that signal."""
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))

            if region_signals_df is not None:
                # Create missing data table with row limit - each signal appears only once with its worst day
                missing_data_alerts_df, total_missing_data_alerts = prepare_missing_data_alerts_table(
                    filtered_df_missing_data,
                    region_signals_df,
                    max_rows=max_table_rows
                )

                table_content = create_reportlab_table(
                    missing_data_alerts_df,
                    "Missing Data Alerts",
                    styles,
                    total_count=total_missing_data_alerts,
                    max_rows=max_table_rows,
                    trend_header='Missing (7d)'
                )
                content.extend(table_content)
                content.append(Spacer(1, 0.3*inch))

            # Add missing data charts without additional header
            content.extend(_chart_flowables(region_missing_data_figures))

        content.extend(_ongoing_section(
            'Missing Data Alerts',
            region_missing_data_ongoing_rows,
            total_missing_data_ongoing,
            styles,
            max_table_rows,
            figures=region_ongoing_missing_data_figures,
            include_trend=True,
            trend_header='Missing (7d)',
        ))

        # Section: System Outages
        # Filter system outages for this region (or show all for "All Regions")
        if region == "All Regions":
            region_system_outages = system_outages_df if not system_outages_df.empty else pd.DataFrame()
        else:
            region_system_outages = system_outages_df[system_outages_df['Region'] == region] if not system_outages_df.empty else pd.DataFrame()

        ongoing_system_outages = ongoing_alerts.get('system_outages')
        if _has_rows(ongoing_system_outages) and region != "All Regions":
            ongoing_system_outages = ongoing_system_outages[ongoing_system_outages['Region'] == region]
        if _has_rows(ongoing_system_outages):
            region_system_outages_ongoing_rows, total_system_outages_ongoing = prepare_system_outages_table(
                ongoing_system_outages,
                max_rows=max_table_rows,
            )
        else:
            region_system_outages_ongoing_rows = pd.DataFrame()
            total_system_outages_ongoing = 0

        # A region earns a report when it has anything to say, new or ongoing.
        region_has_alerts = any([
            region_phase_figures,
            region_detector_figures,
            region_ped_figures,
            region_missing_data_figures,
            region_phase_skip_figures,
            not region_phase_skip_rows.empty,
            not region_clearance_rows.empty,
            not region_overlap_dual_rows.empty,
            not region_general_phase_rows.empty,
            not region_overlap_conflict_rows.empty,
            not region_preempt_rows.empty,
            not region_system_outages.empty,
            not region_maxout_ongoing_rows.empty,
            not region_detector_ongoing_rows.empty,
            not region_ped_ongoing_rows.empty,
            not region_missing_data_ongoing_rows.empty,
            not region_phase_skip_ongoing_rows.empty,
            not region_clearance_ongoing_rows.empty,
            not region_general_phase_ongoing_rows.empty,
            not region_overlap_conflict_ongoing_rows.empty,
            not region_preempt_ongoing_rows.empty,
            not region_system_outages_ongoing_rows.empty,
        ])

        if not region_system_outages.empty:
            content.append(Paragraph("New System-Wide Outages", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """The following table shows dates when more than 30% of devices in this region experienced missing data, 
            indicating a system-wide outage. During these periods, individual device missing data alerts are suppressed as they 
            likely represent infrastructure or date pipeline issues, not device-specific problems."""
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
              # Create system outages table
            system_outages_table_df, total_system_outages = prepare_system_outages_table(
                region_system_outages,
                max_rows=max_table_rows
            )

            table_content = create_reportlab_table(
                system_outages_table_df,
                "System-Wide Outages",
                styles,
                total_count=total_system_outages,
                max_rows=max_table_rows,
                include_trend=False
            )
            content.extend(table_content)
            content.append(Spacer(1, 0.3*inch))

        content.extend(_ongoing_section(
            'System-Wide Outages',
            region_system_outages_ongoing_rows,
            total_system_outages_ongoing,
            styles,
            max_table_rows,
        ))

        # Build the PDF with custom canvas for proper page numbering
        doc.build(content,
                 onFirstPage=header_footer.firstPage,
                 onLaterPages=header_footer.laterPages,
                 canvasmaker=make_canvas)
        
        if region_has_alerts:
            buffer_objects.append((region, buffer))
            log_message(f"Report for {region} generated in memory.", 1, verbosity)

    # Return dict mapping region name to BytesIO
    return {region: buf for region, buf in buffer_objects}
