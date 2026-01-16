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
    create_reportlab_table
)
from .utils import log_message

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
        save_to_disk: bool = False,
        max_table_rows: int = 10,
        verbosity: int = 1,
        phase_skip_rows: pd.DataFrame = None,
        phase_skip_figures: List[tuple[plt.Figure, str]] = None,
        phase_skip_alerts_df: Optional[pd.DataFrame] = None,
        phase_skip_threshold: Optional[float] = None,
        joke_index: int = None,
        custom_logo_path: str = None
) -> Dict[str, BytesIO]:
    """Generate PDF reports for each region with the plots.
    
    Args:
        filtered_df_maxouts: DataFrame with phase termination alerts
        filtered_df_actuations: DataFrame with detector health alerts
        filtered_df_ped: DataFrame with pedestrian alerts
        ped_hourly_df: DataFrame with pedestrian hourly data
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
        joke_index: Specific joke index to use (0-based), None for date-based cycling
        custom_logo_path: Path to custom logo file, None for default ODOT logo
        
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

    if not regions and signals_df is not None:
        regions.update(signals_df['Region'].unique().tolist())

    if regions:
        regions.add("All Regions")
        regions = sorted(regions)
    allowed_phase_skip_pairs = None
    if phase_skip_alerts_df is not None and not phase_skip_alerts_df.empty:
        allowed_phase_skip_pairs = phase_skip_alerts_df[['DeviceId', 'Phase']].drop_duplicates()
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
        intro_text = f"""This report for {region} includes alerts for phase skips, increased percent maxout, vehicle & pedestrian detector performance, and data completeness. 
        These are new alerts only, recurring issues are not shown but will be added in a future update.
        """
        content.append(Paragraph(intro_text, styles['Normal']))
        content.append(Spacer(1, 0.2*inch))
        
        # Joke section
        content.append(Paragraph(joke_title, styles['SectionHeading']))
        content.append(Paragraph(joke_text, styles['Normal']))
        content.append(Spacer(1, 0.3*inch))

        # Section: Phase Terminations - Changed to a single header
        if len(filtered_df_maxouts) > 0 and region_phase_figures:
            content.append(Paragraph("Phase Termination Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """The following tables and charts display phase termination patterns that have been flagged as anomalous. 
            Points marked with dots in the charts indicate periods where the system detected unusual max-out or force-off behavior."""
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
                    trend_header='MaxOut (21d)'
                )
                content.extend(table_content)
                content.append(Spacer(1, 0.3*inch))
            
            # Add phase termination charts without additional header
            for fig in region_phase_figures:
                # Wrap each chart in a KeepTogether to ensure it stays on one page
                chart_elements = []
                chart_elements.append(MatplotlibFigure(fig, width=6.5*inch, height=2.8*inch))
                content.append(KeepTogether(chart_elements))
                content.append(Spacer(1, 0.15*inch))
                plt.close(fig)

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

        if (region_phase_skip_rows is not None and not region_phase_skip_rows.empty) or region_phase_skip_figures:
            content.append(Paragraph("Phase Skip Alerts", styles['SectionHeading']))
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

            for fig in region_phase_skip_figures:
                chart_elements = []
                chart_elements.append(MatplotlibFigure(fig, width=6.5*inch, height=2.8*inch))
                content.append(KeepTogether(chart_elements))
                content.append(Spacer(1, 0.15*inch))
                plt.close(fig)

        # Section: Detector Health - Changed to a single header
        if len(filtered_df_actuations) > 0 and region_detector_figures:
            content.append(Paragraph("Detector Health Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """The following tables and charts display detector health metrics that have been flagged as anomalous. 
            Points marked with dots in the charts indicate periods where the system detected unusual detector behavior."""
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
            
            if region_signals_df is not None:
                # Create detector health table with row limit
                detector_alerts_df, total_detector_alerts = prepare_detector_health_alerts_table(
                    filtered_df_actuations, 
                    region_signals_df,
                    max_rows=max_table_rows
                )
                
                table_content = create_reportlab_table(
                    detector_alerts_df, 
                    "Detector Health Alerts", 
                    styles,
                    total_count=total_detector_alerts,
                    max_rows=max_table_rows,
                    trend_header='Count (21d)'
                )
                content.extend(table_content)
                content.append(Spacer(1, 0.3*inch))
            
            # Add detector health charts without additional header
            for fig in region_detector_figures:
                # Wrap each chart in a KeepTogether to ensure it stays on one page
                chart_elements = []
                chart_elements.append(MatplotlibFigure(fig, width=6.5*inch, height=2.8*inch))
                content.append(KeepTogether(chart_elements))
                content.append(Spacer(1, 0.15*inch))
                plt.close(fig)


        # Section: Ped Detector Health
        if len(filtered_df_ped) > 0 and region_ped_figures:
            content.append(Paragraph("Pedestrian Detector Alerts", styles['SectionHeading']))
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
            
            # Add detector health charts without additional header
            for fig in region_ped_figures:
                # Wrap each chart in a KeepTogether to ensure it stays on one page
                chart_elements = []
                chart_elements.append(MatplotlibFigure(fig, width=6.5*inch, height=2.8*inch))
                content.append(KeepTogether(chart_elements))
                content.append(Spacer(1, 0.15*inch))
                plt.close(fig)

        # Section: Missing Data - Changed to a single header
        if len(filtered_df_missing_data) > 0 and region_missing_data_figures:
            content.append(Paragraph("Missing Data Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """The following tables and charts display missing data patterns that have been flagged as anomalous. 
            Higher values indicate a greater percentage of missing data. Points marked with dots in the charts indicate periods 
            where the system detected significant data loss which may affect signal operation analysis."""
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
                    trend_header='Missing (21d)'
                )
                content.extend(table_content)
                content.append(Spacer(1, 0.3*inch))
              # Add missing data charts without additional header
            for fig in region_missing_data_figures:
                # Wrap each chart in a KeepTogether to ensure it stays on one page
                chart_elements = []
                chart_elements.append(MatplotlibFigure(fig, width=6.5*inch, height=2.8*inch))
                content.append(KeepTogether(chart_elements))
                content.append(Spacer(1, 0.15*inch))
                plt.close(fig)        # Section: System Outages
        # Filter system outages for this region (or show all for "All Regions")
        if region == "All Regions":
            region_system_outages = system_outages_df if not system_outages_df.empty else pd.DataFrame()
        else:
            region_system_outages = system_outages_df[system_outages_df['Region'] == region] if not system_outages_df.empty else pd.DataFrame()

        region_has_alerts = any([
            region_phase_figures,
            region_detector_figures,
            region_ped_figures,
            region_missing_data_figures,
            region_phase_skip_figures,
            not region_phase_skip_rows.empty,
            not region_system_outages.empty
        ])
        
        if not region_system_outages.empty:
            content.append(Paragraph("System-Wide Outages", styles['SectionHeading']))
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
