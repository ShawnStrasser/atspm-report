import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, FrameBreak, KeepTogether
from reportlab.platypus.flowables import Flowable
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from io import BytesIO
import matplotlib.pyplot as plt
from datetime import datetime
import os
from typing import List, Tuple, Union
from table_generation import (
    prepare_phase_termination_alerts_table,
    prepare_detector_health_alerts_table,
    prepare_ped_alerts_table,
    prepare_missing_data_alerts_table,
    create_reportlab_table
)
from utils import log_message # Import the utility function

# Load jokes of the week
JOKES_CSV_PATH = os.path.join(os.path.dirname(__file__), "jokes.csv")
try:
    jokes_df = pd.read_csv(JOKES_CSV_PATH, parse_dates=['Date'])
    jokes_df.sort_values('Date', inplace=True)
except Exception as e:
    print(f"Warning: Could not load jokes.csv: {e}")
    jokes_df = pd.DataFrame(columns=['Date', 'Joke'])


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

        # Title "Signals Weekly" - right aligned
        canvas.setFont('Helvetica-Bold', 24)
        canvas.setFillColor(colors.black)
        title_text = "Signals Weekly"
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
        subtitle = "Delivering Weekly Traffic Signal Insights"
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
        phase_figures: List[tuple[plt.Figure, str]],
        detector_figures: List[tuple[plt.Figure, str]],
        ped_figures: List[tuple[plt.Figure, str]],
        missing_data_figures: List[tuple[plt.Figure, str]],
        signals_df: pd.DataFrame = None,
        output_path: str = "ATSPM_Report_{region}.pdf",
        save_to_disk: bool = True,
        max_table_rows: int = 10,
        verbosity: int = 1) -> Union[List[str], Tuple[List[BytesIO], List[str]]]:
    """Generate PDF reports for each region with the plots.
    
    Args:
        filtered_df_maxouts: DataFrame with phase termination alerts
        filtered_df_actuations: DataFrame with detector health alerts
        filtered_df_missing_data: DataFrame with missing data alerts
        phase_figures: List of (figure, region) tuples for phase termination
        detector_figures: List of (figure, region) tuples for detector health
        missing_data_figures: List of (figure, region) tuples for missing data
        signals_df: DataFrame with signal information
        output_path: Path template for saving reports
        save_to_disk: If True, save reports to disk, otherwise return BytesIO objects
        max_table_rows: Maximum number of rows to show in each alert table
        verbosity: Verbosity level (0=silent, 1=info, 2=debug)
        
    Returns:
        If save_to_disk is True: List of file paths where reports were saved
        If save_to_disk is False: Tuple of (List of BytesIO objects, List of region names)
    """
    # Get unique regions
    regions = set(region for _, region in phase_figures + detector_figures + missing_data_figures)
    generated_paths = []
    buffer_objects = []
    region_names = []

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

        # Create the document
        region_path = output_path.format(region=region.replace(" ", "_"))
        
        # Create header/footer handler
        header_footer = HeaderFooter(
            logo_path=os.path.join("images", "logo.jpg"),
            signal_head_path=os.path.join("images", "signal_head.jpg"),
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
        if save_to_disk:
            doc = SimpleDocTemplate(
                region_path,
                pagesize=letter,
                leftMargin=0.5*inch,
                rightMargin=0.5*inch,
                topMargin=1.2*inch,  # Increased top margin to prevent text encroachment
                bottomMargin=0.5*inch
            )
        else:
            # Create a BytesIO buffer for this report
            buffer = BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=letter,
                leftMargin=0.5*inch,
                rightMargin=0.5*inch,
                topMargin=1.2*inch,  # Increased top margin to prevent text encroachment
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
        intro_text = f"""This report for {region} includes alerts for increased percent maxout, vehicle & pedestrian detector alerts, and data completeness. 
        These are new alerts, focusing on the most critical issues within the last week. Signals Weekly is still in development, so please provide feedback.
        """
        content.append(Paragraph(intro_text, styles['Normal']))
        content.append(Spacer(1, 0.2*inch))

        # Joke of the Week section
        content.append(Paragraph("Joke of the Week", styles['SectionHeading']))
        # Select most recent joke up to today
        today_date = datetime.today().date()
        recent = jokes_df[jokes_df['Date'].dt.date <= today_date]
        joke_text = recent.iloc[-1]['Joke'] if not recent.empty else "No joke available this week."
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
                    max_rows=max_table_rows
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
                    max_rows=max_table_rows
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
                    max_rows=max_table_rows
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
                    max_rows=max_table_rows
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
                plt.close(fig)

        # Build the PDF with custom canvas for proper page numbering
        doc.build(content,
                 onFirstPage=header_footer.firstPage,
                 onLaterPages=header_footer.laterPages,
                 canvasmaker=make_canvas)
        
        if save_to_disk:
            generated_paths.append(region_path)
            log_message(f"Report for {region} saved to {region_path}", 1, verbosity)
        else:
            buffer_objects.append(buffer)
            region_names.append(region)
            log_message(f"Report for {region} generated in memory.", 1, verbosity)

    if save_to_disk:
        return generated_paths
    else:
        # Ensure we return consistent types even if All Regions report is skipped
        if not buffer_objects and not region_names: # If no individual reports AND no all regions report
             return [], []
        elif not buffer_objects: # If individual reports but no all regions report
             return [], region_names # Should not happen if individual reports exist, but handle defensively
        elif not region_names: # If all regions report but no individual reports (should not happen with new logic)
             return buffer_objects, [] # Should not happen
        else:
             return buffer_objects, region_names # Return dict of buffers and region names