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
    prepare_missing_data_alerts_table,
    create_reportlab_table
)


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
        filtered_df: pd.DataFrame, 
        filtered_df_actuations: pd.DataFrame,
        filtered_df_missing_data: pd.DataFrame,
        phase_figures: List[tuple[plt.Figure, str]],
        detector_figures: List[tuple[plt.Figure, str]],
        missing_data_figures: List[tuple[plt.Figure, str]],
        signals_df: pd.DataFrame = None,
        output_path: str = "ATSPM_Report_{region}.pdf",
        save_to_disk: bool = True,
        max_table_rows: int = 10) -> Union[List[str], Tuple[List[BytesIO], List[str]]]:
    """Generate PDF reports for each region with the plots.
    
    Args:
        filtered_df: DataFrame with phase termination alerts
        filtered_df_actuations: DataFrame with detector health alerts
        filtered_df_missing_data: DataFrame with missing data alerts
        phase_figures: List of (figure, region) tuples for phase termination
        detector_figures: List of (figure, region) tuples for detector health
        missing_data_figures: List of (figure, region) tuples for missing data
        signals_df: DataFrame with signal information
        output_path: Path template for saving reports
        save_to_disk: If True, save reports to disk, otherwise return BytesIO objects
        max_table_rows: Maximum number of rows to show in each alert table
        
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
        # Filter figures for this region
        region_phase_figures = [fig for fig, reg in phase_figures if reg == region]
        region_detector_figures = [fig for fig, reg in detector_figures if reg == region]
        region_missing_data_figures = [fig for fig, reg in missing_data_figures if reg == region]
        
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

        # Introduction text
        intro_text = f"""This report provides insights into traffic signal performance metrics, detector health, and data completeness for {region}. 
        The analysis highlights potential issues requiring attention based on statistical anomalies."""
        content.append(Paragraph(intro_text, styles['Normal']))
        content.append(Spacer(1, 0.2*inch))

        # Section: Phase Terminations - Changed to a single header
        if len(filtered_df) > 0 and region_phase_figures:
            content.append(Paragraph("Phase Termination Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """The following tables and charts display phase termination patterns that have been flagged as anomalous. 
            Points marked with dots in the charts indicate periods where the system detected unusual max-out or force-off behavior."""
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
            
            # Filter signals for this region
            region_signals_df = signals_df[signals_df['Region'] == region] if signals_df is not None else None
            
            if region_signals_df is not None:
                # Create phase termination table with row limit
                phase_alerts_df, total_phase_alerts = prepare_phase_termination_alerts_table(
                    filtered_df, 
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
            
            # Filter signals for this region
            region_signals_df = signals_df[signals_df['Region'] == region] if signals_df is not None else None
            
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

        # Section: Missing Data - Changed to a single header
        if len(filtered_df_missing_data) > 0 and region_missing_data_figures:
            content.append(Paragraph("Missing Data Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            explanation = """The following tables and charts display missing data patterns that have been flagged as anomalous. 
            Higher values indicate a greater percentage of missing data. Points marked with dots in the charts indicate periods 
            where the system detected significant data loss which may affect signal operation analysis."""
            content.append(Paragraph(explanation, styles['Normal']))
            content.append(Spacer(1, 0.2*inch))
            
            # Filter signals for this region
            region_signals_df = signals_df[signals_df['Region'] == region] if signals_df is not None else None
            
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
        else:
            buffer_objects.append(buffer)
            region_names.append(region)

    # Now create the All Regions report with consolidated tables and figures
    all_region = "All Regions"
    all_region_path = output_path.format(region=all_region.replace(" ", "_"))
    
    # Create header/footer handler for All Regions report
    header_footer = HeaderFooter(
        logo_path=os.path.join("images", "logo.jpg"),
        signal_head_path=os.path.join("images", "signal_head.jpg"),
        region=all_region
    )

    # Create document with custom canvas for All Regions
    def make_all_regions_canvas(*args, **kwargs):
        canvas = PageNumCanvas(*args, **kwargs)
        canvas.set_footer_handler(
            lambda c, page_num, num_pages: draw_page_footer(c, page_num, num_pages, all_region)
        )
        return canvas

    # Determine if we're writing to disk or memory for All Regions report
    if save_to_disk:
        doc = SimpleDocTemplate(
            all_region_path,
            pagesize=letter,
            leftMargin=0.5*inch,
            rightMargin=0.5*inch,
            topMargin=1.2*inch,  # Increased top margin to match individual region reports
            bottomMargin=0.5*inch
        )
    else:
        # Create a BytesIO buffer for this report
        all_buffer = BytesIO()
        doc = SimpleDocTemplate(
            all_buffer,
            pagesize=letter,
            leftMargin=0.5*inch,
            rightMargin=0.5*inch,
            topMargin=1.2*inch,  # Increased top margin to match individual region reports
            bottomMargin=0.5*inch
        )

    # Content building for All Regions
    content = []

    # Add report title and date for All Regions
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

    # Introduction text for All Regions
    intro_text = f"""This comprehensive report provides insights into traffic signal performance metrics, detector health, and data completeness across all regions. 
    The analysis consolidates data from all signals and highlights potential issues requiring attention based on statistical anomalies."""
    content.append(Paragraph(intro_text, styles['Normal']))
    content.append(Spacer(1, 0.2*inch))
    
    # Section: Consolidated Phase Termination Analysis - Changed to a single header
    if len(filtered_df) > 0:
        content.append(Paragraph("Phase Termination Alerts", styles['SectionHeading']))
        content.append(Spacer(1, 0.1*inch))

        explanation = """The following tables and charts display phase termination patterns that have been flagged as anomalous across all regions. 
        Points marked with dots in the charts indicate periods where the system detected unusual max-out or force-off behavior."""
        content.append(Paragraph(explanation, styles['Normal']))
        content.append(Spacer(1, 0.2*inch))
        
        worst_device_id = None
        worst_phase = None
        worst_device_region = None
        
        if signals_df is not None:
            # Use all signals regardless of region for the All Regions report
            # Create phase termination table with row limit
            phase_alerts_df, total_phase_alerts = prepare_phase_termination_alerts_table(
                filtered_df, 
                signals_df,  # Using all signals
                max_rows=max_table_rows * 2  # Allow more rows for the combined report
            )
            
            table_content = create_reportlab_table(
                phase_alerts_df, 
                "Phase Termination Alerts", # Removed "- All Regions" to avoid duplication
                styles,
                total_count=total_phase_alerts,
                max_rows=max_table_rows * 2
            )
            content.extend(table_content)
            content.append(Spacer(1, 0.3*inch))
            
            # Get the worst device and phase from the first row of the table
            if not phase_alerts_df.empty:
                # Need to find DeviceId and Phase for the first row
                first_row = phase_alerts_df.iloc[0]
                signal_name = first_row['Signal']
                phase_num = first_row['Phase']
                
                # Get DeviceId for this signal name
                matching_device_rows = signals_df[signals_df['Name'] == signal_name]
                if not matching_device_rows.empty:
                    worst_device_id = matching_device_rows.iloc[0]['DeviceId']
                    worst_phase = phase_num
                    worst_device_region = matching_device_rows.iloc[0]['Region']
        
        # First, add a note indicating which device/phase this chart represents
        if worst_device_id is not None and worst_phase is not None and worst_device_region is not None:
            device_info = f"Chart for the most critical issue:"
            chart_description = Paragraph(device_info, styles['Normal'])
            
            # Try to find a matching figure for this worst device
            worst_figure = None
            worst_device_figures = []
            
            # First collect all figures that match the region
            for fig, reg in phase_figures:
                if reg == worst_device_region:
                    worst_device_figures.append(fig)
            
            chart_elements = []
            chart_elements.append(chart_description)
            chart_elements.append(Spacer(1, 0.1*inch))
            
            # If we have figures for this region, use the first one
            if worst_device_figures:
                chart_elements.append(MatplotlibFigure(worst_device_figures[0], width=6.5*inch, height=2.8*inch))
            elif phase_figures:
                # If no figures match the region, fall back to the first figure
                chart_elements.append(MatplotlibFigure(phase_figures[0][0], width=6.5*inch, height=2.8*inch))
                
                # Add an explanatory note about the fallback
                fallback_note = "Note: The chart shown may not represent the exact device in the first table row due to data availability."
                chart_elements.append(Paragraph(fallback_note, styles['Normal']))
            
            # Keep the description and chart together
            content.append(KeepTogether(chart_elements))
            content.append(Spacer(1, 0.15*inch))
        elif phase_figures:
            # Fallback if we couldn't get worst device info
            content.append(MatplotlibFigure(phase_figures[0][0], width=6.5*inch, height=2.8*inch))
            content.append(Spacer(1, 0.15*inch))

    # Section: Consolidated Detector Health - Changed to a single header
    if len(filtered_df_actuations) > 0:
        content.append(Paragraph("Detector Health Alerts", styles['SectionHeading']))
        content.append(Spacer(1, 0.1*inch))

        explanation = """The following tables and charts display detector health metrics that have been flagged as anomalous across all regions. 
        Points marked with dots in the charts indicate periods where the system detected unusual detector behavior."""
        content.append(Paragraph(explanation, styles['Normal']))
        content.append(Spacer(1, 0.2*inch))
        
        worst_device_id = None
        worst_detector = None
        worst_device_region = None
        
        if signals_df is not None:
            # Use all signals regardless of region for the All Regions report
            # Create detector health table with row limit
            detector_alerts_df, total_detector_alerts = prepare_detector_health_alerts_table(
                filtered_df_actuations, 
                signals_df,  # Using all signals
                max_rows=max_table_rows * 2  # Allow more rows for the combined report
            )
            
            table_content = create_reportlab_table(
                detector_alerts_df, 
                "Detector Health Alerts", # Removed "- All Regions" to avoid duplication
                styles,
                total_count=total_detector_alerts,
                max_rows=max_table_rows * 2
            )
            content.extend(table_content)
            content.append(Spacer(1, 0.3*inch))
            
            # Get the worst device and detector from the first row of the table
            if not detector_alerts_df.empty:
                # Need to find DeviceId and Detector for the first row
                first_row = detector_alerts_df.iloc[0]
                signal_name = first_row['Signal']
                detector_num = first_row['Detector']
                
                # Get DeviceId for this signal name
                matching_device_rows = signals_df[signals_df['Name'] == signal_name]
                if not matching_device_rows.empty:
                    worst_device_id = matching_device_rows.iloc[0]['DeviceId']
                    worst_detector = detector_num
                    worst_device_region = matching_device_rows.iloc[0]['Region']
        
        # First, add a note indicating which device/detector this chart represents
        if worst_device_id is not None and worst_detector is not None and worst_device_region is not None:
            device_info = f"Chart for the most critical issue:"
            chart_description = Paragraph(device_info, styles['Normal'])
            
            # Try to find a matching figure for this worst device
            worst_device_figures = []
            
            # First collect all figures that match the region
            for fig, reg in detector_figures:
                if reg == worst_device_region:
                    worst_device_figures.append(fig)
            
            chart_elements = []
            chart_elements.append(chart_description)
            chart_elements.append(Spacer(1, 0.1*inch))
            
            # If we have figures for this region, use the first one
            if worst_device_figures:
                chart_elements.append(MatplotlibFigure(worst_device_figures[0], width=6.5*inch, height=2.8*inch))
            elif detector_figures:
                # If no figures match the region, fall back to the first figure
                chart_elements.append(MatplotlibFigure(detector_figures[0][0], width=6.5*inch, height=2.8*inch))
                
                # Add an explanatory note about the fallback
                fallback_note = "Note: The chart shown may not represent the exact device in the first table row due to data availability."
                chart_elements.append(Paragraph(fallback_note, styles['Normal']))
            
            # Keep the description and chart together
            content.append(KeepTogether(chart_elements))
            content.append(Spacer(1, 0.15*inch))
        elif detector_figures:
            # Fallback if we couldn't get worst device info
            content.append(MatplotlibFigure(detector_figures[0][0], width=6.5*inch, height=2.8*inch))
            content.append(Spacer(1, 0.15*inch))
    
    # Section: Consolidated Missing Data - Changed to a single header
    if len(filtered_df_missing_data) > 0:
        content.append(Paragraph("Missing Data Alerts", styles['SectionHeading']))
        content.append(Spacer(1, 0.1*inch))

        explanation = """The following tables and charts display missing data patterns that have been flagged as anomalous across all regions. 
        Higher values indicate a greater percentage of missing data. Points marked with dots in the charts indicate periods 
        where the system detected significant data loss which may affect signal operation analysis."""
        content.append(Paragraph(explanation, styles['Normal']))
        content.append(Spacer(1, 0.2*inch))
        
        worst_device_id = None
        worst_device_region = None
        
        if signals_df is not None:
            # Use all signals regardless of region for the All Regions report
            # Create missing data table with row limit
            missing_data_alerts_df, total_missing_data_alerts = prepare_missing_data_alerts_table(
                filtered_df_missing_data, 
                signals_df,  # Using all signals
                max_rows=max_table_rows * 2  # Allow more rows for the combined report
            )
            
            table_content = create_reportlab_table(
                missing_data_alerts_df, 
                "Missing Data Alerts",  # Removed "- All Regions" to avoid duplication
                styles,
                total_count=total_missing_data_alerts,
                max_rows=max_table_rows * 2
            )
            content.extend(table_content)
            content.append(Spacer(1, 0.3*inch))
            
            # Get the worst device from the first row of the table
            if not missing_data_alerts_df.empty:
                # Need to find DeviceId for the first row
                first_row = missing_data_alerts_df.iloc[0]
                signal_name = first_row['Signal']
                
                # Get DeviceId for this signal name
                matching_device_rows = signals_df[signals_df['Name'] == signal_name]
                if not matching_device_rows.empty:
                    worst_device_id = matching_device_rows.iloc[0]['DeviceId']
                    worst_device_region = matching_device_rows.iloc[0]['Region']
        
        # First, add a note indicating which device this chart represents
        if worst_device_id is not None and worst_device_region is not None:
            device_info = f"Chart for the most critical issue:"
            chart_description = Paragraph(device_info, styles['Normal'])
            
            # Try to find a matching figure for this worst device
            worst_device_figures = []
            
            # First collect all figures that match the region
            for fig, reg in missing_data_figures:
                if reg == worst_device_region:
                    worst_device_figures.append(fig)
            
            chart_elements = []
            chart_elements.append(chart_description)
            chart_elements.append(Spacer(1, 0.1*inch))
            
            # If we have figures for this region, use the first one
            if worst_device_figures:
                chart_elements.append(MatplotlibFigure(worst_device_figures[0], width=6.5*inch, height=2.8*inch))
            elif missing_data_figures:
                # If no figures match the region, fall back to the first figure
                chart_elements.append(MatplotlibFigure(missing_data_figures[0][0], width=6.5*inch, height=2.8*inch))
                
                # Add an explanatory note about the fallback
                fallback_note = "Note: The chart shown may not represent the exact device in the first table row due to data availability."
                chart_elements.append(Paragraph(fallback_note, styles['Normal']))
            
            # Keep the description and chart together
            content.append(KeepTogether(chart_elements))
            content.append(Spacer(1, 0.15*inch))
        elif missing_data_figures:
            # Fallback if we couldn't get worst device info
            content.append(MatplotlibFigure(missing_data_figures[0][0], width=6.5*inch, height=2.8*inch))
            content.append(Spacer(1, 0.15*inch))

    # Build the All Regions PDF with custom canvas
    doc.build(content,
             onFirstPage=header_footer.firstPage,
             onLaterPages=header_footer.laterPages,
             canvasmaker=make_all_regions_canvas)
    
    # Add the All Regions report to the output
    if save_to_disk:
        generated_paths.append(all_region_path)
    else:
        buffer_objects.append(all_buffer)
        region_names.append("All")  # Use "All" to match the emails.csv entry

    if save_to_disk:
        return generated_paths
    else:
        return buffer_objects, region_names