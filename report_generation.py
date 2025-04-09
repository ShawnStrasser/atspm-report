from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak
from reportlab.platypus.flowables import Flowable
from reportlab.lib.units import inch
from io import BytesIO
import matplotlib.pyplot as plt
from datetime import datetime
import os
from typing import List


class HeaderFooter:
    """Handles header and footer for the PDF report"""
    def __init__(self, logo_path: str, signal_head_path: str):
        self.logo_path = logo_path
        self.signal_head_path = signal_head_path

    def firstPage(self, canvas, doc):
        """Header and footer for the first page only"""
        # Save state
        canvas.saveState()

        # ----- HEADER -----
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

        # Title "ATSPM Report" on the right
        canvas.setFont('Helvetica-Bold', 24)
        canvas.setFillColor(colors.black)
        title_text = "ATSPM Report"
        title_width = canvas.stringWidth(title_text, "Helvetica-Bold", 24)
        canvas.drawString(doc.width + doc.leftMargin - title_width - 1.2*inch,
                         doc.height + doc.topMargin - 0.3*inch, title_text)

        # Traffic light image
        try:
            if os.path.exists(self.signal_head_path):
                canvas.drawImage(self.signal_head_path,
                               doc.width + doc.leftMargin - 0.8*inch,
                               doc.height + doc.topMargin - 0.7*inch,
                               width=0.6*inch,
                               height=0.7*inch,
                               preserveAspectRatio=True)
            else:
                print(f"Warning: Signal image not found at {self.signal_head_path}")
        except Exception as e:
            print(f"Error loading signal image: {e}")

        # Subtitle
        canvas.setFont('Times-Italic', 12)
        subtitle = "Automated Traffic Signal Performance Measures"
        subtitle_width = canvas.stringWidth(subtitle, "Times-Italic", 12)
        canvas.drawString(doc.width + doc.leftMargin - subtitle_width - 1.2*inch,
                         doc.height + doc.topMargin - 0.55*inch, subtitle)

        # Draw horizontal line
        canvas.setStrokeColor(colors.black)
        canvas.setLineWidth(1)
        canvas.line(doc.leftMargin, doc.height + doc.topMargin - 0.8*inch,
                   doc.width + doc.leftMargin, doc.height + doc.topMargin - 0.8*inch)

        # ----- FOOTER -----
        self._drawFooter(canvas, doc)

        # Restore state
        canvas.restoreState()

    def laterPages(self, canvas, doc):
        """Only footer for later pages (no header)"""
        canvas.saveState()
        self._drawFooter(canvas, doc)
        canvas.restoreState()

    def _drawFooter(self, canvas, doc):
        """Common footer for all pages"""
        canvas.setFont('Helvetica', 10)
        page_text = f"Page {doc.page}"
        text_width = canvas.stringWidth(page_text, 'Helvetica', 10)
        canvas.drawString(doc.width/2 + doc.leftMargin - text_width/2,
                         doc.bottomMargin - 20, page_text)


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


def generate_pdf_report(filtered_df: 'pd.DataFrame', 
                       filtered_df_actuations: 'pd.DataFrame',
                       phase_figures: List[plt.Figure],
                       detector_figures: List[plt.Figure],
                       output_path: str = "ATSPM_Report.pdf") -> str:
    """Generate a PDF report with the plots.
    
    Args:
        filtered_df: DataFrame with phase termination alerts
        filtered_df_actuations: DataFrame with detector health alerts
        phase_figures: List of matplotlib figures for phase termination plots
        detector_figures: List of matplotlib figures for detector health plots
        output_path: Path where to save the PDF report
        
    Returns:
        str: Path to the generated PDF file
    """
    # Create the document with minimal margins except for top margin on first page
    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        leftMargin=0.5*inch,
        rightMargin=0.5*inch,
        topMargin=1*inch,  # Top margin for first page
        bottomMargin=0.5*inch
    )

    # Get the current date for the report
    today = datetime.today().strftime("%B %d, %Y")

    # Define styles
    styles = getSampleStyleSheet()

    # Modify existing styles
    styles['Title'].fontSize = 16
    styles['Title'].spaceAfter = 12
    styles['Title'].leading = 18

    # Create custom styles
    styles.add(ParagraphStyle(
        name='SectionHeading',
        parent=styles['Heading2'],
        fontSize=14,
        spaceAfter=8,
        textColor=colors.navy
    ))

    # Start building content
    content = []

    # Add report title and date
    content.append(Paragraph(f"ATSPM Report - {today}", styles['Title']))
    content.append(Spacer(1, 0.1*inch))

    # Introduction text
    intro_text = """This report provides insights into traffic signal performance metrics and detector health. 
    The analysis highlights potential issues requiring attention based on statistical anomalies."""
    content.append(Paragraph(intro_text, styles['Normal']))
    content.append(Spacer(1, 0.2*inch))

    # Section 1: Phase Terminations
    if len(filtered_df) > 0 and phase_figures:
        content.append(Paragraph("1. Phase Termination Analysis", styles['SectionHeading']))
        content.append(Spacer(1, 0.1*inch))

        explanation = """The following charts display phase termination patterns that have been flagged as anomalous. 
        Points marked with dots indicate periods where the system detected unusual max-out or force-off behavior."""
        content.append(Paragraph(explanation, styles['Normal']))
        content.append(Spacer(1, 0.2*inch))

        # Add phase termination plots
        for fig in phase_figures:
            content.append(MatplotlibFigure(fig, width=6.5*inch, height=2.8*inch))
            content.append(Spacer(1, 0.15*inch))
            plt.close(fig)

    # Section 2: Detector Health
    if len(filtered_df_actuations) > 0 and detector_figures:
        content.append(Paragraph("2. Detector Health Analysis", styles['SectionHeading']))
        content.append(Spacer(1, 0.1*inch))

        explanation = """The following charts display detector health metrics that have been flagged as anomalous. 
        Points marked with dots indicate periods where the system detected unusual detector behavior."""
        content.append(Paragraph(explanation, styles['Normal']))
        content.append(Spacer(1, 0.2*inch))

        # Add detector health plots
        for fig in detector_figures:
            content.append(MatplotlibFigure(fig, width=6.5*inch, height=2.8*inch))
            content.append(Spacer(1, 0.15*inch))
            plt.close(fig)

    # Create header/footer handler
    header_footer = HeaderFooter(
        logo_path=os.path.join("images", "logo.jpg"),
        signal_head_path=os.path.join("images", "signal_head.jpg")
    )

    # Build the PDF with header on first page only, footer on all pages
    doc.build(content,
             onFirstPage=header_footer.firstPage,
             onLaterPages=header_footer.laterPages)

    return output_path