import pandas as pd
import importlib.resources
import weakref
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import CondPageBreak, SimpleDocTemplate, Paragraph, Spacer, Image, KeepTogether, Table, TableStyle
from reportlab.platypus.flowables import Flowable
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from io import BytesIO
from xml.sax.saxutils import escape
import matplotlib.pyplot as plt
from datetime import datetime, date
import os
from pathlib import Path
from typing import List, Dict, Optional
from .table_generation import (
    ongoing_counted_to,
    prepare_phase_termination_alerts_table,
    prepare_phase_skip_alerts_table,
    prepare_missing_data_alerts_table,
    prepare_system_outages_table,
    prepare_clearance_interval_alerts_table,
    prepare_overlap_dual_indications_table,
    prepare_alarms_table,
    prepare_preempt_alerts_table,
    prepare_signal_conflicts_table,
    prepare_atspm_detector_health_table,
    create_reportlab_table,
    ONGOING_SINCE_COLUMN,
    MISSING_HOURLY_HEADER,
)
from .atspm_detector_health import ALERT_TYPE as ATSPM_DETECTOR_HEALTH, FAULT_STATUSES, PED, VEHICLE
from .preempt_cascade import SECTION_EV, SECTION_RAIL, SECTION_TSP
from .coordination_check import (
    ALERT_TYPE as COORDINATION_ALERT_TYPE,
    SECTION_TITLE as COORDINATION_TITLE,
)
from .signal_focus import SECTION_TITLE as SIGNAL_FOCUS_TITLE
from . import measure_checks, timing_checks
from .utils import log_message

# Repeat alerts are normally held back so they do not crowd out new ones. When the
# caller passes them in, they go in one "Ongoing Issues" section at the end of the
# report, after every new alert: one subsection per check - its own table and
# charts - in the same order as the checks above it.
ONGOING_TITLE = 'Ongoing Issues'


# Stacked two-panel preempt charts take their height from the image, which is
# drawn at close to print width so the text keeps its size.
PREEMPT_CHART_HEIGHT = None
HEADING_ROOM = 2.0*inch
TIMING_CHART_HEIGHT = timing_checks.CHART_HEIGHT_INCHES*inch


def _measure_sections(alerts, ongoing, signals_df, region, max_rows, ongoing_limit, figures, ongoing_figures,
                      waiting=None) -> list:
    """(title, rows, total, ongoing rows, ongoing total, charts, ongoing charts) of each section of the checks on atspm's
    per-phase measures, in report order: the phase timing section, then the delay rise and the two trends. waiting: every
    ongoing alert, for the ongoing totals when the rotation cut them (see _waiting_total)."""
    alerts, ongoing = alerts or {}, ongoing or {}
    sections = []
    for check in timing_checks.SECTION_TITLES:
        rows, total = timing_checks.prepare_timing_table(alerts.get(timing_checks.ALERT_TYPE), signals_df, region, max_rows, check)
        later, total_later = timing_checks.prepare_timing_table(ongoing.get(timing_checks.ALERT_TYPE), signals_df, region,
                                                                ongoing_limit, check)
        total_later = _waiting_total(waiting, timing_checks.ALERT_TYPE, total_later, lambda frame: timing_checks.prepare_timing_table(
            frame, signals_df, region, 1, check))
        sections.append((timing_checks.SECTION_TITLES[check], rows, total, later, total_later,
                         [fig for fig, reg in timing_checks.section_figures(figures or [], check) if reg == region],
                         [fig for fig, reg in timing_checks.section_figures(ongoing_figures or [], check) if reg == region]))
    for alert_type in (measure_checks.DELAY_RISE, measure_checks.SPLIT_FAIL_TREND, measure_checks.PLATOON_TREND):
        rows, total = measure_checks.prepare_measure_table(alerts.get(alert_type), signals_df, region, max_rows)
        later, total_later = measure_checks.prepare_measure_table(ongoing.get(alert_type), signals_df, region, ongoing_limit)
        total_later = _waiting_total(waiting, alert_type, total_later, lambda frame: measure_checks.prepare_measure_table(
            frame, signals_df, region, 1))
        sections.append((measure_checks.SECTION_TITLES[alert_type], rows, total, later, total_later, [], []))
    return sections


def _waiting_total(waiting, alert_type, total, table) -> int:
    """An Ongoing table's total once the rotation chose which alerts it shows: everything the report has of that table,
    those waiting their turn too ('Showing 15 of 40'). waiting: alert type -> every ongoing alert (None: not rotated, the
    table's own total stands); table(frame) -> (rows, total) of the same table built from a frame."""
    frame = (waiting or {}).get(alert_type)
    if frame is None or frame.empty:
        return total
    return table(frame)[1]


def _preempt_rows(alerts: Optional[pd.DataFrame], signals_df, region, max_rows, section=None):
    """The table rows of one preempt section (alerts whose Class is section; alerts without a Class are emergency vehicle)."""
    if alerts is None or alerts.empty or signals_df is None:
        return pd.DataFrame(), 0
    if section is not None:
        classes = alerts['Class'] if 'Class' in alerts.columns else pd.Series(None, index=alerts.index, dtype=object)
        alerts = alerts[classes.fillna(SECTION_EV) == section]
        if alerts.empty:
            return pd.DataFrame(), 0
    return prepare_preempt_alerts_table(alerts, signals_df, region=region, max_rows=max_rows)


def _section_figures(figures, section, region) -> list:
    """The charts of one preempt section in one report: figures is {section: [(figure, region)]}, or a plain list (old
    callers), which goes with the emergency vehicle section."""
    if isinstance(figures, dict):
        figures = figures.get(section) or []
    elif section != SECTION_EV:
        figures = []
    return [fig for fig, reg in (figures or []) if reg == region]


# The preempt sections, in report order: (Class, heading, ongoing title, sparkline header).
PREEMPT_SECTION_TEXT = [
    (SECTION_EV, 'Emergency Vehicle Preempt', 'Emergency Vehicle Preempt Alerts',
     'Calls/Day (6wk)'),
    (SECTION_TSP, 'Transit Signal Priority', 'Transit Signal Priority Alerts',
     'Bus Calls/Day (6wk)'),
    (SECTION_RAIL, 'Rail', 'Rail Preempt Alerts',
     'Calls/Day (6wk)'),
]


ATSPM_DETECTOR_HEALTH_TITLE = 'Detector Health'
ATSPM_HOURLY_HEADER = 'Hourly Actuations'
ATSPM_HOURLY_PRESSES_HEADER = 'Hourly Presses'


ATSPM_PED_HEALTH_TITLE = 'Pedestrian Button Health'


# With the urgency tiers the faults are two tables: those measured to cost a phase its service or to delay other phases, the
# most costly first, with their charts; then those with no measured effect, which still need fixing for the data.
ATSPM_HARM_TABLE = 'Affecting Operations'
ATSPM_DATA_TABLE = 'Data Only'


def _atspm_detector_health_rows(alerts, signals_df, statuses, region, max_rows, story_header, detector_type=VEHICLE,
                                hourly_counts=None, ped_hourly_counts=None, expected_counts=None, impact=None, max_signals=0):
    if alerts is None or alerts.empty or signals_df is None:
        return pd.DataFrame(), 0
    return prepare_atspm_detector_health_table(
        alerts, signals_df, statuses, region=region, max_rows=max_rows,
        story_header=story_header, detector_type=detector_type, hourly_counts=hourly_counts,
        ped_hourly_counts=ped_hourly_counts, expected_counts=expected_counts, impact=impact, max_signals=max_signals,
    )


def _has_rows(df: Optional[pd.DataFrame]) -> bool:
    """True when a DataFrame exists and holds at least one row."""
    return df is not None and not df.empty


def _chart_flowables(figures: list, height: float = 2.8*inch, width: float = 6.5*inch) -> list:
    """Lay out alert charts one per row, each kept whole on a single page."""
    elements = []
    for figure in figures:
        # A chart of another shape among them (a coordination impact chart) keeps its own proportions.
        own = getattr(figure, 'report_own_proportions', False)
        elements.append(KeepTogether([MatplotlibFigure(figure, width=width, height=None if own else height)]))
        elements.append(Spacer(1, 0.15*inch))
    return elements


def _coordination_group_flowables(groups: list, styles) -> list:
    """Coordination groups (coordination_check.CoordinationGroup), each its signals' names, its map and its charts two
    to a row; no table (user, 2026-11-17)."""
    elements = []
    half = 3.6*inch
    for group in groups:
        head = [Paragraph(f'<b>{escape(str(group.title))}</b>', styles['Normal']), Spacer(1, 0.06*inch)]
        if group.map is not None:
            head.append(MatplotlibFigure(group.map, width=6.5*inch, height=None))
            head.append(Spacer(1, 0.08*inch))
        cells = [MatplotlibFigure(chart, width=half, height=None) for chart in group.charts]
        if len(cells) == 1:         # one chart (a clock alert, or one day): full width under the map
            grid = [[MatplotlibFigure(group.charts[0], width=6.5*inch, height=None)]]
            table = Table(grid, colWidths=[2 * half])
        else:
            grid = [cells[k:k + 2] + [''] * (2 - len(cells[k:k + 2])) for k in range(0, len(cells), 2)]
            table = Table(grid, colWidths=[half, half]) if grid else None
        if table is not None:
            table.setStyle(TableStyle([('VALIGN', (0, 0), (-1, -1), 'TOP'), ('LEFTPADDING', (0, 0), (-1, -1), 0),
                                       ('RIGHTPADDING', (0, 0), (-1, -1), 0), ('BOTTOMPADDING', (0, 0), (-1, -1), 4)]))
        elements.append(KeepTogether(head + ([table] if table is not None else [])) if len(grid) <= 1
                        else KeepTogether(head))
        if table is not None and len(grid) > 1:
            elements.append(table)
        elements.append(Spacer(1, 0.25*inch))
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
        chart_height: float = 2.8*inch,
        chart_width: float = 6.5*inch,
        keep_start: bool = False,
) -> list:
    """Build one check's subsection of the Ongoing Issues section, or nothing when there is nothing ongoing."""
    if not _has_rows(rows_df):
        return []

    table = create_reportlab_table(
        rows_df,
        f'Ongoing {title}',
        styles,
        total_count=total_count,
        max_rows=max_rows,
        include_trend=include_trend,
        trend_header=trend_header,
    )
    elements = [Paragraph(title, styles['SubsectionHeading'])]
    elements.append(Spacer(1, 0.1*inch))
    if keep_start:
        elements.insert(0, _keep_with_table_start(table, 1.2*inch))
    elements.extend(table)
    elements.append(Spacer(1, 0.3*inch))
    elements.extend(_chart_flowables(figures or [], chart_height, chart_width))
    return elements


def _ongoing_issues(subsections: list, styles) -> list:
    """The Ongoing Issues section: its heading over the checks' subsections, or nothing without any."""
    if not subsections:
        return []
    return [
        # The heading goes to the next page unless the first subsection's heading room fits under it too.
        CondPageBreak(HEADING_ROOM + 1.0*inch),
        Paragraph(ONGOING_TITLE, styles['SectionHeading']),
        Spacer(1, 0.2*inch),
        *subsections,
    ]


def _keep_with_table_start(table: list, heading_height: float = 0.7*inch) -> CondPageBreak:
    """Goes to the next page unless this heading and the table's first signal fit on this one, so a heading and a table header
    are not left alone at the foot of a page."""
    return CondPageBreak(heading_height + getattr(table[-1], 'start_height', 0.0))


def _focus_table(told: list, drawn: set, styles) -> Table:
    """A signal's problems: problem type, then its problems one to a line. A line whose kind of chart is not among those drawn
    says so; a row with no chart at all says it once, under its type. A line whose chart is not drawn gives its numbers (detail)."""
    body = ParagraphStyle('FocusCell', parent=styles['Normal'], fontSize=9, leading=11.5)
    head = ParagraphStyle('FocusHead', parent=body, fontName='Helvetica-Bold', textColor=colors.white)
    table = [[Paragraph('Problem type', head), Paragraph('Problems', head)]]
    for row in told:
        charted = [] if row.get('no_chart') else [line for line in row['lines'] if line['chart'] in drawn]
        kind = f"<b>{escape(row['type'])}</b>" + ('' if charted else '<br/><i>No chart</i>')
        lines = [escape(line['text'] if line['chart'] in drawn else line.get('detail', line['text']))
                 + (' <i>(no chart)</i>' if charted and line['chart'] not in drawn else '') for line in row['lines']]
        table.append([Paragraph(kind, body), Paragraph('<br/>'.join(lines), body)])
    flowable = Table(table, colWidths=[1.5*inch, 6.0*inch], repeatRows=1)
    flowable.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.navy),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    return flowable


def _signal_focus_section(rows: pd.DataFrame, figures: Optional[dict], styles) -> list:
    """The Signal Focus section: one signal at a time, a subtitle with its name, its problems in a table, its charts in the table's order."""
    elements = [
        Paragraph(SIGNAL_FOCUS_TITLE, styles['SectionHeading']),
        Spacer(1, 0.15*inch),
    ]
    for _, row in rows.iterrows():
        charts = (figures or {}).get(row['DeviceId'], [])
        elements.append(KeepTogether([
            Paragraph(f"{row['Rank']}. {escape(str(row['Signal']))}", styles['SubsectionHeading']),
            _focus_table(row['Rows'], {one for _, _, _, kind in charts for one in ((kind,) if isinstance(kind, str) else kind)}, styles),
        ]))
        elements.append(Spacer(1, 0.15*inch))
        for figure, width, height, _ in charts:
            elements.append(KeepTogether([MatplotlibFigure(figure, width=width, height=height)]))
            elements.append(Spacer(1, 0.15*inch))
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


# Each figure's PNG, kept while the figure lives. A chart can appear in its
# region's report and in All Regions; it is rendered once for both.
_FIGURE_PNGS = weakref.WeakKeyDictionary()


def figure_png(figure: plt.Figure) -> bytes:
    """The figure as the PNG the report embeds."""
    png = _FIGURE_PNGS.get(figure)
    if png is None:
        buf = BytesIO()
        figure.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        png = _FIGURE_PNGS[figure] = buf.getvalue()
    return png


class MatplotlibFigure(Flowable):
    """A Flowable wrapper for matplotlib figures"""
    def __init__(self, figure: plt.Figure, width: float = 6.5*inch, height: Optional[float] = 3*inch):
        Flowable.__init__(self)
        self.figure = figure
        self.width = width
        self.height = height
        if height is None:
            # Keep the saved image's own proportions rather than stretching it.
            pixel_width, pixel_height = ImageReader(BytesIO(figure_png(figure))).getSize()
            self.height = width * pixel_height / pixel_width

    def draw(self):
        try:
            # Use ReportLab's canvas to draw the image
            img = Image(BytesIO(figure_png(self.figure)), width=self.width, height=self.height)
            img.drawOn(self.canv, 0, 0)
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


def generate_pdf_report(*args, report_today=None, **kwargs) -> Dict[str, BytesIO]:
    """Generate PDF reports for each region with the plots (_generate_pdf_report has the arguments).

    report_today: the day the Ongoing columns' day counts count to, the day after the report day (generator._report_today);
        None counts to today.
    """
    with ongoing_counted_to(report_today):
        return _generate_pdf_report(*args, **kwargs)


def _generate_pdf_report(
        filtered_df_maxouts: pd.DataFrame,
        filtered_df_missing_data: pd.DataFrame,
        system_outages_df: pd.DataFrame,
        phase_figures: List[tuple[plt.Figure, str]],
        missing_data_figures: List[tuple[plt.Figure, str]],
        signals_df: pd.DataFrame = None,
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
        atspm_detector_health_df: Optional[pd.DataFrame] = None,
        atspm_detector_hourly: Optional[Dict[tuple, list]] = None,
        atspm_detector_expected: Optional[Dict[tuple, list]] = None,
        atspm_ped_hourly: Optional[Dict[tuple, list]] = None,
        table_row_limits: Optional[Dict[str, int]] = None,
        joke_index: int = None,
        custom_logo_path: str = None,
        ongoing_alerts: Optional[Dict[str, pd.DataFrame]] = None,
        ongoing_phase_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        ongoing_missing_data_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        ongoing_phase_skip_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        preempt_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        ongoing_preempt_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        atspm_detector_health_figures: Optional[Dict[str, List[tuple[plt.Figure, str]]]] = None,
        ongoing_atspm_detector_health_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        ongoing_atspm_ped_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        signal_focus: Optional[Dict[str, pd.DataFrame]] = None,
        signal_focus_figures: Optional[Dict[str, list]] = None,
        data_stopped_at=None,
        missing_data_hourly: Optional[pd.DataFrame] = None,
        system_outages_hourly: Optional[pd.DataFrame] = None,
        coordination_alerts_df: Optional[pd.DataFrame] = None,
        coordination_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        ongoing_coordination_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        measure_alerts: Optional[Dict[str, pd.DataFrame]] = None,
        timing_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        ongoing_timing_figures: Optional[List[tuple[plt.Figure, str]]] = None,
        table_signal_limits: Optional[Dict[str, int]] = None,
        ongoing_rotated: bool = False,
        ongoing_waiting: Optional[Dict[str, pd.DataFrame]] = None,
) -> Dict[str, BytesIO]:
    """Generate PDF reports for each region with the plots.

    table_signal_limits: signals a table lists (0 or missing: no limit), keys 'data_only' (Detector Health Data Only, new
        and ongoing), 'phase_skips' (new and ongoing) and 'alarms' (Controller Alarms, the most alarms first).
    ongoing_rotated: the ongoing alerts were already cut to what each region's report shows (table_limits.rotate_ongoing),
        so a region's Ongoing tables list all of theirs; All Regions keeps its row limits over what the regions listed.
        Coordination and the system outages keep their own limits.
    ongoing_waiting: with ongoing_rotated, every ongoing alert before the cut: the Ongoing tables' totals count them all.

    Args:
        filtered_df_maxouts: DataFrame with phase termination alerts
        filtered_df_missing_data: DataFrame with missing data alerts
        system_outages_df: DataFrame with system-wide outages (Date, Region, MissingData)
        phase_figures: List of (figure, region) tuples for phase termination
        missing_data_figures: List of (figure, region) tuples for missing data
        signals_df: DataFrame with signal information
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
        atspm_detector_hourly: {(DeviceId, Detector): actuations per hour} for the sparkline of
            the detector health tables
        atspm_detector_expected: {(DeviceId, Detector): usual actuations per hour}, a thin line behind the sparkline
        atspm_ped_hourly: {(DeviceId, input): presses per hour}, the same for ped buttons
        table_row_limits: Table -> its own row limit instead of max_table_rows. Keys: 'detector_faults',
            'lane_closure', 'timing_plan', 'ped_faults', 'ped_setup', and 'ongoing' for every Ongoing table
        atspm_detector_health_df: Detector health alerts from atspm-detector after
            suppression (faults and information rows)
        joke_index: Specific joke index to use (0-based), None for date-based cycling
        custom_logo_path: Path to custom logo file, None for default ODOT logo
        ongoing_alerts: Alert type -> repeat alerts held back by suppression, carrying an
            OngoingSince column. Each type present adds an 'Ongoing ...' section after the
            matching 'New ...' one; pass None or empty frames for new alerts only
        ongoing_phase_figures: Charts for the ongoing phase termination section
        ongoing_missing_data_figures: Charts for the ongoing missing data section
        ongoing_phase_skip_figures: Charts for the ongoing phase skip section
        preempt_figures: {section: (figure, region) charts} for new preempt alerts, one entry per preempt section
            (Emergency Vehicle, TSP, Rail); a plain list goes with the emergency vehicle section
        ongoing_preempt_figures: The same for ongoing preempt alerts
        atspm_detector_health_figures: 'fault', 'closure' and 'plan' -> (figure, region) charts of the
            detector health faults, lane closures and timing plans
        ongoing_atspm_detector_health_figures: The same for ongoing detector health faults
        signal_focus: Report -> its rows from build_signal_focus. Each report's rows lead it, and its other tables
            leave those signals out
        signal_focus_figures: DeviceId -> (figure, width, height, chart kind) charts of the section's signals
        data_stopped_at: When the data stopped for the whole network, which the report says first, if it did
        missing_data_hourly: Share of missing records per device and hour (DeviceId, TimeStamp, MissingData), which puts
            the missing data tables by hour; without it they name each signal's worst day
        system_outages_hourly: The same per region (Region, TimeStamp, MissingData), for the system outage tables
        coordination_alerts_df: Coordination plan and clock alerts after suppression (see coordination_check); their
            section lists every signal, Signal Focus ones included, as Signal Focus does not tell them
        coordination_figures: (figure, region) charts of the new coordination alerts, one per alert
        ongoing_coordination_figures: The same for the ongoing ones
        measure_alerts: Alerts of the checks on atspm's per-phase measures after suppression, by alert type
            (measure_checks: delay_rise, split_failure_trend, platoon_ratio_trend; timing_checks: phase_timing), each
            in its own section after the coordination one; their held-back repeats come in ongoing_alerts
        timing_figures / ongoing_timing_figures: (figure, region) charts of the phase timing alerts

    Returns:
        Dict mapping region name to BytesIO containing PDF bytes
    """
    # Get unique regions from figure collections and Phase Skip tables
    figure_collections = [
        phase_figures or [],
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
        atspm_detector_health_df,
        coordination_alerts_df,
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
    signal_focus = signal_focus or {}
    table_row_limits = table_row_limits or {}
    table_signal_limits = table_signal_limits or {}
    data_only_signals = table_signal_limits.get('data_only', 0)
    phase_skip_signals = table_signal_limits.get('phase_skips', 0)
    # Each report's tables read their signal names from here, and a table leaves out a signal it has no name for, so
    # a signal in that report's Signal Focus goes from all of them.
    all_signals_df = signals_df

    # Get joke for this report
    joke_text = get_joke(joke_index)
    joke_title = "Joke of the Week"

    # Process each individual region first
    for region in regions:
        log_message(f"Generating report for {region}...", 1, verbosity)
        # Filter figures for this region
        region_phase_figures = [fig for fig, reg in phase_figures if reg == region]
        region_atspm_health_figures = {
            key: [fig for fig, reg in figs if reg == region]
            for key, figs in (atspm_detector_health_figures or {}).items()
        }
        region_ongoing_atspm_health_figures = [
            fig for fig, reg in (ongoing_atspm_detector_health_figures or []) if reg == region
        ]
        region_ongoing_atspm_ped_figures = [
            fig for fig, reg in (ongoing_atspm_ped_figures or []) if reg == region
        ]
        region_missing_data_figures = [fig for fig, reg in missing_data_figures if reg == region]
        region_ongoing_phase_figures = [fig for fig, reg in (ongoing_phase_figures or []) if reg == region]
        region_ongoing_missing_data_figures = [
            fig for fig, reg in (ongoing_missing_data_figures or []) if reg == region
        ]
        region_ongoing_phase_skip_figures = [
            fig for fig, reg in (ongoing_phase_skip_figures or []) if reg == region
        ]

        region_focus_rows = signal_focus.get(region, pd.DataFrame())
        ongoing_limit = table_row_limits.get('ongoing', max_table_rows)
        # Rotated: a region's ongoing alerts are already the ones its report shows, all of them (0 = no row limit).
        # The rotation already chose each region's ongoing alerts; the row limit still holds, whole signals kept
        # (a phase skip table of 5 signals ran to 39 rows without it, replay of 2026-11-16).
        rotated_limit = ongoing_limit
        if not region_focus_rows.empty:
            signals_df = all_signals_df[~all_signals_df['DeviceId'].astype(str).isin(region_focus_rows['DeviceId'])]
        else:
            signals_df = all_signals_df

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
        # The checks' ongoing subsections, which go together at the end of the report.
        ongoing_content = []

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

        # Introduction text: what it says of recurring issues depends on what ends up in this report.
        intro_position = len(content)
        content.append(Spacer(1, 0.2*inch))

        # Joke section comes first in the report body.
        content.append(Paragraph(joke_title, styles['SectionHeading']))
        content.append(Paragraph(joke_text, styles['Normal']))
        content.append(Spacer(1, 0.3*inch))

        # Signal Focus: the worst signals across the checks, told once, before the checks themselves.
        if not region_focus_rows.empty:
            content.extend(_signal_focus_section(region_focus_rows, signal_focus_figures, styles))

        # Section: Controller Alarms
        if alarms_df is not None and not alarms_df.empty and signals_df is not None:
            region_alarm_rows, total_alarm_alerts = prepare_alarms_table(
                alarms_df,
                signals_df,
                region=region,
                max_rows=max_table_rows,
                max_signals=table_signal_limits.get('alarms', 0),
            )
        else:
            region_alarm_rows = pd.DataFrame()
            total_alarm_alerts = 0

        if not region_alarm_rows.empty:
            content.append(Paragraph('Controller Alarms', styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            content.extend(create_reportlab_table(
                region_alarm_rows,
                'Controller Alarms',
                styles,
                total_count=total_alarm_alerts,
                max_rows=max_table_rows,
                include_trend='Sparkline_Data' in region_alarm_rows.columns,
                trend_header='Daily (6wk)',
            ))
            content.append(Spacer(1, 0.3*inch))

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
                    max_rows=rotated_limit,
                )
            )
            total_clearance_ongoing = _waiting_total(
                ongoing_waiting, 'clearance_intervals', total_clearance_ongoing,
                lambda frame: prepare_clearance_interval_alerts_table(frame, signals_df, region=region, max_rows=1))
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
                    max_rows=rotated_limit,
                )
            )
            total_general_phase_ongoing = _waiting_total(
                ongoing_waiting, 'general_phase_conflicts', total_general_phase_ongoing,
                lambda frame: prepare_signal_conflicts_table(frame, signals_df, region=region, max_rows=1))
        else:
            region_general_phase_ongoing_rows = pd.DataFrame()
            total_general_phase_ongoing = 0

        if not region_general_phase_rows.empty:
            content.append(Paragraph('New General Phase Conflicts', styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            content.extend(create_reportlab_table(
                region_general_phase_rows,
                'General Phase Conflicts',
                styles,
                total_count=total_general_phase_conflicts,
                max_rows=max_table_rows,
                include_trend=False,
            ))
            content.append(Spacer(1, 0.3*inch))

        ongoing_content.extend(_ongoing_section(
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
                    max_rows=rotated_limit,
                )
            )
            total_overlap_conflict_ongoing = _waiting_total(
                ongoing_waiting, 'overlap_conflicts', total_overlap_conflict_ongoing,
                lambda frame: prepare_signal_conflicts_table(frame, signals_df, region=region, max_rows=1))
        else:
            region_overlap_conflict_ongoing_rows = pd.DataFrame()
            total_overlap_conflict_ongoing = 0

        if not region_overlap_conflict_rows.empty:
            content.append(Paragraph('New Overlap Conflicts', styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            content.extend(create_reportlab_table(
                region_overlap_conflict_rows,
                'Overlap Conflicts',
                styles,
                total_count=total_overlap_conflicts,
                max_rows=max_table_rows,
                include_trend=False,
            ))
            content.append(Spacer(1, 0.3*inch))

        ongoing_content.extend(_ongoing_section(
            'Overlap Conflicts',
            region_overlap_conflict_ongoing_rows,
            total_overlap_conflict_ongoing,
            styles,
            max_table_rows,
        ))

        if _has_rows(region_clearance_rows):
            content.append(Paragraph('New Clearance Interval Alerts', styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            content.extend(create_reportlab_table(
                region_clearance_rows,
                'Clearance Interval Alerts',
                styles,
                total_count=total_clearance_alerts,
                max_rows=max_table_rows,
                include_trend=False,
            ))
            content.append(Spacer(1, 0.3*inch))

        ongoing_content.extend(_ongoing_section(
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
                max_rows=rotated_limit,
            )
            total_maxout_ongoing = _waiting_total(
                ongoing_waiting, 'maxout', total_maxout_ongoing,
                lambda frame: prepare_phase_termination_alerts_table(frame, region_signals_df, max_rows=1))
        else:
            region_maxout_ongoing_rows = pd.DataFrame()
            total_maxout_ongoing = 0

        if len(filtered_df_maxouts) > 0 and region_phase_figures:
            content.append(Paragraph("New Phase Termination Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            
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

        ongoing_content.extend(_ongoing_section(
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
                max_rows=max_table_rows,
                max_signals=phase_skip_signals,
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
                max_rows=rotated_limit,
                max_signals=phase_skip_signals,
            )
            total_phase_skip_ongoing = _waiting_total(
                ongoing_waiting, 'phase_skips', total_phase_skip_ongoing,
                lambda frame: prepare_phase_skip_alerts_table(
                    phase_skip_rows, signals_df, region=region, allowed_pairs=frame[['DeviceId', 'Phase']].drop_duplicates(),
                    min_total_skips=phase_skip_threshold if phase_skip_threshold is not None else 0, max_rows=1))
        else:
            region_phase_skip_ongoing_rows = pd.DataFrame()
            total_phase_skip_ongoing = 0

        if (
            (region_phase_skip_rows is not None and not region_phase_skip_rows.empty)
            or region_phase_skip_figures
        ):
            content.append(Paragraph("New Phase Skip Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

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

        ongoing_content.extend(_ongoing_section(
            'Phase Skip Alerts',
            region_phase_skip_ongoing_rows,
            total_phase_skip_ongoing,
            styles,
            max_table_rows,
            figures=region_ongoing_phase_skip_figures,
        ))

        # Section: Detector Health, from atspm-detector. Faults first, then the
        # lane closures and timing plan changes as information.
        ongoing_atspm_health = ongoing_alerts.get(ATSPM_DETECTOR_HEALTH)
        fault_limit = table_row_limits.get('detector_faults', max_table_rows)
        closure_limit = table_row_limits.get('lane_closure', max_table_rows)
        plan_limit = table_row_limits.get('timing_plan', max_table_rows)
        # With the urgency tiers, ped buttons are in one priority order with the vehicle detectors.
        ranked_together = _has_rows(atspm_detector_health_df) and 'TriageRank' in atspm_detector_health_df.columns
        fault_type = None if ranked_together else VEHICLE
        region_atspm_fault_rows, total_atspm_faults = _atspm_detector_health_rows(
            atspm_detector_health_df, signals_df, FAULT_STATUSES, region, fault_limit, 'What Is Wrong', fault_type,
            hourly_counts=atspm_detector_hourly, expected_counts=atspm_detector_expected, ped_hourly_counts=atspm_ped_hourly,
        )
        region_atspm_closure_rows, total_atspm_closures = _atspm_detector_health_rows(
            atspm_detector_health_df, signals_df, ('lane_closure',), region, closure_limit, 'What Happened',
            hourly_counts=atspm_detector_hourly, expected_counts=atspm_detector_expected,
        )
        region_atspm_plan_rows, total_atspm_plans = _atspm_detector_health_rows(
            atspm_detector_health_df, signals_df, ('timing_plan',), region, plan_limit, 'What Happened',
            hourly_counts=atspm_detector_hourly, expected_counts=atspm_detector_expected,
        )
        ongoing_ranked_together = _has_rows(ongoing_atspm_health) and 'TriageRank' in ongoing_atspm_health.columns
        ongoing_fault_limit = min(fault_limit, rotated_limit) if rotated_limit > 0 else 0
        region_atspm_fault_ongoing_rows, total_atspm_faults_ongoing = _atspm_detector_health_rows(
            ongoing_atspm_health, signals_df, FAULT_STATUSES, region, ongoing_fault_limit, 'What Is Wrong',
            None if ongoing_ranked_together else VEHICLE,
            hourly_counts=atspm_detector_hourly, expected_counts=atspm_detector_expected, ped_hourly_counts=atspm_ped_hourly,
        )
        total_atspm_faults_ongoing = _waiting_total(
            ongoing_waiting, ATSPM_DETECTOR_HEALTH, total_atspm_faults_ongoing,
            lambda frame: _atspm_detector_health_rows(frame, signals_df, FAULT_STATUSES, region, 1, 'What Is Wrong',
                                                      None if ongoing_ranked_together else VEHICLE))

        def fault_parts(alerts, limit, waiting=None):
            """The faults in two tables, [(title, rows, total)], those affecting operations first; the Data Only table
            stops at data_only_signals signals. waiting: see _waiting_total."""
            parts = []
            for title, impact in ((ATSPM_HARM_TABLE, True), (ATSPM_DATA_TABLE, False)):
                rows, total = _atspm_detector_health_rows(
                    alerts, signals_df, FAULT_STATUSES, region, limit, 'What Is Wrong', None, hourly_counts=atspm_detector_hourly,
                    expected_counts=atspm_detector_expected, ped_hourly_counts=atspm_ped_hourly, impact=impact,
                    max_signals=0 if impact else data_only_signals)
                total = _waiting_total(waiting, ATSPM_DETECTOR_HEALTH, total, lambda frame: _atspm_detector_health_rows(
                    frame, signals_df, FAULT_STATUSES, region, 1, 'What Is Wrong', None, impact=impact))
                parts.append((title, rows, total))
            return parts

        atspm_chart = dict(height=None, width=7.5*inch)
        if not (region_atspm_fault_rows.empty and region_atspm_closure_rows.empty and region_atspm_plan_rows.empty):
            fault_table = []
            if not region_atspm_fault_rows.empty:
                fault_table = create_reportlab_table(
                    region_atspm_fault_rows,
                    ATSPM_DETECTOR_HEALTH_TITLE,
                    styles,
                    total_count=total_atspm_faults,
                    max_rows=fault_limit,
                    include_trend='Sparkline_Data' in region_atspm_fault_rows.columns,
                    trend_header=ATSPM_HOURLY_HEADER,
                )
                content.append(_keep_with_table_start(fault_table, 1.2*inch if ranked_together else 0.7*inch))
            content.append(Paragraph(ATSPM_DETECTOR_HEALTH_TITLE, styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            if fault_table and ranked_together:
                # Two tables: the faults that affect operations, with their charts, then the ones that are data only.
                shown = 0
                for position, (title, rows, total) in enumerate(fault_parts(atspm_detector_health_df, fault_limit)):
                    if rows.empty:
                        continue
                    part = create_reportlab_table(rows, title, styles, total_count=total, max_rows=fault_limit,
                                                  include_trend='Sparkline_Data' in rows.columns, trend_header=ATSPM_HOURLY_HEADER)
                    if shown:    # the first part's start was kept with the section heading above
                        content.append(_keep_with_table_start(part, 0.9*inch))
                    shown += 1
                    content.append(Paragraph(title, styles['SubsectionHeading']))
                    content.append(Spacer(1, 0.1*inch))
                    content.extend(part)
                    content.append(Spacer(1, 0.3*inch))
                    if position == 0:
                        content.extend(_chart_flowables(region_atspm_health_figures.get('fault', []), **atspm_chart))
            elif fault_table:
                content.extend(fault_table)
                content.append(Spacer(1, 0.3*inch))
                content.extend(_chart_flowables(region_atspm_health_figures.get('fault', []), **atspm_chart))
            for rows, total, title, key, limit in (
                    (region_atspm_closure_rows, total_atspm_closures, 'Lane Closures', 'closure', closure_limit),
                    (region_atspm_plan_rows, total_atspm_plans, 'Timing Plan Changes', 'plan', plan_limit)):
                if rows.empty:
                    continue
                content.append(Paragraph(title, styles['SubsectionHeading']))
                content.append(Spacer(1, 0.2*inch))
                content.extend(create_reportlab_table(
                    rows, title, styles, total_count=total, max_rows=limit,
                    include_trend='Sparkline_Data' in rows.columns, trend_header=ATSPM_HOURLY_HEADER,
                ))
                content.append(Spacer(1, 0.3*inch))
                content.extend(_chart_flowables(region_atspm_health_figures.get(key, []), **atspm_chart))

        # Only faults carry on into an Ongoing section: a lane closure or plan
        # change that repeats is not something anyone needs reminding of.
        if ongoing_ranked_together:
            for position, (title, rows, total) in enumerate(fault_parts(ongoing_atspm_health, ongoing_fault_limit, ongoing_waiting)):
                ongoing_content.extend(_ongoing_section(
                    f"{ATSPM_DETECTOR_HEALTH_TITLE}: {title}", rows, total, styles, fault_limit,
                    figures=region_ongoing_atspm_health_figures if position == 0 else None,
                    include_trend='Sparkline_Data' in rows.columns, trend_header=ATSPM_HOURLY_HEADER,
                    chart_height=None, chart_width=7.5*inch, keep_start=True,
                ))
        else:
            ongoing_content.extend(_ongoing_section(
                ATSPM_DETECTOR_HEALTH_TITLE,
                region_atspm_fault_ongoing_rows,
                total_atspm_faults_ongoing,
                styles,
                fault_limit,
                figures=region_ongoing_atspm_health_figures,
                include_trend='Sparkline_Data' in region_atspm_fault_ongoing_rows.columns,
                trend_header=ATSPM_HOURLY_HEADER,
                chart_height=None,
                chart_width=7.5*inch,
                keep_start=True,
            ))

        # Section: Pedestrian Button Health, from atspm-detector, laid out like the
        # Detector Health section: faults first, then set-up issues as information.
        ped_fault_limit = table_row_limits.get('ped_faults', max_table_rows)
        ped_setup_limit = table_row_limits.get('ped_setup', max_table_rows)
        region_ped_fault_rows, total_ped_faults = (pd.DataFrame(), 0) if ranked_together else _atspm_detector_health_rows(
            atspm_detector_health_df, signals_df, FAULT_STATUSES, region, ped_fault_limit, 'What Is Wrong', PED,
            hourly_counts=atspm_ped_hourly,
        )
        region_ped_setup_rows, total_ped_setup = _atspm_detector_health_rows(
            atspm_detector_health_df, signals_df, ('config_issue',), region, ped_setup_limit, 'What Happened', PED,
            hourly_counts=atspm_ped_hourly,
        )
        region_ped_fault_ongoing_rows, total_ped_faults_ongoing = (pd.DataFrame(), 0) if ongoing_ranked_together else _atspm_detector_health_rows(
            ongoing_atspm_health, signals_df, FAULT_STATUSES, region, min(ped_fault_limit, rotated_limit) if rotated_limit > 0 else 0,
            'What Is Wrong', PED,
            hourly_counts=atspm_ped_hourly,
        )
        if not ongoing_ranked_together:
            total_ped_faults_ongoing = _waiting_total(
                ongoing_waiting, ATSPM_DETECTOR_HEALTH, total_ped_faults_ongoing,
                lambda frame: _atspm_detector_health_rows(frame, signals_df, FAULT_STATUSES, region, 1, 'What Is Wrong', PED))
        region_ped_charts = region_atspm_health_figures.get('ped_fault', [])
        if not (region_ped_fault_rows.empty and region_ped_setup_rows.empty and not region_ped_charts):
            content.append(Paragraph(ATSPM_PED_HEALTH_TITLE, styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            if not region_ped_fault_rows.empty:
                content.extend(create_reportlab_table(
                    region_ped_fault_rows,
                    ATSPM_PED_HEALTH_TITLE,
                    styles,
                    total_count=total_ped_faults,
                    max_rows=ped_fault_limit,
                    include_trend='Sparkline_Data' in region_ped_fault_rows.columns,
                    trend_header=ATSPM_HOURLY_PRESSES_HEADER,
                ))
                content.append(Spacer(1, 0.3*inch))
            content.extend(_chart_flowables(region_ped_charts, **atspm_chart))
            if not region_ped_setup_rows.empty:
                content.append(Paragraph('Button Set-Up Issues', styles['SubsectionHeading']))
                content.append(Spacer(1, 0.2*inch))
                content.extend(create_reportlab_table(
                    region_ped_setup_rows, 'Button Set-Up Issues', styles, total_count=total_ped_setup,
                    max_rows=ped_setup_limit, include_trend='Sparkline_Data' in region_ped_setup_rows.columns,
                    trend_header=ATSPM_HOURLY_PRESSES_HEADER,
                ))
                content.append(Spacer(1, 0.3*inch))

        ongoing_content.extend(_ongoing_section(
            ATSPM_PED_HEALTH_TITLE,
            region_ped_fault_ongoing_rows,
            total_ped_faults_ongoing,
            styles,
            ped_fault_limit,
            figures=region_ongoing_atspm_ped_figures,
            include_trend='Sparkline_Data' in region_ped_fault_ongoing_rows.columns,
            trend_header=ATSPM_HOURLY_PRESSES_HEADER,
            chart_height=None,
            chart_width=7.5*inch,
        ))
        if ongoing_ranked_together and region_ongoing_atspm_ped_figures:
            ongoing_content.append(Paragraph(ATSPM_PED_HEALTH_TITLE, styles['SubsectionHeading']))
            ongoing_content.append(Spacer(1, 0.1*inch))
            ongoing_content.extend(_chart_flowables(region_ongoing_atspm_ped_figures, **atspm_chart))

        # Sections: Emergency Vehicle Preempt, TSP and Rail, toward the end of the report, before the data loss sections.
        region_preempt_any = False
        region_preempt_ongoing_any = False
        for section, heading, ongoing_title, trend_header in PREEMPT_SECTION_TEXT:
            rows, total = _preempt_rows(preempt_alerts_df, signals_df, region, max_table_rows, section)
            ongoing_rows, total_ongoing = _preempt_rows(
                ongoing_alerts.get('preempts'), signals_df, region, rotated_limit, section)
            total_ongoing = _waiting_total(ongoing_waiting, 'preempts', total_ongoing,
                                           lambda frame: _preempt_rows(frame, signals_df, region, 1, section))
            region_preempt_any = region_preempt_any or not rows.empty
            region_preempt_ongoing_any = region_preempt_ongoing_any or not ongoing_rows.empty
            if not rows.empty:
                content.append(Paragraph(heading, styles['SectionHeading']))
                content.append(Spacer(1, 0.1*inch))
                content.extend(create_reportlab_table(
                    rows,
                    heading,
                    styles,
                    total_count=total,
                    max_rows=max_table_rows,
                    trend_header=trend_header,
                ))
                content.append(Spacer(1, 0.3*inch))
                content.extend(_chart_flowables(_section_figures(preempt_figures, section, region), PREEMPT_CHART_HEIGHT))
            ongoing_content.extend(_ongoing_section(
                ongoing_title,
                ongoing_rows,
                total_ongoing,
                styles,
                max_table_rows,
                figures=_section_figures(ongoing_preempt_figures, section, region),
                include_trend=True,
                trend_header=trend_header,
                chart_height=PREEMPT_CHART_HEIGHT,
            ))

        # Section: Coordination Plans and Clocks: each group's map and charts, no table (coordination_check). Signal
        # Focus does not tell these, so its signals stay in.
        coordination_groups = [group for group, reg in (coordination_figures or []) if reg == region]
        ongoing_coordination_groups = [group for group, reg in (ongoing_coordination_figures or []) if reg == region]
        if coordination_groups:
            content.append(Paragraph(COORDINATION_TITLE, styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
            content.extend(_coordination_group_flowables(coordination_groups, styles))
        if ongoing_coordination_groups:
            ongoing_content.extend([Paragraph(COORDINATION_TITLE, styles['SubsectionHeading']), Spacer(1, 0.1*inch)]
                                   + _coordination_group_flowables(ongoing_coordination_groups, styles))

        # Sections: checks on atspm's per-phase measures (each off unless switched on). Signal Focus does not tell these.
        measure_any = False
        for title, rows, total, ongoing_rows, total_ongoing, figures, ongoing_figures in _measure_sections(
                measure_alerts, ongoing_alerts, all_signals_df, region, max_table_rows, rotated_limit,
                timing_figures, ongoing_timing_figures, waiting=ongoing_waiting):
            if not rows.empty:
                measure_any = True
                content.append(Paragraph(title, styles['SectionHeading']))
                content.append(Spacer(1, 0.1*inch))
                content.extend(create_reportlab_table(rows, title, styles, total_count=total, max_rows=max_table_rows,
                                                      include_trend=False))
                content.append(Spacer(1, 0.3*inch))
                content.extend(_chart_flowables(figures, TIMING_CHART_HEIGHT))
            if not ongoing_rows.empty:
                measure_any = True
            ongoing_content.extend(_ongoing_section(title, ongoing_rows, total_ongoing, styles, max_table_rows,
                                                    figures=ongoing_figures, chart_height=TIMING_CHART_HEIGHT))

        # Section: Missing Data - Changed to a single header
        missing_trend_header = MISSING_HOURLY_HEADER if _has_rows(missing_data_hourly) else 'Missing (7d)'
        if _has_rows(ongoing_alerts.get('missing_data')) and region_signals_df is not None:
            region_missing_data_ongoing_rows, total_missing_data_ongoing = prepare_missing_data_alerts_table(
                ongoing_alerts['missing_data'],
                region_signals_df,
                max_rows=rotated_limit,
                hourly=missing_data_hourly,
            )
            total_missing_data_ongoing = _waiting_total(
                ongoing_waiting, 'missing_data', total_missing_data_ongoing,
                lambda frame: prepare_missing_data_alerts_table(frame, region_signals_df, max_rows=1, hourly=missing_data_hourly))
        else:
            region_missing_data_ongoing_rows = pd.DataFrame()
            total_missing_data_ongoing = 0

        if len(filtered_df_missing_data) > 0 and region_missing_data_figures:
            content.append(Paragraph("New Missing Data Alerts", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))

            if region_signals_df is not None:
                # Create missing data table with row limit - each signal appears only once with its worst day
                missing_data_alerts_df, total_missing_data_alerts = prepare_missing_data_alerts_table(
                    filtered_df_missing_data,
                    region_signals_df,
                    max_rows=max_table_rows,
                    hourly=missing_data_hourly,
                )

                table_content = create_reportlab_table(
                    missing_data_alerts_df,
                    "Missing Data Alerts",
                    styles,
                    total_count=total_missing_data_alerts,
                    max_rows=max_table_rows,
                    trend_header=missing_trend_header,
                )
                content.extend(table_content)
                content.append(Spacer(1, 0.3*inch))

            # Add missing data charts without additional header
            content.extend(_chart_flowables(region_missing_data_figures))

        ongoing_content.extend(_ongoing_section(
            'Missing Data Alerts',
            region_missing_data_ongoing_rows,
            total_missing_data_ongoing,
            styles,
            max_table_rows,
            figures=region_ongoing_missing_data_figures,
            include_trend=True,
            trend_header=missing_trend_header,
        ))

        # Section: System Outages
        # Filter system outages for this region (or show all for "All Regions")
        if region == "All Regions":
            region_system_outages = system_outages_df if not system_outages_df.empty else pd.DataFrame()
        else:
            region_system_outages = system_outages_df[system_outages_df['Region'] == region] if not system_outages_df.empty else pd.DataFrame()

        region_names = all_signals_df['Region'].dropna().unique().tolist() if all_signals_df is not None else None
        ongoing_system_outages = ongoing_alerts.get('system_outages')
        if _has_rows(ongoing_system_outages) and region != "All Regions":
            ongoing_system_outages = ongoing_system_outages[ongoing_system_outages['Region'] == region]
        if _has_rows(ongoing_system_outages):
            region_system_outages_ongoing_rows, total_system_outages_ongoing = prepare_system_outages_table(
                ongoing_system_outages,
                max_rows=max_table_rows,
                all_regions=region_names,
                hourly=system_outages_hourly,
            )
        else:
            region_system_outages_ongoing_rows = pd.DataFrame()
            total_system_outages_ongoing = 0

        # A region earns a report when it has anything to say, new or ongoing.
        region_has_alerts = any([
            not region_focus_rows.empty,
            region_phase_figures,
            region_missing_data_figures,
            region_phase_skip_figures,
            not region_phase_skip_rows.empty,
            not region_clearance_rows.empty,
            not region_overlap_dual_rows.empty,
            not region_general_phase_rows.empty,
            not region_overlap_conflict_rows.empty,
            region_preempt_any,
            # Lane closures, plan changes and ped set-up issues ride along but are no reason to send a report.
            not region_atspm_fault_rows.empty,
            not region_ped_fault_rows.empty,
            not region_system_outages.empty,
            not region_maxout_ongoing_rows.empty,
            not region_missing_data_ongoing_rows.empty,
            not region_phase_skip_ongoing_rows.empty,
            not region_clearance_ongoing_rows.empty,
            not region_general_phase_ongoing_rows.empty,
            not region_overlap_conflict_ongoing_rows.empty,
            region_preempt_ongoing_any,
            bool(coordination_groups),
            bool(ongoing_coordination_groups),
            measure_any,
            not region_atspm_fault_ongoing_rows.empty,
            not region_ped_fault_ongoing_rows.empty,
            not region_system_outages_ongoing_rows.empty,
        ])

        if data_stopped_at is not None:
            stopped = pd.Timestamp(data_stopped_at)
            content.insert(intro_position, Spacer(1, 0.1*inch))
            content.insert(intro_position, Paragraph(
                f"<b>Data stopped updating at {stopped:%a} {stopped.month}/{stopped.day}/{stopped:%Y %H:%M}.</b>", styles['Normal']))

        if not region_system_outages.empty:
            content.append(Paragraph("New System-Wide Outages", styles['SectionHeading']))
            content.append(Spacer(1, 0.1*inch))
              # Create system outages table
            system_outages_table_df, total_system_outages = prepare_system_outages_table(
                region_system_outages,
                max_rows=max_table_rows,
                all_regions=region_names,
                hourly=system_outages_hourly,
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

        ongoing_content.extend(_ongoing_section(
            'System-Wide Outages',
            region_system_outages_ongoing_rows,
            total_system_outages_ongoing,
            styles,
            max_table_rows,
        ))

        # Every check's ongoing issues, together after all the new alerts.
        content.extend(_ongoing_issues(ongoing_content, styles))

        # A heading goes to the next page unless the start of its table fits under it.
        for position in range(len(content) - 1, 0, -1):
            item = content[position]
            if isinstance(item, Paragraph) and item.style.name in ('SectionHeading', 'SubsectionHeading'):
                content.insert(position, CondPageBreak(HEADING_ROOM))

        # Build the PDF with custom canvas for proper page numbering; a region
        # with nothing to report gets no PDF, so it is not laid out either.
        if region_has_alerts:
            doc.build(content,
                     onFirstPage=header_footer.firstPage,
                     onLaterPages=header_footer.laterPages,
                     canvasmaker=make_canvas)
            buffer_objects.append((region, buffer))
            log_message(f"Report for {region} generated in memory.", 1, verbosity)

    # Return dict mapping region name to BytesIO
    return {region: buf for region, buf in buffer_objects}
