"""The emailed report's health tables and charts: sparklines inside their cells, the Det column, charts that read."""

from io import BytesIO

import numpy as np
import pandas as pd
from PIL import Image as PILImage
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Image

from atspm_report.atspm_detector_health import build_atspm_detector_health_alerts, detector_info
from atspm_report.generator import _hourly_actuations
from atspm_report.health_story import detector_label, health_story
from atspm_report.table_generation import (
    SPARKLINE_WIDTH, TABLE_AVAILABLE_WIDTH, _sparkline_png, create_reportlab_table, prepare_atspm_detector_health_table,
)
from atspm_report.visualization import create_atspm_detector_health_plots
from test_atspm_detector_health import CONFIG, SIGNALS, _finding, _findings


def _painted_rows(png):
    """Rows of the image with anything drawn on them, from the top."""
    alpha = np.asarray(PILImage.open(BytesIO(png)).convert('RGBA'))[:, :, 3]
    return alpha.shape[0], np.flatnonzero((alpha > 40).any(axis=1))


def test_sparkline_line_at_its_lowest_value_is_not_cut_off():
    height, rows = _painted_rows(_sparkline_png([0, 0, 5, 9, 0, 0, 0, 3, 0, 0], 1.0, 0.25, '#1f77b4'))
    assert rows.min() > 0 and rows.max() < height - 1


def test_flat_zero_sparkline_shows_a_whole_line():
    height, rows = _painted_rows(_sparkline_png([0] * 20, 1.0, 0.25, '#1f77b4'))
    assert rows.max() < height - 1 and len(rows) >= 2   # 1 pt at 150 dpi is two rows, not half of it


def test_sparkline_leaves_a_gap_for_hours_without_data():
    alpha = np.asarray(PILImage.open(BytesIO(_sparkline_png([3, 5, 4, float('nan'), float('nan'), float('nan'), 4, 5, 3],
                                                            1.0, 0.25, '#1f77b4'))).convert('RGBA'))[:, :, 3]
    middle = alpha[:, alpha.shape[1] // 2]
    assert (middle > 40).sum() == 0
    assert _sparkline_png([float('nan')] * 5, 1.0, 0.25, '#1f77b4')   # nothing to draw is still an image


def test_detector_label_names_the_detector_without_the_function_other():
    assert detector_label(22, 'det 22: Ph8 Count (covers 1 lane) - Stuck on: x') == 'Det 22, Ph8 Count, 1 lane'
    assert detector_label(33, 'det 33: Ph2 Other - Counts fall: x') == 'Det 33, Ph2'
    assert detector_label(33, 'det 33: Ph2 Other (covers 2 lanes) - x') == 'Det 33, Ph2, 2 lanes'
    assert detector_label(7, 'det 7: Other - x') == 'Det 7'
    assert detector_label(5, 'det 5: went silent') == 'Det 5'
    assert detector_label(4, 'stuck ON: held 4 h', 'Button') == 'Button 4'


def test_story_can_leave_the_detector_to_its_column():
    reason = 'det 22: Ph8 Count (covers 1 lane) - Stuck on: x'
    periods = [{'start': '2026-10-31 00:41:00', 'end': '2026-10-31 08:57:00', 'what': 'stuck on'}]
    assert health_story(reason, periods, name_detector=False) == '<b>Held ON</b> 8 h 16 min, Sat 10/31 00:41-08:57.'


def test_fault_table_has_the_det_column_and_a_sparkline_inside_each_cell():
    alerts, _ = build_atspm_detector_health_alerts(_findings(), report_date='2026-10-21', info=CONFIG)
    hourly = {('dev-a', int(detector)): [0.0, 4.0, 9.0, float('nan'), 2.0, 0.0] for detector in alerts['Detector']}
    rows, total = prepare_atspm_detector_health_table(alerts, SIGNALS, ('bad',), hourly_counts=hourly)
    assert total == 1 and list(rows.columns) == ['Signal', 'Det', 'What Is Wrong', 'Sparkline_Data']
    assert rows.iloc[0]['Det'].startswith('Det ') and ', Ph' in rows.iloc[0]['Det']
    assert 'Ph6 Advance' not in rows.iloc[0]['What Is Wrong'] and 'Ph2 Advance' not in rows.iloc[0]['What Is Wrong']

    table = create_reportlab_table(rows, 'Detector Health', getSampleStyleSheet(), total_count=total,
                                   include_trend=True, trend_header='Hourly Actuations')[-1]
    table.wrap(TABLE_AVAILABLE_WIDTH, 600)
    assert sum(table._colWidths) <= TABLE_AVAILABLE_WIDTH + 0.01
    image = table._cellvalues[1][-1]
    image = image[0] if isinstance(image, (list, tuple)) else image
    assert isinstance(image, Image) and image.drawWidth == SPARKLINE_WIDTH
    assert image.drawWidth <= table._colWidths[-1] and image.drawHeight <= table._rowHeights[1]


def test_ped_table_has_no_status_and_names_the_button():
    findings = pd.DataFrame([_finding('dev-a', 4, '2026-10-20 06:00', '2026-10-20 10:00', 'stuck on', 0.9,
                                      'stuck ON: held 4 h through 80 walks (limit 5 min, 3 walks, or 3 min through 2 walks)',
                                      kind='Ped')])
    alerts, _ = build_atspm_detector_health_alerts(findings, report_date='2026-10-20')
    rows, _ = prepare_atspm_detector_health_table(alerts, SIGNALS, ('bad',), detector_type='Ped')
    assert list(rows.columns) == ['Signal', 'Button', 'What Is Wrong'] and rows.iloc[0]['Button'] == 'Button 4'


def test_hourly_actuations_add_up_and_leave_hours_without_data_empty():
    start = pd.Timestamp('2026-10-20')
    counts = pd.DataFrame({
        'TimeStamp': [start, start + pd.Timedelta(minutes=45), start + pd.Timedelta(hours=2)],
        'DeviceId': ['dev-a'] * 3, 'Detector': [3] * 3, 'Total': [4, 6, 0],
    })
    series = _hourly_actuations(counts, start, start + pd.Timedelta(hours=4))[('dev-a', 3)]
    assert series[0] == 10 and series[2] == 0 and pd.isna(series[1]) and pd.isna(series[3]) and len(series) == 4
    assert _hourly_actuations(counts.head(0), start, start + pd.Timedelta(hours=4)) == {}


def _chart():
    findings = _findings()
    findings.loc[findings['Detector'] == 2, ['End', 'Confidence']] = [pd.Timestamp('2026-10-19 21:52:15'), 0.9]
    alerts, _ = build_atspm_detector_health_alerts(findings, report_date='2026-10-20', info=CONFIG)
    faults = alerts[alerts['Status'] == 'bad']
    times = pd.date_range('2026-10-18', '2026-10-21', freq='15min', inclusive='left')
    counts = pd.DataFrame([{'TimeStamp': t, 'DeviceId': 'dev-a', 'Detector': d, 'Total': 0}
                           for t in times for d in (1, 2, 3)])
    window = (pd.Timestamp('2026-10-18'), pd.Timestamp('2026-10-21'))
    return create_atspm_detector_health_plots(faults, SIGNALS, 3, counts, detector_info(CONFIG), window=window)[0][0]


def test_lines_at_zero_are_drawn_over_the_axis_line():
    ax = _chart().axes[0]
    spine = ax.spines['bottom'].get_zorder()
    lines = ax.get_lines()
    drawn = [line for line in lines if str(line.get_label()).startswith(('Det', 'Other'))]
    assert drawn and all(line.get_zorder() > spine and not line.get_clip_on() for line in drawn)


def test_chart_text_is_larger_and_the_legend_sits_above_the_plot():
    fig = _chart()
    assert fig.texts[0].get_fontsize() >= 11 and fig.texts[1].get_fontsize() >= 9.5
    ax = fig.axes[0]
    legend = ax.get_legend()
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    assert legend.get_window_extent(renderer).y0 >= ax.get_window_extent(renderer).y1 - 1
    width = fig.get_window_extent(renderer).width
    for text in fig.texts:
        assert text.get_window_extent(renderer).x1 <= width


def test_a_long_chart_caption_wraps_instead_of_losing_its_end():
    alerts, _ = build_atspm_detector_health_alerts(_findings(), report_date='2026-10-21', info=CONFIG)
    faults = alerts[alerts['Status'] == 'bad'].copy()
    number = int(faults['Detector'].iloc[0])
    faults['Reason'] = f"det {number}: Ph6 Advance - Counts fall{' very' * 30}: x"
    times = pd.date_range('2026-10-20', '2026-10-21', freq='15min', inclusive='left')
    counts = pd.DataFrame([{'TimeStamp': t, 'DeviceId': 'dev-a', 'Detector': number, 'Total': 2} for t in times])
    fig = create_atspm_detector_health_plots(faults, SIGNALS, 3, counts, detector_info(CONFIG),
                                             window=(pd.Timestamp('2026-10-20'), pd.Timestamp('2026-10-21')))[0][0]
    caption = ' '.join(text.get_text() for text in fig.texts[1:])
    assert caption.endswith(f'det {number}') and '…' not in caption and len(fig.texts) > 2
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    width = fig.get_window_extent(renderer).width
    assert all(text.get_window_extent(renderer).x1 <= width for text in fig.texts)


def _tick_labels_overlap(fig, ax):
    """True when two of the axis's shown x tick labels touch; also returns the labels."""
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    shown = [label for label in ax.get_xticklabels() if label.get_visible() and label.get_text()]
    boxes = sorted((label.get_window_extent(renderer) for label in shown), key=lambda box: box.x0)
    return any(a.x1 > b.x0 for a, b in zip(boxes, boxes[1:])), [label.get_text() for label in shown]


def test_detector_chart_day_labels_do_not_overlap_the_hours():
    import pytest
    for days in (1, 2, 3, 7, 14):
        end = pd.Timestamp('2026-10-21')
        start = end - pd.Timedelta(days=days)
        findings = _findings()
        alerts, _ = build_atspm_detector_health_alerts(findings, report_date='2026-10-20', info=CONFIG)
        faults = alerts[alerts['Status'] == 'bad']
        times = pd.date_range(start, end, freq='15min', inclusive='left')
        counts = pd.DataFrame([{'TimeStamp': t, 'DeviceId': 'dev-a', 'Detector': d, 'Total': 3} for t in times for d in (1, 2, 3)])
        figures = create_atspm_detector_health_plots(faults, SIGNALS, 3, counts, detector_info(CONFIG), window=(start, end))[0]
        if not figures:
            pytest.fail(f'no chart for {days} days')
        fig = figures[0]
        strip = fig.axes[-1]
        overlap, labels = _tick_labels_overlap(fig, strip)
        assert not overlap, (days, labels)
        assert len(labels) >= 3, (days, labels)
        if days > 1:
            assert any('/' in label for label in labels), (days, labels)   # the days are still named


def test_signal_focus_overview_time_labels_do_not_overlap():
    from atspm_report.focus_overview import create_focus_overview
    for days in (1, 2, 3, 7):
        start = pd.Timestamp('2026-10-12')
        end = start + pd.Timedelta(days=days)
        hours = pd.date_range(start, end, freq='h', inclusive='left')
        panel = {'kind': 'maxout', 'title': 'Percent Max Out', 'series': pd.Series(0.2, index=hours),
                 'band': pd.DataFrame({'lo': 0.1, 'hi': 0.3}, index=hours)}
        fig = create_focus_overview('Alpha', (start, end), panels=[panel])
        overlap, labels = _tick_labels_overlap(fig, fig.axes[-1])
        assert not overlap and len(labels) >= 3, (days, labels)
