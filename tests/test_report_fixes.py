"""The repeated hour of the clock change, the outage table, the health narrative and the table widths."""

import sys
from pathlib import Path

import pandas as pd
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase.pdfmetrics import stringWidth

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

try:   # ODOT's own ingest script, not in the package
    import odot_ingest
except ImportError:
    odot_ingest = None
needs_odot = pytest.mark.skipif(odot_ingest is None, reason="ODOT's own script, not in the package")
from atspm_report.data_processing import process_missing_data
from atspm_report.generator import _split_system_outages
from atspm_report.health_story import evidence_text, health_story
from atspm_report.table_generation import (
    TABLE_AVAILABLE_WIDTH,
    _column_widths,
    create_reportlab_table,
    prepare_system_outages_table,
)


# ---- clock change ----

@needs_odot
def test_clock_change_hour_is_first_sunday_of_november():
    assert odot_ingest.clock_change_hour(2026) == (pd.Timestamp('2026-11-01 01:00'), pd.Timestamp('2026-11-01 02:00'))
    assert odot_ingest.clock_change_hour(2025) == (pd.Timestamp('2025-11-02 01:00'), pd.Timestamp('2025-11-02 02:00'))
    assert odot_ingest.clock_change_hour(2027) == (pd.Timestamp('2027-11-07 01:00'), pd.Timestamp('2027-11-07 02:00'))


@needs_odot
def test_staging_filter_only_for_windows_touching_the_hour():
    assert odot_ingest._clock_change_filter('2026-10-31 21:00', '2026-11-01 00:00') == ('', [])
    assert odot_ingest._clock_change_filter('2026-11-01 02:00', '2026-11-01 05:00') == ('', [])
    sql, params = odot_ingest._clock_change_filter('2026-11-01 00:00', '2026-11-01 03:00')
    assert sql.count('NOT') == 1 and sql.count('?') == 2
    assert [pd.Timestamp(p) for p in params] == [pd.Timestamp('2026-11-01 01:00'), pd.Timestamp('2026-11-01 02:00')]
    assert odot_ingest._clock_change_filter(None, '2026-11-02 00:00')[1]


@needs_odot
def test_stored_rows_leave_out_the_hour():
    bins = pd.DataFrame({'TimeStamp': pd.to_datetime(['2026-11-01 00:45', '2026-11-01 01:00', '2026-11-01 01:45',
                                                      '2026-11-01 02:00', '2025-11-02 01:15', '2026-10-31 01:15'])})
    kept = odot_ingest.without_clock_change_hour(bins, 'actuations')
    assert list(kept['TimeStamp'].astype(str)) == ['2026-11-01 00:45:00', '2026-11-01 02:00:00', '2026-10-31 01:15:00']


@needs_odot
def test_timeline_rows_crossing_the_hour_go_too():
    rows = pd.DataFrame({
        'StartTime': pd.to_datetime(['2026-11-01 00:59:50', '2026-11-01 01:30:00', '2026-11-01 01:59:50',
                                     '2026-11-01 00:30:00', '2026-11-01 00:10:00', '2026-11-01 02:00:00', '2026-11-01 01:10:00']),
        'EndTime': pd.to_datetime(['2026-11-01 01:00:10', '2026-11-01 01:31:00', '2026-11-01 02:00:10',
                                   '2026-11-01 03:00:00', '2026-11-01 00:20:00', '2026-11-01 02:05:00', None]),
    })
    kept = odot_ingest.without_clock_change_hour(rows, 'timeline')
    assert list(kept.index) == [4, 5]


def test_the_short_day_is_not_missing_data():
    day = '2026-11-01'
    hours = [pd.Timestamp(day) + pd.Timedelta(minutes=15 * i) for i in range(96)]
    kept = [t for t in hours if not (pd.Timestamp(day) + pd.Timedelta(hours=1) <= t < pd.Timestamp(day) + pd.Timedelta(hours=2))]
    other = [pd.Timestamp('2026-10-31') + pd.Timedelta(minutes=15 * i) for i in range(96)]
    frame = pd.DataFrame({'DeviceId': ['a'] * (len(kept) + 96), 'TimeStamp': kept + other})
    result = process_missing_data(frame)
    assert result['MissingData'].abs().max() < 1e-9
    # A caller whose data still holds the hour reads as complete, not as negative missing data.
    frame = pd.DataFrame({'DeviceId': ['a'] * 96, 'TimeStamp': hours})
    assert process_missing_data(frame)['MissingData'].iloc[0] == 0.0


# ---- outages ----

def _outage_days(*spec):
    latest = pd.Timestamp('2026-11-01')
    return pd.DataFrame([{'Date': pd.Timestamp(day), 'Region': region, 'MissingData': missing, 'LatestDataDay': latest}
                         for day, region, missing in spec])


FIVE = [f'Region {n}' for n in range(1, 6)]


def _real_days():
    """What the local data held for Oct 29 to Nov 1: all five regions out, Oct 31 fine."""
    return _outage_days(*[(day, region, missing) for day, missing in
                          (('2026-10-29', 1.0), ('2026-10-30', 0.68), ('2026-11-01', 0.6)) for region in FIVE])


def test_outage_table_has_one_row_per_real_outage():
    table, total = prepare_system_outages_table(_real_days(), all_regions=FIVE)
    assert total == 2
    assert list(table['Dates']) == ['11/1/26', '10/29/26 - 10/30/26']
    assert list(table['Regions']) == ['All 5 regions', 'All 5 regions']
    assert list(table['Days']) == [1, 2]
    assert list(table['Missing Data %']) == [0.6, 1.0]
    # New outages: Dates say when and how long; an Ongoing column belongs to the Ongoing table only.
    assert 'Ongoing' not in table.columns


def test_ongoing_outage_table_has_the_ongoing_column():
    days = _real_days().assign(OngoingSince=pd.Timestamp('2026-10-29'))
    table, _ = prepare_system_outages_table(days, all_regions=FIVE)
    assert list(table['Ongoing']) == ['11/1/26 (1d)', 'Ended 10/30/26']


def test_outage_table_of_one_region_names_it():
    days = _real_days()
    table, _ = prepare_system_outages_table(days[days['Region'] == 'Region 2'], all_regions=FIVE)
    assert list(table['Regions']) == ['Region 2', 'Region 2']


def test_old_unrelated_outages_do_not_make_a_new_one_ongoing():
    # The history that said 53d: separate one-day outages, the last one a week before these.
    past = pd.DataFrame({'Region': ['Region 1'] * 3, 'Date': pd.to_datetime(['2026-09-11', '2026-10-16', '2026-10-23'])})
    days = _real_days()
    new, ongoing = _split_system_outages(days, days, past)
    assert len(new) == len(days) and ongoing.empty
    assert 'Ongoing' not in prepare_system_outages_table(new, all_regions=FIVE)[0].columns   # the New table


def test_outage_reported_before_is_ongoing_from_the_day_it_began():
    days = _outage_days(('2026-10-30', 'Region 1', 0.7), ('2026-10-31', 'Region 1', 0.7), ('2026-11-01', 'Region 1', 0.7))
    past = pd.DataFrame({'Region': ['Region 1'], 'Date': pd.to_datetime(['2026-10-30'])})
    new, ongoing = _split_system_outages(days, days, past)
    assert new.empty and len(ongoing) == 3
    assert set(ongoing['OngoingSince']) == {pd.Timestamp('2026-10-30')}
    table, total = prepare_system_outages_table(ongoing, all_regions=FIVE)
    assert total == 1 and list(table['Ongoing']) == ['10/30/26 (3d)']


def test_reported_outage_that_ended_is_not_shown_again():
    days = _outage_days(('2026-10-29', 'Region 1', 0.7), ('2026-10-30', 'Region 1', 0.7))
    past = pd.DataFrame({'Region': ['Region 1'], 'Date': pd.to_datetime(['2026-10-29'])})
    new, ongoing = _split_system_outages(days, days, past)
    assert new.empty and ongoing.empty


def test_outage_days_stay_one_row_per_region_and_day_for_the_history():
    days = _real_days()
    new, _ = _split_system_outages(days, days, pd.DataFrame())
    assert {'Region', 'Date'} <= set(new.columns) and len(new) == 15


# ---- narrative ----

def _periods(*spans, what='stuck on'):
    return [{'start': a, 'end': z, 'what': what} for a, z in spans]


def test_few_periods_keep_exact_times_and_lead_with_the_fault():
    story = health_story('det 22: Ph8 Count (covers 1 lane) - Stuck on: held ON 8 h 16 min from Sat 00:41',
                         _periods(('2026-10-31 00:41:00', '2026-10-31 08:57:00')),
                         [{'what': 'stuck on', 'detail': 'held ON 8 h 16 min from Sat 00:41', 'confidence': 0.9}])
    assert story == '<b>Held ON</b> 8 h 16 min, Sat 10/31 00:41-08:57. Ph8 Count, 1 lane.'


def test_walks_of_the_worst_hold_are_not_read_as_the_total():
    evidence = [{'what': 'stuck on', 'detail': 'stuck ON: held 1 h 30 min through 218 walks (limit 10 min)', 'confidence': 0.9}]
    one = health_story('', _periods(('2026-10-31 00:00:00', '2026-10-31 01:30:00')), evidence, bold=False)
    assert one == 'Held ON 1 h 30 min, Sat 10/31 00:00-01:30: through 218 walks.'
    spans = [(f'2026-10-31 0{hour}:00:00', f'2026-10-31 0{hour}:30:00') for hour in range(5)]
    many = health_story('', _periods(*spans), evidence, bold=False)
    assert many.endswith('up to 218 walks in one hold.')


def test_many_periods_are_grouped():
    spans = [(f'2026-10-{day} {hour:02d}:00:00', f'2026-10-{day} {hour:02d}:30:00') for day in (30, 31) for hour in (0, 1, 2, 3, 4)]
    story = health_story('det 1: held', _periods(*spans), bold=False)
    assert story == 'Held ON 10 times, 5 h in total, mostly nights, Oct 30 to Oct 31.'


def test_long_rows_name_what_does_not_fit():
    spans = [(f'2026-10-31 {hour:02d}:00:00', f'2026-10-31 {hour:02d}:30:00') for hour in range(12)]
    evidence = [
        {'what': 'too-fast actuations', 'detail': '15,834 of 64,362 ONs follow the previous ON within 0.6 s (246 per 1,000; limit 188)', 'confidence': 0.95},
        {'what': 'counts dropped', 'detail': "counts and occupancy both fell from Sat 10:00 to Sun 02:00: 1123 per hour where its phase mates' traffic gives ~2945 (GEH 36.0)", 'confidence': 0.9},
        {'what': 'no daily pattern', 'detail': 'counts do not follow the day: ~11 an hour by day, ~4 at night, while the rest of the signal drops from ~2,070 to ~301 (noise, not traffic)', 'confidence': 0.85},
        {'what': 'too many for the traffic', 'detail': 'busiest 5 min 3,072 veh/h/lane vs limit 2,100', 'confidence': 0.8},
        {'what': 'spiky counts', 'detail': '6 bursts a day in 10-min counts at 3 x its usual level or more (largest GEH 26)', 'confidence': 0.7},
    ]
    periods = (_periods(*spans, what='too-fast actuations') + _periods(('2026-10-31 10:00:00', '2026-11-01 00:00:00'), what='counts dropped')
               + _periods(*spans, what='too many for the traffic') + _periods(*spans, what='spiky counts'))
    story = health_story('det 39: Ph6 Advance (covers 1 lane) - x', periods, evidence, bold=False)
    assert story.startswith('Too-fast actuations 12 times, 6 h in total, Oct 31: 25% of ONs come within 0.6 s of the last (limit 19%).')
    assert '~1,123 an hour where ~2,945 expected' in story
    assert story.endswith('Ph6 Advance, 1 lane.')
    assert len(story) < 290


def test_evidence_of_an_unknown_wording_is_kept_without_its_times():
    assert evidence_text('new check', 'odd thing Sat 10:00-Sat 12:00 at P2') == 'odd thing at Ph2'
    assert evidence_text('counts too high', 'counts rose to 1404000000000.0 x what its phase mates gave') == 'over 100 x what its phase mates gave'
    assert evidence_text('silent', 'something new') == 'something new'
    assert evidence_text('new check', 'rose a lot: (7 h, GEH 68)') == 'rose a lot'
    assert evidence_text('counts too high', 'counts rose far above its phase mates, which gave almost nothing: (7 h, GEH 68)') == (
        'far above its phase mates, which gave almost nothing')
    assert len(evidence_text('new check', 'word ' * 100)) < 130


def test_story_is_safe_markup():
    story = health_story('det 1: x', _periods(('2026-10-31 00:00:00', '2026-10-31 01:00:00'), what='a <b & c'))
    assert 'A &lt;b &amp; c' in story


# ---- widths ----

def _preempt_rows():
    return pd.DataFrame({
        'Signal': ['05018-OR99W at Clear Lake-Irving and Somewhere'] * 2,
        'Preempt': ['5', '6'],
        'Type': ['Long Hold', 'Chatter'],
        'Base/Day': ['4.5', '22.7'],
        'Last 7d/Day': ['2.7', '16.3'],
        'Details': ['7 calls over 60 min, longest 10.1 h on 10/30 22:30; other rail preempts here served 49 normal calls',
                    '114 of 114 calls under 3 s'],
        'Ongoing': ['9/29/26 (35d)', '9/29/26 (35d)'],
        'Sparkline_Data': [[1, 2, 3, 2, 1], [2, 3, 2, 3, 2]],
    })


def _rendered(rows, trend_header='Calls/Day (6wk)'):
    return create_reportlab_table(rows, 'Preempt', getSampleStyleSheet(), total_count=len(rows),
                                  include_trend=True, trend_header=trend_header)[-1]


def test_preempt_table_fits_the_page_and_the_sparkline_fits_its_cell():
    table = _rendered(_preempt_rows())
    widths = table._colWidths
    assert sum(widths) <= TABLE_AVAILABLE_WIDTH + 0.01
    image = table._cellvalues[1][-1]
    image = image[0] if isinstance(image, (list, tuple)) else image
    # The trend column keeps its full width, and the image stays inside it past the padding.
    assert widths[-1] == 1.2 * inch
    assert image.drawWidth <= widths[-1] - 6
    # Every column is at least as wide as the words of its header at the padding it got.
    assert widths[list(_preempt_rows().columns).index('Ongoing')] >= 40


def test_headers_wrap_or_fit_rather_than_run_into_the_next_cell():
    long_header = 'Calls per Day (six weeks of them)'
    table = _rendered(_preempt_rows(), trend_header=long_header)
    header = table._cellvalues[0]
    widths = table._colWidths
    assert not isinstance(header[-1], str)    # a Paragraph, which wraps
    for name, width in zip(header, widths):
        if isinstance(name, str):
            assert stringWidth(name, 'Helvetica-Bold', 10) <= width


def test_trend_column_is_not_shrunk_in_squeezed_tables():
    rows = _preempt_rows().drop(columns='Sparkline_Data')
    rows['Trend'] = ''
    widths, _ = _column_widths(rows, list(rows.columns), 1.2 * inch)
    assert widths[-1] == 1.2 * inch and sum(widths) <= TABLE_AVAILABLE_WIDTH + 0.01


def test_story_column_gets_the_room():
    rows = pd.DataFrame({'Signal': ['2B555-OR127 CornPass @ Wagon Way and a long name'], 'Det': ['22'], 'Status': ['Bad'],
                         'What Is Wrong': ['Held ON 3 h.'], 'Ongoing': ['9/29/26 (35d)']})
    widths, _ = _column_widths(rows, list(rows.columns))
    assert widths[3] >= 3.5 * inch and sum(widths) <= TABLE_AVAILABLE_WIDTH + 0.01


def test_tables_without_a_sentence_are_left_to_reportlab():
    rows = pd.DataFrame({'Signal': ['A'], 'Phase': ['2']})
    assert _column_widths(rows, list(rows.columns)) == (None, False)


def test_new_ped_labels_have_a_category_a_heading_and_plain_evidence():
    from atspm_report.health_story import HEADINGS, evidence_text
    odot_power_bi = pytest.importorskip('odot_power_bi')   # ODOT's own script, not in the package
    CATEGORIES, PERIOD_CATEGORY = odot_power_bi.CATEGORIES, odot_power_bi.PERIOD_CATEGORY
    for label in ('sustained pressing', 'walks without presses'):
        assert PERIOD_CATEGORY[label] in CATEGORIES and label in HEADINGS
    assert evidence_text('sustained pressing', 'sustained pressing: 2,608 presses on Sun 10-11, 5.7 x its usual ~460 a day, with 14 15-minute periods of 30+ presses at 10+ a walk') == \
        '2,608 presses in a day where ~460 usual; 14 heavy quarter hours'
    assert evidence_text('walks without presses', 'walks jumped with no presses from Sun 09-27 10:30: 37 walks in the next 2.5 h where those hours usually bring ~4; at its earlier walks per press they would mean ~60 presses') == \
        '37 walks in 2.5 h where ~4 usual, with no press'


def test_an_alarm_with_a_prompt_row_and_a_final_row_counts_once():
    from atspm_report.alarm_processing import summarize_daily_alarms
    start = pd.Timestamp('2026-10-20 08:00')
    timeline = pd.DataFrame({'DeviceId': ['A', 'A'], 'StartTime': [start, start], 'EventClass': ['MMU Flash', 'MMU Flash']})
    assert summarize_daily_alarms(timeline)['Count'].tolist() == [1]


def test_a_ped_stop_judged_against_its_own_weeks_says_why_it_is_not_a_quiet_day():
    from atspm_report.health_story import evidence_text
    own = ("no presses for 37 logged hours, Tue 11-10 11:00 to the end of the log; its longest quiet spell in the 28 days "
           "before was 15 h, and a usual day brings ~40 presses; the signal's other buttons kept 100 % of their usual "
           "presses; no walks of its phase came")
    assert evidence_text('sudden stop', own) == (
        'none for 37 logged hours; its longest quiet spell in the 4 weeks before was 15 hours; the other buttons carried on')
    alone = own.replace("; the signal's other buttons kept 100 % of their usual presses", '')
    assert evidence_text('sudden stop', alone) == 'none for 37 logged hours; its longest quiet spell in the 4 weeks before was 15 hours'
    window = ('no presses for 41 logged hours, Mon 11-09 12:00 to Wed 11-11 05:00; its own level expects ~33 (chance about '
              '1 in 2,000 allowing for clumps); last button event is an OFF, so not held on; no walks: quiet, or a dead button (inferred)')
    assert evidence_text('sudden stop', window) == 'none for 41 logged hours where ~33 expected; quiet, or a dead button'
