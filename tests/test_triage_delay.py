"""Delay per vehicle in the detector triage's Impact: wording and the number shown only, never the tier."""

import duckdb
import pandas as pd

from atspm_report import measure_inputs as mi
from atspm_report.detector_triage import OTHER_PHASES
from test_detector_triage import FAULT, WEEKDAYS, in_fault, rank

WAIT_UP = lambda ts, phase: (1, 4, 40.0) if in_fault(ts) and phase == 4 else (1, 4, 20.0)
PHASE_WAIT_TEXT = 'Phase wait on other phases up 20 s on average over 4 h: Ph4 20 to 40 s'


def measures(delay_in_fault=25.0, delay=10.0, vehicles=20, phase=4):
    """arrival_on_green and approach_delay of signal A's phase: `vehicles` a bin, `delay` s each (delay_in_fault in the fault)."""
    bins = [ts for day in WEEKDAYS for ts in pd.date_range(day, periods=96, freq='15min')]
    aog = pd.DataFrame({'TimeStamp': bins, 'DeviceId': 'A', 'Phase': phase, 'Total_Actuations': vehicles, 'Percent_AOG': 0.5})
    each = [delay_in_fault if in_fault(ts) else delay for ts in bins]
    ad = pd.DataFrame({'TimeStamp': bins, 'DeviceId': 'A', 'Phase': phase,
                       'Delay_Seconds': [vehicles * d for d in each], 'Queued_Delay_Seconds': 0.0})
    return ad, aog


def test_harm_is_told_as_delay_per_vehicle_where_it_was_measured():
    ad, aog = measures()
    plain = rank([FAULT], WAIT_UP).iloc[0]
    told = rank([FAULT], WAIT_UP, approach_delay=ad, arrival_on_green=aog).iloc[0]
    assert plain.ImpactText == PHASE_WAIT_TEXT
    assert told.ImpactText == 'Delay per vehicle on other phases up 15 s on average over 4 h: Ph4 10 to 25 s'
    # The tier, its numbers and the Signal Focus lines (and their wait chart) do not change.
    for column in ('Tier', 'HurtPhases', 'OtherWaitUp', 'HarmProbability', 'ImpactLines'):
        assert told[column] == plain[column]
    assert told.Tier == OTHER_PHASES


def test_relations_give_the_same_words():
    ad, aog = measures()
    con = duckdb.connect()
    told = rank([FAULT], WAIT_UP, approach_delay=con.from_df(ad), arrival_on_green=con.from_df(aog)).iloc[0]
    assert told.ImpactText.startswith('Delay per vehicle on other phases up 15 s')


def test_without_the_tables_or_with_too_few_arrivals_the_phase_wait_wording_stays():
    assert rank([FAULT], WAIT_UP, approach_delay=None, arrival_on_green=None).iloc[0].ImpactText == PHASE_WAIT_TEXT
    ad, aog = measures(vehicles=5)          # 20 an hour: under DELAY_MIN_VEHICLES_PER_HOUR
    assert rank([FAULT], WAIT_UP, approach_delay=ad, arrival_on_green=aog).iloc[0].ImpactText == PHASE_WAIT_TEXT


def test_delay_that_did_not_rise_keeps_the_phase_wait_wording():
    ad, aog = measures(delay_in_fault=10.0)
    assert rank([FAULT], WAIT_UP, approach_delay=ad, arrival_on_green=aog).iloc[0].ImpactText == PHASE_WAIT_TEXT


def test_hours_whose_advance_zones_were_faulting_are_not_used():
    ad, aog = measures()
    detectors = pd.DataFrame([('A', 5, 2, 'Presence'), ('A', 6, 4, 'Presence'), ('A', 9, 4, 'Advance')],
                             columns=['DeviceId', 'Detector', 'Phase', 'Function'])
    # Ph4's advance zone (detector 9) was stuck all through the fault: no delay from it then.
    faults = mi.fault_spans(pd.DataFrame({'DeviceId': ['A'], 'DetectorType': ['Vehicle'], 'Detector': [9],
                                          'Start': [pd.Timestamp('2026-10-27 07:00')], 'End': [pd.Timestamp('2026-10-27 13:00')],
                                          'Label': ['stuck on'], 'Confidence': [0.9]}), detectors)
    row = rank([FAULT], WAIT_UP, approach_delay=ad, arrival_on_green=aog, measure_faults=faults).iloc[0]
    assert row.ImpactText == PHASE_WAIT_TEXT


def test_never_for_a_phase_whose_advance_zone_is_the_faulty_detector():
    ad, aog = measures()
    detectors = [('A', 5, 2, 'Presence'), ('A', 6, 4, 'Presence'), ('A', 7, 4, 'Advance')]
    together = [FAULT, ('A', 7, '2026-10-27 08:00', '2026-10-27 12:00', 'stuck on', 0.8)]
    notes = []
    ranked = rank(together, WAIT_UP, detectors=detectors, approach_delay=ad, arrival_on_green=aog, delay_notes=notes)
    assert not ranked['ImpactText'].str.contains('Delay per vehicle').any()
    assert all(how != 'delay' for *_, how in notes)


def test_some_phases_without_delay_keep_their_phase_wait_after_the_delay():
    ad, aog = measures()
    detectors = [('A', 5, 1, 'Presence'), ('A', 6, 4, 'Presence'), ('A', 8, 2, 'Presence')]
    row = rank([('A', 5, '2026-10-27 08:00', '2026-10-27 12:00', 'stuck on', 0.8)],
               lambda ts, phase: (1, 4, 40.0) if in_fault(ts) else (1, 4, 20.0),
               detectors=detectors, approach_delay=ad, arrival_on_green=aog).iloc[0]
    assert row.ImpactText.startswith('Delay per vehicle on other phases up 15 s on average over 4 h: Ph4 10 to 25 s')
    assert row.ImpactText.endswith('; phase wait Ph2 20 to 40 s')


def test_tables_with_no_rows_for_the_harmed_signal_keep_the_phase_wait_wording():
    ad, aog = measures()
    other = rank([FAULT], WAIT_UP, approach_delay=ad.assign(DeviceId='Z'), arrival_on_green=aog.assign(DeviceId='Z')).iloc[0]
    empty = rank([FAULT], WAIT_UP, approach_delay=ad.head(0), arrival_on_green=aog.head(0)).iloc[0]
    assert other.ImpactText == PHASE_WAIT_TEXT and empty.ImpactText == PHASE_WAIT_TEXT
