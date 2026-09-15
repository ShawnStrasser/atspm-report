import pandas as pd

from atspm_report.overlap_dual_indication_processing import (
    process_overlap_dual_indications,
)


def _timeline():
    return pd.DataFrame([
        {
            'DeviceId': '1001',
            'StartTime': '2026-07-05 08:00:00',
            'EndTime': '2026-07-05 08:00:20',
            'IsValid': True,
            'EventClass': 'Green',
            'EventValue': 3,
        },
        {
            'DeviceId': '1001',
            'StartTime': '2026-07-05 08:00:15',
            'EndTime': '2026-07-05 08:00:18',
            'IsValid': True,
            'EventClass': 'Overlap Yellow',
            'EventValue': 3,
        },
        {
            'DeviceId': '1001',
            'StartTime': '2026-07-05 08:00:18',
            'EndTime': '2026-07-05 08:00:25',
            'IsValid': False,
            'EventClass': 'Overlap Red',
            'EventValue': 3,
        },
        {
            'DeviceId': '1001',
            'StartTime': '2026-07-05 08:00:15',
            'EndTime': '2026-07-05 08:00:18',
            'IsValid': True,
            'EventClass': 'Overlap Yellow',
            'EventValue': 5,
        },
        {
            'DeviceId': '1001',
            'StartTime': '2026-07-05 09:00:00',
            'EndTime': '2026-07-05 09:00:20',
            'IsValid': False,
            'EventClass': 'Green',
            'EventValue': 7,
        },
        {
            'DeviceId': '1001',
            'StartTime': '2026-07-05 09:00:15',
            'EndTime': '2026-07-05 09:00:18',
            'IsValid': True,
            'EventClass': 'Overlap Yellow',
            'EventValue': 7,
        },
        {
            'DeviceId': '1001',
            'StartTime': '2026-07-05 10:00:00',
            'EndTime': '2026-07-05 10:00:20',
            'IsValid': True,
            'EventClass': 'Green',
            'EventValue': 1,
        },
        {
            'DeviceId': '1001',
            'StartTime': '2026-07-05 10:00:15',
            'EndTime': '2026-07-05 10:00:18',
            'IsValid': True,
            'EventClass': 'Overlap Red',
            'EventValue': 1,
        },
        {
            'DeviceId': '2002',
            'StartTime': '2026-07-05 10:00:00',
            'EndTime': '2026-07-05 10:00:20',
            'IsValid': False,
            'EventClass': 'Red',
            'EventValue': 2,
        },
    ])


def test_overlap_dual_indications_are_disabled_by_default():
    conflicts = process_overlap_dual_indications(_timeline())
    assert conflicts.empty


def test_all_events_overlapping_invalid_intervals_are_excluded():
    conflicts = process_overlap_dual_indications(
        _timeline(),
        {
            'overlap_dual_indications_enabled': True,
            'overlap_dual_indication_phases': [1, 3, 7],
        },
    )

    assert len(conflicts) == 1
    assert conflicts.iloc[0]['Phase'] == 1
    assert conflicts.iloc[0]['OverlapIndication'] == 'Overlap Red'
    assert conflicts.iloc[0]['DurationSeconds'] == 3.0


def test_overlap_dual_indications_require_configured_phase():
    conflicts = process_overlap_dual_indications(
        _timeline(),
        {
            'overlap_dual_indications_enabled': True,
            'overlap_dual_indication_phases': [5],
        },
    )
    assert conflicts.empty
