import pandas as pd

from atspm_report.phase_conflict_processing import (
    OVERLAP_CONFLICT_PAIRS,
    STANDARD_CONFLICT_PAIRS,
    process_general_phase_conflicts,
    process_overlap_conflicts,
)
from atspm_report.table_generation import prepare_signal_conflicts_table


def _row(
    event_class,
    number,
    start,
    end,
    device_id='1',
    is_valid=True,
):
    return {
        'DeviceId': device_id,
        'StartTime': pd.Timestamp(start),
        'EndTime': pd.Timestamp(end),
        'Duration': (pd.Timestamp(end) - pd.Timestamp(start)).total_seconds(),
        'IsValid': is_valid,
        'EventClass': event_class,
        'EventValue': number,
    }


def test_standard_conflict_matrix_matches_dual_ring_sequence():
    assert set(STANDARD_CONFLICT_PAIRS) == {
        (1, 2), (1, 3), (1, 4), (1, 7), (1, 8),
        (2, 3), (2, 4), (2, 7), (2, 8),
        (3, 4), (3, 5), (3, 6),
        (4, 5), (4, 6),
        (5, 6), (5, 7), (5, 8),
        (6, 7), (6, 8),
        (7, 8),
    }


def test_overlap_conflict_matrix_uses_compatible_overlap_groups():
    assert set(OVERLAP_CONFLICT_PAIRS) == {
        (1, 3), (1, 4), (1, 7), (1, 8),
        (2, 3), (2, 4), (2, 7), (2, 8),
        (3, 5), (4, 5), (5, 7), (5, 8),
        (3, 6), (4, 6), (6, 7), (6, 8),
    }


def test_general_phase_conflicts_allow_touching_boundaries():
    timeline = pd.DataFrame([
        _row('Red', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:05'),
        _row('Green', 2, '2026-07-06 10:00:05', '2026-07-06 10:00:20'),
    ])
    conflicts = process_general_phase_conflicts(
        timeline,
        {'general_phase_conflicts_enabled': True},
    )
    assert conflicts.empty


def test_general_phase_conflicts_include_red_clearance_overlap():
    timeline = pd.DataFrame([
        _row('Red', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:05'),
        _row('Green', 2, '2026-07-06 10:00:04', '2026-07-06 10:00:20'),
        _row('Green', 5, '2026-07-06 10:00:01', '2026-07-06 10:00:10'),
    ])
    conflicts = process_general_phase_conflicts(
        timeline,
        {'general_phase_conflicts_enabled': True},
    )
    assert len(conflicts) == 1
    assert conflicts.iloc[0]['Movement1Number'] == 1
    assert conflicts.iloc[0]['Movement1Indication'] == 'Red'
    assert conflicts.iloc[0]['Movement2Number'] == 2
    assert conflicts.iloc[0]['DurationSeconds'] == 1.0


def test_general_phase_conflicts_exclude_intervals_touched_by_invalid_data():
    timeline = pd.DataFrame([
        _row('Green', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:10'),
        _row('Yellow', 2, '2026-07-06 10:00:05', '2026-07-06 10:00:08'),
        _row('Green', 6, '2026-07-06 10:00:06', '2026-07-06 10:00:07', is_valid=False),
    ])
    conflicts = process_general_phase_conflicts(
        timeline,
        {'general_phase_conflicts_enabled': True},
    )
    assert conflicts.empty


def test_general_phase_conflicts_exclude_configured_device_ids():
    timeline = pd.DataFrame([
        _row('Green', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:10', device_id='keep'),
        _row('Green', 2, '2026-07-06 10:00:01', '2026-07-06 10:00:09', device_id='keep'),
        _row('Green', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:10', device_id='skip'),
        _row('Green', 2, '2026-07-06 10:00:01', '2026-07-06 10:00:09', device_id='skip'),
    ])
    conflicts = process_general_phase_conflicts(
        timeline,
        {
            'general_phase_conflicts_enabled': True,
            'general_phase_conflict_excluded_device_ids': ['skip'],
        },
    )
    assert len(conflicts) == 1
    assert set(conflicts['DeviceId']) == {'keep'}


def test_overlap_conflicts_use_green_yellow_and_configured_overlaps_only():
    timeline = pd.DataFrame([
        _row('Overlap Yellow', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:05'),
        _row('Green', 2, '2026-07-06 10:00:01', '2026-07-06 10:00:04'),
        _row('Green', 3, '2026-07-06 10:00:02', '2026-07-06 10:00:04'),
        _row('Overlap Red', 1, '2026-07-06 10:01:00', '2026-07-06 10:01:05'),
        _row('Green', 4, '2026-07-06 10:01:02', '2026-07-06 10:01:04'),
        _row('Overlap Yellow', 2, '2026-07-06 10:02:00', '2026-07-06 10:02:05'),
        _row('Green', 1, '2026-07-06 10:02:02', '2026-07-06 10:02:04'),
    ])
    conflicts = process_overlap_conflicts(
        timeline,
        {
            'overlap_conflicts_enabled': True,
            'overlap_conflict_numbers': [1],
        },
    )
    assert len(conflicts) == 1
    assert conflicts.iloc[0]['Movement1Type'] == 'Overlap'
    assert conflicts.iloc[0]['Movement1Number'] == 1
    assert conflicts.iloc[0]['Movement1Indication'] == 'Overlap Yellow'
    assert conflicts.iloc[0]['Movement2Type'] == 'Phase'
    assert conflicts.iloc[0]['Movement2Number'] == 3


def test_overlap_conflicts_ignore_stale_overlap_yellow():
    """A dropped overlap termination event leaves a multi-minute overlap yellow
    hanging over a legitimately-green conflicting phase. That is missing data,
    not a conflict."""
    timeline = pd.DataFrame([
        _row('Overlap Yellow', 1, '2026-07-06 10:00:00', '2026-07-06 10:02:06'),
        _row('Green', 3, '2026-07-06 10:01:30', '2026-07-06 10:01:38'),
    ])
    conflicts = process_overlap_conflicts(
        timeline,
        {
            'overlap_conflicts_enabled': True,
            'overlap_conflict_numbers': [1],
        },
    )
    assert conflicts.empty


def test_overlap_conflicts_keep_plausible_overlap_yellow():
    """An overlap yellow inside the plausible range still reports."""
    timeline = pd.DataFrame([
        _row('Overlap Yellow', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:05'),
        _row('Green', 3, '2026-07-06 10:00:01', '2026-07-06 10:00:04'),
    ])
    conflicts = process_overlap_conflicts(
        timeline,
        {
            'overlap_conflicts_enabled': True,
            'overlap_conflict_numbers': [1],
        },
    )
    assert len(conflicts) == 1
    assert conflicts.iloc[0]['Movement1Indication'] == 'Overlap Yellow'


def test_overlap_conflicts_allow_same_compatible_group_movements():
    timeline = pd.DataFrame([
        _row('Overlap Green', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:10'),
        _row('Green', 2, '2026-07-06 10:00:01', '2026-07-06 10:00:09'),
        _row('Overlap Green', 2, '2026-07-06 10:00:02', '2026-07-06 10:00:08'),
        _row('Yellow', 5, '2026-07-06 10:00:03', '2026-07-06 10:00:07'),
        _row('Overlap Yellow', 6, '2026-07-06 10:00:04', '2026-07-06 10:00:06'),
        _row('Overlap Yellow', 7, '2026-07-06 10:01:00', '2026-07-06 10:01:10'),
        _row('Overlap Green', 8, '2026-07-06 10:01:01', '2026-07-06 10:01:09'),
    ])
    conflicts = process_overlap_conflicts(
        timeline,
        {
            'overlap_conflicts_enabled': True,
            'overlap_conflict_numbers': [1, 2, 6, 7, 8],
        },
    )
    assert conflicts.empty


def test_overlap_conflicts_exclude_configured_device_ids():
    timeline = pd.DataFrame([
        _row('Overlap Green', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:10', device_id='keep'),
        _row('Green', 3, '2026-07-06 10:00:01', '2026-07-06 10:00:09', device_id='keep'),
        _row('Overlap Green', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:10', device_id='skip'),
        _row('Green', 3, '2026-07-06 10:00:01', '2026-07-06 10:00:09', device_id='skip'),
    ])
    conflicts = process_overlap_conflicts(
        timeline,
        {
            'overlap_conflicts_enabled': True,
            'overlap_conflict_numbers': [1],
            'overlap_conflict_excluded_device_ids': ['skip'],
        },
    )
    assert len(conflicts) == 1
    assert set(conflicts['DeviceId']) == {'keep'}


def test_overlap_to_overlap_conflict_is_not_duplicated():
    timeline = pd.DataFrame([
        _row('Overlap Green', 1, '2026-07-06 10:00:00', '2026-07-06 10:00:10'),
        _row('Overlap Green', 3, '2026-07-06 10:00:05', '2026-07-06 10:00:08'),
    ])
    conflicts = process_overlap_conflicts(
        timeline,
        {
            'overlap_conflicts_enabled': True,
            'overlap_conflict_numbers': [1, 3],
        },
    )
    assert len(conflicts) == 1
    assert conflicts.iloc[0]['Movement1Number'] == 1
    assert conflicts.iloc[0]['Movement2Number'] == 3


def test_prepare_signal_conflicts_table_groups_by_signal_and_pair():
    conflicts = pd.DataFrame([
        {
            'DeviceId': '2',
            'Date': pd.Timestamp('2026-07-06'),
            'ConflictStart': pd.Timestamp('2026-07-06 10:00:00'),
            'ConflictEnd': pd.Timestamp('2026-07-06 10:00:01'),
            'DurationSeconds': 1.0,
            'Movement1Type': 'Phase',
            'Movement1Number': 1,
            'Movement1Indication': 'Green',
            'Movement2Type': 'Phase',
            'Movement2Number': 2,
            'Movement2Indication': 'Green',
        },
        {
            'DeviceId': '2',
            'Date': pd.Timestamp('2026-07-06'),
            'ConflictStart': pd.Timestamp('2026-07-06 10:05:00'),
            'ConflictEnd': pd.Timestamp('2026-07-06 10:05:05'),
            'DurationSeconds': 5.0,
            'Movement1Type': 'Phase',
            'Movement1Number': 1,
            'Movement1Indication': 'Yellow',
            'Movement2Type': 'Phase',
            'Movement2Number': 2,
            'Movement2Indication': 'Green',
        },
        {
            'DeviceId': '1',
            'Date': pd.Timestamp('2026-07-06'),
            'ConflictStart': pd.Timestamp('2026-07-06 09:00:00'),
            'ConflictEnd': pd.Timestamp('2026-07-06 09:00:03'),
            'DurationSeconds': 3.0,
            'Movement1Type': 'Overlap',
            'Movement1Number': 1,
            'Movement1Indication': 'Overlap Green',
            'Movement2Type': 'Phase',
            'Movement2Number': 3,
            'Movement2Indication': 'Green',
        },
    ])
    signals = pd.DataFrame([
        {'DeviceId': '1', 'Name': 'A Signal', 'Region': 'Region 1'},
        {'DeviceId': '2', 'Name': 'B Signal', 'Region': 'Region 1'},
    ])

    table, total = prepare_signal_conflicts_table(conflicts, signals, max_rows=10)

    assert total == 2
    assert list(table.columns) == ['Signal', 'Pair', 'Conflicts', 'Max Event', 'Max Duration']
    assert table['Signal'].tolist() == ['A Signal', 'B Signal']
    assert table['Pair'].tolist() == ['Ovlp 1 / Ph 3', 'Ph 1 / Ph 2']
    assert table['Conflicts'].tolist() == [1, 2]
    assert table['Max Duration'].tolist() == ['3.0s', '5.0s']
    assert table.iloc[1]['Max Event'] == '7/6/26 10:05:00.0 AM'
