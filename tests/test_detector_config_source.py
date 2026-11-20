"""Which detector configuration the report uses: the Signal Analysis Tool workbook's, except each detector where the AI
model confidently disagrees, and the whole signal at more than 3 of those (never at a signal with a rejection);
detectors missing from the workbook come from the model."""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pytest  # noqa: E402

h = pytest.importorskip('odot_detector_health')   # ODOT's own script, not in the package

DEV = 'aaaa-1'
PROBS = ['PAdvance', 'PPresence', 'PCount', 'PYellowRed', 'POther', 'PMid', 'PBike']


def _workbook(rows):
    """rows: (detector, phase, function)."""
    return pd.DataFrame([{'DeviceId': DEV, 'Phase': str(p), 'Function': f, 'Parameter': str(d), 'Overlap': ''}
                         for d, p, f in rows]).astype(str)


def _model(rows, prob=0.97):
    """rows: (detector, phase, function)."""
    out = []
    for d, p, f in rows:
        row = {'TSSU': '01001', 'DeviceId': DEV, 'Detector': str(d), 'Phase': str(p), 'PhaseProb': str(prob),
               'Function': f, 'FunctionProb': str(prob), 'Lanes': '1,2'}
        row.update({c: '0.01' for c in PROBS})
        out.append(row)
    return pd.DataFrame(out)


def _feedback(rows=()):
    """rows: (detector, model phase, model function, verdict)."""
    return pd.DataFrame([{'DeviceId': DEV, 'Detector': str(d), 'ModelPhase': str(p), 'ModelFunction': f, 'Verdict': v}
                         for d, p, f, v in rows],
                        columns=['DeviceId', 'Detector', 'ModelPhase', 'ModelFunction', 'Verdict'])


def _signal(disagreements: int, total: int = 8):
    """A workbook config of `total` stop-bar Count zones on phase 2; the model calls the first `disagreements` of them
    Presence on phase 6 and agrees on the rest."""
    book = [(d, 2, 'Count') for d in range(1, total + 1)]
    model = [(d, 6, 'Presence') if d <= disagreements else (d, 2, 'Count') for d in range(1, total + 1)]
    return _workbook(book), _model(model)


def _row(table, detector):
    return table[table['Detector'] == detector].iloc[0]


def test_workbook_config_is_used_where_the_model_agrees():
    book, model = _signal(disagreements=0)
    out = h.choose_detector_config(book, model, _feedback())
    assert set(out['source']) == {'workbook'}
    assert (_row(out, 1)['phase'], _row(out, 1)['function']) == (2.0, 'Count')
    assert _row(out, 1)['lanes'] == 2.0 and _row(out, 1)['p_presence'] == 0.01   # lanes and probabilities: the model's


def test_one_confident_disagreement_switches_that_detector_only():
    book, model = _signal(disagreements=1)
    out = h.choose_detector_config(book, model, _feedback())
    assert (_row(out, 1)['phase'], _row(out, 1)['function'], _row(out, 1)['source']) == (6.0, 'Presence', 'model')
    assert set(out.loc[out['Detector'] != 1, 'source']) == {'workbook'}


def test_three_confident_disagreements_switch_detector_by_detector():
    book, model = _signal(disagreements=3)
    model.loc[model['Detector'] == '8', ['Phase', 'Function', 'PhaseProb', 'FunctionProb']] = ['4', 'Advance', '0.6', '0.6']
    out = h.choose_detector_config(book, model, _feedback())
    assert list(out.loc[out['source'] == 'model', 'Detector']) == [1, 2, 3]
    # an unconfident model answer does not replace the workbook's at a signal with 3 or fewer
    assert (_row(out, 8)['phase'], _row(out, 8)['function'], _row(out, 8)['source']) == (2.0, 'Count', 'workbook')


def test_more_than_three_confident_disagreements_switch_the_whole_signal():
    book, model = _signal(disagreements=4)
    model.loc[model['Detector'] == '8', ['Phase', 'Function', 'PhaseProb', 'FunctionProb']] = ['4', 'Advance', '0.6', '0.6']
    out = h.choose_detector_config(book, model, _feedback())
    assert set(out['source']) == {'model'}
    assert (_row(out, 4)['phase'], _row(out, 4)['function']) == (6.0, 'Presence')
    assert (_row(out, 8)['phase'], _row(out, 8)['function']) == (4.0, 'Advance')


def test_confidence_limit_is_inclusive():
    book, model = _signal(disagreements=2)
    model.loc[model['Detector'] == '1', ['PhaseProb', 'FunctionProb']] = [str(h.CONFIDENT_PROB), '0.99']
    model.loc[model['Detector'] == '2', ['PhaseProb', 'FunctionProb']] = ['0.99', '0.899']   # the lower one counts
    out = h.choose_detector_config(book, model, _feedback())
    assert _row(out, 1)['source'] == 'model'
    assert (_row(out, 2)['function'], _row(out, 2)['source']) == ('Count', 'workbook')


def test_unconfident_disagreements_do_not_count():
    book, model = _signal(disagreements=6)
    model[['PhaseProb', 'FunctionProb']] = '0.85'      # Medium confidence only
    out = h.choose_detector_config(book, model, _feedback())
    assert set(out['source']) == {'workbook'}


def test_disagreement_outside_the_reviewed_functions_does_not_count():
    book = _workbook([(1, 2, 'Mid'), (2, 2, 'Other')])
    model = _model([(1, 2, 'Other'), (2, 2, 'Mid')])
    out = h.choose_detector_config(book, model, _feedback())
    assert set(out['source']) == {'workbook'} and list(out['function']) == ['Mid', 'Other']


def test_a_rejection_keeps_the_whole_signal_on_the_workbook():
    book, model = _signal(disagreements=6)
    out = h.choose_detector_config(book, model, _feedback([(3, 6, 'Presence', 'rejected')]))
    assert set(out['source']) == {'workbook'}
    assert _row(out, 1)['function'] == 'Count'


def test_any_rejection_at_the_signal_counts():
    book, model = _signal(disagreements=6)
    out = h.choose_detector_config(book, model, _feedback([(3, 4, 'Advance', 'rejected')]))
    assert set(out['source']) == {'workbook'}


def test_an_accepted_suggestion_is_not_a_rejection():
    book, model = _signal(disagreements=6)
    out = h.choose_detector_config(book, model, _feedback([(3, 6, 'Presence', 'accepted')]))
    assert set(out['source']) == {'model'}


def test_a_rejected_detector_takes_nothing_from_the_model():
    book = _workbook([(1, 2, 'Count'), (2, 4, 'Advance_Presence'), (3, '', 'Presence')])
    model = _model([(1, 6, 'Presence'), (2, 4, 'Mid'), (3, 8, 'Presence')])
    out = h.choose_detector_config(book, model, _feedback([(2, 4, 'Mid', 'rejected'), (3, 8, 'Presence', 'rejected')]))
    assert set(out['source']) == {'workbook'}
    assert (_row(out, 1)['phase'], _row(out, 1)['function']) == (2.0, 'Count')
    assert _row(out, 2)['function'] == 'Other'         # the workbook type is not one health knows; the model's is not used
    assert pd.isna(_row(out, 3)['phase']) and _row(out, 3)['function'] == 'Presence'


def test_missing_detectors_are_filled_from_the_model():
    book = _workbook([(1, 2, 'Count'), (2, 4, 'Advance_Presence'), (3, '', 'Presence')])
    model = _model([(1, 2, 'Count'), (2, 4, 'Mid'), (3, 8, 'Presence'), (9, 6, 'Advance'), (10, '', '')])
    out = h.choose_detector_config(book, model, _feedback())
    assert (_row(out, 9)['phase'], _row(out, 9)['function'], _row(out, 9)['source']) == (6.0, 'Advance', 'model fill')
    assert _row(out, 2)['function'] == 'Mid'           # a workbook type health does not know takes the model's
    assert _row(out, 3)['phase'] == 8.0                # a workbook row with no phase takes the model's
    assert pd.isna(_row(out, 10)['phase']) and _row(out, 10)['function'] == 'Other'   # the model has no answer
    assert len(out) == 5


def test_load_detector_config_reads_the_files(tmp_path, capsys):
    book, model = _signal(disagreements=2)
    paths = {name: tmp_path / f'{name}.csv' for name in ('book', 'model', 'feedback')}
    book.to_csv(paths['book'], index=False)
    model.to_csv(paths['model'], index=False)
    _feedback().to_csv(paths['feedback'], index=False)
    h._config_summary_printed = False
    out = h.load_detector_config(paths['book'], paths['model'], paths['feedback'])
    assert list(out.columns) == ['DeviceId', 'Detector', 'phase', 'function', 'lanes', *h.PROBABILITY_COLUMNS.values()]
    assert '2 detectors at 1 of them switched to the AI model' in capsys.readouterr().out
