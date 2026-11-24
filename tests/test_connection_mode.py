"""Connection mode: inputs read from atspm's tables on a DuckDB connection, the state and the problems kept in the
report's own tables, and an analytics run that leaves the state alone."""
import os
import sys
from pathlib import Path

import duckdb
import matplotlib
import pandas as pd

matplotlib.use('Agg')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from atspm_report import ReportGenerator, connection  # noqa: E402

DATA = Path(__file__).parent / 'data'
CONFIG = {'historical_window_days': 21, 'alert_flagging_days': 7, 'figures_per_device': 1, 'verbosity': 0}


def _database():
    con = duckdb.connect()
    frames = {
        'terminations': pd.read_parquet(DATA / 'terminations.parquet'),
        'has_data': pd.read_parquet(DATA / 'has_data.parquet'),
        'actuations': pd.read_parquet(DATA / 'detector_counts.parquet'),
        'ped': pd.read_parquet(DATA / 'full_ped.parquet'),
    }
    for name, frame in frames.items():
        con.register('f', frame)
        con.execute(f'CREATE TABLE "{name}" AS SELECT * FROM f')
        con.unregister('f')
    signals = pd.read_parquet(DATA / 'signals.parquet')
    latest = pd.to_datetime(frames['has_data']['TimeStamp']).max().normalize()
    return con, signals, latest, frames


def test_the_inputs_are_read_from_atspms_tables():
    con, signals, latest, frames = _database()
    report_day = latest - pd.Timedelta(days=1)
    inputs = connection.load_inputs(con, report_day, signals=signals, config=CONFIG)
    assert set(inputs) >= {'signals', 'terminations', 'has_data', 'pedestrian', 'expected_data_end'}
    assert pd.to_datetime(inputs['has_data']['TimeStamp']).max() < latest       # cut at the report day's end
    # missing tables (no phase_wait, no timeline, ...) are left out, not errors
    assert 'phase_wait' not in inputs and 'timeline' not in inputs
    # with as_of, today's rows come too (only for the Today column)
    with_today = connection.load_inputs(con, report_day, signals=signals, config=CONFIG, as_of=latest + pd.Timedelta(hours=6))
    assert pd.to_datetime(with_today['has_data']['TimeStamp']).max() >= latest


def test_a_report_run_saves_its_state_and_an_analytics_run_does_not():
    con, signals, latest, frames = _database()
    report_day = (latest - pd.Timedelta(days=1)).date()
    analytics = connection.run(con, report_day, signals=signals, config=CONFIG, as_of=latest + pd.Timedelta(hours=6),
                               analytics_only=True)
    assert analytics['reports'] == {}
    assert not con.execute("SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'atspm_report' "
                           "AND table_name LIKE 'past_alerts_%'").fetchone()[0]
    problems = con.execute("SELECT Mode, ReportDate, Today FROM atspm_report.signal_problems").df()
    assert set(problems['Mode']) <= {'analytics'}

    report = connection.run(con, report_day, signals=signals, config=CONFIG)
    saved = con.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name = 'atspm_report'").df()['table_name']
    filed = {t for t in saved if t.startswith('past_alerts_')}
    assert filed == {f"past_alerts_{a}" for a, frame in report['updated_past_alerts'].items() if not frame.empty}
    assert set(con.execute("SELECT Mode FROM atspm_report.signal_problems").df()['Mode']) <= {'report'}
    # the same as generate() on the same frames
    direct = ReportGenerator(CONFIG).generate(**connection.load_inputs(con, report_day, signals=signals, config=CONFIG),
                                              report_date=report_day)
    for alert_type, frame in direct['alerts'].items():
        assert len(report['alerts'][alert_type]) == len(frame), alert_type

    # an analytics run after it changes none of the saved state
    before = {t: con.execute(f'SELECT * FROM atspm_report."{t}"').df() for t in filed}
    connection.run(con, report_day, signals=signals, config=CONFIG, as_of=latest + pd.Timedelta(hours=6), analytics_only=True)
    for table, frame in before.items():
        pd.testing.assert_frame_equal(con.execute(f'SELECT * FROM atspm_report."{table}"').df(), frame)
    # state round trip: the next report reads what the last one saved
    state = connection.load_state(con)
    assert {k for k, v in state['past_alerts'].items() if not v.empty} == {t[len('past_alerts_'):] for t in filed}
