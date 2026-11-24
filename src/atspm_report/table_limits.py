"""How much of each problem a report lists, and which ongoing problems it shows this time.

The limits count signals, not rows: a signal's rows stay together. A new alert a table leaves out is not filed in the
alert history (generator), so it comes up on a later run instead of being suppressed unseen. Ongoing alerts were told
before, so they are filed either way; the rotation only decides which of them a report shows, cycling through them so
the same ones do not fill the tables every week, and bringing the worse ones round more often.

Rotation: an ongoing alert's priority is its severity weight times the days since it was last shown in an Ongoing table
(or since its current run began, OngoingSince, when that is later or it was never shown). A signal takes the priority
of its highest alert in that table, and each region's report shows the signals with the highest priority, up to the
table's limit. The weight runs from 1 (the mildest of the table's alerts, or no severity value) to 3 (the worst), by
rank, so a worse alert comes round up to three times as often and the mildest still come round.
"""

from typing import Optional

import pandas as pd

from .atspm_detector_health import ALERT_TYPE as DETECTOR_HEALTH, FAULT_STATUSES, affects_operations
from .preempt_cascade import SECTION_EV
from .measure_checks import DELAY_RISE, PLATOON_TREND, SPLIT_FAIL_TREND
from . import timing_insights

STATE_COLUMNS = ['AlertType', 'Key', 'LastShown']

# The severity of each rotated alert type: (column the alerts already have, True when a higher value is worse). An alert
# with several rows takes its worst value (over its alert days only, where rows carry Alert).
SEVERITY = {
    'maxout': ('Percent MaxOut', True),               # share of the phase's cycles that maxed out
    'missing_data': ('MissingData', True),            # share of the day's records missing
    'phase_skips': ('TotalSkips', True),              # skips in the day
    'clearance_intervals': ('MaxAbsDelta', True),     # seconds the clearance was off at worst
    'general_phase_conflicts': ('DurationSeconds', True),   # longest conflict
    'overlap_conflicts': ('DurationSeconds', True),
    'preempts': ('Score', True),                      # the preempt check's own score, the table's order
    DETECTOR_HEALTH: ('Score', False),                # detector health score: lower is worse
    timing_insights.ALERT_TYPE: ('Severity', True),   # hours a day in the state (gap-outs a day for Premature Gap-Out)
    # The measure checks have no one measure of how bad: every alert weighs 1.
    DELAY_RISE: None,
    SPLIT_FAIL_TREND: None,
    PLATOON_TREND: None,
}

# The two Detector Health tables with the urgency tiers (labels of the rotation's tables).
HARM_TABLE, DATA_ONLY_TABLE = 'Affecting Operations', 'Data Only'
ONE_TABLE = 'Ongoing'    # the label of a type with one Ongoing table
WEIGHT_MIN, WEIGHT_MAX = 1.0, 3.0


def empty_state() -> pd.DataFrame:
    return pd.DataFrame({'AlertType': pd.Series(dtype=object), 'Key': pd.Series(dtype=object),
                         'LastShown': pd.Series(dtype='datetime64[ns]')})


def normalize_state(state: Optional[pd.DataFrame]) -> pd.DataFrame:
    """The last-shown state with its columns and types, empty for None."""
    if state is None or state.empty:
        return empty_state()
    missing = set(STATE_COLUMNS) - set(state.columns)
    if missing:
        raise ValueError(f"ongoing last-shown state lacks columns {sorted(missing)}")
    state = state[STATE_COLUMNS].copy()
    state['AlertType'] = state['AlertType'].astype(str)
    state['Key'] = state['Key'].astype(str)
    state['LastShown'] = pd.to_datetime(state['LastShown']).dt.tz_localize(None).dt.normalize().astype('datetime64[ns]')
    return state.drop_duplicates(['AlertType', 'Key'], keep='last').reset_index(drop=True)


def _id_text(value) -> str:
    if value is None or (not isinstance(value, (list, tuple)) and pd.isna(value)):
        return ''
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if hasattr(value, 'item'):     # numpy scalars
        return _id_text(value.item())
    return str(value)


def alert_keys(frame: pd.DataFrame, id_cols: list) -> pd.Series:
    """One text key per row from its id columns, the same for 2 and 2.0."""
    return pd.Series(['|'.join(_id_text(value) for value in values) for values in zip(*(frame[col] for col in id_cols))],
                     index=frame.index, dtype=object)


def table_of(alert_type: str, frame: pd.DataFrame) -> pd.Series:
    """The Ongoing table each row is in (None: a row no Ongoing table shows)."""
    if alert_type == DETECTOR_HEALTH:
        faults = frame['Status'].isin(FAULT_STATUSES)
        if 'TriageRank' in frame.columns:
            label = pd.Series(DATA_ONLY_TABLE, index=frame.index, dtype=object).where(~affects_operations(frame), HARM_TABLE)
        else:
            label = frame['DetectorType'].astype(object)
        return label.where(faults, None)
    if alert_type == 'preempts':
        classes = frame['Class'] if 'Class' in frame.columns else pd.Series(None, index=frame.index, dtype=object)
        return classes.astype(object).where(classes.notna(), SECTION_EV)
    if alert_type == timing_insights.ALERT_TYPE and 'Check' in frame.columns:
        return frame['Check'].astype(object)
    return pd.Series(ONE_TABLE, index=frame.index, dtype=object)


def _weights(alerts: pd.DataFrame, severity) -> pd.Series:
    """WEIGHT_MIN to WEIGHT_MAX by the rank of each alert's severity in its table; WEIGHT_MIN without one."""
    weight = pd.Series(WEIGHT_MIN, index=alerts.index)
    if severity is None or 'Severity' not in alerts.columns:
        return weight
    _, worse_high = severity
    for _, group in alerts.groupby('Table', sort=False):
        values = pd.to_numeric(group['Severity'], errors='coerce')
        known = values.dropna()
        if known.empty:
            continue
        if known.nunique() == 1:
            weight.loc[known.index] = WEIGHT_MAX
            continue
        rank = known.rank(method='average', ascending=worse_high)    # 1 = mildest
        weight.loc[known.index] = WEIGHT_MIN + (WEIGHT_MAX - WEIGHT_MIN) * (rank - 1) / (rank.max() - 1)
    return weight


def rotate_ongoing(ongoing: dict, signals: pd.DataFrame, state: Optional[pd.DataFrame], limits: dict, id_columns: dict,
                   today, hidden: Optional[dict] = None, retention_days: int = 728) -> tuple:
    """(ongoing alerts cut to what each region's report shows, updated last-shown state).

    ongoing: alert type -> its ongoing alerts (with OngoingSince). limits: alert type, or (alert type, table label),
    -> how many rows each region's table shows, whole signals in turn (the first always); a type without a limit is
    left as it is. id_columns: alert type ->
    the columns naming one alert. today: the report's date. hidden: {report: signals} its Signal Focus lists, which leave
    its tables, so they take no place in them. State rows not shown for retention_days are dropped.
    The All Regions report shows what the region reports listed, as its tables would.
    """
    state = normalize_state(state)
    today = pd.Timestamp(today).normalize()
    hidden = hidden or {}
    regions = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')['Region']
    shown_rows = []
    result = dict(ongoing)
    for alert_type, frame in ongoing.items():
        if alert_type not in limits or frame is None or frame.empty:
            continue
        id_cols = id_columns[alert_type]
        rows = frame.assign(DeviceId=frame['DeviceId'].astype(str))
        rows = rows.assign(Table=table_of(alert_type, rows), Key=alert_keys(rows, id_cols),
                           Region=rows['DeviceId'].map(regions))
        judged = rows[rows['Table'].notna() & rows['Region'].notna()]
        if judged.empty:
            result[alert_type] = frame[rows['Table'].isna().to_numpy()]
            continue
        severity = SEVERITY.get(alert_type)
        alert_days = judged[judged['Alert'] == 1] if 'Alert' in judged.columns else judged
        alert_days = alert_days if not alert_days.empty else judged
        aggregations = {'DeviceId': ('DeviceId', 'first'), 'Region': ('Region', 'first')}
        if 'OngoingSince' in judged.columns:
            aggregations['OngoingSince'] = ('OngoingSince', 'min')
        alerts = judged.groupby(['Table', 'Key'], as_index=False, sort=False).agg(**aggregations)
        if severity is not None and severity[0] in alert_days.columns:
            column, worse_high = severity
            worst = pd.to_numeric(alert_days[column], errors='coerce').groupby(
                [alert_days['Table'], alert_days['Key']]).agg('max' if worse_high else 'min').rename('Severity')
            alerts = alerts.join(worst, on=['Table', 'Key'])
        alerts['Weight'] = _weights(alerts, severity)
        last = state[state['AlertType'] == alert_type].set_index('Key')['LastShown']
        since = pd.to_datetime(alerts['OngoingSince']) if 'OngoingSince' in alerts.columns else pd.Series(pd.NaT, index=alerts.index)
        since = since.dt.tz_localize(None).dt.normalize() if since.dt.tz is not None else since.dt.normalize()
        # reindex, not map: pandas 3 cannot map through an empty datetime Series
        last = pd.to_datetime(last).astype('datetime64[ns]').groupby(level=0).max()
        shown_last = pd.Series(last.reindex(alerts['Key']).to_numpy(), index=alerts.index, dtype='datetime64[ns]')
        waiting_from = pd.concat([shown_last, since.astype('datetime64[ns]')], axis=1).max(axis=1)
        alerts['Days'] = (today - waiting_from).dt.days.fillna(1).clip(lower=1)
        alerts['Priority'] = alerts['Weight'] * alerts['Days']
        by_signal = alerts.groupby(['Table', 'DeviceId', 'Region'], as_index=False).agg(
            Priority=('Priority', 'max'), Weight=('Weight', 'max'), Rows=('Key', 'nunique'))
        by_signal = by_signal.sort_values(['Priority', 'Weight', 'DeviceId'], ascending=[False, False, True], kind='stable')
        chosen = []
        for (table, region), group in by_signal.groupby(['Table', 'Region'], sort=False):
            limit = limits.get((alert_type, table), limits[alert_type])
            if not limit or limit <= 0:
                chosen.append(group)
                continue
            # the limit counts the table's rows: signals in turn, each with all its rows, while they fit (the first
            # always does), so a signal with many rows does not make the table longer than before
            group = group[~group['DeviceId'].isin(hidden.get(region, ()))]
            fits = (group['Rows'].cumsum() <= limit).to_numpy(copy=True)   # pandas 3: to_numpy is read-only
            fits[:1] = True
            chosen.append(group[fits & (fits.cumprod() == 1)])
        chosen = pd.concat(chosen, ignore_index=True)[['Table', 'DeviceId']] if chosen else pd.DataFrame(columns=['Table', 'DeviceId'])
        picked = pd.MultiIndex.from_frame(chosen.astype(str))
        keep = rows['Table'].isna() | pd.MultiIndex.from_frame(rows[['Table', 'DeviceId']].astype(str)).isin(picked)
        result[alert_type] = frame[keep.to_numpy()]
        shown = rows[keep & rows['Table'].notna()]
        shown_rows.append(pd.DataFrame({'AlertType': alert_type, 'Key': shown['Key'].unique()}))
    if shown_rows:
        shown = pd.concat(shown_rows, ignore_index=True).assign(LastShown=today).astype({'LastShown': 'datetime64[ns]'})
        kept = state.merge(shown[['AlertType', 'Key']], on=['AlertType', 'Key'], how='left', indicator=True)
        state = pd.concat([kept[kept['_merge'] == 'left_only'][STATE_COLUMNS], shown], ignore_index=True)
    state = state[state['LastShown'] >= today - pd.Timedelta(days=retention_days)]
    return result, normalize_state(state)


def listed_phase_skips(alerts: pd.DataFrame, phase_skip_rows: pd.DataFrame, signals: pd.DataFrame, max_rows: int,
                       max_signals: int, min_total_skips=0, hidden: Optional[dict] = None) -> pd.DataFrame:
    """The new phase skip alerts the reports' tables list (prepare_phase_skip_alerts_table in each region's report and in
    All Regions, as report_generation calls it), with the alerts of the Signal Focus signals, which that section tells.
    Only these are filed in the history: an alert no table had room for comes up on a later run."""
    if alerts is None or alerts.empty or not max_signals or max_signals <= 0 or signals is None or signals.empty:
        return alerts
    if phase_skip_rows is None or phase_skip_rows.empty:
        return alerts
    from .table_generation import prepare_phase_skip_alerts_table
    hidden = hidden or {}
    info = signals.assign(DeviceId=signals['DeviceId'].astype(str))
    pairs = alerts[['DeviceId', 'Phase']].drop_duplicates()
    keep = set().union(*hidden.values()) if hidden else set()
    for report in ['All Regions'] + sorted(info['Region'].dropna().unique().tolist()):
        scope = info[~info['DeviceId'].isin(hidden.get(report, ()))]
        rows, _ = prepare_phase_skip_alerts_table(phase_skip_rows, scope, region=report, allowed_pairs=pairs,
                                                  min_total_skips=min_total_skips, max_rows=max_rows, max_signals=max_signals)
        if rows.empty:
            continue
        in_report = scope if report == 'All Regions' else scope[scope['Region'] == report]
        keep.update(in_report.loc[in_report['Name'].isin(rows['Signal']), 'DeviceId'])
    return alerts[alerts['DeviceId'].astype(str).isin(keep)]


def listed_by_table(alerts: pd.DataFrame, signals: pd.DataFrame, table, hidden: Optional[dict] = None) -> pd.DataFrame:
    """The new alerts some report's table lists, by its own table function: table(alerts, report signals, report) ->
    (rows with a Signal column, total), run for All Regions and each region over that report's signals (Signal Focus
    signals left out: that section tells them, so their alerts are kept). Only these are filed in the history, so an
    alert no table had room for comes up on a later run instead of being held back unseen."""
    if alerts is None or alerts.empty or signals is None or signals.empty:
        return alerts
    hidden = hidden or {}
    info = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId')
    keep = set().union(*hidden.values()) if hidden else set()
    for report in ['All Regions'] + sorted(info['Region'].dropna().unique().tolist()):
        scope = info if report == 'All Regions' else info[info['Region'] == report]
        scope = scope[~scope['DeviceId'].isin(hidden.get(report, ()))]
        rows, _ = table(alerts, scope, report)
        if rows is None or rows.empty or 'Signal' not in rows.columns:
            continue
        keep.update(scope.loc[scope['Name'].isin(rows['Signal']), 'DeviceId'])
    return alerts[alerts['DeviceId'].astype(str).isin(keep)]
