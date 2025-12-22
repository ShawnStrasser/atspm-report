import ibis
import pandas as pd

PHASE_SKIP_PHASE_WAITS_COLUMNS = [
    'DeviceId', 'Timestamp', 'Phase', 'PhaseWaitTime', 'PreemptFlag', 'MaxCycleLength'
]

PHASE_SKIP_ALERT_COLUMNS = [
    'DeviceId', 'Phase', 'Date', 'MaxCycleLength', 'MaxWaitTime', 'TotalSkips'
]
CYCLE_LENGTH_MULTIPLIER = 1.5
FREE_SIGNAL_THRESHOLD = 180


def transform_phase_skip_raw_data(raw_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform staged device events into phase wait and phase skip alert tables.

    Args:
        raw_data: DataFrame containing columns deviceid, timestamp, eventid, parameter.

    Returns:
        Tuple of (phase_waits_df, alert_rows_df) with normalized column names.
    """
    if raw_data is None or raw_data.empty:
        return (
            pd.DataFrame(columns=PHASE_SKIP_PHASE_WAITS_COLUMNS),
            pd.DataFrame(columns=PHASE_SKIP_ALERT_COLUMNS)
        )

    ibis.options.interactive = True
    con = ibis.duckdb.connect()
    raw_data_tbl = con.create_table('raw_data', raw_data, overwrite=True)

    # 1. Preempt Pairs
    preempt_events = raw_data_tbl.filter(
        raw_data_tbl.eventid.isin([102, 104]) & raw_data_tbl.timestamp.notnull()
    )

    w = ibis.window(
        group_by=[preempt_events.deviceid, preempt_events.parameter],
        order_by=[preempt_events.timestamp, preempt_events.eventid]
    )

    preempt_pairs = preempt_events.select(
        deviceid=preempt_events.deviceid,
        preempt_number=preempt_events.parameter,
        start_time=preempt_events.timestamp,
        end_time=preempt_events.timestamp.lead().over(w)
    )

    # 2. Valid Preempt Intervals
    valid_preempt_intervals = preempt_pairs.filter(
        preempt_pairs.start_time.notnull() &
        preempt_pairs.end_time.notnull() &
        (preempt_pairs.end_time > preempt_pairs.start_time)
    )

    # 3. Phase Waits
    phase_waits = raw_data_tbl.filter(
        (raw_data_tbl.eventid >= 612) & (raw_data_tbl.eventid <= 627)
    ).select(
        deviceid=raw_data_tbl.deviceid,
        timestamp=raw_data_tbl.timestamp,
        phase=raw_data_tbl.eventid - 611,
        parameter=raw_data_tbl.parameter
    )

    # 4. Max Cycles
    max_cycles = raw_data_tbl.filter(
        raw_data_tbl.eventid == 132
    ).group_by(raw_data_tbl.deviceid).aggregate(
        max_cycle_length=raw_data_tbl.parameter.max()
    )

    # 5. Preempt Windows
    joined_preempt = valid_preempt_intervals.left_join(
        max_cycles, valid_preempt_intervals.deviceid == max_cycles.deviceid
    )

    max_cycle_len = joined_preempt.max_cycle_length
    cycle_seconds = (max_cycle_len * CYCLE_LENGTH_MULTIPLIER).ceil().cast('int')

    added_seconds = ibis.ifelse(
        max_cycle_len > 0,
        cycle_seconds,
        FREE_SIGNAL_THRESHOLD
    )

    window_end = joined_preempt.end_time + (added_seconds * ibis.interval(seconds=1))

    preempt_windows = joined_preempt.select(
        deviceid=joined_preempt.deviceid,
        window_start=joined_preempt.start_time,
        window_end=window_end
    )

    # 6. Final Join and Aggregation
    pw = phase_waits.alias('pw')
    p = preempt_windows.alias('p')
    mc = max_cycles.alias('mc')

    joined = pw.left_join(p, pw.deviceid == p.deviceid) \
               .left_join(mc, pw.deviceid == mc.deviceid) \
               .select(
                   deviceid=pw.deviceid,
                   timestamp=pw.timestamp,
                   phase=pw.phase,
                   parameter=pw.parameter,
                   window_start=p.window_start,
                   window_end=p.window_end,
                   max_cycle_length=mc.max_cycle_length
               )

    is_preempted = (joined.timestamp >= joined.window_start) & (joined.timestamp < joined.window_end)

    result = joined.group_by([
        joined.deviceid,
        joined.timestamp,
        joined.phase,
        joined.parameter,
        joined.max_cycle_length
    ]).aggregate(
        preempt_flag=ibis.coalesce(is_preempted.any(), False)
    ).order_by([joined.deviceid, joined.timestamp])

    result = result.mutate(phase_wait_time=result.parameter).drop('parameter')

    phase_waits_df = result.to_pandas()
    phase_waits_tbl = con.create_table('phase_waits', phase_waits_df, overwrite=True)

    # Alert Generation using Ibis
    threshold = ibis.ifelse(
        ibis.coalesce(phase_waits_tbl.max_cycle_length, 0) > 0,
        phase_waits_tbl.max_cycle_length * CYCLE_LENGTH_MULTIPLIER,
        FREE_SIGNAL_THRESHOLD
    )

    alerts = phase_waits_tbl.filter(
        (phase_waits_tbl.preempt_flag == False) &
        (phase_waits_tbl.phase_wait_time > threshold)
    )

    alert_rows_df = alerts.group_by([
        alerts.deviceid,
        alerts.phase,
        alerts.timestamp.truncate('D').name('date'),
        alerts.max_cycle_length
    ]).aggregate(
        max_wait_time=alerts.phase_wait_time.max(),
        total_skips=alerts.count()
    ).order_by(ibis.desc('total_skips')).to_pandas()

    con = None

    phase_waits_df = phase_waits_df.rename(columns={
        'deviceid': 'DeviceId',
        'timestamp': 'Timestamp',
        'phase': 'Phase',
        'phase_wait_time': 'PhaseWaitTime',
        'preempt_flag': 'PreemptFlag',
        'max_cycle_length': 'MaxCycleLength'
    })
    if not phase_waits_df.empty:
        phase_waits_df['Timestamp'] = pd.to_datetime(phase_waits_df['Timestamp'])
        phase_waits_df['DeviceId'] = phase_waits_df['DeviceId'].astype(str)
        phase_waits_df['Phase'] = phase_waits_df['Phase'].astype(int)

    alert_rows_df = alert_rows_df.rename(columns={
        'deviceid': 'DeviceId',
        'phase': 'Phase',
        'date': 'Date',
        'max_cycle_length': 'MaxCycleLength',
        'max_wait_time': 'MaxWaitTime',
        'total_skips': 'TotalSkips'
    })
    if not alert_rows_df.empty:
        alert_rows_df['DeviceId'] = alert_rows_df['DeviceId'].astype(str)
        alert_rows_df['Phase'] = alert_rows_df['Phase'].astype(int)
        alert_rows_df['Date'] = pd.to_datetime(alert_rows_df['Date']).dt.normalize()

    return (
        phase_waits_df.reindex(columns=PHASE_SKIP_PHASE_WAITS_COLUMNS),
        alert_rows_df.reindex(columns=PHASE_SKIP_ALERT_COLUMNS)
    )
