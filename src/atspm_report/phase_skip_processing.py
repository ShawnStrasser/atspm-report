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

    phase_waits_sql = f"""
    WITH preempt_pairs AS (
        SELECT
            deviceid,
            parameter AS preempt_number,
            timestamp AS start_time,
            LEAD(timestamp) OVER (PARTITION BY deviceid, parameter ORDER BY timestamp, eventid) AS end_time
        FROM raw_data
        WHERE eventid IN (102, 104)
          AND timestamp IS NOT NULL
    ),
    valid_preempt_intervals AS (
        SELECT
            deviceid,
            preempt_number,
            start_time,
            end_time
        FROM preempt_pairs
        WHERE start_time IS NOT NULL
          AND end_time IS NOT NULL
          AND end_time > start_time
    ),
    phase_waits AS (
        SELECT
            deviceid,
            timestamp,
            eventid - 611 AS phase,
            parameter
        FROM raw_data
        WHERE eventid BETWEEN 612 AND 627
    ),
    max_cycles AS (
        SELECT
            deviceid,
            MAX(parameter) AS max_cycle_length
        FROM raw_data
        WHERE eventid = 132
        GROUP BY deviceid
    ),
    preempt_windows AS (
        SELECT
            v.deviceid,
            v.start_time AS window_start,
            v.end_time
            + (
                CASE
                    WHEN mc.max_cycle_length > 0 THEN
                        INTERVAL '1 second' * CAST(
                            CEIL(mc.max_cycle_length * {CYCLE_LENGTH_MULTIPLIER}) AS INT
                        )
                    ELSE
                        INTERVAL '1 second' * {FREE_SIGNAL_THRESHOLD}
                END
            ) AS window_end
        FROM valid_preempt_intervals v
        LEFT JOIN max_cycles mc
          ON v.deviceid = mc.deviceid
    )
    SELECT
        pw.deviceid,
        pw.timestamp,
        pw.phase,
        pw.parameter AS phase_wait_time,
        COALESCE(bool_or(
            pw.timestamp >= p.window_start
            AND pw.timestamp < p.window_end
        ), FALSE) AS preempt_flag,
        mc.max_cycle_length
    FROM phase_waits pw
    LEFT JOIN preempt_windows p
      ON pw.deviceid = p.deviceid
    LEFT JOIN max_cycles mc
      ON pw.deviceid = mc.deviceid
    GROUP BY pw.deviceid, pw.timestamp, pw.phase, pw.parameter, mc.max_cycle_length
    ORDER BY pw.deviceid, pw.timestamp
    """

    phase_waits_df = con.sql(phase_waits_sql).to_pandas()
    phase_waits_tbl = con.create_table('phase_waits', phase_waits_df, overwrite=True)

    alert_sql = f"""
    SELECT 
        deviceid, 
        phase,
        date_trunc('day', timestamp) AS date,
        max_cycle_length,
        max(phase_wait_time) AS max_wait_time,
        COUNT(*) AS total_skips
    FROM phase_waits
    WHERE preempt_flag = FALSE 
      AND phase_wait_time > (
          CASE
              WHEN COALESCE(max_cycle_length, 0) > 0 THEN
                  max_cycle_length * {CYCLE_LENGTH_MULTIPLIER}
              ELSE
                  {FREE_SIGNAL_THRESHOLD}
          END
      )
    GROUP BY ALL
    ORDER BY total_skips DESC
    """
    alert_rows_df = con.sql(alert_sql).to_pandas()

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
