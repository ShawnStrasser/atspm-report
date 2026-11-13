"""Signal Focus overview: one chart that lines up what failed with what it did, on one time axis.

Top to bottom, only the parts a signal's table names:
- Faults: one row per group of detectors or ped buttons with the same fault times (when every detector was down at once, one
  row for them all), stuck on in dark red, sending nothing in light red, any other fault in orange (a fault with no time of its
  own too, across its whole row).
- Percent max out (up to 100%), average wait and greens of the phases the table names, hour by hour as a line through the middle
  of each hour, over their normal range, smoothed. A long stretch with no wait measured while the signal sent data (no calls
  logged, as when every detector is down) says so.
- Skips and preempt calls, as ticks; a row with none in these days says so, with the last one before them when known.
The fault times are shaded lightly behind the measures, so a rise lines up with its cause, and hours the signal sent no data are
hatched in every part. It covers the same days as the signal's detector charts.

The normal range of an hour is the middle NORMAL_SHARE of the same hour on the BASELINE_DAYS before the chart (weekdays and
weekends apart), leaving out first the days least like the others: a fault on one of them would widen it.
"""

import math

import matplotlib.dates as mdates
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter

from .duckdb_utils import TableLike, run_sql
from .health_story import HELD_LABELS, NO_TIMES, SILENT_LABELS

BASELINE_DAYS = 21
NORMAL_SHARE = 0.8
MIN_NORMAL_DAYS = 3
# A day this many times further from the usual day than the median day is is left out of the normal range.
UNUSUAL_DAY = 2.5
# A day needs this many hours of data to judge it.
MIN_DAY_HOURS = 12
# Hours with at least this share of their records missing are drawn as no data.
NO_DATA_SHARE = 0.5
# An hour needs this many cycles for its share of cycles with a walk.
MIN_CYCLES = 4
# Rows of the fault strip; more groups than this share one last row.
MAX_FAULT_ROWS = 6
# A row names its detectors up to this many, then counts them.
MAX_NAMED = 4
# Detectors share a row when their fault times overlap by this share of their time together.
SIMILAR = 0.8
# A piece of a fault this short left beside the time every detector was down is part of it, not a row of its own.
MIN_LEFTOVER = pd.Timedelta(minutes=30)

# WHOLE: a finding judged over all the days shown, with no time of its own (no daily pattern, misses vehicles at night): drawn
# and named as any other fault in its row, but not shaded behind the measures, which it would cover from end to end.
HELD, SILENT, OTHER, WHOLE = 'held', 'silent', 'other', 'whole'
STOPPED_LABELS = SILENT_LABELS + ('sudden stop',)
STATE_ORDER = (WHOLE, OTHER, SILENT, HELD)   # drawn in this order, the worst on top
STATE_COLOR = {HELD: '#B3261E', SILENT: '#F2A7A2', OTHER: '#F2B045', WHOLE: '#F2B045'}
STATE_SHADE = {HELD: '#F3C9C6', SILENT: '#FCE9E7', OTHER: '#FDEFD6'}
STATE_WORDS = {HELD: 'Stuck on', SILENT: 'Sending nothing', OTHER: 'Other fault', WHOLE: 'Other fault'}
LEGEND_STATE = {WHOLE: OTHER}   # the legend entry a state shares
BAND_COLOR, LINE_COLOR, MUTED, GRID = '#D6D6D6', '#262626', '#666666', '#E6E6E6'
NO_DATA_EDGE = '#B5B5B5'
# Behind a note written over a part, so the shading and hatching under it do not hide it.
NOTE_BOX = {'facecolor': 'white', 'edgecolor': 'none', 'alpha': 0.85, 'boxstyle': 'round,pad=0.25'}
# The normal range is smoothed over this many hours, centred.
SMOOTH_HOURS = 3
# A panel names a stretch of this many hours or more inside the data with nothing measured (its 'blank' text).
MIN_BLANK_HOURS = 3

MAX_OUT, WAIT, GREEN = 'max out', 'wait', 'green'
PANEL_TITLES = {MAX_OUT: 'Percent Max Out', WAIT: 'Average Wait', GREEN: 'Greens per Hour'}
# What a wait panel says over a stretch with no wait: wait runs from a call to its green.
NO_WAIT = 'no wait measured: no calls logged'


# ---------------------------------------------------------------- data

def _ids(devices) -> str:
    return ', '.join("'" + str(device).replace("'", "''") + "'" for device in devices)


def hourly_terminations(terminations: TableLike, devices, start, end) -> pd.DataFrame:
    """Greens per phase and hour, and how many ran to max (MaxOut, ForceOff as the max out check counts them): DeviceId, Phase,
    TimeStamp, MaxOut, Total."""
    if terminations is None or not len(devices):
        return pd.DataFrame(columns=['DeviceId', 'Phase', 'TimeStamp', 'MaxOut', 'Total'])
    return run_sql(f"""
        SELECT CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(Phase AS INTEGER) AS Phase, DATE_TRUNC('hour', TimeStamp) AS TimeStamp,
               CAST(SUM(CASE WHEN PerformanceMeasure IN ('MaxOut', 'ForceOff') THEN Total ELSE 0 END) AS DOUBLE) AS MaxOut,
               CAST(SUM(Total) AS DOUBLE) AS Total
        FROM t
        WHERE CAST(DeviceId AS VARCHAR) IN ({_ids(devices)})
          AND TimeStamp >= TIMESTAMP '{pd.Timestamp(start)}' AND TimeStamp < TIMESTAMP '{pd.Timestamp(end)}'
        GROUP BY ALL
    """, t=terminations)


def hourly_cycles(term_hourly: pd.DataFrame, start, end) -> pd.DataFrame:
    """Cycles per hour of each signal: the greens of its busiest phase over [start, end), the one on recall that every cycle
    serves, which also counts the cycles of a signal running free. DeviceId, TimeStamp, Cycles."""
    if term_hourly is None or term_hourly.empty:
        return pd.DataFrame(columns=['DeviceId', 'TimeStamp', 'Cycles'])
    stamps = pd.to_datetime(term_hourly['TimeStamp'])
    rows = term_hourly[(stamps >= pd.Timestamp(start)) & (stamps < pd.Timestamp(end))]
    busiest = rows.groupby(['DeviceId', 'Phase'])['Total'].sum().reset_index().sort_values(['DeviceId', 'Total', 'Phase'], ascending=[True, False, True])
    busiest = busiest.drop_duplicates('DeviceId')[['DeviceId', 'Phase']]
    return rows.merge(busiest, on=['DeviceId', 'Phase'])[['DeviceId', 'TimeStamp', 'Total']].rename(columns={'Total': 'Cycles'})


def walk_share(walks: pd.DataFrame, cycles: pd.DataFrame, start, end) -> dict:
    """{phase: share of each hour's cycles that had a walk} for one signal from start to end, from its walks per 15 minutes
    (TimeStamp, Phase, PedServices) and cycles per hour (TimeStamp, Cycles). A phase walks once a cycle at most, so its walks over
    the cycles is that share. An hour with fewer than MIN_CYCLES cycles has none."""
    hours = pd.date_range(start, end, freq='h', inclusive='left')
    if walks is None or walks.empty or cycles is None or cycles.empty:
        return {}
    per_hour = cycles.groupby(pd.to_datetime(cycles['TimeStamp']).dt.floor('h'))['Cycles'].sum().reindex(hours)
    per_hour = per_hour.where(per_hour >= MIN_CYCLES)
    shares = {}
    for phase, rows in walks.groupby('Phase'):
        count = rows.groupby(pd.to_datetime(rows['TimeStamp']).dt.floor('h'))['PedServices'].sum().reindex(hours).fillna(0)
        if count.sum() > 0:
            shares[int(phase)] = (count / per_hour).clip(upper=1.0)
    return shares


def measure(kind: str, data: pd.DataFrame, phases) -> pd.Series:
    """One signal's hourly measure over the given phases: kind MAX_OUT (share of greens that ran to max) or GREEN (greens) from
    hourly_terminations, WAIT (average wait, s) from waits per 15 minutes (TimeStamp, Phase, AvgPhaseWait)."""
    if data is None or data.empty:
        return pd.Series(dtype=float)
    rows = data[data['Phase'].isin(list(phases))] if phases else data
    hour = pd.to_datetime(rows['TimeStamp']).dt.floor('h')
    if kind == WAIT:
        return rows.groupby(hour)['AvgPhaseWait'].mean()
    sums = rows.groupby(hour)[['MaxOut', 'Total']].sum()
    if kind == GREEN:
        return sums['Total']
    return sums['MaxOut'] / sums['Total'].where(sums['Total'] > 0)


def normal_range(hourly: pd.Series, start, end) -> pd.DataFrame:
    """(lo, hi) of each hour from start to end: the middle NORMAL_SHARE of the same hour of the day on the BASELINE_DAYS before
    start, weekdays and weekends apart, without the days least like the others. Empty where there are under MIN_NORMAL_DAYS."""
    hours = pd.date_range(start, end, freq='h', inclusive='left')
    empty = pd.DataFrame({'lo': np.nan, 'hi': np.nan}, index=hours)
    if hourly is None or hourly.empty:
        return empty
    start = pd.Timestamp(start)
    series = hourly[(hourly.index >= start - pd.Timedelta(days=BASELINE_DAYS)) & (hourly.index < start)].dropna()
    if series.empty:
        return empty
    days = series.groupby([series.index.normalize(), series.index.hour]).mean().unstack()
    days = days[days.notna().sum(axis=1) >= MIN_DAY_HOURS]
    if days.empty:
        return empty
    # Unusual days against all of them, weekdays and weekends together: a fault on half the few weekend days would be their usual.
    distance = (days - days.median()).abs().mean(axis=1)
    typical = distance.median()
    if typical > 0:
        days = days[distance <= UNUSUAL_DAY * typical]
    weekend = pd.Series(days.index.dayofweek >= 5, index=days.index)
    tail = (1 - NORMAL_SHARE) / 2
    bands = {}
    for is_weekend in (False, True):
        group = days[weekend == is_weekend]
        if len(group) < MIN_NORMAL_DAYS:
            group = days    # too few of this kind of day: all days
        if len(group) >= MIN_NORMAL_DAYS:
            bands[is_weekend] = (group.quantile(tail), group.quantile(1 - tail))
    def edge(hour, side):
        band = bands.get(hour.dayofweek >= 5)
        return band[side].get(hour.hour, np.nan) if band is not None else np.nan
    lo = [edge(hour, 0) for hour in hours]
    hi = [edge(hour, 1) for hour in hours]
    return pd.DataFrame({'lo': lo, 'hi': hi}, index=hours)


def _state(what, period, periods):
    if what in HELD_LABELS:
        return HELD
    return SILENT if what in STOPPED_LABELS else OTHER


def _clip(spans, start, end) -> list:
    return [(max(a, start), min(b, end), state) for a, b, state in spans if b > start and a < end]


def _minus(spans, holes) -> list:
    """spans with the holes cut out, without the slivers under MIN_LEFTOVER a cut leaves: the same fault as the hole's."""
    out = []
    for a, b, state in spans:
        pieces = [(a, b)]
        for hole_a, hole_b in holes:
            pieces = [part for x, y in pieces for part in ((x, min(y, hole_a)), (max(x, hole_b), y)) if part[0] < part[1]]
        out += [(x, y, state) for x, y in pieces if (x, y) == (a, b) or y - x >= MIN_LEFTOVER]
    return out


def _by_state(spans) -> dict:
    merged = {}
    for a, b, state in sorted(spans, key=lambda span: span[0]):
        rows = merged.setdefault(state, [])
        if rows and a <= rows[-1][1]:
            rows[-1] = (rows[-1][0], max(b, rows[-1][1]))
        else:
            rows.append((a, b))
    return merged


def _length(spans) -> pd.Timedelta:
    return sum((b - a for rows in _by_state(spans).values() for a, b in rows), pd.Timedelta(0))


def _alike(first, second) -> bool:
    """Whether two detectors' fault times are much the same: the same kind of fault over SIMILAR of their time together."""
    one, other = _by_state(first), _by_state(second)
    shared = sum((max(min(b, d) - max(a, c), pd.Timedelta(0)) for state in one for a, b in one[state] for c, d in other.get(state, [])),
                 pd.Timedelta(0))
    together = _length(first) + _length(second) - shared
    return together > pd.Timedelta(0) and shared >= SIMILAR * together


def _union(spans) -> list:
    return [(a, b, state) for state, rows in _by_state(spans).items() for a, b in rows]


def _spans_of(row, start, end) -> list:
    """(start, end, state) of a fault row's periods in [start, end); a finding without times of its own covers it all."""
    periods = row.get('Periods')
    periods = periods if isinstance(periods, (list, tuple)) else []
    spans = []
    for period in periods:
        what = period.get('what')
        a, b = pd.to_datetime(period.get('start'), errors='coerce'), pd.to_datetime(period.get('end'), errors='coerce')
        if what in NO_TIMES:
            spans.append((start, end, WHOLE))
        elif pd.notna(a) and pd.notna(b) and b > a:
            spans.append((a, b, _state(what, period, periods)))
    if not spans and not periods:
        spans.append((start, end, WHOLE))
    return _clip(spans, start, end)


def _row_label(item: str, numbers: list) -> str:
    numbers = sorted(numbers)
    if len(numbers) > MAX_NAMED:
        return f"{len(numbers)} {'buttons' if item == 'Button' else 'detectors'}"
    return f"{item} {', '.join(str(number) for number in numbers)}"


def fault_rows(faults: pd.DataFrame, start, end, together=None, active=0) -> list:
    """The fault strip's rows, {label, spans: [(start, end, state)]}, of a signal's fault rows (DetectorType, Detector, Periods),
    most urgent first as given: the detectors or buttons with the same fault times share a row. together: [(start, end)] when every
    one of its `active` detectors was down at once, a row of its own, which the other rows then leave out."""
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    rows = []
    holes = [(pd.Timestamp(a), pd.Timestamp(b)) for a, b in (together or [])]
    if holes:
        down = {}
        for _, fault in faults.iterrows():
            for a, b, state in _spans_of(fault, start, end):
                down.setdefault(state, []).append((a, b))
        spans = []
        for a, b in holes:
            # Held ON or sending nothing, by which most of them were.
            held = sum((max(min(b, y) - max(a, x), pd.Timedelta(0)) for x, y in down.get(HELD, [])), pd.Timedelta(0))
            silent = sum((max(min(b, y) - max(a, x), pd.Timedelta(0)) for x, y in down.get(SILENT, [])), pd.Timedelta(0))
            spans.append((a, b, HELD if held > silent else SILENT))
        rows.append({'label': f"All {active} detectors" if active else 'All detectors', 'spans': _clip(spans, start, end)})
    ordered = []
    for _, fault in faults.iterrows():
        spans = _minus(_spans_of(fault, start, end), holes)
        if not spans:
            continue
        item = 'Button' if str(fault.get('DetectorType')) == 'Ped' else 'Det'
        group = next((group for group in ordered if group['item'] == item and _alike(group['spans'], spans)), None)
        if group is None:
            ordered.append({'item': item, 'numbers': [int(fault['Detector'])], 'spans': spans})
        else:
            group['numbers'].append(int(fault['Detector']))
            group['spans'] = _union(group['spans'] + spans)
    room = MAX_FAULT_ROWS - len(rows)
    for group in ordered[:room - 1] if len(ordered) > room else ordered:
        rows.append({'label': _row_label(group['item'], group['numbers']), 'spans': group['spans']})
    if len(ordered) > room:
        rest = ordered[room - 1:]
        numbers = [number for group in rest for number in group['numbers']]
        rows.append({'label': f"{len(numbers)} more", 'spans': [span for group in rest for span in group['spans']]})
    return rows


def no_data_hours(missing_hourly: pd.DataFrame, device, start, end) -> list:
    """[(start, end)] of the hours the signal sent no data, from missing_hourly (DeviceId, TimeStamp, MissingData), merged."""
    if missing_hourly is None or missing_hourly.empty:
        return []
    rows = missing_hourly[(missing_hourly['DeviceId'].astype(str) == str(device)) & (missing_hourly['MissingData'] >= NO_DATA_SHARE)]
    stamps = sorted(stamp for stamp in pd.to_datetime(rows['TimeStamp']) if pd.Timestamp(start) <= stamp < pd.Timestamp(end))
    spans = []
    for stamp in stamps:
        if spans and spans[-1][1] == stamp:
            spans[-1] = (spans[-1][0], stamp + pd.Timedelta(hours=1))
        else:
            spans.append((stamp, stamp + pd.Timedelta(hours=1)))
    return spans


def signal_phases(terminations: TableLike, devices, start, end) -> dict:
    """{device: sorted phases with a green from start to end}: the phases each signal runs."""
    if terminations is None or not len(devices):
        return {}
    rows = run_sql(f"""
        SELECT DISTINCT CAST(DeviceId AS VARCHAR) AS DeviceId, CAST(Phase AS INTEGER) AS Phase
        FROM t
        WHERE CAST(DeviceId AS VARCHAR) IN ({_ids(devices)}) AND Total > 0
          AND TimeStamp >= TIMESTAMP '{pd.Timestamp(start)}' AND TimeStamp < TIMESTAMP '{pd.Timestamp(end)}'
    """, t=terminations)
    return {device: sorted(group['Phase'].astype(int)) for device, group in rows.groupby('DeviceId')}


def phase_words(phases, all_phases=None) -> str:
    """'all phases' (every phase the signal runs, all_phases), 'phase 4', 'phases 2 & 6', 'phases 1, 3 & 4'."""
    numbers = sorted({int(phase) for phase in phases})
    if not numbers or (all_phases is not None and len(all_phases) and set(numbers) >= set(all_phases)):
        return 'all phases'
    if len(numbers) == 1:
        return f"phase {numbers[0]}"
    return f"phases {', '.join(str(n) for n in numbers[:-1])} & {numbers[-1]}"


# ---------------------------------------------------------------- drawing

FAULT_ROW_H, PANEL_H, TICK_ROW_H, TITLE_H, GAP_H, AXIS_H = 0.2, 0.95, 0.2, 0.22, 0.12, 0.3
WIDTH, LEFT, RIGHT = 7.5, 0.13, 0.975


def _time_axis(ax, start, end, labels):
    one_day = end - start <= pd.Timedelta(days=1)
    ax.set_xlim(start, end)
    ax.xaxis.set_major_locator(mdates.HourLocator(byhour=range(0, 24, 3) if one_day else [0, 6, 12, 18]))

    def tick(value, _):
        moment = mdates.num2date(value).replace(tzinfo=None)
        if moment >= end - pd.Timedelta(minutes=1):
            return ''
        if one_day:
            return moment.strftime('%H:%M')
        return f"{moment:%a} {moment.month}/{moment.day}" if moment.hour == 0 else moment.strftime('%H:%M')

    ax.xaxis.set_major_formatter(FuncFormatter(tick))
    ax.tick_params(axis='x', labelsize=8.5, labelbottom=labels, length=3 if labels else 0)
    for day in pd.date_range(start, end, freq='D')[1:-1]:
        ax.axvline(day, color='#999999', linewidth=0.8, zorder=1)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)


def _title(ax, text):
    ax.set_title(text, loc='left', fontsize=9.5, fontweight='bold', pad=3, color='#222222')


def _shade(ax, spans):
    for state in STATE_ORDER:
        for a, b, one in spans:
            if one == state and state in STATE_SHADE:
                ax.axvspan(a, b, color=STATE_SHADE[state], linewidth=0, zorder=0)


def _hatch(ax, gaps):
    for a, b in gaps:
        ax.axvspan(a, b, facecolor='white', edgecolor=NO_DATA_EDGE, hatch='////', linewidth=0, zorder=0.5)


HALF_HOUR = pd.Timedelta(minutes=30)


def smooth(values: pd.Series, hours: int = SMOOTH_HOURS) -> pd.Series:
    """values (hourly) as a centred mean over `hours` hours, blank where they were blank."""
    return values.rolling(hours, center=True, min_periods=1).mean().where(values.notna())


def _centred(series: pd.Series, start, end):
    """x, y of an hourly series drawn as a line through the middle of each hour, from the window's start to its end: the first and
    last hours reach the edges. A blank hour breaks the line."""
    x = [stamp + HALF_HOUR for stamp in series.index]
    y = series.tolist()
    if len(y) and pd.notna(y[0]) and series.index[0] == pd.Timestamp(start):
        x, y = [pd.Timestamp(start)] + x, [y[0]] + y
    if len(y) and pd.notna(y[-1]) and series.index[-1] + pd.Timedelta(hours=1) == pd.Timestamp(end):
        x, y = x + [pd.Timestamp(end)], y + [y[-1]]
    return x, y


def _alone(series: pd.Series) -> pd.Series:
    """The hours with a value between two blank ones, which a line cannot show."""
    values = series.notna()
    return series[values & ~values.shift(1, fill_value=False) & ~values.shift(-1, fill_value=False)]


def _blanks(series: pd.Series, gaps, start, end) -> list:
    """[(start, end)] of the stretches of MIN_BLANK_HOURS or more with nothing measured while the signal sent data."""
    hours = pd.date_range(start, end, freq='h', inclusive='left')
    blank = series.reindex(hours).isna()
    for a, b in gaps:
        blank[(hours >= a) & (hours < b)] = False
    spans, first = [], None
    for stamp, empty in list(blank.items()) + [(pd.Timestamp(end), False)]:
        if empty and first is None:
            first = stamp
        elif not empty and first is not None:
            if stamp - first >= pd.Timedelta(hours=MIN_BLANK_HOURS):
                spans.append((first, stamp))
            first = None
    return spans


def create_focus_overview(name: str, window, faults: list = None, panels: list = None, ticks: list = None, gaps: list = None):
    """The overview chart of one Signal Focus signal, or None when it has nothing to show.

    faults: fault_rows(). panels: {kind, title, series (hourly over the window), band (normal_range), blank: text for a stretch
    with nothing measured, optional}. ticks: {title, rows: [(label, times[, text when it has none])], empty: text for a row with
    none}. gaps: no_data_hours()."""
    from .visualization import new_figure   # here: visualization imports this module

    faults, panels, ticks, gaps = faults or [], panels or [], ticks or [], gaps or []
    if not (faults or panels or ticks):
        return None
    start, end = pd.Timestamp(window[0]), pd.Timestamp(window[1])
    shade = [span for row in faults for span in row['spans']]
    states = {state for _, _, state in shade}

    parts = []   # (kind, item, height of its plot)
    if faults:
        parts.append(('faults', faults, FAULT_ROW_H * len(faults)))
    parts += [('panel', panel, PANEL_H) for panel in panels]
    parts += [('ticks', tick, TICK_ROW_H * max(len(tick['rows']), 1)) for tick in ticks]

    handles = []
    if panels:
        handles += [Patch(color=BAND_COLOR, label='Normal range'), Line2D([], [], color=LINE_COLOR, linewidth=1.3, label='Measured')]
    shown = {LEGEND_STATE.get(state, state) for state in states}
    handles += [Patch(color=STATE_COLOR[state], label=STATE_WORDS[state]) for state in (HELD, SILENT, OTHER) if state in shown]
    if gaps:
        handles.append(Patch(facecolor='white', edgecolor=NO_DATA_EDGE, hatch='////', linewidth=0.6, label='No data'))
    legend_rows = math.ceil(len(handles) / 6) if handles else 0
    head = 0.3 + 0.2 * legend_rows
    height = head + sum(TITLE_H + plot + GAP_H for _, _, plot in parts) + AXIS_H
    fig = new_figure((WIDTH, height))
    fig.text(0.01, 1 - 0.06 / height, f"{name}  ·  overview", fontsize=11, fontweight='bold', va='top')
    if handles:
        fig.legend(handles=handles, loc='upper left', bbox_to_anchor=(LEFT - 0.01, 1 - 0.3 / height), ncol=min(len(handles), 6),
                   frameon=False, fontsize=8.5, handlelength=1.6, columnspacing=1.4, borderaxespad=0)

    top = height - head
    axes = []
    for k, (kind, item, plot) in enumerate(parts):
        top -= TITLE_H
        ax = fig.add_axes([LEFT, (top - plot) / height, RIGHT - LEFT, plot / height], sharex=axes[0] if axes else None)
        top -= plot + GAP_H
        axes.append(ax)
        last = k == len(parts) - 1
        if kind == 'faults':
            _title(ax, 'Detector Faults' if not any(row['label'].startswith('Button') for row in item) else 'Faults')
            ax.set_facecolor('#EFEFEF')
            for row_number, row in enumerate(item):
                for state in STATE_ORDER:
                    for a, b, one in row['spans']:
                        if one == state:
                            ax.barh(row_number, mdates.date2num(b) - mdates.date2num(a), left=mdates.date2num(a), height=0.78,
                                    color=STATE_COLOR[state], linewidth=0, zorder=2)
            ax.set_yticks(range(len(item)))
            ax.set_yticklabels([row['label'] for row in item], fontsize=8.5)
            ax.set_ylim(len(item) - 0.5, -0.5)
            ax.tick_params(axis='y', length=0)
            _hatch(ax, gaps)
        elif kind == 'panel':
            _title(ax, item['title'])
            _shade(ax, shade)
            _hatch(ax, gaps)
            band = item['band']
            if band is not None and band['hi'].notna().any():
                x, lo = _centred(smooth(band['lo']), start, end)
                _, hi = _centred(smooth(band['hi']), start, end)
                ax.fill_between(x, lo, hi, color=BAND_COLOR, linewidth=0, zorder=1)
            series = item['series'].reindex(pd.date_range(start, end, freq='h', inclusive='left'))
            percent = item['kind'] == MAX_OUT
            # A line at 100% sits on the top edge of a max out panel: drawn over it, not cut in half.
            x, y = _centred(series, start, end)
            ax.plot(x, y, color=LINE_COLOR, linewidth=1.2, zorder=3, clip_on=not percent)
            alone = _alone(series)
            if not alone.empty:
                ax.plot(alone.index + HALF_HOUR, alone.values, linestyle='none', marker='o', markersize=2.6, color=LINE_COLOR,
                        zorder=3, clip_on=not percent)
            if item.get('blank'):
                blanks = _blanks(series, gaps, start, end)
                text_width = 0.0075 * len(item['blank']) * (end - start)   # about how long the text is on the time axis
                fits = [(a, b) for a, b in blanks if b - a >= text_width] or sorted(blanks, key=lambda span: span[1] - span[0])[-1:]
                for a, b in fits:
                    ax.text(a + (b - a) / 2, 0.62, item['blank'], transform=ax.get_xaxis_transform(), ha='center', va='center',
                            fontsize=8.5, color=MUTED, zorder=4, bbox=NOTE_BOX)
            peak = np.nanmax([np.nanmax(series.values) if series.notna().any() else 0,
                              np.nanmax(band['hi'].values) if band is not None and band['hi'].notna().any() else 0])
            y_top = 1.0 if percent else max(peak * 1.12, 1)
            ax.set_ylim(-0.03 * y_top, y_top)   # a line at zero shows above the axis
            if percent:
                ax.yaxis.set_major_locator(mtick.FixedLocator([0, 0.5, 1.0]))
                ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
            else:
                ax.yaxis.set_major_locator(mtick.MaxNLocator(nbins=3, min_n_ticks=2))
                if item['kind'] == WAIT:
                    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:.0f} s"))
            ax.tick_params(axis='y', labelsize=8.5)
            ax.grid(True, axis='y', color=GRID, linewidth=0.6)
            ax.set_axisbelow(True)
        else:
            _title(ax, item['title'])
            _shade(ax, shade)
            _hatch(ax, gaps)
            # A row is (label, times) or (label, times, what to say when it has none in these days).
            rows = [tuple(row) + (None,) * (3 - len(row)) for row in item['rows']]
            for row_number, (_, times, note) in enumerate(rows):
                stamps = [stamp for stamp in pd.to_datetime(pd.Series(times, dtype=object)) if start <= stamp < end]
                if stamps:
                    ax.vlines(stamps, row_number - 0.38, row_number + 0.38, color=LINE_COLOR, linewidth=1.1, zorder=3)
                else:
                    # None in these days, said on its row, so an empty row does not read as a chart that failed to draw.
                    ax.text(start + (end - start) / 2, row_number, note or item.get('empty', 'none in these days'), ha='center',
                            va='center', fontsize=8.5, color='#444444', zorder=4, bbox=NOTE_BOX)
            if not rows:
                ax.text(start + (end - start) / 2, 0, item.get('empty', 'none in these days'), ha='center', va='center',
                        fontsize=8.5, color='#444444', zorder=4, bbox=NOTE_BOX)
            ax.set_yticks(range(len(rows)))
            ax.set_yticklabels([label for label, _, _ in rows], fontsize=8.5)
            ax.set_ylim(max(len(rows), 1) - 0.5, -0.5)
            ax.tick_params(axis='y', length=0)
        _time_axis(ax, start, end, labels=last)
    return fig
