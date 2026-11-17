"""The impact chart of a coordination alert: what the odd plan or clock did to traffic, by hour of day.

Two panels, arrivals on green and delay per vehicle (measure_inputs), on the approaches of the signal that have advance
zones (the two measures exist only there), all of them summed together. Lines:
- the days it was out of step with its group (strong line);
- the days of the same kind it ran with its group (dashed). A signal wrong on every day of that kind (08043 runs free
  every weekend) has none: its own weekdays are drawn dashed instead, and labelled weekdays;
- the group's median on the out-of-step days (grey), for reference;
- the next signal: the group's nearest member by distance, all its approaches with advance zones, on the same days
  (solid) and on the comparison days (dashed). No input says which approach the odd signal feeds, so it is the nearest
  signal rather than one approach of it.
Hours out of step are shaded when the alert names them and they are not the whole day. Hours whose zones were faulting
are left out by the caller (measure_inputs.unmasked) and show as gaps.

A Clock alert: the signal's own stamps are moved by its offset (in whole hours) to true time; out of step is every day
since the offset began, the comparison the days before it, and the group its neighbours.

Drawn only when there is enough data on both sides (MIN_DAYS days each, MIN_HOURS hours with MIN_ARRIVALS_PER_HOUR
arrivals or more on both lines) and something to see (the signal or the next one worse by MIN_AOG_POINTS of arrivals on
green or MIN_DELAY_SECONDS of delay per vehicle, the study's ordinary day-to-day spread). Otherwise None: no chart and
no empty frame.
"""

from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd

from . import measure_inputs as mi

MIN_DAYS = 2
MIN_HOURS = 3
MIN_ARRIVALS_PER_HOUR = 20
MIN_AOG_POINTS = 5.0
MIN_DELAY_SECONDS = 1.5
CHART_SIZE = (7.2, 4.8)
CHART_HEIGHT_INCHES = 6.5 * CHART_SIZE[1] / CHART_SIZE[0]
STRONG, NEXT, GREY = '#c4302b', '#2f6db3', '#8c8c8c'
WEEKEND_KINDS = {'Saturdays', 'Sundays'}


def _clock_hour(text) -> float:
    hours, minutes = str(text).split(':')
    return int(hours) + int(minutes) / 60.0


def _days(entries) -> list:
    return sorted({pd.Timestamp(d).normalize() for entry in (entries or []) for d in (entry.get('Days') or [])})


def _by_hour(hours: pd.DataFrame, devices, days) -> pd.DataFrame:
    """Per hour of day over these devices and days: veh, gre, dsum summed, days with data, aog (%) and dpv (s); NaN where
    the hour has under MIN_ARRIVALS_PER_HOUR arrivals a day."""
    mine = hours[hours['DeviceId'].isin(list(devices)) & hours['day'].isin(list(days))]
    out = pd.DataFrame(index=range(24), columns=['veh', 'gre', 'dsum', 'ndays'], dtype=float).fillna(0.0)
    if not mine.empty:
        g = mine.groupby('hh').agg(veh=('veh', 'sum'), gre=('gre', 'sum'), dsum=('dsum', 'sum'), ndays=('day', 'nunique'))
        out.loc[g.index, g.columns] = g.to_numpy(float)
    enough = (out['ndays'] > 0) & (out['veh'] >= MIN_ARRIVALS_PER_HOUR * out['ndays'].clip(lower=1))
    out['aog'] = np.where(enough, 100.0 * out['gre'] / out['veh'].replace(0, np.nan), np.nan)
    out['dpv'] = np.where(enough, out['dsum'] / out['veh'].replace(0, np.nan), np.nan)
    out['days'] = mine['day'].nunique() if not mine.empty else 0
    return out


def _miles(a, b) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 3958.8 * 2 * math.asin(math.sqrt(h))


def _nearest(device, members, signals: Optional[pd.DataFrame], hours: pd.DataFrame):
    """The member nearest the signal that has any measures, or None."""
    if signals is None or not {'Latitude', 'Longitude'} <= set(signals.columns):
        return None
    where = signals.assign(DeviceId=signals['DeviceId'].astype(str)).drop_duplicates('DeviceId').set_index('DeviceId')
    if device not in where.index:
        return None
    here = (where.at[device, 'Latitude'], where.at[device, 'Longitude'])
    with_data = set(hours['DeviceId'])
    options = [(_miles(here, (where.at[m, 'Latitude'], where.at[m, 'Longitude'])), m) for m in members
               if m in where.index and m in with_data and pd.notna(where.at[m, 'Latitude'])]
    return min(options)[1] if options else None


def impact_data(alert, group: list, mismatched: list, matched: list, hourly: pd.DataFrame,
                signals: Optional[pd.DataFrame] = None) -> Optional[dict]:
    """The lines of the chart (see the module), or None when they do not meet the minimum. hourly: DeviceId, Phase, hr,
    veh, gre, dsum (measure_inputs.hourly_delay, faulting hours already left out)."""
    if hourly is None or hourly.empty:
        return None
    device = str(alert['DeviceId'])
    hours = hourly.assign(DeviceId=hourly['DeviceId'].astype(str), hr=pd.to_datetime(hourly['hr']))
    clock = alert.get('Cause') == 'Clock'
    if clock:
        shift = int(round(float(alert.get('ClockOffsetSeconds') or 0) / 3600.0))
        own = hours['DeviceId'] == device
        hours.loc[own, 'hr'] = hours.loc[own, 'hr'] - pd.Timedelta(hours=shift)
    hours = hours.assign(day=hours['hr'].dt.normalize(), hh=hours['hr'].dt.hour)
    mine_days = sorted(hours.loc[hours['DeviceId'] == device, 'day'].unique())
    shade = []
    if clock:
        since = pd.Timestamp(alert.get('ClockSince')).normalize() if pd.notna(alert.get('ClockSince')) else None
        if since is None:
            return None
        bad = [d for d in mine_days if d >= since]
        good = [d for d in mine_days if d < since]
        bad_label, good_label = f"since {since.month}/{since.day}", 'before'
    else:
        have = set(mine_days)
        bad = [d for d in _days(mismatched) if d in have]
        kinds = {entry.get('DayType') for entry in (mismatched or [])}
        # Matched lists every day it ran with its group in those hours, whatever its kind: the same kind first.
        in_step = [d for d in _days(matched) if d in have and d not in set(bad)]
        good = [d for d in in_step if mi.day_kind(d) in kinds]
        bad_label = 'weekends' if kinds == WEEKEND_KINDS else ' and '.join(sorted(k.lower() for k in kinds if k))
        good_label = bad_label + ', in step'
        if not good and kinds and kinds <= WEEKEND_KINDS:
            off = set(mi.holidays(min(mine_days), max(mine_days))) if mine_days else set()
            good = [d for d in in_step if mi.day_kind(d) == 'Weekdays' and pd.Timestamp(d).date() not in off] or \
                [d for d in mine_days if mi.day_kind(d) == 'Weekdays' and pd.Timestamp(d).date() not in off]
            good_label = 'weekdays'
        for entry in mismatched or []:
            start, end = _clock_hour(entry.get('Start', '0:00')), _clock_hour(entry.get('End', '0:00'))
            end = 24.0 if end <= start else end
            if (start, end) != (0.0, 24.0):
                shade.append((start, end))
    if len(bad) < MIN_DAYS or len(good) < MIN_DAYS:
        return None
    own_bad, own_good = _by_hour(hours, [device], bad), _by_hour(hours, [device], good)
    if own_bad['days'].iloc[0] < MIN_DAYS or own_good['days'].iloc[0] < MIN_DAYS:
        return None
    window = np.zeros(24, dtype=bool)
    for start, end in shade:
        window[int(math.floor(start)):int(math.ceil(end))] = True
    if not window.any():
        window[:] = True
    both = window & own_bad['dpv'].notna().to_numpy() & own_good['dpv'].notna().to_numpy()
    if both.sum() < MIN_HOURS:
        return None
    members = [str(m) for m in group if str(m) != device]
    next_signal = _nearest(device, members, signals, hours)
    lines = {'own_bad': own_bad, 'own_good': own_good}
    if next_signal is not None:
        lines['next_bad'] = _by_hour(hours, [next_signal], bad)
        lines['next_good'] = _by_hour(hours, [next_signal], good)
    per_member = [_by_hour(hours, [m], bad) for m in members]
    if per_member:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', RuntimeWarning)   # an hour no member has data: all-NaN median
            lines['group'] = pd.DataFrame({column: np.nanmedian(np.vstack([f[column].to_numpy(float) for f in per_member]), axis=0)
                                           for column in ('aog', 'dpv')})

    def worse(b, g):
        ok = window & b['dpv'].notna().to_numpy() & g['dpv'].notna().to_numpy()
        if ok.sum() < MIN_HOURS:
            return False
        weights = b['veh'].to_numpy(float)[ok]
        aog_drop = np.average(g['aog'].to_numpy(float)[ok] - b['aog'].to_numpy(float)[ok], weights=weights)
        delay_rise = np.average(b['dpv'].to_numpy(float)[ok] - g['dpv'].to_numpy(float)[ok], weights=weights)
        return aog_drop >= MIN_AOG_POINTS or delay_rise >= MIN_DELAY_SECONDS
    if not (worse(own_bad, own_good) or ('next_bad' in lines and worse(lines['next_bad'], lines['next_good']))):
        return None
    return {'lines': lines, 'shade': shade, 'bad_label': bad_label, 'good_label': good_label, 'next': next_signal,
            'bad_days': bad, 'good_days': good}


def draw_impact_chart(alert, data: dict, names: dict):
    """The figure for impact_data()'s lines. names: DeviceId -> signal number."""
    import warnings
    from .visualization import new_figure
    device = str(alert['DeviceId'])
    me = names.get(device, device)
    figure = new_figure(CHART_SIZE, dpi=110)
    axes = [figure.add_subplot(2, 1, k) for k in (1, 2)]
    x = np.arange(24) + 0.5
    lines = data['lines']
    bad, good = data['bad_label'], data['good_label']
    nxt = names.get(data['next'], data['next']) if data['next'] else None
    for ax, column, label in ((axes[0], 'aog', 'Arrivals on green (%)'), (axes[1], 'dpv', 'Delay per vehicle (s)')):
        handles = []
        handles += ax.plot(x, lines['own_bad'][column], color=STRONG, lw=2.6, label=f"{me}, {bad}", zorder=5)
        handles += ax.plot(x, lines['own_good'][column], color=STRONG, lw=1.6, ls='--', label=f"{me}, {good}", zorder=4)
        if 'next_bad' in lines:
            handles += ax.plot(x, lines['next_bad'][column], color=NEXT, lw=1.6, label=f"{nxt} (nearest), {bad}", zorder=3)
            handles += ax.plot(x, lines['next_good'][column], color=NEXT, lw=1.2, ls='--', label=f"{nxt} (nearest), {good}", zorder=3)
        if 'group' in lines:
            handles += ax.plot(x, lines['group'][column], color=GREY, lw=1.2, label=f"Group median, {bad}", zorder=2)
        for start, end in data['shade']:
            span = ax.axvspan(start, end, color='#f1e4c8', lw=0, zorder=0, label='Out of step with its group')
            if (start, end) == data['shade'][0]:
                handles.append(span)
        ax.set_xlim(0, 24)
        ax.set_xticks(range(0, 25, 3))
        ax.set_xticklabels([f'{h}:00' for h in range(0, 25, 3)])
        ax.set_ylabel(label, fontsize=8)
        ax.set_ylim(bottom=0)
        ax.tick_params(labelsize=8)
        ax.grid(axis='y', color='#e6e6e6', lw=0.8)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)
    axes[0].set_ylim(0, 100)
    axes[0].tick_params(labelbottom=False)
    title = figure.suptitle(f"{me} arrivals on green and delay per vehicle by hour", fontsize=10, y=0.985)
    # Legend above the plots, under the title: its own band at the top of the figure.
    legend = figure.legend(handles=handles, labels=[h.get_label() for h in handles], loc='upper center',
                           bbox_to_anchor=(0.5, 0.94), ncol=3, fontsize=8, frameon=False, handlelength=2.4,
                           columnspacing=1.2)
    rows = math.ceil(len(handles) / 3)
    # Laid out without the title and legend (their room is kept by rect), then counted again, so a tight crop keeps them.
    for artist in (title, legend):
        artist.set_in_layout(False)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        figure.tight_layout(rect=(0, 0, 1, 0.92 - 0.045 * rows))
    for artist in (title, legend):
        artist.set_in_layout(True)
    figure.report_own_proportions = True
    return figure


def coordination_impact_chart(alert, group: list, mismatched: list, matched: list, measures: Optional[dict] = None):
    """create_coordination_impact_chart's body: measures {'hourly': hourly delay table, 'signals': signals table}."""
    if not measures or measures.get('hourly') is None:
        return None
    signals = measures.get('signals')
    data = impact_data(alert, group, mismatched, matched, measures['hourly'], signals)
    if data is None:
        return None
    from .coordination_check import signal_number
    names = {}
    if signals is not None and 'Name' in signals.columns:
        names = {str(d): signal_number(n) for d, n in zip(signals['DeviceId'], signals['Name'])}
    return draw_impact_chart(alert, data, names)
