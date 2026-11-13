"""Preempt cascades and call durations.

An emergency vehicle driving a corridor preempts one signal after another, and
a train preempts every crossing on its line. Counted per signal, those calls
make neighbouring preempts rise and fall together, so a single quiet week on a
fire route reads as two or three "stopped" preempts at once. This module keeps
a record of every preempt call with its timestamp and uses the signals'
coordinates to learn which preempts cascade into which:

* a link A/p -> B/q is a pair of signals within ``max_miles`` of each other
  where calls at B/q start after calls at A/p within the time it takes to drive
  between them at ``min_mph`` to ``max_mph`` (plus some slack for the distance
  at which detection picks the vehicle up), far more often than chance;
* the links re-learn every run from the calls before the recent window, so the
  table of cascade patterns follows route and service changes.

The links are then used three ways. A frequency alert (see
preempt_processing) is overruled when the whole cascade went quiet with it, or
confirmed when its neighbours kept sending vehicles it no longer answers. A
cascade break is raised when vehicles seen at the preempts on both sides of a
signal stop calling the one between them, which catches a dead input even when
its own daily count is too low for the frequency check. Durations get their own
checks: a call held far longer than normal (stuck input) unless a neighbour was
held at the same time (a stopped train, a long incident), and an input whose
calls are mostly a second or two long (chatter).

A missing call is only evidence while the signal was reporting. Given the
has_data table (one row per 15-minute bin a signal reported in), vehicles that
passed a signal while it was not reporting are left out of the break check and
the review, and a long call that spans a gap is not taken as stuck, since the
event that ended it may be the one lost.

Rail and emergency vehicle preempts behave very differently in duration. Which
preempt numbers are rail is agency-specific, so it is a setting; with no setting
every preempt is held to the looser rail limits.

Every call is also given a class (see classify_preempt_calls): emergency
vehicle, a bus priority call that pulsed the preempt input (with or without
running a preempt), rail, or unclassified. A controller set up for transit
signal priority (TSP) logs a bus as TSP events only (check-in 112 to check-out
115) and no preempt call at all, so normal TSP is never in the calls. A burst
of short pulses on a preempt input is either a bus priority signal the
controller is reading as preempt calls (TSP check-ins come with the bursts) or
an input chattering (no check-ins). The emergency vehicle checks above read
emergency vehicle (and unclassified) calls, chatter included; the bus calls get
alerts of their own (build_bus_preempt_alerts).
"""

from __future__ import annotations

import math
from typing import Iterable, Optional

import duckdb
import numpy as np
import pandas as pd

from .utils import log_message, text_equals


PREEMPT_CALL_COLUMNS = ['DeviceId', 'Preempt', 'StartTime', 'Duration', 'IsValid', 'Class']
# Raw preempt events (DeviceId, TimeStamp, EventId, Parameter = the preempt number).
PREEMPT_EVENT_COLUMNS = ['DeviceId', 'TimeStamp', 'EventId', 'Parameter']

# ---- Call classes ----------------------------------------------------------
CLASS_EV = 'Emergency Vehicle'
# A bus priority call (a burst of pulses on a preempt input, with TSP check-ins) that ran no preempt: each pulse
# still cancels the bus's priority request and starts it again.
CLASS_TSP = 'TSP'
# The same, which the controller ran as a full preempt.
CLASS_BUS_PREEMPT = 'Bus Ran Preempt'
CLASS_RAIL = 'Rail'
# Calls of 1.5 to 5 s, or unclosed, that are neither beside a constant call nor
# part of a burst of pulses.
CLASS_UNCLASSIFIED = 'Unclassified'
CALL_CLASSES = (CLASS_EV, CLASS_TSP, CLASS_BUS_PREEMPT, CLASS_RAIL, CLASS_UNCLASSIFIED)
# The three report sections, which are also the Class of an alert.
SECTION_EV, SECTION_TSP, SECTION_RAIL = CLASS_EV, CLASS_TSP, CLASS_RAIL
PREEMPT_SECTIONS = (SECTION_EV, SECTION_TSP, SECTION_RAIL)
# Call classes the TSP section covers (bus calls on a preempt input); every other
# non-rail call is checked as an emergency vehicle call, so chatter alerts there.
TSP_CLASSES = (CLASS_TSP, CLASS_BUS_PREEMPT)

# A call shorter than this is a pulse. Bus pulses run about 0.6 s on and 1.5 to
# 2.5 s off, 3 to 7 to a burst; over six weeks of a network's calls, 5% were
# pulses, 0.4% fell between 1.5 and 5 s and 94% were constant calls.
PULSE_MAX_SECONDS = 1.5
# A call at least this long is a constant call: an emergency vehicle preempt.
CONSTANT_MIN_SECONDS = 5.0
# Calls on one input whose next call starts within this of the previous one's
# end are one episode (one vehicle), classed together: pulses beside a constant
# call are part of an emergency vehicle preempt.
EPISODE_GAP_SECONDS = 30.0
# An episode with no constant or unclosed call is a burst of pulses when more
# than this share of its calls are pulses: bus bursts now and then hold one
# pulse a little longer (1.6 to 1.7 s among 0.4 to 0.7 s ones).
BURST_MIN_PULSE_SHARE = 0.5
# Preempt entry (105) or TSP check-in (112) this close to an episode belongs to it.
EVIDENCE_SLACK_SECONDS = 5.0
# An input carries bus priority calls when at least this share of its bursts came with a TSP check-in, among the
# bursts since the signal's first check-in on record. All of that input's bursts are then bus calls, the ones from
# before check-ins were recorded included. Over three days at five such inputs, 36 of 36 bursts had a check-in.
BUS_INPUT_MIN_CHECK_IN_SHARE = 0.5
EVENT_CALL_ON, EVENT_CALL_OFF, EVENT_ENTRY, EVENT_TSP_CHECK_IN = 102, 104, 105, 112

# Bus calls on a preempt input: at least this many in the recent window raise an
# alert. Ran As Preempt when that many ran as preempts, else Priority Interrupted.
BUS_PREEMPT_MIN_EPISODES = 5
DIRECTION_BUS_PREEMPT = 'Ran As Preempt'
DIRECTION_BUS_INTERRUPTED = 'Priority Interrupted'
BUS_DIRECTIONS = (DIRECTION_BUS_PREEMPT, DIRECTION_BUS_INTERRUPTED)
BUS_PREEMPT_ADVICE = (
    'Priority may shorten or lengthen greens but must not skip phases; these can pass over '
    "waiting phases and cut walks. Check this preempt input's settings."
)
BUS_INTERRUPTED_ADVICE = (
    "Each pulse cancels the bus's priority request and starts it again. Check this preempt input's settings."
)
CASCADE_LINK_COLUMNS = [
    'DeviceId', 'Preempt', 'NextDeviceId', 'NextPreempt', 'Miles',
    'Calls', 'Followed', 'FollowRate', 'Lift', 'MedianLagSeconds',
]

# Six weeks, the same depth as the daily count history.
PREEMPT_CALL_HISTORY_DAYS = 42

CASCADE_MAX_MILES = 2.0
CASCADE_MIN_MPH = 20.0
CASCADE_MAX_MPH = 45.0
# Detection ranges differ between approaches, so the travel window is widened
# by this much on both ends.
CASCADE_SLACK_SECONDS = 30.0

# A link needs this many followed calls, a shrunk follow rate of at least this
# much, and this many times the co-occurrences chance would give.
LINK_MIN_FOLLOWED = 4
LINK_MIN_FOLLOW_RATE = 0.3
LINK_MIN_LIFT = 20.0

# Verdicts on frequency alerts.
REVIEW_MIN_EXPECTED_FOLLOWS = 3.0   # neighbours sent enough vehicles to judge
REVIEW_MAX_FOLLOW_SHARE = 0.25      # ...and this preempt answered at most this share
REVIEW_QUIET_RATIO = 0.35           # neighbours at or below this share of their usual calls
REVIEW_RISE_RATIO = 2.0             # neighbours at or above this multiple of their usual calls

# Upstream calls closer together than this are taken as one vehicle, so a
# vehicle seen at several upstream signals is not counted once per signal.
# Two miles at 20 mph is six minutes.
VEHICLE_GAP_SECONDS = 360.0

# Cascade breaks: vehicles seen on both sides of a preempt that skipped it.
# X has to lie on the way from U to D: the U -> X -> D distance may be at most
# this multiple of U -> D. Vehicles join and leave a corridor part way, so a
# vehicle seen at U and then at D says nothing about an X off that path.
BREAK_MAX_DETOUR = 1.25
BREAK_MIN_PASSES = 3
# Passes seen before the recent window; fewer leaves the usual rate unknown.
BREAK_MIN_BASELINE_PASSES = 5
BREAK_MAX_RATIO = 0.5
BREAK_MAX_P_VALUE = 1e-3

# Duration checks, in seconds.
STUCK_SECONDS_EV = 600.0
STUCK_SECONDS_RAIL = 3600.0
# A neighbour held at least this share as long, overlapping, explains a long call.
STUCK_NEIGHBOUR_SHARE = 0.5
CHATTER_MAX_SECONDS = 3.0
CHATTER_MIN_PER_DAY = 2.0
CHATTER_MIN_SHARE = 0.5

# False calls: an input whose calls are not vehicles. Vehicles reach a signal at different speeds, so an input's
# calls vary in length, and a vehicle driving a road calls the next signal too. Over six weeks of a network's
# emergency vehicle calls the typical input had 12% of its calls within 0.3 s of its median length (95% of inputs
# under 34%), and 85% of calls had a call at a signal within two miles inside the drive time (90% of inputs over
# 60%). A call has a shortest length (5.2 s in that network, 3% of all calls; 0.2% are shorter): what a trigger
# of a moment is held for. A vehicle seen for a moment makes one too, so some calls of that length are normal;
# an input with most of its calls at it is triggering by itself, unless nearby signals saw those calls (of 23
# inputs with most of their calls at one length, 14 were vehicles). Being seen nearby only removes alerts and is
# never told in the report: a signal that is merely close is weaker evidence than a learned link.
SHORTEST_CALL_QUANTILE = 0.005       # the shortest call length: this share of all calls (chatter aside) is shorter
SAME_LENGTH_SECONDS = 0.3            # calls this close to the shortest length are of that length
FALSE_CALLS_MIN_PER_DAY = 1.0        # calls of the shortest length a day
FALSE_CALLS_MIN_SHARE = 0.5          # ...making up at least this share of the input's calls
FALSE_CALLS_MAX_SEEN_SHARE = 0.3     # ...of which at most this share were seen at a nearby signal; an Increase
                                     # with more of its calls seen than this is real traffic
# An input with no neighbouring signal to be seen at is judged on its calls alone, so the limits are tighter. Of the
# inputs nearby signals could check over six weeks, 8 of the 10 that met the limits above were faults (a chance of
# 0.75 for the next one, from a uniform prior) and 6 of the 6 that met these were (0.88).
FALSE_CALLS_ALONE_MIN_PER_DAY = 1.5
FALSE_CALLS_ALONE_MIN_SHARE = 0.8
DIRECTION_FALSE_CALLS = 'False Calls'
# Usual call lengths are this middle share of the network's calls.
USUAL_LENGTH_QUANTILES = (0.05, 0.95)

VERDICT_CONFIRMED = 'Confirmed'
VERDICT_OVERRULED = 'Overruled'

DIRECTION_STUCK = 'Stuck On'
DIRECTION_CHATTER = 'Chatter'
# Vehicles seen at the linked preempts on both sides, none of which called this one.
DIRECTION_BREAK = 'Missed Vehicles'
DIRECTION_LONG_HOLD = 'Long Hold'

# Preempts drawn on one alert's chart: the alert's own plus linked ones.
CHART_MAX_SERIES = 5

# has_data records reporting in bins of this size.
REPORTING_BIN = '15min'


def _empty_calls() -> pd.DataFrame:
    return pd.DataFrame(columns=PREEMPT_CALL_COLUMNS)


def _clean_calls(calls: Optional[pd.DataFrame]) -> pd.DataFrame:
    if calls is None or calls.empty:
        return _empty_calls()
    out = calls.reindex(columns=PREEMPT_CALL_COLUMNS).copy()
    out['DeviceId'] = out['DeviceId'].astype(str)
    out['Preempt'] = pd.to_numeric(out['Preempt'], errors='coerce')
    out['StartTime'] = pd.to_datetime(out['StartTime'], errors='coerce')
    out['Duration'] = pd.to_numeric(out['Duration'], errors='coerce').astype(float)
    out['IsValid'] = out['IsValid'].fillna(False).astype(bool)
    out['Class'] = out['Class'].astype(object).where(out['Class'].notna(), None)
    out = out.dropna(subset=['Preempt', 'StartTime'])
    out = out[out['Preempt'] > 0]
    out['Preempt'] = out['Preempt'].astype(int)
    return out.reset_index(drop=True)


def _rail_set(rail_preempts) -> set:
    return set() if rail_preempts is None else {int(p) for p in rail_preempts}


def call_section(classes, preempts, rail_preempts=None) -> np.ndarray:
    """The report section (SECTION_EV, SECTION_TSP or SECTION_RAIL) of each call class and preempt number.

    Rail inputs are rail whatever their calls look like; TSP and bus calls that ran
    as preempts are TSP; everything else, unclassified and calls stored before
    classes existed (None) included, is checked as emergency vehicle preempt.
    """
    rail = _rail_set(rail_preempts)
    classes = pd.Series(classes, dtype=object).reset_index(drop=True)
    preempts = pd.to_numeric(pd.Series(preempts).reset_index(drop=True), errors='coerce')
    return np.select(
        [preempts.isin(rail).to_numpy(), classes.isin(TSP_CLASSES).to_numpy()],
        [SECTION_RAIL, SECTION_TSP], default=SECTION_EV,
    ).astype(object)


def _episode_ids(calls: pd.DataFrame, gap_seconds: float = EPISODE_GAP_SECONDS) -> np.ndarray:
    """Episode number of each call; ``calls`` sorted by DeviceId, Preempt, StartTime.

    A call starts a new episode when it starts more than ``gap_seconds`` after the
    latest end of the input's calls before it. An unclosed call ends where it starts.
    """
    if calls.empty:
        return np.zeros(0, dtype=int)
    duration = calls['Duration'].where(calls['IsValid']).fillna(0).clip(lower=0)
    ends = calls['StartTime'] + pd.to_timedelta(duration, unit='s')
    keys = [calls['DeviceId'], calls['Preempt']]
    reach = ends.groupby(keys).cummax().groupby(keys).shift()
    new = reach.isna() | ((calls['StartTime'] - reach).dt.total_seconds() > gap_seconds)
    return new.cumsum().to_numpy()


def _clean_events(events: Optional[pd.DataFrame]) -> pd.DataFrame:
    if events is None or len(events) == 0 or not set(PREEMPT_EVENT_COLUMNS) <= set(events.columns):
        return pd.DataFrame(columns=PREEMPT_EVENT_COLUMNS)
    out = pd.DataFrame({
        'DeviceId': events['DeviceId'].astype(str),
        'TimeStamp': pd.to_datetime(events['TimeStamp'], errors='coerce').astype('datetime64[ns]'),
        'EventId': pd.to_numeric(events['EventId'], errors='coerce'),
        'Parameter': pd.to_numeric(events['Parameter'], errors='coerce'),
    }).dropna()
    out['EventId'] = out['EventId'].astype(int)
    out['Parameter'] = out['Parameter'].astype(int)
    return out.reset_index(drop=True)


def _evidence(episodes: pd.DataFrame, marks: pd.DataFrame, by: list, slack_seconds: float) -> np.ndarray:
    """True for each episode (DeviceId, Preempt, t0, t1) with a mark (by + TimeStamp) from t0 - slack to t1 + slack."""
    if episodes.empty or marks.empty:
        return np.zeros(len(episodes), dtype=bool)
    slack = pd.Timedelta(seconds=slack_seconds)
    left = episodes.assign(_row=np.arange(len(episodes)), _from=(episodes['t0'] - slack).astype('datetime64[ns]'))
    right = marks[by + ['TimeStamp']].rename(columns={'TimeStamp': '_mark'})
    found = pd.merge_asof(
        left.sort_values('_from'), right.sort_values('_mark'),
        left_on='_from', right_on='_mark', by=by, direction='forward',
    )
    hit = found['_mark'].notna() & (found['_mark'] <= found['t1'] + slack)
    out = np.zeros(len(episodes), dtype=bool)
    out[found['_row'].to_numpy()] = hit.to_numpy()
    return out


def classify_preempt_calls(
    calls: Optional[pd.DataFrame],
    events: Optional[pd.DataFrame] = None,
    rail_preempts: Optional[Iterable[int]] = None,
) -> pd.DataFrame:
    """Give every call a Class: CLASS_EV, CLASS_TSP, CLASS_BUS_PREEMPT, CLASS_RAIL or CLASS_UNCLASSIFIED.

    A constant call (CONSTANT_MIN_SECONDS or more) is an emergency vehicle
    preempt. A burst of pulses (each under PULSE_MAX_SECONDS) is never normal: a
    controller set up for TSP logs no preempt calls for a bus. Calls on one input
    are grouped into episodes (EPISODE_GAP_SECONDS) and each episode is classed
    as a whole:

    1. an input in ``rail_preempts``: Rail;
    2. any constant call: Emergency Vehicle (pulses beside it are part of it);
    3. not a burst of pulses (an unclosed call, or calls of 1.5 to 5 s that are
       not more than BURST_MIN_PULSE_SHARE pulses): Unclassified;
    4. a burst on an input that does not carry bus calls: Emergency Vehicle, an
       input chattering, which the chatter check reads;
    5. a burst on a bus input with a preempt entry (105): Bus Ran Preempt;
    6. other bursts on a bus input: TSP (the bus priority call ran no preempt).

    An input carries bus calls when TSP check-ins (112) come with its bursts
    (BUS_INPUT_MIN_CHECK_IN_SHARE). The check-in carries a TSP number rather than
    the input, so it is matched by signal alone. ``events`` are the raw preempt
    events (PREEMPT_EVENT_COLUMNS); without them, or at a signal that logs no
    check-ins, every burst is chatter.
    """
    calls = _clean_calls(calls)
    if calls.empty:
        return calls
    calls = calls.sort_values(['DeviceId', 'Preempt', 'StartTime'], kind='stable').reset_index(drop=True)
    duration = calls['Duration'].where(calls['IsValid'])
    episode = _episode_ids(calls)
    ends = calls['StartTime'] + pd.to_timedelta(duration.fillna(0).clip(lower=0), unit='s')
    frame = pd.DataFrame({
        'Episode': episode, 'DeviceId': calls['DeviceId'], 'Preempt': calls['Preempt'],
        'Start': calls['StartTime'], 'End': ends,
        'Constant': duration >= CONSTANT_MIN_SECONDS, 'Pulse': duration < PULSE_MAX_SECONDS,
        'Unclosed': duration.isna(),
    })
    episodes = frame.groupby('Episode').agg(
        DeviceId=('DeviceId', 'first'), Preempt=('Preempt', 'first'), t0=('Start', 'min'), t1=('End', 'max'),
        Constant=('Constant', 'any'), Unclosed=('Unclosed', 'any'), PulseShare=('Pulse', 'mean'),
    )
    episodes['t0'] = episodes['t0'].astype('datetime64[ns]')
    episodes['t1'] = episodes['t1'].astype('datetime64[ns]')

    entry = np.zeros(len(episodes), dtype=bool)
    bus = np.zeros(len(episodes), dtype=bool)
    events = _clean_events(events)
    pulses = (~episodes['Constant'] & ~episodes['Unclosed'] & (episodes['PulseShare'] > BURST_MIN_PULSE_SHARE)).to_numpy()
    if not events.empty and pulses.any():
        bursts = episodes[pulses].reset_index(drop=True)
        entries = events[events['EventId'] == EVENT_ENTRY].rename(columns={'Parameter': 'Preempt'})
        check_ins = events[events['EventId'] == EVENT_TSP_CHECK_IN]
        entry[pulses] = _evidence(bursts, entries, ['DeviceId', 'Preempt'], EVIDENCE_SLACK_SECONDS)
        if not check_ins.empty:
            bursts['CheckIn'] = _evidence(bursts, check_ins, ['DeviceId'], EVIDENCE_SLACK_SECONDS)
            first_check_in = bursts['DeviceId'].map(check_ins.groupby('DeviceId')['TimeStamp'].min())
            share = bursts[bursts['t1'] >= first_check_in].groupby(['DeviceId', 'Preempt'])['CheckIn'].mean()
            bus_inputs = share[share >= BUS_INPUT_MIN_CHECK_IN_SHARE].index
            bus[pulses] = pd.MultiIndex.from_frame(bursts[['DeviceId', 'Preempt']]).isin(bus_inputs)

    rail = episodes['Preempt'].isin(_rail_set(rail_preempts)).to_numpy()
    episodes['Class'] = np.select(
        [rail, episodes['Constant'].to_numpy(), ~pulses, ~bus, entry],
        [CLASS_RAIL, CLASS_EV, CLASS_UNCLASSIFIED, CLASS_EV, CLASS_BUS_PREEMPT],
        default=CLASS_TSP,
    )
    calls['Class'] = episodes['Class'].reindex(episode).to_numpy()
    return calls


def preempt_episode_days(calls: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Episodes (vehicles, or bus calls) per signal, preempt, day and class: DeviceId, Preempt, Date, Class, Count.

    ``calls`` carry Class; an episode is counted on the day it starts, under its class.
    """
    cols = ['DeviceId', 'Preempt', 'Date', 'Class', 'Count']
    calls = _clean_calls(calls)
    if calls.empty:
        return pd.DataFrame(columns=cols)
    calls = calls.sort_values(['DeviceId', 'Preempt', 'StartTime'], kind='stable').reset_index(drop=True)
    calls['Episode'] = _episode_ids(calls)
    episodes = calls.groupby('Episode').agg(DeviceId=('DeviceId', 'first'), Preempt=('Preempt', 'first'),
                                            Start=('StartTime', 'min'), Class=('Class', 'first'))
    episodes['Date'] = episodes['Start'].dt.normalize()
    return (episodes.groupby(['DeviceId', 'Preempt', 'Date', 'Class'], as_index=False).size()
            .rename(columns={'size': 'Count'})[cols])


def bus_calls_text(direction, ran, total) -> str:
    """A bus alert's counts in a few words, for a table cell or a chart title."""
    if direction == DIRECTION_BUS_INTERRUPTED:
        return (f'{int(total)} bus calls in the last 7 days pulsed this preempt input; '
                f'{int(ran) or "none"} ran as full preempts')
    return f'{int(ran)} of {int(total)} bus calls in the last 7 days ran as full preempts'


def build_bus_preempt_alerts(
    calls: pd.DataFrame,
    report_date,
    recent_days: int = 7,
    min_episodes: int = BUS_PREEMPT_MIN_EPISODES,
) -> pd.DataFrame:
    """Inputs whose bus priority calls pulsed the preempt input in the recent window.

    ``calls`` carry Class (classify_preempt_calls). A bus call is one episode of
    TSP or Bus Ran Preempt calls. An input alerts when it has at least
    ``min_episodes`` bus calls in the last ``recent_days``: Ran As Preempt when
    that many ran as preempts, else Priority Interrupted. RecentPerDay and
    BaselinePerDay are the input's bus calls per day in and before the window,
    DailyCounts the same per day; Score is the ones that ran as preempts per day.
    """
    cols = ['DeviceId', 'Preempt', 'Class', 'Direction', 'Score', 'Note', 'BusCalls', 'RanAsPreempt',
            'BaselinePerDay', 'RecentPerDay', 'DailyCounts', 'Date']
    calls = _clean_calls(calls)
    bus = calls[calls['Class'].isin(TSP_CLASSES)]
    if bus.empty:
        return pd.DataFrame(columns=cols)
    bus = bus.sort_values(['DeviceId', 'Preempt', 'StartTime'], kind='stable').reset_index(drop=True)
    bus['Episode'] = _episode_ids(bus)
    episodes = bus.groupby('Episode').agg(DeviceId=('DeviceId', 'first'), Preempt=('Preempt', 'first'),
                                          Start=('StartTime', 'min'), Class=('Class', 'first'))
    start, end = _recent_window(report_date, recent_days)
    episodes = episodes[episodes['Start'] < end]
    episodes['Ran'] = episodes['Class'] == CLASS_BUS_PREEMPT
    recent = episodes[episodes['Start'] >= start]
    summary = recent.groupby(['DeviceId', 'Preempt']).agg(BusCalls=('Ran', 'size'), RanAsPreempt=('Ran', 'sum'))
    summary = summary[summary['BusCalls'] >= min_episodes]
    if summary.empty:
        return pd.DataFrame(columns=cols)
    first_day = calls['StartTime'].min().normalize()
    days = pd.date_range(first_day, end - pd.Timedelta(days=1), freq='D')
    base_days = max(int((start - first_day).days), 1)
    rows = []
    for (device, preempt), s in summary.iterrows():
        mine = episodes[(episodes['DeviceId'] == device) & (episodes['Preempt'] == preempt)]
        daily = mine.groupby(mine['Start'].dt.normalize()).size()
        ran, total = int(s['RanAsPreempt']), int(s['BusCalls'])
        if ran >= min_episodes:
            direction = DIRECTION_BUS_PREEMPT
            note = (f'Bus priority calls ran as full preempts: {ran} of {total} bus calls in the last '
                    f'{recent_days} days. {BUS_PREEMPT_ADVICE}')
        else:
            direction = DIRECTION_BUS_INTERRUPTED
            note = (f'Bus priority calls pulsed this preempt input: {total} in the last {recent_days} days, '
                    f'{ran} ran as full preempts. {BUS_INTERRUPTED_ADVICE}')
        rows.append({
            'DeviceId': device, 'Preempt': int(preempt), 'Class': SECTION_TSP,
            'Direction': direction, 'Score': ran / recent_days, 'Note': note,
            'BusCalls': total, 'RanAsPreempt': ran,
            'BaselinePerDay': int((mine['Start'] < start).sum()) / base_days,
            'RecentPerDay': total / recent_days,
            'DailyCounts': [int(daily.get(day, 0)) for day in days],
            'Date': pd.Timestamp(report_date).normalize(),
        })
    return pd.DataFrame(rows, columns=cols)


def reporting_bins(has_data: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """The bins each signal reported in (columns DeviceId, Bin), or None when unknown."""
    if has_data is None or has_data.empty:
        return None
    bins = pd.DataFrame({
        'DeviceId': has_data['DeviceId'].astype(str),
        'Bin': pd.to_datetime(has_data['TimeStamp'], errors='coerce').dt.floor(REPORTING_BIN),
    }).dropna().drop_duplicates().reset_index(drop=True)
    return bins if not bins.empty else None


def _reporting(devices, times, bins: Optional[pd.DataFrame]) -> np.ndarray:
    """True where the device reported in the bin holding each time.

    Times outside the span ``bins`` covers count as reporting: no record there
    is not evidence of a gap.
    """
    devices = pd.Series(devices, dtype=object).astype(str).reset_index(drop=True)
    if bins is None or devices.empty:
        return np.ones(len(devices), dtype=bool)
    query = pd.DataFrame({
        'DeviceId': devices,
        'Bin': pd.to_datetime(pd.Series(times)).reset_index(drop=True).dt.floor(REPORTING_BIN),
    })
    known = query['Bin'].between(bins['Bin'].min(), bins['Bin'].max()).to_numpy()
    seen = query.merge(bins.assign(Seen=True), on=['DeviceId', 'Bin'], how='left')['Seen']
    return ~known | seen.fillna(False).astype(bool).to_numpy()


def _reporting_throughout(devices, starts, ends, bins: Optional[pd.DataFrame]) -> np.ndarray:
    """True where the device reported in every bin from each start to its end."""
    n = len(starts)
    if bins is None or n == 0:
        return np.ones(n, dtype=bool)
    spans = pd.DataFrame({
        'DeviceId': pd.Series(devices, dtype=object).astype(str).to_numpy(),
        'Row': np.arange(n),
        'Bin': [pd.date_range(s.floor(REPORTING_BIN), e.floor(REPORTING_BIN), freq=REPORTING_BIN)
                for s, e in zip(pd.to_datetime(pd.Series(starts)), pd.to_datetime(pd.Series(ends)))],
    }).explode('Bin').dropna(subset=['Bin'])
    ok = pd.Series(_reporting(spans['DeviceId'], spans['Bin'], bins)).groupby(spans['Row'].to_numpy()).all()
    return ok.reindex(range(n), fill_value=True).to_numpy()


def preempt_calls_from_timeline(timeline: Optional[pd.DataFrame]) -> pd.DataFrame:
    """One row per preempt call (a timeline ``Preempt`` interval)."""
    if timeline is None or timeline.empty or 'EventClass' not in timeline.columns:
        return _empty_calls()
    rows = timeline[text_equals(timeline['EventClass'], 'Preempt')]
    return _clean_calls(rows.rename(columns={'EventValue': 'Preempt'}))


def update_preempt_calls(
    new_calls: Optional[pd.DataFrame],
    past_calls: Optional[pd.DataFrame],
    retention_days: int = PREEMPT_CALL_HISTORY_DAYS,
) -> pd.DataFrame:
    """Merge newly read calls into the stored record and drop expired days.

    Days present in ``new_calls`` replace the stored calls for those days, so
    rerunning a day, or passing several days of timeline, never duplicates.
    """
    past = _clean_calls(past_calls)
    new = _clean_calls(new_calls)
    if not new.empty and not past.empty:
        refreshed = set(new['StartTime'].dt.normalize().unique())
        past = past[~past['StartTime'].dt.normalize().isin(refreshed)]
    frames = [df for df in (past, new) if not df.empty]
    if not frames:
        return _empty_calls()
    combined = pd.concat(frames, ignore_index=True)
    if retention_days > 0:
        cutoff = combined['StartTime'].max().normalize() - pd.Timedelta(days=retention_days - 1)
        combined = combined[combined['StartTime'] >= cutoff]
    return combined.sort_values(['StartTime', 'DeviceId']).reset_index(drop=True)


def _coordinate_columns(signals: pd.DataFrame):
    lower = {str(col).lower(): col for col in signals.columns}
    lat = next((lower[k] for k in ('latitude', 'lat') if k in lower), None)
    lon = next((lower[k] for k in ('longitude', 'lon', 'lng', 'long') if k in lower), None)
    return lat, lon


def signal_neighbours(signals: Optional[pd.DataFrame], max_miles: float = CASCADE_MAX_MILES) -> pd.DataFrame:
    """Ordered pairs of signals within ``max_miles`` (columns DeviceId, NextDeviceId, Miles).

    Empty when the signals table carries no coordinates, which switches the
    cascade checks off.
    """
    empty = pd.DataFrame(columns=['DeviceId', 'NextDeviceId', 'Miles'])
    if signals is None or signals.empty:
        return empty
    lat_col, lon_col = _coordinate_columns(signals)
    if lat_col is None or lon_col is None:
        return empty
    sig = pd.DataFrame({
        'DeviceId': signals['DeviceId'].astype(str),
        'lat': pd.to_numeric(signals[lat_col], errors='coerce'),
        'lon': pd.to_numeric(signals[lon_col], errors='coerce'),
    }).dropna().drop_duplicates('DeviceId').reset_index(drop=True)
    sig = sig[(sig['lat'] != 0) & (sig['lon'] != 0)]
    if len(sig) < 2:
        return empty
    lat = np.radians(sig['lat'].to_numpy())
    lon = np.radians(sig['lon'].to_numpy())
    dlat = lat[:, None] - lat[None, :]
    dlon = lon[:, None] - lon[None, :]
    a = np.sin(dlat / 2) ** 2 + np.cos(lat)[:, None] * np.cos(lat)[None, :] * np.sin(dlon / 2) ** 2
    miles = 3958.8 * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    i, j = np.nonzero((miles <= max_miles) & ~np.eye(len(sig), dtype=bool))
    ids = sig['DeviceId'].to_numpy()
    return pd.DataFrame({'DeviceId': ids[i], 'NextDeviceId': ids[j], 'Miles': miles[i, j]})


def cascade_follows(
    calls: pd.DataFrame,
    neighbours: pd.DataFrame,
    min_mph: float = CASCADE_MIN_MPH,
    max_mph: float = CASCADE_MAX_MPH,
    slack_seconds: float = CASCADE_SLACK_SECONDS,
) -> pd.DataFrame:
    """Every call that a call at a neighbouring signal started within the travel window of.

    One row per source call and following preempt: DeviceId, Preempt,
    StartTime (the source call), NextDeviceId, NextPreempt, NextStartTime (the
    first following call), Miles.
    """
    cols = ['DeviceId', 'Preempt', 'StartTime', 'NextDeviceId', 'NextPreempt', 'NextStartTime', 'Miles']
    calls = _clean_calls(calls)
    if calls.empty or neighbours is None or neighbours.empty:
        return pd.DataFrame(columns=cols)
    con = duckdb.connect()
    try:
        con.register('calls', calls[['DeviceId', 'Preempt', 'StartTime']])
        con.register('nb', neighbours)
        # The first call of each neighbouring preempt at or after the start of
        # the travel window (an as-of join), kept when it is inside the window:
        # the earliest call in the window, without pairing every call with
        # every neighbouring call.
        return con.execute(f"""
            WITH windows AS (
                SELECT a.DeviceId, a.Preempt, a.StartTime, n.NextDeviceId, p.Preempt AS NextPreempt, n.Miles,
                       a.StartTime + to_seconds(greatest(0, n.Miles / {max_mph} * 3600 - {slack_seconds})) AS WindowStart,
                       a.StartTime + to_seconds(n.Miles / {min_mph} * 3600 + {slack_seconds}) AS WindowEnd
                FROM calls a
                JOIN nb n ON n.DeviceId = a.DeviceId
                JOIN (SELECT DISTINCT DeviceId, Preempt FROM calls) p ON p.DeviceId = n.NextDeviceId
            )
            SELECT w.DeviceId, w.Preempt, w.StartTime,
                   w.NextDeviceId, w.NextPreempt,
                   min(b.StartTime) AS NextStartTime, any_value(w.Miles) AS Miles
            FROM windows w
            ASOF JOIN calls b ON b.DeviceId = w.NextDeviceId AND b.Preempt = w.NextPreempt
             AND b.StartTime >= w.WindowStart
            WHERE b.StartTime <= w.WindowEnd
            GROUP BY w.DeviceId, w.Preempt, w.StartTime, w.NextDeviceId, w.NextPreempt
        """).df()
    finally:
        con.close()


def _window_seconds(miles, min_mph, max_mph, slack_seconds):
    return (miles / min_mph * 3600 + slack_seconds) - np.maximum(0, miles / max_mph * 3600 - slack_seconds)


def build_cascade_links(
    calls: pd.DataFrame,
    follows: pd.DataFrame,
    before=None,
    min_followed: int = LINK_MIN_FOLLOWED,
    min_follow_rate: float = LINK_MIN_FOLLOW_RATE,
    min_lift: float = LINK_MIN_LIFT,
    min_mph: float = CASCADE_MIN_MPH,
    max_mph: float = CASCADE_MAX_MPH,
    slack_seconds: float = CASCADE_SLACK_SECONDS,
    rail_preempts: Optional[Iterable[int]] = None,
) -> pd.DataFrame:
    """Learn which preempts cascade into which from calls before ``before``.

    FollowRate is shrunk towards a half ((followed + 1) / (calls + 2)) so a link
    seen four times out of four is not taken as certain. Lift compares the
    followed count with what two independent preempts at those call rates would
    produce by chance inside the travel window. With ``rail_preempts`` a link
    only joins two rail inputs or two emergency vehicle inputs: a train does not
    cascade into a fire truck.
    """
    calls = _clean_calls(calls)
    if calls.empty or follows is None or follows.empty:
        return pd.DataFrame(columns=CASCADE_LINK_COLUMNS)
    if before is not None:
        before = pd.Timestamp(before)
        calls = calls[calls['StartTime'] < before]
        follows = follows[follows['StartTime'] < before]
    if calls.empty or follows.empty:
        return pd.DataFrame(columns=CASCADE_LINK_COLUMNS)

    span_seconds = max(
        (calls['StartTime'].max() - calls['StartTime'].min()).total_seconds(), 86400.0
    )
    counts = calls.groupby(['DeviceId', 'Preempt']).size()
    f = follows.copy()
    f['Lag'] = (pd.to_datetime(f['NextStartTime']) - pd.to_datetime(f['StartTime'])).dt.total_seconds()
    links = (
        f.groupby(['DeviceId', 'Preempt', 'NextDeviceId', 'NextPreempt'], as_index=False)
        .agg(Miles=('Miles', 'first'), Followed=('StartTime', 'nunique'), MedianLagSeconds=('Lag', 'median'))
    )
    links['Calls'] = [counts.get((d, p), 0) for d, p in zip(links['DeviceId'], links['Preempt'])]
    next_calls = np.array([counts.get((d, p), 0) for d, p in zip(links['NextDeviceId'], links['NextPreempt'])])
    chance = links['Calls'] * next_calls / span_seconds * _window_seconds(
        links['Miles'], min_mph, max_mph, slack_seconds
    )
    links['Lift'] = links['Followed'] / np.maximum(chance, 1e-9)
    links['FollowRate'] = (links['Followed'] + 1) / (links['Calls'] + 2)
    links = links[
        (links['Followed'] >= min_followed)
        & (links['FollowRate'] >= min_follow_rate)
        & (links['Lift'] >= min_lift)
    ]
    if rail_preempts is not None:
        rail = _rail_set(rail_preempts)
        links = links[links['Preempt'].isin(rail) == links['NextPreempt'].isin(rail)]
    return links[CASCADE_LINK_COLUMNS].reset_index(drop=True)


def _recent_window(report_date, recent_days):
    end = pd.Timestamp(report_date).normalize() + pd.Timedelta(days=1)
    return end - pd.Timedelta(days=recent_days), end


def review_preempt_alerts(
    alerts: pd.DataFrame,
    calls: pd.DataFrame,
    follows: pd.DataFrame,
    links: pd.DataFrame,
    report_date,
    recent_days: int = 7,
    reporting: Optional[pd.DataFrame] = None,
    min_mph: float = CASCADE_MIN_MPH,
    slack_seconds: float = CASCADE_SLACK_SECONDS,
) -> pd.DataFrame:
    """Add Cascade (Confirmed / Overruled / blank) and Note to frequency alerts.

    ``links`` must be learned from calls before the recent window. A vehicle
    only counts as one this preempt should have answered when its signal was
    reporting (``reporting``, from reporting_bins) from the upstream call to
    the latest the vehicle could have arrived.

    Decrease: confirmed when linked neighbours sent enough vehicles that this
    preempt should have answered and it answered almost none; overruled when it
    answered any of them, or when the neighbours went just as quiet (the vehicle,
    route or train stopped, not the detector).
    Increase: overruled when the neighbours rose as well (real extra traffic);
    confirmed when they did not.
    """
    out = alerts.copy()
    out['Cascade'] = ''
    out['Note'] = ''
    if out.empty or links is None or links.empty:
        return out
    calls = _clean_calls(calls)
    start, end = _recent_window(report_date, recent_days)
    base_calls = calls[calls['StartTime'] < start]
    recent_calls = calls[(calls['StartTime'] >= start) & (calls['StartTime'] < end)]
    base_days = max(base_calls['StartTime'].dt.normalize().nunique(), 1)
    base_n = base_calls.groupby(['DeviceId', 'Preempt']).size()
    recent_n = recent_calls.groupby(['DeviceId', 'Preempt']).size()
    rf = follows[(follows['StartTime'] >= start) & (follows['StartTime'] < end)]
    followed_keys = set(zip(rf['DeviceId'], rf['Preempt'], rf['StartTime'], rf['NextDeviceId'], rf['NextPreempt']))

    for idx, row in out.iterrows():
        node = (str(row['DeviceId']), int(row['Preempt']))
        upstream = links[(links['NextDeviceId'] == node[0]) & (links['NextPreempt'] == node[1])]
        downstream = links[(links['DeviceId'] == node[0]) & (links['Preempt'] == node[1])]
        partners = set(zip(upstream['DeviceId'], upstream['Preempt'])) | set(
            zip(downstream['NextDeviceId'], downstream['NextPreempt'])
        )
        partners.discard(node)
        if not partners:
            out.at[idx, 'Note'] = 'no linked preempts to compare with'
            continue
        expected_partner = sum(base_n.get(p, 0) for p in partners) / base_days * recent_days
        observed_partner = sum(recent_n.get(p, 0) for p in partners)
        ratio = observed_partner / expected_partner if expected_partner > 0 else np.nan
        # One vehicle often calls several upstream preempts; group their calls
        # into vehicles so each counts once. A vehicle's chance of reaching X
        # is its best link's follow rate, which already allows for vehicles
        # that leave the corridor part way.
        up_calls = recent_calls.merge(
            upstream[['DeviceId', 'Preempt', 'FollowRate', 'Miles']], on=['DeviceId', 'Preempt']
        ).sort_values('StartTime')
        if not up_calls.empty:
            latest = up_calls['StartTime'] + pd.to_timedelta(
                up_calls['Miles'] / min_mph * 3600 + slack_seconds, unit='s'
            )
            up_calls = up_calls[_reporting_throughout(
                [node[0]] * len(up_calls), up_calls['StartTime'], latest, reporting
            )]
        expected_follow, observed_follow = 0.0, 0
        if not up_calls.empty:
            up_calls['Followed'] = [
                (d, p, t, node[0], node[1]) in followed_keys
                for d, p, t in zip(up_calls['DeviceId'], up_calls['Preempt'], up_calls['StartTime'])
            ]
            up_calls['Vehicle'] = _group_vehicles(
                up_calls['StartTime'], up_calls['StartTime'], VEHICLE_GAP_SECONDS
            )
            vehicles = up_calls.groupby('Vehicle').agg(Rate=('FollowRate', 'max'), Followed=('Followed', 'any'))
            expected_follow = float(vehicles['Rate'].sum())
            observed_follow = int(vehicles['Followed'].sum())
        partner_text = f'{len(partners)} linked preempt{"s" if len(partners) != 1 else ""}'

        if row['Direction'] == 'Decrease':
            if expected_follow >= REVIEW_MIN_EXPECTED_FOLLOWS and observed_follow <= REVIEW_MAX_FOLLOW_SHARE * expected_follow:
                verdict = VERDICT_CONFIRMED
                note = f'{partner_text} kept calling; answered {observed_follow} of ~{expected_follow:.0f} expected vehicles'
            elif observed_follow >= 1:
                # Any vehicle it answered shows the input works. Answering only
                # some is normal: vehicles leave the corridor part way.
                verdict = VERDICT_OVERRULED
                note = f'answered {observed_follow} cascading vehicle{"s" if observed_follow != 1 else ""} in the last {recent_days} days'
            elif expected_partner > 0 and ratio <= REVIEW_QUIET_RATIO:
                verdict = VERDICT_OVERRULED
                note = f'{partner_text} also quiet ({observed_partner} calls vs ~{expected_partner:.0f} usual)'
            else:
                continue
        else:
            if expected_partner > 0 and ratio >= REVIEW_RISE_RATIO:
                verdict = VERDICT_OVERRULED
                note = f'{partner_text} rose too ({observed_partner} calls vs ~{expected_partner:.0f} usual)'
            else:
                verdict = VERDICT_CONFIRMED
                note = f'{partner_text} unchanged ({observed_partner} calls vs ~{expected_partner:.0f} usual)'
        out.at[idx, 'Cascade'] = verdict
        out.at[idx, 'Note'] = note
    return out


def _group_vehicles(starts: pd.Series, ends: pd.Series, gap_seconds: float) -> np.ndarray:
    """Number the rows by vehicle: a row starting no later than ``gap_seconds``
    after the previous rows' latest end belongs to the same vehicle.

    ``starts`` must be sorted.
    """
    labels = np.zeros(len(starts), dtype=int)
    current, reach = 0, None
    gap = pd.Timedelta(seconds=gap_seconds)
    for i, (t0, t1) in enumerate(zip(starts, ends)):
        if reach is not None and t0 > reach + gap:
            current += 1
            reach = None
        reach = t1 if reach is None else max(reach, t1)
        labels[i] = current
    return labels


def _binom_cdf(k: int, n: int, p: float) -> float:
    return float(sum(math.comb(n, i) * p ** i * (1 - p) ** (n - i) for i in range(k + 1)))


def build_cascade_break_alerts(
    calls: pd.DataFrame,
    follows: pd.DataFrame,
    links: pd.DataFrame,
    report_date,
    recent_days: int = 7,
    min_passes: int = BREAK_MIN_PASSES,
    min_baseline_passes: int = BREAK_MIN_BASELINE_PASSES,
    max_ratio: float = BREAK_MAX_RATIO,
    max_p_value: float = BREAK_MAX_P_VALUE,
    max_detour: float = BREAK_MAX_DETOUR,
    reporting: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Preempts that vehicles seen on both sides of them stopped calling.

    For every linked chain U -> X -> D (U and D different signals), a U call
    followed by a D call inside the U -> D travel window is a vehicle that
    drove past X, provided X lies on the way (U -> X -> D at most
    ``max_detour`` times U -> D). Passes that overlap in time are one vehicle
    seen through several chains and count once. X should have been called
    between them. Its pass-through
    rate before the recent window is compared with the recent one; a break
    needs ``min_passes`` recent passes, ``min_baseline_passes`` earlier ones
    to know the usual rate by, a recent rate at most ``max_ratio`` of the
    usual, and a binomial tail below ``max_p_value``. Passes while X was not
    reporting (``reporting``, from reporting_bins) are left out, recent and
    baseline alike.
    """
    cols = ['DeviceId', 'Preempt', 'Direction', 'Passes', 'Called', 'BaselineRate', 'Score', 'Note']
    if links is None or links.empty or follows is None or follows.empty:
        return pd.DataFrame(columns=cols)
    calls = _clean_calls(calls)
    start, end = _recent_window(report_date, recent_days)

    legs = links[['DeviceId', 'Preempt', 'NextDeviceId', 'NextPreempt', 'Miles']]
    chains = legs.merge(
        legs.rename(columns={'DeviceId': 'X', 'Preempt': 'PX', 'NextDeviceId': 'D',
                             'NextPreempt': 'PD', 'Miles': 'MilesXD'}),
        left_on=['NextDeviceId', 'NextPreempt'], right_on=['X', 'PX'],
    )
    chains = chains[chains['D'] != chains['DeviceId']]
    chains = chains.assign(Via=chains['Miles'] + chains['MilesXD'])[
        ['DeviceId', 'Preempt', 'X', 'PX', 'D', 'PD', 'Via']
    ].drop_duplicates(['DeviceId', 'Preempt', 'X', 'PX', 'D', 'PD'])
    if chains.empty:
        return pd.DataFrame(columns=cols)

    con = duckdb.connect()
    try:
        con.register('chains', chains)
        con.register('fol', follows[['DeviceId', 'Preempt', 'StartTime', 'NextDeviceId', 'NextPreempt', 'NextStartTime', 'Miles']])
        con.register('calls', calls[['DeviceId', 'Preempt', 'StartTime']])
        passes = con.execute("""
            WITH v AS (
                SELECT c.X, c.PX, f.StartTime AS t0, f.NextStartTime AS t1
                FROM chains c JOIN fol f
                  ON f.DeviceId = c.DeviceId AND f.Preempt = c.Preempt
                 AND f.NextDeviceId = c.D AND f.NextPreempt = c.PD
                WHERE c.Via <= f.Miles * ?
            ), v1 AS (SELECT DISTINCT X, PX, t0, t1 FROM v)
            SELECT v1.X, v1.PX, v1.t0, v1.t1,
                   bool_or(x.StartTime IS NOT NULL) AS hit
            FROM v1 LEFT JOIN calls x
              ON x.DeviceId = v1.X AND x.Preempt = v1.PX
             AND x.StartTime BETWEEN v1.t0 - INTERVAL 60 SECOND AND v1.t1 + INTERVAL 60 SECOND
            GROUP BY v1.X, v1.PX, v1.t0, v1.t1
        """, [max_detour]).df()
    finally:
        con.close()
    if passes.empty:
        return pd.DataFrame(columns=cols)

    passes = passes[passes['t0'] < end].reset_index(drop=True)
    margin = pd.Timedelta(seconds=60)
    passes = passes[_reporting_throughout(passes['X'], passes['t0'] - margin, passes['t1'] + margin, reporting)]
    if passes.empty:
        return pd.DataFrame(columns=cols)
    passes = passes.sort_values(['X', 'PX', 't0']).reset_index(drop=True)
    # Overlapping passes are one vehicle seen through several chains; it
    # called X if any of them did.
    passes['Vehicle'] = 0
    for _, idx in passes.groupby(['X', 'PX']).groups.items():
        passes.loc[idx, 'Vehicle'] = _group_vehicles(passes.loc[idx, 't0'], passes.loc[idx, 't1'], 0)
    passes = passes.groupby(['X', 'PX', 'Vehicle'], as_index=False).agg(t0=('t0', 'min'), hit=('hit', 'any'))
    passes['recent'] = passes['t0'] >= start
    rows = []
    for (x, px), grp in passes.groupby(['X', 'PX']):
        base = grp[~grp['recent']]
        recent = grp[grp['recent']]
        n, hits = len(recent), int(recent['hit'].sum())
        if n < min_passes or len(base) < min_baseline_passes:
            continue
        rate = (base['hit'].sum() + 1) / (len(base) + 2)
        if hits / n > max_ratio * rate:
            continue
        p_value = _binom_cdf(hits, n, rate)
        if p_value >= max_p_value:
            continue
        rows.append({
            'DeviceId': x, 'Preempt': int(px), 'Direction': DIRECTION_BREAK,
            'Passes': n, 'Called': hits, 'BaselineRate': rate,
            'Score': -math.log10(max(p_value, 1e-15)),
            'Note': f'{n} vehicles passed between linked neighbours, {hits} called (usually {rate:.0%})',
        })
    return pd.DataFrame(rows, columns=cols)


def build_preempt_duration_alerts(
    calls: pd.DataFrame,
    neighbours: Optional[pd.DataFrame],
    report_date,
    recent_days: int = 7,
    rail_preempts: Optional[Iterable[int]] = None,
    stuck_seconds_ev: float = STUCK_SECONDS_EV,
    stuck_seconds_rail: float = STUCK_SECONDS_RAIL,
    chatter_max_seconds: float = CHATTER_MAX_SECONDS,
    chatter_min_per_day: float = CHATTER_MIN_PER_DAY,
    chatter_min_share: float = CHATTER_MIN_SHARE,
    reporting: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Stuck-on and chattering preempt inputs over the recent window.

    Stuck On: a closed call held longer than the stuck limit (rail or EV;
    everything counts as rail when ``rail_preempts`` is None), unless a
    preempt at a neighbouring signal was held at least half as long over the
    same time, or the signal stopped reporting during it (``reporting``, from
    reporting_bins), since the end of the call may be what was lost. Long
    Hold: a rail call held that long while another rail
    preempt at the same signal served normal calls inside it. At a crossing
    that is the advance or companion input staying on (a train parked on the
    approach or a siding) while the crossing itself works, so it is kept
    apart from Stuck On. Chatter: at least ``chatter_min_per_day`` calls a day
    shorter than ``chatter_max_seconds``, making up at least
    ``chatter_min_share`` of the preempt's calls.
    """
    cols = ['DeviceId', 'Preempt', 'Direction', 'Score', 'Note']
    calls = _clean_calls(calls)
    if calls.empty:
        return pd.DataFrame(columns=cols)
    start, end = _recent_window(report_date, recent_days)
    recent = calls[(calls['StartTime'] >= start) & (calls['StartTime'] < end) & calls['IsValid']].copy()
    if recent.empty:
        return pd.DataFrame(columns=cols)
    recent['EndTime'] = recent['StartTime'] + pd.to_timedelta(recent['Duration'], unit='s')
    rail = None if rail_preempts is None else {int(p) for p in rail_preempts}
    recent['Limit'] = stuck_seconds_rail if rail is None else np.where(
        recent['Preempt'].isin(rail), stuck_seconds_rail, stuck_seconds_ev
    )
    rows = []

    long_calls = recent[recent['Duration'] > recent['Limit']]
    long_calls = long_calls[_reporting_throughout(
        long_calls['DeviceId'], long_calls['StartTime'], long_calls['EndTime'], reporting
    )]
    if not long_calls.empty:
        explained = pd.Series(False, index=long_calls.index)
        if neighbours is not None and not neighbours.empty:
            all_calls = calls[calls['IsValid']].copy()
            all_calls['EndTime'] = all_calls['StartTime'] + pd.to_timedelta(all_calls['Duration'], unit='s')
            nb = neighbours.groupby('DeviceId')['NextDeviceId'].agg(set)
            for idx, c in long_calls.iterrows():
                others = all_calls[all_calls['DeviceId'].isin(nb.get(c['DeviceId'], set()))]
                overlap = others[
                    (others['StartTime'] < c['EndTime']) & (others['EndTime'] > c['StartTime'])
                    & (others['Duration'] >= STUCK_NEIGHBOUR_SHARE * c['Duration'])
                ]
                explained[idx] = not overlap.empty
        stuck = long_calls[~explained].copy()
        # Other rail preempts at the same signal that started and ended inside
        # the hold, within their own limit, were served normally.
        served = recent[recent['Duration'] <= recent['Limit']]
        is_rail = (lambda p: True) if rail is None else (lambda p: p in rail)
        stuck['ServedInside'] = [
            int(((served['DeviceId'] == c['DeviceId']) & (served['Preempt'] != c['Preempt'])
                 & served['Preempt'].map(is_rail)
                 & (served['StartTime'] > c['StartTime']) & (served['EndTime'] < c['EndTime'])).sum())
            if is_rail(c['Preempt']) else 0
            for _, c in stuck.iterrows()
        ]
        stuck['Kind'] = np.where(stuck['ServedInside'] > 0, DIRECTION_LONG_HOLD, DIRECTION_STUCK)
        for (device, preempt, kind), grp in stuck.groupby(['DeviceId', 'Preempt', 'Kind']):
            longest = grp.loc[grp['Duration'].idxmax()]
            summary = (f'{len(grp)} call{"s" if len(grp) != 1 else ""} over '
                       f'{grp["Limit"].iloc[0] / 60:.0f} min, longest {longest["Duration"] / 3600:.1f} h '
                       f'on {longest["StartTime"]:%m/%d %H:%M}')
            if kind == DIRECTION_LONG_HOLD:
                note = (f'{summary}; other rail preempts here served {int(grp["ServedInside"].sum())} '
                        f'normal calls during the holds')
            else:
                note = f'{summary}; no neighbour held'
            rows.append({
                'DeviceId': device, 'Preempt': int(preempt), 'Direction': kind,
                'Score': float(grp['Duration'].max() / 3600), 'Note': note,
            })

    recent['Short'] = recent['Duration'] < chatter_max_seconds
    summary = recent.groupby(['DeviceId', 'Preempt']).agg(Calls=('Short', 'size'), Short=('Short', 'sum'))
    summary = summary[
        (summary['Short'] >= chatter_min_per_day * recent_days)
        & (summary['Short'] >= chatter_min_share * summary['Calls'])
    ]
    for (device, preempt), s in summary.iterrows():
        rows.append({
            'DeviceId': device, 'Preempt': int(preempt), 'Direction': DIRECTION_CHATTER,
            'Score': float(s['Short'] / recent_days),
            'Note': f'{int(s["Short"])} of {int(s["Calls"])} calls under {chatter_max_seconds:g} s',
        })
    return pd.DataFrame(rows, columns=cols)


CALL_FINGERPRINT_COLUMNS = ['DeviceId', 'Preempt', 'Calls', 'SameLength', 'SameSeconds', 'SameLengthSeen', 'Seen',
                            'SeenKnown', 'Links', 'SameLengthLinked']


def _seen_nearby(calls: pd.DataFrame, follows: Optional[pd.DataFrame], rail: set) -> np.ndarray:
    """True for each call that a non-rail call at a neighbouring signal came before or after within the drive time."""
    if follows is None or follows.empty or calls.empty:
        return np.zeros(len(calls), dtype=bool)
    follows = follows[~follows['Preempt'].isin(rail) & ~follows['NextPreempt'].isin(rail)]
    seen = set(zip(follows['DeviceId'], follows['Preempt'], follows['StartTime'])) | set(
        zip(follows['NextDeviceId'], follows['NextPreempt'], follows['NextStartTime']))
    return np.array([key in seen for key in zip(calls['DeviceId'], calls['Preempt'], calls['StartTime'])], dtype=bool)


def _linked_follows(follows: Optional[pd.DataFrame], links: Optional[pd.DataFrame]) -> pd.DataFrame:
    """The rows of ``follows`` that run along a learned link."""
    if follows is None or follows.empty or links is None or links.empty:
        return pd.DataFrame(columns=[] if follows is None else follows.columns)
    pair = ['DeviceId', 'Preempt', 'NextDeviceId', 'NextPreempt']
    return follows.merge(links[pair].drop_duplicates(), on=pair)


def call_fingerprints(
    calls: pd.DataFrame,
    follows: Optional[pd.DataFrame],
    neighbours: Optional[pd.DataFrame],
    report_date,
    recent_days: int = 7,
    rail_preempts: Optional[Iterable[int]] = None,
    links: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """What each non-rail input's calls of the recent window look like, one row per input (CALL_FINGERPRINT_COLUMNS).

    Calls, SameLength (calls within SAME_LENGTH_SECONDS of the shortest call length: SHORTEST_CALL_QUANTILE of
    every call's length, chatter aside) and SameSeconds (their median length), SameLengthSeen and Seen (calls
    that a neighbouring signal also had within the drive time, from ``follows``) and SeenKnown (a neighbouring
    signal has calls to be seen at). Links counts the input's linked preempts (``links``, from
    build_cascade_links) and SameLengthLinked the shortest-length calls one of them also had.
    """
    rail = _rail_set(rail_preempts)
    calls = _clean_calls(calls)
    calls = calls[calls['IsValid'] & ~calls['Preempt'].isin(rail)].copy()
    lengths = calls.loc[calls['Duration'] >= CHATTER_MAX_SECONDS, 'Duration']
    shortest = float(lengths.quantile(SHORTEST_CALL_QUANTILE)) if len(lengths) else np.nan
    start, end = _recent_window(report_date, recent_days)
    recent = calls[(calls['StartTime'] >= start) & (calls['StartTime'] < end)].copy()
    if recent.empty:
        return pd.DataFrame(columns=CALL_FINGERPRINT_COLUMNS)
    keys = ['DeviceId', 'Preempt']
    recent['Seen'] = _seen_nearby(recent, follows, rail)
    recent['Linked'] = _seen_nearby(recent, _linked_follows(follows, links), rail)
    known = set() if neighbours is None or neighbours.empty else set(
        neighbours.loc[neighbours['NextDeviceId'].isin(set(calls['DeviceId'])), 'DeviceId'])
    recent['SeenKnown'] = recent['DeviceId'].isin(known)
    recent['Same'] = (recent['Duration'] - shortest).abs() <= SAME_LENGTH_SECONDS
    recent['SameSeconds'] = recent['Duration'].where(recent['Same'])
    recent['SameSeen'] = recent['Same'] & recent['Seen']
    recent['SameLinked'] = recent['Same'] & recent['Linked']
    prints = recent.groupby(keys).agg(
        Calls=('Same', 'size'), SameLength=('Same', 'sum'), SameSeconds=('SameSeconds', 'median'),
        SameLengthSeen=('SameSeen', 'sum'), Seen=('Seen', 'sum'), SeenKnown=('SeenKnown', 'first'),
        SameLengthLinked=('SameLinked', 'sum'),
    ).reset_index()
    partners = {}
    if links is not None and not links.empty:
        for a, pa, b, pb in zip(links['DeviceId'], links['Preempt'], links['NextDeviceId'], links['NextPreempt']):
            if (a, int(pa)) != (b, int(pb)):
                partners.setdefault((a, int(pa)), set()).add((b, int(pb)))
                partners.setdefault((b, int(pb)), set()).add((a, int(pa)))
    prints['Links'] = [len(partners.get((d, int(p)), ())) for d, p in zip(prints['DeviceId'], prints['Preempt'])]
    return prints[CALL_FINGERPRINT_COLUMNS]


def build_false_call_alerts(
    prints: pd.DataFrame,
    recent_days: int = 7,
    min_per_day: float = FALSE_CALLS_MIN_PER_DAY,
    min_share: float = FALSE_CALLS_MIN_SHARE,
    max_seen_share: float = FALSE_CALLS_MAX_SEEN_SHARE,
    alone_min_per_day: float = FALSE_CALLS_ALONE_MIN_PER_DAY,
    alone_min_share: float = FALSE_CALLS_ALONE_MIN_SHARE,
) -> pd.DataFrame:
    """Inputs whose recent calls are not vehicles (``prints`` from call_fingerprints).

    False Calls: at least ``min_per_day`` calls a day of the shortest call length, making up at least ``min_share``
    of the input's calls, of which at most ``max_seen_share`` were seen at a nearby signal. An input with no
    neighbouring signal to be seen at needs ``alone_min_per_day`` and ``alone_min_share`` instead: a vehicle seen
    for a moment makes such a call too. The Note states the calls; what nearby signals saw is told only for the
    input's linked preempts.
    """
    cols = ['DeviceId', 'Preempt', 'Direction', 'Score', 'Note']
    if prints is None or prints.empty:
        return pd.DataFrame(columns=cols)
    checked = prints['SeenKnown'].astype(bool)
    same, total = prints['SameLength'], prints['Calls']
    hit = prints[
        (checked & (same >= min_per_day * recent_days) & (same >= min_share * total)
         & (prints['SameLengthSeen'] <= max_seen_share * same))
        | (~checked & (same >= alone_min_per_day * recent_days) & (same >= alone_min_share * total))
    ]
    rows = []
    for p in hit.itertuples():
        note = (f'{int(p.SameLength)} of {int(p.Calls)} calls in the last {recent_days} days had the same duration, '
                f'lasting about {p.SameSeconds:.1f} s. This does not represent normal EVP calls.')
        if p.Links:
            seen = int(p.SameLengthLinked)
            note += (f" Its {int(p.Links)} linked preempt{'s' if p.Links != 1 else ''} saw "
                     f"{'none' if not seen else seen} of them.")
        rows.append({'DeviceId': p.DeviceId, 'Preempt': int(p.Preempt), 'Direction': DIRECTION_FALSE_CALLS,
                     'Score': float(p.SameLength / recent_days), 'Note': note})
    return pd.DataFrame(rows, columns=cols)


def review_increases(reviewed: pd.DataFrame, false_alerts: pd.DataFrame, prints: pd.DataFrame,
                     max_seen_share: float = FALSE_CALLS_MAX_SEEN_SHARE) -> tuple:
    """Keep an Increase only when its extra calls are not vehicles.

    Returns (reviewed, false_alerts). An Increase on an input in ``false_alerts`` is dropped and that alert's Note
    gains the rise. An Increase whose calls nearby signals saw (more than ``max_seen_share`` of them) is real
    traffic: it is marked Overruled. Other Increases are left as they are.
    """
    if reviewed.empty or prints is None or prints.empty:
        return reviewed, false_alerts
    reviewed, false_alerts = reviewed.copy(), false_alerts.copy()
    by_input = {(p.DeviceId, int(p.Preempt)): p for p in prints.itertuples()}
    false_rows = {(d, int(p)): i for i, d, p in zip(false_alerts.index, false_alerts['DeviceId'], false_alerts['Preempt'])}
    drop = []
    for idx, row in reviewed.iterrows():
        key = (str(row['DeviceId']), int(row['Preempt']))
        if row['Direction'] != 'Increase' or key not in by_input:
            continue
        if key in false_rows:
            false_alerts.at[false_rows[key], 'Note'] += (
                f" Up from {float(row['BaselinePerDay']):.1f} to {float(row['RecentPerDay']):.1f} calls a day.")
            drop.append(idx)
            continue
        p = by_input[key]
        if p.SeenKnown and p.Seen > max_seen_share * p.Calls:
            reviewed.at[idx, 'Cascade'] = VERDICT_OVERRULED
            reviewed.at[idx, 'Note'] = f'nearby signals had {int(p.Seen)} of its {int(p.Calls)} calls'
    return reviewed.drop(index=drop), false_alerts


def evaluate_preempt_cascades(
    frequency_alerts: pd.DataFrame,
    calls: pd.DataFrame,
    signals: Optional[pd.DataFrame],
    report_date,
    recent_days: int = 7,
    rail_preempts: Optional[Iterable[int]] = None,
    max_miles: float = CASCADE_MAX_MILES,
    min_mph: float = CASCADE_MIN_MPH,
    max_mph: float = CASCADE_MAX_MPH,
    overrule: bool = True,
    verbosity: int = 1,
    has_data: Optional[pd.DataFrame] = None,
    false_calls: bool = False,
) -> dict:
    """Run every cascade and duration check for one report day.

    ``false_calls`` adds the False Calls alert (build_false_call_alerts) and overrules an Increase whose calls
    nearby signals saw (review_increases).

    ``has_data`` (DeviceId, TimeStamp per 15-minute bin reported) keeps the
    checks from reading a signal's outage as missed or stuck calls; without it
    every signal is taken as reporting throughout.

    Only emergency vehicle, unclassified and rail calls are read: bus calls on a
    preempt input (Class, from classify_preempt_calls) are left out, so the
    cascade links, breaks and duration checks never see bus pulses.

    Returns a dict with:
      'alerts': frequency alerts that stand (with Cascade and Note), plus
                cascade breaks and duration alerts, all sharing Direction, and
                Class (the report section: SECTION_EV or SECTION_RAIL);
      'overruled': frequency alerts the cascade review overruled;
      'links': the cascade pattern table learned for this run.
    Overruled alerts stay in 'alerts' (marked) when ``overrule`` is False.
    """
    calls = _clean_calls(calls)
    calls = calls[~calls['Class'].isin(TSP_CLASSES)].reset_index(drop=True)
    neighbours = signal_neighbours(signals, max_miles)
    if neighbours.empty:
        log_message('No signal coordinates; preempt cascade checks are off.', 1, verbosity)
    follows = cascade_follows(calls, neighbours, min_mph, max_mph)
    start, _ = _recent_window(report_date, recent_days)
    links = build_cascade_links(calls, follows, before=start, min_mph=min_mph, max_mph=max_mph,
                                rail_preempts=rail_preempts)
    log_message(f'Preempt cascades: {len(links)} links among {len(neighbours)} neighbour pairs.', 1, verbosity)

    reporting = reporting_bins(has_data)
    reviewed = review_preempt_alerts(
        frequency_alerts, calls, follows, links, report_date, recent_days, reporting, min_mph
    )
    duration_alerts = build_preempt_duration_alerts(
        calls, neighbours, report_date, recent_days, rail_preempts, reporting=reporting
    )
    false_alerts = pd.DataFrame()
    if false_calls:
        prints = call_fingerprints(calls, follows, neighbours, report_date, recent_days, rail_preempts, links)
        false_alerts = build_false_call_alerts(prints, recent_days)
        # An input already told as chatter is not told twice.
        chatter = duration_alerts[duration_alerts['Direction'] == DIRECTION_CHATTER]
        told = set(zip(chatter['DeviceId'], chatter['Preempt']))
        new = np.array([key not in told for key in zip(false_alerts['DeviceId'], false_alerts['Preempt'])], dtype=bool)
        false_alerts = false_alerts[new].reset_index(drop=True)
        reviewed, false_alerts = review_increases(reviewed, false_alerts, prints)
    is_overruled = reviewed['Cascade'] == VERDICT_OVERRULED
    overruled = reviewed[is_overruled].reset_index(drop=True)
    kept = reviewed[~is_overruled] if overrule else reviewed

    extra_frames = [
        df for df in (
            build_cascade_break_alerts(calls, follows, links, report_date, recent_days, reporting=reporting),
            duration_alerts,
            false_alerts,
        ) if not df.empty
    ]
    extra = pd.concat(extra_frames, ignore_index=True) if extra_frames else pd.DataFrame()
    if not extra.empty:
        start_date = start.normalize()
        recent = calls[calls['StartTime'] >= start_date]
        base = calls[calls['StartTime'] < start_date]
        base_days = max(base['StartTime'].dt.normalize().nunique(), 1)
        rn = recent.groupby(['DeviceId', 'Preempt']).size()
        bn = base.groupby(['DeviceId', 'Preempt']).size()
        extra['BaselinePerDay'] = [bn.get((d, p), 0) / base_days for d, p in zip(extra['DeviceId'], extra['Preempt'])]
        extra['RecentPerDay'] = [rn.get((d, p), 0) / recent_days for d, p in zip(extra['DeviceId'], extra['Preempt'])]
        extra['Date'] = pd.Timestamp(report_date).normalize()
        extra['Cascade'] = ''
    alerts = pd.concat([df for df in (kept, extra) if not df.empty], ignore_index=True) if not (kept.empty and extra.empty) else kept
    alerts = alerts.reset_index(drop=True)
    if not alerts.empty:
        sections = call_section([None] * len(alerts), alerts['Preempt'], rail_preempts)
        if 'Class' in alerts.columns:
            sections = alerts['Class'].where(alerts['Class'].notna(), pd.Series(sections, index=alerts.index))
        alerts['Class'] = sections
        alerts['ChartSeries'] = _chart_series(alerts, links, rail_preempts)
    return {'alerts': alerts, 'overruled': overruled, 'links': links}


def _chart_series(alerts: pd.DataFrame, links: pd.DataFrame, rail_preempts) -> list:
    """The preempts to draw with each alert, its own first.

    Linked preempts (strongest links first) show whether the route or rail
    line changed with it; other preempts at the same signal with the same kind
    of alert, and for rail its companion preempts, show the crossing as a whole.
    """
    rail = None if rail_preempts is None else {int(p) for p in rail_preempts}
    series = []
    for row in alerts.itertuples():
        node = (str(row.DeviceId), int(row.Preempt))
        picks = [node]
        same_kind = alerts[
            (alerts['DeviceId'].astype(str) == node[0]) & (alerts['Direction'] == row.Direction)
        ]['Preempt'].astype(int)
        picks += [(node[0], p) for p in sorted(same_kind)]
        if rail is not None and node[1] in rail:
            picks += [(node[0], p) for p in sorted(rail)]
        if links is not None and not links.empty:
            out = links[(links['DeviceId'] == node[0]) & (links['Preempt'] == node[1])]
            inc = links[(links['NextDeviceId'] == node[0]) & (links['NextPreempt'] == node[1])]
            linked = pd.concat([
                out[['NextDeviceId', 'NextPreempt', 'Followed']].set_axis(['D', 'P', 'Followed'], axis=1),
                inc[['DeviceId', 'Preempt', 'Followed']].set_axis(['D', 'P', 'Followed'], axis=1),
            ]).sort_values('Followed', ascending=False)
            picks += [(str(d), int(p)) for d, p in zip(linked['D'], linked['P'])]
        unique = list(dict.fromkeys(picks))[:CHART_MAX_SERIES]
        series.append([[d, p] for d, p in unique])
    return series
