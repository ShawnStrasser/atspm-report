# atspm-report

[![Unit Tests](https://github.com/ShawnStrasser/atspm-report/actions/workflows/pr-tests.yml/badge.svg)](https://github.com/ShawnStrasser/atspm-report/actions/workflows/pr-tests.yml)
[![PyPI version](https://img.shields.io/pypi/v/atspm-report.svg)](https://pypi.org/project/atspm-report/)
[![codecov](https://codecov.io/gh/ShawnStrasser/atspm-report/branch/main/graph/badge.svg)](https://codecov.io/gh/ShawnStrasser/atspm-report)
[![Python versions](https://img.shields.io/pypi/pyversions/atspm-report.svg)](https://pypi.org/project/atspm-report/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Turns the output tables of the [atspm](https://github.com/ShawnStrasser/atspm) package into per-region PDF reports of **new** traffic signal issues. Repeat alerts are suppressed against history you store between runs, so each report shows what changed, not what is already known.

![Example Report](images/example_report.png)

```bash
pip install atspm-report
```

## What it does

`ReportGenerator(config).generate(...)` takes pandas DataFrames or DuckDB relations, runs the detectors, suppresses repeats, and returns PDFs as `BytesIO` plus the alert tables behind them.

It does **not** fetch data, read or write files, send email, or schedule anything. You supply the inputs, save the PDFs, and persist the returned history for the next run.

## Alert types

| Section | Input | Method |
|---|---|---|
| Phase termination (max-out) | `terminations` | CUSUM on daily percent max-out per phase, kept only if still high at the end of the day (hourly drain) |
| Detector health | `atspm_detector_findings` (+ `detector_counts` for the charts) | Bad and suspect vehicle detectors from [atspm-detector](https://pypi.org/project/atspm-detector/) on raw events, with fault periods; lane closures and timing plan changes as information |
| Pedestrian button health | `atspm_detector_findings` (+ `ped_press_counts` and `pedestrian` for the charts) | Ped buttons stuck on, pressing like a machine or gone silent, from the same table, with charts of presses and walks; ped set-up issues as information |
| Missing data | `has_data` | CUSUM on daily share of missing 15-min bins; shown hour by hour over the last 7 days (when, and how many hours) |
| System outage | `has_data` | Region-wide missing data >= 30% for a day; shown as the hours the region averaged 30% or more missing |
| Phase skip | `phase_wait` | Skips summed per phase over the retention window |
| Clearance intervals | `timeline` | Yellow/red durations vs. each phase's median and absolute minimums |
| Controller alarms | `timeline` | Alarms that fired again today, with six-week totals |
| Emergency vehicle preempt | `timeline` (+ `preempt_events`) | Recent week's calls vs. a negative binomial fitted to each preempt's baseline, gated to fault-sized changes (tripled and +5/day, or silent); chatter, stuck on and missed vehicles from the stored calls |
| Transit Signal Priority | `timeline` + `preempt_events` | Bus priority calls that pulsed a preempt input (5 or more in 7 days): Ran As Preempt, or Priority Interrupted when no preempt ran |
| Rail | `timeline` | The inputs in `preempt_rail_numbers`: long hold, stuck on, and the frequency check |
| Phase / overlap conflicts | `timeline` | Interval overlap of conflicting indications (opt-in) |
| Controller clocks | `timeline` (the stored preempt calls) + coordinates | A signal whose emergency vehicle calls line up with nearby signals' calls a fixed time apart: its clock is off (on by default) |
| Coordination plans | `coordination_agg` (six weeks, with `Pattern`) + coordinates | A signal that, on one kind of day, runs another plan, runs free or changes plans at other hours than the coordinated signals around it (opt-in) |
| Sudden rise in delay | `approach_delay` + `arrival_on_green` (+ `platoon_ratio`) | Delay per vehicle up 5 s and 50 % over the same hours of earlier days of the kind, 3 sd, 4+ hours (opt-in) |
| Rising split failures / falling platoon ratio | `split_failures` / `platoon_ratio` | The max-out method on each phase's daily value; silent until 18 days exist (opt-in) |
| Coordination holding | `controller_events` (raw events) | Per cycle, whether a signal holds its own programmed plan: patterns out of step on 20%+ of their cycles (with the cause: a walk past its split, a preempt or TSP) and slow transitions after a plan change; one day is enough (opt-in, `signal_problems` only, see below) |
| Timing Insights | `split_failures` + `terminations` (+ `pedestrian`, `coordination_agg`, `actuations`) | How each phase uses its green over the last six weekdays: premature gap-outs, maxing out despite low demand, max recall despite detection, needs more green, max capacity, downstream bottleneck (opt-in, see below) |

Pass only the inputs you have; every argument except `signals` is optional and a missing input just leaves its section out.

## Preempt, TSP and rail

A controller set up for transit signal priority (TSP) logs a bus as TSP events only (check-in 112 to check-out 115) and no preempt call, so normal TSP is never among the preempt calls. A burst of short pulses on a preempt input is a fault: a bus priority signal read as preempt calls (TSP check-ins come with the bursts) or an input chattering (no check-ins). Rail has its own inputs (`preempt_rail_numbers`). Every call gets a `Class`, by episode (calls on one input less than 30 s apart):

| Class | Episode |
|---|---|
| Rail | an input in `preempt_rail_numbers` |
| Emergency Vehicle | any call of 5 s or more (pulses beside it included), or a burst of pulses (more than half the calls under 1.5 s, none over 5 s) on an input that is not a bus input: chatter |
| Bus Ran Preempt | a burst of pulses on a bus input, with a preempt entry (105) |
| TSP | a burst of pulses on a bus input that ran no preempt |
| Unclassified | calls of 1.5 to 5 s, or unclosed, that are neither; checked with emergency vehicle calls |

Each class has its own report section, in this order, before the missing data sections: **Emergency Vehicle Preempt** (frequency, chatter, stuck on, missed vehicles; cascades learned from these calls only), **Transit Signal Priority** (bus calls on a preempt input: Ran As Preempt, which can pass over waiting phases and cut walks, or Priority Interrupted, where each pulse cancels the bus's priority request and starts it again) and **Rail** (long hold, stuck on, frequency). A bus input is one where at least half the bursts since the signal's first check-in on record came with a TSP check-in (112, within 5 s, matched by signal); its earlier bursts are bus calls too. Without `preempt_events`, or at a signal that logs no check-ins, every burst is chatter.

`preempt_false_calls_enabled` (on by default) adds **False Calls** to the emergency vehicle section: an input with 1 or more calls a day at the shortest call length (within 0.3 s of the length only 0.5% of all calls are shorter than), making up at least half of its calls, of which at most 30% were seen at a signal within two miles inside the drive time. A trigger of a moment is held for that shortest length. A vehicle seen for a moment makes such a call too, so some are normal; an input with most of its calls at that length, which no nearby signal sees, is triggering by itself. An input with no nearby signal to check against needs 1.5 or more such calls a day making up at least 80% of its calls. The alert states the calls ("74 of 74 calls in the last 7 days had the same duration, lasting about 5.2 s. This does not represent normal EVP calls."); what other signals saw is told only for the input's linked preempts. An Increase on such an input is told as False Calls, an Increase whose calls nearby signals saw (over 30% of them) is real traffic and is not reported, and the charts shade the usual call lengths (the middle 90% of all calls).

## Coordination plans and clocks

One section, **Coordination Plans and Clocks**, after the rail section, with no table: each group's alerts are shown together, as the alerted signals' names, a map and the group's charts (below). Signal Focus does not tell these.

- **Clock** (`coordination_clock_check_enabled`, on by default). An emergency vehicle calls one signal after another within seconds to a minute, and each controller stamps its calls with its own clock. A signal whose calls line up with calls at two or more signals within a mile an hour apart (3 or more calls since then, no longer at the same time, a 1 in 1,000 chance or less) has a clock an hour off, from the clock change it missed when one lies between. Any other offset of 5 minutes or more needs 8 calls and 3 signals. Bus and rail calls are left out. It reads the stored preempt calls, so it runs every report (under half a second for a whole network). Its chart plots each call against the matching call nearby, minutes apart by date.
- **Plan** (`coordination_plan_check_enabled`, off by default). Pass six weeks of `coordination_agg` with `Pattern` and `CycleLength` (pairs need about five days; weekend findings need three weekends).
  - **Groups.** Where the signals table has a `corridor` column, a signal with a value is grouped as it says: signals with the same value are a group, and a value no other signal has means it runs on its own and is never judged. Signals with no value (missing or blank) are grouped from what they run, and only with each other: signals within 1.5 miles that run matching cycles (equal, half or double) at least half the time they are coordinated run together, and a group is a chain of them no more than half a mile apart (a signal with data in line between two of them, within half a mile of both, links them). A signal this leaves without a group is added to one when it sits in line between two signals that run together, within half a mile of the nearer, or runs the plan of a signal within half a mile for part of the day; it is judged against them, they are not judged against it (the in-line rule has its own switch, `coordination_position_check_enabled`, off by default). A signal is not added past a signal with data outside the group, nor part time when a signal with no data is nearer, nor to signals of another system where the signals table has `group_name`.
  - **Judging.** For each 15 minutes a signal's group agrees on a state (2 or more and two thirds of those reporting). A signal is told when, on 75% (and at least 3) of its last 10 weekdays, 4 Saturdays or 4 Sundays, it ran something none of its group ran while the group agreed on another plan, for 30 minutes or more at the same hours, and the latest such day judged in those hours still showed it: a weekend day plan out of step ("Weekends all day it runs free while its group runs 75 s"), or a plan change early or late. A difference on weekdays or every day is told too: it may be set up that way or set wrong. A signal found out of step does not excuse a neighbour running what it runs at any hour, nor count towards what its group runs in the hours it is out of step. Pattern 255 is left unjudged; a plan that logs cycle length 0 gets the cycle it usually logs; after a gap in a signal's data its plan is not trusted until it records a change or the day ends. Times are as each controller logged them. A signal with a clock alert gets no plan alert.
  - **Confidence.** High for a clock alert and for a group member that sits inside its group, is out of step two hours or more in all, while no nearby signal outside the group runs the same cycle; Check otherwise. Every alert is told once and then held back; only High ones go on to the Ongoing list.
  - **Shown.** Alerts of one group are shown together: a map of the signals around it (the group by what it ran: black when it ran as one, else Group A, B... in colours; the alerted signals outlined in their line's colour; other signals grey, hollow without data; a plain light-grey road map underneath, left out when it cannot be fetched, and no map without coordinates) and a chart of cycle length by time of day for each of the last 7 days that differs from the days after it, most recent first, only days on which a signal drawn on its own runs something else than its group for 30 minutes or more. The alerted signals and any group signal that ran with no other are drawn on their own; nothing is shaded. A run shows `coordination_figures` groups a report, clock alerts and High first; a group not shown is not filed as told, so it comes up on a later run.

## Checks on the raw controller events

Both off by default, both read `generate(controller_events=...)` (`DeviceId`, `TimeStamp`, `EventId`, `Parameter`; a DuckDB relation is queried on its own connection), every signal in one query. Connection mode reads them from `HiResEvents` only when one is switched on, bounded to the report day and the ids needed. Their results are in `signal_problems` (each line with a `grade`, `urgent` or `review`); neither adds a report section.

- **Flash verification** (`flash_verification_enabled`; events 1, 173, 174 over the report day, and up to `as_of`). A flash the signal did not stop for (a green began within 3 minutes after at least half of its onsets; 174 onsets less than 60 s apart are one burst) is the controller's MMU, fault monitor or local flash input flipping, not a signal in flash: "MMU flash logged 3,140 times while the signal kept cycling", a review item. A real flash is urgent only while still on at the end of the data (the last 173 is that flash, the 174 bit still set); one that is over is a review item ("; not in flash at the end of the data"). 'MMU Flash' (174) is left out where 'Flash - MMU' (173) tells the same flash, in the Controller Alarms table too. With `as_of`, the Alarms rows' `Today` is True while the signal is still in flash at `as_of`, False when it is over. `alarms` gains `Onsets`, `KeptCycling`, `Ongoing` and `OngoingNow`.
- **Coordination holding** (`coordination_holding_enabled`; events 1, 8, 11, 21, 102, 112, 131-150, 152, 316, 318, 320 over the report day, plus 131-149 of the day before so the plan at midnight is known). Each cycle is judged against the programmed plan: the coordinated phase's split window from master zero, offset and splits (the offset reference and coordinated phase inferred from the cycles, used when they fit 85% of 30+ cycles; else the actual cycle, 316 or local zero to local zero, within 2 s of the programmed one). A pattern out of step on 20% or more of its steady cycles (36+ over an hour or more; urgent at 33%) is "not holding coordination", with its cause when one explains half of the out-of-step cycles: a side-street walk running past its split, or a preempt or TSP check-in. A change into a plan that takes 10 minutes or more to get back in step (3 in-step cycles in a row; urgent at 30) is a slow transition, or "not back in step within N minutes" when it never is. `signal_problems` category `Coordination`; the rows, with their facts, are in the result as `coordination_holding` (`coordination_holding.COLUMNS`).

The impact chart (`create_coordination_impact_chart`: arrivals on green and delay per vehicle by hour on the days a signal was out of step against the days it was not) is no longer drawn in the report; the function is kept for your own use.

## Timing Insights

With `timing_insights_enabled`, the report ends (before Ongoing Issues) with a **Timing Insights** section: one table per check, each under a line saying what it means, every row with its chart, the worst first, `timing_insights_rows` (3) rows a report. Alerts a table leaves out are not filed as told, so they come up on later runs; the rest are held back as repeats like any alert. Signal Focus does not tell these.

Everything is judged from the 15-minute tables, by hour from 7:00 to 19:00 on the last six weekdays (holidays left out). Hours with a known fault on the phase's stop-bar zone, or with green occupancy never under 97% (a zone held on), are left out. Nothing is read from controller settings: a phase is on recall when it ran to its limit (max-out or force-off) on 98% of its greens with ped service on under half of them. Phases 2 and 6 are coordinated while a pattern with a cycle length runs, and standard ring and barrier phasing is assumed (2 and 6, 4 and 8 end at the barrier together). A phase is listed when it is in the state 2 hours a day or more on 3 of the 6 days.

| Table | Listed when |
|---|---|
| Premature Gap-Out | 15% of its gap-outs end in a split failure, 20 or more a day, overall and on 3 of the days (20+ gap-outs each). In each bin the split failures, at most its gap-outs, count as at a gap-out in the share the gap-outs have of its greens |
| Maxing Out despite Low Demand | Not coordinated or on recall, no ped service, runs to its limit on half its greens, occupancy under 40%, split failures under 2%; its barrier partner under the same limits |
| Max Recall despite Detection | On recall, not coordinated, occupancy above 0 and under 40%, split failures under 2%, its partner under the same limits. A signal whose every measured phase is on recall (fixed time with detection) goes last |
| Needs More Green | Runs to its limit on half its greens with 20% split failures while a phase with spare (split failures under 2%, occupancy under 50%) can give it time: its own ring and side, any phase in a single ring, or across the barrier when every ring serving that side has one. The table names them |
| Max Capacity | One row per signal: a phase short of green, no measured phase with spare, and 3 in 4 of the phases it serves with 10% split failures. Not listed with a Downstream Bottleneck |
| Downstream Bottleneck | Stop-bar count zones (`Function` 'Count' in `atspm_detector_config`, with `lanes`): a bin from 7:00 to 19:00 with occupancy 85%+, split failures on 20% of cycles and fewer than half the vehicles the green carries at the phase's usual best is blocked; listed when blocked an hour a day on 3 of the days, and in under half its bins at that occupancy |

The charts show by hour, one line per phase with the phase in question thick: percent max-out/force-off, percent split failures and green occupancy ratio with its ideal range (40% to 70%: below 40% split failures stay under 1% of cycles, from 70% they pass 5%). Premature Gap-Out shows split failures with those at a gap-out dashed, and Downstream Bottleneck adds volume (vehicles per hour of green per lane) against occupancy, the blocked bins red.

## Signal Focus

Each report opens with the signals worst off across more than one area: at most 3 a report (`signal_focus_signals`, `0` leaves the section out; the All Regions report takes the top 3 of all regions, each region's report its own top 3), one at a time, each with a subtitle, a table of problem type and measured problems, and its charts in the table's order. The score counts new problems only, weighted to the most recent day of data; missing data never counts. The bar is set for one to three signals over all regions on an ordinary day, so many region reports have none; a day when many signals fail at once has more, three a report. A signal selected there leaves every other table and chart of that report.

The score counts new alerts only. Repeats that suppression held back add nothing, but once a signal is selected the narrative tells them too, marked as ongoing.

Each signal's charts open with one overview on the days its detector charts cover: a row per group of detectors or ped buttons with much the same fault times (stuck on, sending nothing, other faults, which include a fault with no set time), then the percent max out (up to 100%), average phase wait and greens of the phases its table names ("all phases" when that is every phase the signal runs), hour by hour as plain lines against their smoothed normal range (the same hour on the three weeks before, without the days least like the others), its skips and preempt calls, and the hours with no data, all on one time axis with the fault times shaded behind the measures. A long stretch with no wait while the signal sent data says no calls were logged (wait runs from a call to its green, so there is none when every detector is down), and a skip or preempt row with none in these days says so, with the day of the last preempt call before them. The detector charts follow, then the preempt section's charts of its preempt alerts, then its ped button chart. When every detector was down at once, the overview has one row for them all and there are no detector charts.

Score = the severity of each area, plus 1 for each area, plus 2 for each pair of problems that belong together. A signal needs 2 or more areas and a score of 7 or more, from problems that belong together (`signal_focus_related_only`, on by default): a group of its problems that share a phase (a detector or button fault and max outs, skips or another fault on its phase or a phase it was measured to hurt) or a flash alarm and a conflict has to span 2 or more areas and score 7 by itself. Problems that only happen at the same signal, such as a detector fault on one phase and a preempt input, qualify only when there are 3 or more different kinds of them (each check is one kind; a fault and its measured effect are one). The signal's score is still that of all its new problems. A detector or button fault measured to affect operations (service lost, or harm to other phases, see below) is two areas, the detection and how the signal ran. So harm to three or more phases qualifies with nothing else (3, plus 2 for the further phases, plus 2 areas); harm to one phase, or a fault with no measured effect, needs a second problem.

| Severity | Problem |
|---|---|
| 5 | Conflicts |
| 4 | Service lost on a detector's or button's phase (see below), a preempt fault (stuck on, long hold, chatter, missed vehicles, bus calls on a preempt input), an MMU or fault monitor flash |
| 3 | Harm to other phases (a point more for each further phase, up to 2), phase skips at least once a day |
| 2 | Max outs, a change in preempt calls, other alarms |
| 1 | Detector and ped button faults with no measured effect, rare skips, missing data |
| 5 x the square root of the share of samples | Clearance: how common it is, not that it happened. 1% of samples scores 0.5, 25% scores 2.5. Long clearances count half |

A problem covering 3 or more detectors, phases or movements scores a point more (not clearance). Two problems belong together when one can explain the other: a stuck detector and max outs on its phase, a silent detector and skips on its phase, a detector or button measured to slow a phase that also alerts, a ped button and max outs or skips on its phase, a flash alarm and a conflict. Measured: they share a phase. Inferred: the one caused the other, so the narrative says likely. Lane closures, timing plan changes and ped set-up issues score nothing and are named in the narrative.

## Detector triage

With `detector_triage_enabled`, Detector Health lists vehicle detectors and ped buttons together in two tables. **Affecting Operations** holds the faults measured to cost service, the most costly first, with a short Impact column that names the phases affected, and the charts. **Data Only - not Affecting Operations** holds the rest, with no charts: they still need fixing for the data. Each fault's hours are compared, phase by phase, with the same clock hours on the same day type on other days:

| Tier | Meaning | Test |
|---|---|---|
| Service lost | Its own phase gets far fewer greens or walks than normal | Vehicle: at least 8 normal services, at most 30% of normal, below the 10th percentile of normal days, 50 points below the other phases, own max outs up under 20 points. Ped: at least 3 normal walks, at most 40% of normal and below the lowest normal day |
| Harm to other phases | Other phases wait longer or max out more, weighted by the traffic on them, and the fault is the likelier cause | Phase wait (the time from a phase's call to its green, averaged over the phases by their traffic) up 3 s, and that rise times the vehicles counted on them at least 2 hours a day, or max outs up 15 points and at least 100 vehicles a day, with at least a 50% chance that the fault caused a rise that large (below) |
| Data only | No operational effect measured | The detector still needs fixing for its data |
| Not measured | Phase unknown, no normal to compare, too few services, or the whole signal stopped | |

**Is the rise the fault, or an ordinary day?** Waits differ from day to day with no fault at all, so a rise over the usual day proves little. For each fault day the traffic-weighted change of the other phases is worked out, and the same for every normal day in the same clock hours. Bayes' rule then weighs four explanations of the fault days:

- An ordinary day, predicted from the normal days. A rise of a few seconds counts when they agree closely; a large one does not when they are few or scattered. A signal with few normal days is taken to vary as signals usually do (the weighted wait by about 12% of its usual value).
- An unusual day for another reason (a crash, a closure, another fault), as often as the normal days had one. A normal day that rose as much as the fault day makes this likely.
- Something else that changed for all the fault days (a timing change, road works). It counts when none of the report's own days is a normal day that would have shown it, as with a fault that lasts all of them.
- The fault.

It starts from how likely such a fault is to hurt other phases at all: 30% for a stuck input, 15% for a detector that is silent or counts wrong, 5% for no daily pattern or a silent button, times the health check's confidence in the fault, and doubled when the fault visibly made its own phase run more (half again its normal greens or walks, or 20 points more max outs). The assumptions are cautious: with real fault times moved to signals that had no fault then (1,591 cases over five days), the test called harm on 0.6% of them, where a fixed rise of 5 s called it on 1.3%.

**Faults that overlap.** A change in hours two faults share cannot be split between them. Faults of one signal with much the same times are one event: judged once, and one row in the table, naming them all. An event is judged on the hours no other event of the signal was going on when it has at least an hour of them and a quarter of its own, and otherwise on all its hours.

Max outs on the detector's own phase do not rank: a phase that maxes out from its own detector is getting more time. Service lost comes first, largest loss first; then harm, largest phase wait rise times vehicles counted first; then the rest, bad before suspect, stuck on, silent, counts low, counts high, no daily pattern, longest first. It never changes which detectors alert. The history records what the two tables listed.

With `approach_delay` and `arrival_on_green`, a harmed phase with 30 or more arrivals an hour through the fault and 3 normal days is told by its delay per vehicle instead ("Delay per vehicle on other phases up 8 s on average over 4.5 h: Ph2 12 to 21 s; phase wait Ph7 53 to 69 s"), never where one of its advance zones is the faulty detector or was faulting, and not when the delay did not rise. Only the words and the number change: harm is still judged on phase wait.

Measured: greens, walks, presses, max outs and phase wait during the fault hours. Inferred: that the detector caused the change. Phase wait is a measure of the phases, not of how long each vehicle was held: the Impact column says "Phase wait on other phases up 20 s on average", and the rise times the vehicles counted is a weight for the threshold and the order, not a vehicle delay. It needs `atspm_detector_config`, `terminations`, `phase_wait` and `has_data`, and for all its measures `pedestrian`, `atspm_ped_inputs`, `detector_usual_counts` and `coordination_agg` with `CycleLength`. The result `silent_ped_phases` lists phases with no press at all that normally get many; a button with no press is not on the ped health list, so this is for review and adds no alerts.

## Inputs

All inputs except `signals` come straight from atspm's output tables. `DeviceId` may be int or string in any table; it is cast to string internally.

**`signals` (required, you provide this)** one row per signal:

| Column | Notes |
|---|---|
| `DeviceId` | Unique controller id, must match the atspm tables |
| `Name` | Display name, e.g. `04100-Pacific at Hill` |
| `Region` | Grouping key, one PDF per region |
| `Latitude`, `Longitude` | Optional. Coordinates: neighbour checks (preempt cascades, clock check, learned coordination groups) and the coordination maps |
| `corridor` | Optional. The coordinated group each signal belongs to, for the coordination plan check: same value, same group; a value of its own, never judged; blank, grouped from its data |
| `group_name` | Optional. The agency's system name; a signal is never added to a learned coordination group of another system |

**atspm tables** and how much history to pass each run:

| Argument | atspm table | Window |
|---|---|---|
| `terminations` | `terminations` | ~21 days |
| `detector_counts` | `actuations` (15-minute counts) | Days covered by `atspm_detector_findings`, chart data only |
| `has_data` | `has_data` | ~21 days |
| `pedestrian` | `ped` | ~21 days |
| `phase_wait` | `phase_wait` | 14 days (`phase_skip_retention_days`) |
| `coordination_agg` | `coordination_agg` | same as `phase_wait`, chart decoration only; 42 days with `Pattern` for the coordination plan check |
| `timeline` | `timeline` | 1 day, or up to 42 days (see below) |
| `preempt_events` | raw events 102, 104, 105 (and 112 where logged) | The days of the stored preempt calls (42); classes bus calls |
| `arrival_on_green`, `approach_delay` | `arrival_on_green`, `approach_delay` | ~21 days (triage wording, impact charts); 36 for the delay rise check |
| `platoon_ratio` | `platoon_ratio` | ~21 days for its trend, 36 for the delay rise check; else not needed |
| `split_failures` | `split_failures` | ~21 days for its trend; the last six weekdays, with `Green_Time`, for Timing Insights; else not needed |
| `actuations` | `actuations` (15-minute counts) | The last six weekdays for Timing Insights' Downstream Bottleneck; only the count zones are read |
| `controller_events` | raw events (`HiResEvents`) | The report day, only for `flash_verification_enabled` and `coordination_holding_enabled` (see Checks on the raw controller events) |

The four measure tables are optional, and a DuckDB relation is filtered to the signals and days needed before anything is read. They are never averaged over bins: platoon ratio is `SUM(Green_Actuations) / SUM(Total_Actuations) / (SUM(Green_Seconds) / (bins x 900))`, delay per vehicle `SUM(delay) / SUM(arrival_on_green.Total_Actuations)` with the bins joined first. The delay is atspm's Queue Model delay, `Queue_Delay_Seconds` (atspm 3.1 with `queue_model=True`), when `approach_delay` has that column, else `Delay_Seconds + Queued_Delay_Seconds`; a row where `Queue_Delay_Seconds` is NULL (a day from before the Queue Model was on) takes the latter too. A baseline that spans the day the Queue Model was turned on mixes the two, and the Queue Model's delay is usually the larger. No measure is used from a detector while it is faulting: split failures come from a phase's stop-bar zones and the other three from its advance zones, and hours in which one of them had a fault (health findings, timeline Stuck On and Erratic, and days in `atspm_detector_minutes`) are left out.

The CUSUM detectors compute each entity's baseline from everything you pass, so keep the window consistent.

The latest day in `timeline` is the report day, and the clearance and conflict checks only ever look at that day. The controller alarm and preempt checks need six weeks of daily counts, which you can supply either way:

- **One day of timeline.** Persist the returned `updated_alarm_history` and `updated_preempt_history` and pass them back as `alarm_history` and `preempt_history`; the counts build up over runs. Days present in the timeline replace the matching history rows, so re-running a day does not double count it.
- **Up to 42 days of timeline.** The counts are rebuilt from it every run and there is nothing to persist. Older days can be trimmed to the `ALARM_EVENT_CLASSES` and `Preempt` rows to keep it small. Add **`device_days`** (columns `DeviceId`, `Date`: the days each signal reported data) so the preempt check counts a reported day with no calls as zero; without it, reported days are inferred from each day's timeline span, which a trimmed day no longer has.

`detector_counts` is only the 15-minute detector counts (`TimeStamp`, `DeviceId`, `Detector`, `Total`) drawn in the detector health charts. Pass just the days the health covers; a longer window only costs memory.

`atspm_detector_findings` is atspm-detector's findings table for vehicle detectors and ped buttons together, one row per finding (no row = no finding), not an atspm table: `DeviceId`, `DetectorType` (`Vehicle` or `Ped`), `Detector`, `Start`, `End`, `Label`, `Confidence`, `Detail`. Install the extra (`pip install 'atspm-report[detector]'`) and run it on your DuckDB connection over a few days of raw events (EventIds 1, 7-11, 43, 44, 81, 82, 131, 150, 173, and for ped buttons 21-23, 45, 67, 89, 90) and a detector table (DeviceId, Detector, phase, function, lanes):

```python
from atspm_report import run_atspm_detector_findings
findings = run_atspm_detector_findings(con, 'events', 'detectors', start=start, end=end)
```

A detector or button is reported when its minutes add up over the days. Each day adds to two running totals: Confidence 0.8 or more to the bad total (30 minutes a day forgiven, alert over 120, capped at 180), Confidence 0.5 to under 0.8 to the slower suspect total (15 forgiven, a day counts at most 90 for a detector and 120 for a ped button, alert over 240, capped at 270). One suspect day never alerts; several days to a week can. Save `updated_atspm_detector_minutes` and pass it back as `atspm_detector_minutes` next run.

## Usage

```python
from pathlib import Path
import pandas as pd
from atspm_report import ReportGenerator

STATE = Path('state')
STATE.mkdir(exist_ok=True)

def load(name):
    path = STATE / f'{name}.parquet'
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()

config = {'verbosity': 1}   # every key has a default, see below

result = ReportGenerator(config).generate(
    signals=signals,                 # your own table
    terminations=terminations,       # atspm outputs, pandas or DuckDB
    detector_counts=actuations,
    has_data=has_data,
    pedestrian=ped,
    phase_wait=phase_wait,
    coordination_agg=coordination_agg,
    timeline=timeline,
    # history from the previous run (empty on the first run)
    past_alerts={k: load(f'past_{k}') for k in ReportGenerator.ALERT_TYPES},
    alarm_history=load('alarm_history'),
    preempt_history=load('preempt_history'),
)

# 1. Persist history for the next run. Store it verbatim.
for alert_type, df in result['updated_past_alerts'].items():
    df.to_parquet(STATE / f'past_{alert_type}.parquet', index=False)
# Only needed when timeline is a single day (see Inputs).
result['updated_alarm_history'].to_parquet(STATE / 'alarm_history.parquet', index=False)
result['updated_preempt_history'].to_parquet(STATE / 'preempt_history.parquet', index=False)

# 2. Do something with the PDFs. Empty dict means nothing new today.
for region, pdf in result['reports'].items():
    Path(f'report_{region}.pdf').write_bytes(pdf.getvalue())
```

DuckDB relations work anywhere a DataFrame does, so the inputs can come straight from a DuckDB database:

```python
import duckdb
con = duckdb.connect('atspm.duckdb', read_only=True)
result = ReportGenerator(config).generate(
    signals=con.table('signals'),
    terminations=con.table('terminations').filter(f"TimeStamp >= TIMESTAMP '{start}'"),
    # ...
)
```

Version 2.0 dropped Ibis: inputs are pandas DataFrames or DuckDB relations, and the package needs Python 3.12+, DuckDB 2.0+ and pandas 3.1+. Ibis tables are no longer accepted; pass `ibis_table.to_pandas()`, or query the database with DuckDB directly. The input `detector_health` is now `detector_counts`. See [CHANGELOG.md](CHANGELOG.md) for everything that changed.

### Returns

| Key | Contents |
|---|---|
| `reports` | `{region: BytesIO}` PDFs, only for regions with content |
| `alerts` | `{alert_type: DataFrame}` new alerts shown in the PDFs |
| `hourly_data` | `maxout_hourly`, and with `has_data` `missing_hourly` and `outage_hourly`: the share of 15-minute records missing per hour, by signal (last 7 days) and by region |
| `ongoing_alerts` | Suppressed repeats with an `OngoingSince` column (only when `include_ongoing_issues`) |
| `updated_past_alerts` | **Persist.** Next run's `past_alerts` |
| `updated_alarm_history` | **Persist.** Next run's `alarm_history` |
| `updated_preempt_history` | **Persist.** Next run's `preempt_history` |
| `alarms` | Controller alarms listed this run |
| `coordination_holding` | Coordination holding findings (only with `coordination_holding_enabled`) |

### Connection mode, report day and analytics runs

An application whose atspm 3 writes its tables on a DuckDB connection (conn mode) can run the report there, as atspm does, instead of building frames:

```python
from atspm_report import connection

# the daily report for yesterday: inputs from atspm's tables, state kept in the schema atspm_report
result = connection.run(con, report_date, signals=signals_df, config=config, detector_config=detectors_df,
                        as_of=now, save=False)       # save=False: call connection.save_state(con, result) once it is sent
# between reports: what a report would find, with nothing drawn and no state touched
connection.run(con, report_date, signals=signals_df, config=config, as_of=now, analytics_only=True)
# today so far: what started today up to now that no report told
connection.run(con, now.date(), signals=signals_df, config=config, as_of=now, analytics_only=True)
```

Each run writes `atspm_report.signal_problems` (one row per signal and area, with `ReportDate`, `AsOf`, `Mode`, `RunAt` and `Today`); a run on today so far writes `atspm_report.signal_problems_today` instead (`Mode` 'today').

`generate()` takes the same three arguments directly:

- `report_date`: the day the report covers. Every input is cut at the midnight after it (by default the latest timeline day is the report day).
- `as_of`: with `report_date`, the rows from the report day's end up to `as_of` say whether each problem still shows today (`signal_problems['Today']`: max outs, phase skips and missing data, set against the same clock hours of the report day; with `flash_verification_enabled` also alarms, still in flash at `as_of`; None where that cannot be told). They never change what the report finds.
- `analytics_only`: no charts and no PDF; every `updated_*` state comes back as passed in.
- Today so far (`report_date` the day of `as_of`, analytics only): today's hours up to `as_of` are judged, so problems that started today are found during the day. Max outs and missing data set each day's same clock hours against today's; an interval still going at `as_of` waits. Left out, as part of a day cannot tell them: preempt frequency and the preempt kinds counted per day (a call stuck on or held long is told), coordination plans and clocks, the trends, Timing Insights and the detector health (whole-day checks). `signal_problems` holds only what is new and last seen today.

### State between runs

`generate()` never writes anything. Save `updated_past_alerts` and pass it back unchanged next run; do the same with `updated_alarm_history` and `updated_preempt_history` unless you pass a multi-day `timeline`, in which case those two are rebuilt every run and need not be stored. Every retention and suppression rule is already applied inside, so do not filter, dedupe, or reshape them. Dropping them does not error; it silently resets suppression, alarm totals, or the preempt baseline.

## Configuration

Pass a plain dict; every key has a default.

| Key | Default | Effect |
|---|---|---|
| `alert_suppression_days` | `21` | A matching alert this recent in history is held back as ongoing, not reported as new |
| `alert_retention_weeks` | `104` | How long `updated_past_alerts` keeps history |
| `alert_flagging_days` | `7` | Maximum age of a reportable alert, counted back from the day after the report day (the latest `timeline` day), so re-running an old day flags what its own run did. The suppression and phase-skip windows count from that day too |
| `include_ongoing_issues` | `False` | Add an "Ongoing Issues" section at the end of the report, after every new alert, with one subsection per check in the checks' order |
| `phase_skip_alert_threshold` | `1` | Aggregated skips must exceed this |
| `phase_skip_retention_days` | `14` | Trailing days of phase-skip data kept |
| `maxout_cusum_threshold` / `maxout_zscore_threshold` / `maxout_percent_threshold` / `maxout_min_services` | `0.25` / `4.0` / `0.2` / `30` | Phase termination thresholds, all must be exceeded |
| `maxout_drain_enabled` / `maxout_drain_per_hour` / `maxout_drain_threshold` | `True` / `0.1` / `1.0` | A flagged max-out day must still be high at its end: hour by hour, max outs above that hour's usual share add up, less the drain each hour, and must end above the threshold, so a spike that went back to normal is not reported |
| `clearance_yellow_min_seconds` / `clearance_red_min_seconds` / `clearance_tolerance_seconds` | `3.5` / `0.5` / `0.1` | Clearance interval limits |
| `overlap_dual_indications_enabled` + `overlap_dual_indication_phases` | `False`, `[]` | Phase green concurrent with same-numbered overlap yellow/red. Never suppressed |
| `general_phase_conflicts_enabled` | `False` | Standard conflicting phases both green |
| `overlap_conflicts_enabled` + `overlap_conflict_numbers` | `False`, `[]` | Phase/overlap conflicts for the listed overlaps |
| `same_movement_color_conflicts_enabled` | `False` | One movement showing two colors at once |
| `*_excluded_signals` / `*_excluded_device_ids` | `[]` | Skip signals with non-standard phasing from the conflict checks |
| `figures_per_device` | `3` | Charts per alert section |
| `max_table_rows` | `10` | Row cap per report table |
| `table_row_limits` | `{}` | A table's own row cap instead: `detector_faults`, `lane_closure`, `timing_plan`, `ped_faults`, `ped_setup`, and `ongoing` for every Ongoing table |
| `data_only_table_signals` | `10` | Signals per report in Detector Health's Data Only table; the rest are not filed as told and come up later |
| `phase_skip_table_signals` | `5` | Signals per report in each phase skip table (new and ongoing); new skips not shown are not filed |
| `alarm_table_signals` | `10` | Controller Alarms: the signals with the most alarms over six weeks, per report |
| `ongoing_rotation_enabled` | `True` | Ongoing tables take turns: the alerts waiting longest, weighted by how bad they are (pass `ongoing_last_shown`, keep `updated_ongoing_last_shown`) |
| `coordination_clock_check_enabled` | `True` | Controller clocks off against nearby signals, from the preempt calls |
| `coordination_plan_check_enabled` | `False` | Coordination plans out of step with the signal's group; needs six weeks of `coordination_agg` |
| `coordination_position_check_enabled` | `False` | Plan check: also judge a signal that sits between two signals of a group while running none of its plans |
| `coordination_figures` | `5` | Coordination groups shown per report in a run (new and ongoing each), each with its map and charts; the rest come up on later runs and are not held back until then |
| `delay_rise_check_enabled` | `False` | Sudden rise in delay per vehicle |
| `split_failure_trend_enabled` / `platoon_ratio_trend_enabled` | `False` | Rising split failures, falling platoon ratio |
| `ongoing_figures` / `ongoing_preempt_figures` / `ongoing_coordination_figures` | `None` | Charts per Ongoing Issues table, Ongoing preempt charts per section, Ongoing coordination groups (0: none; `None`: `figures_per_device`, `ongoing_figures`, `coordination_figures`) |
| `timing_insights_enabled` / `timing_insights_rows` | `False` / `3` | The Timing Insights section and its rows (each with a chart) per table and report |
| `flash_verification_enabled` | `True` | Flash alarms checked against the raw events: a flipping flash input is a review item, a flash urgent only while still on, Today for alarms |
| `coordination_holding_enabled` | `True` | Coordination holding from the raw events: out-of-step patterns and slow transitions (`signal_problems` category `Coordination`) |
| `preempt_rail_numbers` | `None` | Preempt inputs that are rail (the Rail section, longer stuck-on limit); `None` holds every preempt to the rail limit |
| `custom_logo_path` | `None` | Logo for the PDF header |
| `verbosity` | `1` | `0` silent, `1` info, `2` debug |

The full list with clearance noise filters and chart options is in the `ReportGenerator` docstring.

## License

MIT, see [LICENSE](LICENSE). Contributions welcome; open an issue for problems or help.
