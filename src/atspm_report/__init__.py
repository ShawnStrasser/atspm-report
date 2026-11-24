"""ATSPM Report Generator - Anomaly detection for traffic signal data."""

import pandas as pd
# pandas 2: opt in to the pandas 3 behaviour (no silent downcasting), which
# pandas 3 has by default and no longer lets you set.
if int(pd.__version__.split('.')[0]) < 3:
    pd.set_option('future.no_silent_downcasting', True)

from .generator import ReportGenerator
from .alarm_processing import ALARM_EVENT_CLASSES, ALARM_HISTORY_DAYS, FLASH_EVENT_CLASSES
from .preempt_processing import PREEMPT_HISTORY_DAYS
from .atspm_detector_health import run_atspm_detector_findings
from . import connection

__version__ = "2.1.0"
__all__ = [
    "ReportGenerator",
    "ALARM_EVENT_CLASSES",
    "ALARM_HISTORY_DAYS",
    "FLASH_EVENT_CLASSES",
    "PREEMPT_HISTORY_DAYS",
    "run_atspm_detector_findings",
    "connection",
]
