"""ATSPM Report Generator - Anomaly detection for traffic signal data."""

import pandas as pd
# Opt-in to future behavior to avoid silent downcasting warnings
pd.set_option('future.no_silent_downcasting', True)

from .generator import ReportGenerator
from .alarm_processing import ALARM_EVENT_CLASSES, ALARM_HISTORY_DAYS, FLASH_EVENT_CLASSES
from .preempt_processing import PREEMPT_HISTORY_DAYS

__version__ = "1.2.0"
__all__ = [
    "ReportGenerator",
    "ALARM_EVENT_CLASSES",
    "ALARM_HISTORY_DAYS",
    "FLASH_EVENT_CLASSES",
    "PREEMPT_HISTORY_DAYS",
]
