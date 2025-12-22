"""ATSPM Report Generator - Anomaly detection for traffic signal data."""

import pandas as pd
# Opt-in to future behavior to avoid silent downcasting warnings
pd.set_option('future.no_silent_downcasting', True)

from .generator import ReportGenerator

__version__ = "0.1.0"
__all__ = ["ReportGenerator"]
