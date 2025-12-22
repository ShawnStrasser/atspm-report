# ATSPM Report Package

[![Unit Tests](https://github.com/ShawnStrasser/atspm-report/actions/workflows/pr-tests.yml/badge.svg)](https://github.com/ShawnStrasser/atspm-report/actions/workflows/pr-tests.yml)
[![PyPI version](https://img.shields.io/pypi/v/atspm-report.svg)](https://pypi.org/project/atspm-report/)
[![codecov](https://codecov.io/gh/ShawnStrasser/atspm-report/branch/main/graph/badge.svg)](https://codecov.io/gh/ShawnStrasser/atspm-report)
[![Python versions](https://img.shields.io/pypi/pyversions/atspm-report.svg)](https://pypi.org/project/atspm-report/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Downloads](https://static.pepy.tech/badge/atspm-report)](https://pepy.tech/project/atspm-report)

A Python package for generating daily reports for new traffic signal issues. The generated report highlights new issues that just occurred, and filters out previously flagged issue.

![Example Report](images/example_report.png)

## Alert Types

The package identifies 6 key types of traffic signal performance issues:

### 1. Max-Out Alerts
Detects increased percent max-out compared to historical baseline.

![Example Max-Out Alert](images/example_phase_termination.png)

### 2. Actuation Alerts
Detects worsening detector performance compared to historical baseline.

![Example Detector Alert](images/example_detector.png)

### 3. Pedestrian Alerts
Detects increased ped services or changes in actuations per service ratio compared to historical baseline.

![Example Pedestrian Alert](images/example_ped.png)

### 4. Missing Data Alerts
Detects when signals are offline or missing data more than usual.

### 5. Phase Skip Alerts
Detects when phase wait times (without preempt present) are more than 1.5x the cycle length, indicating a skipped phase. NOTE the input data will change soon!

![Example Phase Skip Alert](images/example_phase_skip.png)

### 6. System Outage Alerts
Detects system-wide outage or data loss.

## Features

- **DataFrame-based API**: All inputs and outputs use pandas DataFrames for maximum flexibility
- **Multi-region reporting**: Automatically generates separate PDF reports for each region
- **Alert suppression**: Configurable alert retention to prevent duplicate alerts
- **Custom branding**: Support for custom logos in generated PDFs
- **Date-based jokes**: Rotating collection of jokes in reports based on current date
- **Cross-platform**: Works on Windows, Linux, and macOS

This tool uses the aggregate data produced by the [atspm Python package](https://github.com/ShawnStrasser/atspm), which transforms raw high-resolution controller data into the aggregated metrics used by this package.

## Installation

```bash
pip install atspm-report
```

## Quick Start

```python
import pandas as pd
from pathlib import Path
from atspm_report import ReportGenerator

# Configure the generator
config = {
    'custom_logo_path': None,  # Use default ODOT logo
    'verbosity': 1,
    'alert_suppression_days': 14,
    'alert_retention_weeks': 3,
}

# Load your data (example using test data)
test_data_dir = Path('tests/data')
signals = pd.read_parquet(test_data_dir / 'signals.parquet')
terminations = pd.read_parquet(test_data_dir / 'terminations.parquet')
detector_health = pd.read_parquet(test_data_dir / 'detector_health.parquet')
has_data = pd.read_parquet(test_data_dir / 'has_data.parquet')
pedestrian = pd.read_parquet(test_data_dir / 'full_ped.parquet')

# Create generator instance
generator = ReportGenerator(config)

# Generate reports
result = generator.generate(
    signals=signals,
    terminations=terminations,
    detector_health=detector_health,
    has_data=has_data,
    pedestrian=pedestrian
)

# Save PDF reports
for region, pdf_bytes in result['reports'].items():
    with open(f'report_{region}.pdf', 'wb') as f:
        pdf_bytes.seek(0)
        f.write(pdf_bytes.read())
    print(f"Generated report for {region}")

# Access alerts
for alert_type, alerts_df in result['alerts'].items():
    if not alerts_df.empty:
        print(f"{alert_type}: {len(alerts_df)} alerts")
``````

## Workflow

The package follows this workflow:

```
Input DataFrames
      ↓
Data Processing & Analysis
      ↓
Alert Detection
      ↓
Alert Suppression (using past_alerts)
      ↓
Statistical Analysis
      ↓
Visualization Generation
      ↓
PDF Report Assembly
      ↓
Output: {reports: Dict, alerts: Dict, updated_past_alerts: Dict}
```

### Processing Steps

1. **Data Validation**: Validates required columns in input DataFrames
2. **Alert Detection**: Analyzes data for 6 alert types (max-outs, actuations, missing data, pedestrian, phase skips, system outages)
3. **Alert Suppression**: Removes alerts that were recently reported (configurable retention period)
4. **Statistical Analysis**: Computes summary statistics for each alert type and region
5. **Visualization**: Creates charts for alert trends over time
6. **PDF Generation**: Assembles all components into professional PDF reports per region

## API Reference

### ReportGenerator

The main class for generating ATSPM reports.

#### Constructor

```python
ReportGenerator(config: dict)
```

**Parameters:**
- `config` (dict): Configuration dictionary with the following keys:
  - `custom_logo_path` (str, optional): Path to custom logo image. If None, uses default ODOT logo
  - `verbosity` (int, optional): Output verbosity level (0=silent, 1=info, 2=debug). Default: 1
  - `alert_suppression_days` (int, optional): Days to suppress repeat alerts. Default: 21
  - `alert_retention_weeks` (int, optional): Weeks to retain past alerts for suppression. Default: 104
  - See Configuration Options table for complete list of available parameters

#### generate()

```python
generator.generate(
    signals: pd.DataFrame,
    terminations: pd.DataFrame = None,
    detector_health: pd.DataFrame = None,
    has_data: pd.DataFrame = None,
    pedestrian: pd.DataFrame = None,
    phase_skip_events: pd.DataFrame = None,
    past_alerts: dict = None,
) -> dict
```

**Parameters:**
- `signals` (pd.DataFrame, **required**): Signal metadata
- `terminations` (pd.DataFrame, optional): Phase termination data for max-out detection
- `detector_health` (pd.DataFrame, optional): Detector actuation data
- `has_data` (pd.DataFrame, optional): Data availability records
- `pedestrian` (pd.DataFrame, optional): Pedestrian activity data
- `phase_skip_events` (pd.DataFrame, optional): Raw phase skip events
- `past_alerts` (dict, optional): Dictionary of past alerts by type for suppression

**Returns:**
- `dict` with keys:
  - `reports` (Dict[str, BytesIO]): PDF reports keyed by region name
  - `alerts` (Dict[str, pd.DataFrame]): Current alerts by type
  - `updated_past_alerts` (Dict[str, pd.DataFrame]): Updated alert history for persistence

## Data Schemas

### Input DataFrames

<details>
<summary><strong>signals</strong> (Required)</summary>

Signal metadata including location and regional assignment.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| DeviceId | str | Unique signal identifier (UUID) | 06ab8bb5-c909-4c5b-869e-86ed06b39188 |
| Name | str | Signal location name | 04100-Pacific at Hill |
| Region | str | Geographic region assignment | Region 2 |

**Sample:**
```python
signals = pd.DataFrame({
    'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188', '3cb7be3e-123d-4f8f-a0d4-4d56c7fab684'],
    'Name': ['04100-Pacific at Hill', '2B528-(OR8) Adair St @ 4th Av'],
    'Region': ['Region 2', 'Region 1']
})
```
</details>

<details>
<summary><strong>terminations</strong> (Optional)</summary>

Phase termination data for detecting max-out conditions.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| TimeStamp | datetime | Event timestamp | 2024-01-15 08:30:00 |
| DeviceId | str | Signal identifier (UUID) | 06ab8bb5-c909-4c5b-869e-86ed06b39188 |
| Phase | int | Phase number (1-8) | 2 |
| PerformanceMeasure | str | Termination type | MaxOut, ForceOff, GapOut |
| Total | int | Number of occurrences | 45 |

**Sample:**
```python
terminations = pd.DataFrame({
    'TimeStamp': pd.to_datetime(['2024-01-15 08:30:00', '2024-01-15 08:35:00', '2024-01-15 08:35:00']),
    'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 3,
    'Phase': [2, 2, 4],
    'PerformanceMeasure': ['MaxOut', 'GapOut', 'ForceOff'],
    'Total': [30, 15, 12]
})
```
</details>

<details>
<summary><strong>detector_health</strong> (Optional)</summary>

Detector actuation counts for health monitoring.

| Column | Type | Description | Example |
|--------|------|-------------|---------|  
| TimeStamp | datetime | Event timestamp | 2024-01-15 00:00:00 |
| DeviceId | str | Signal identifier (UUID) | 06ab8bb5-c909-4c5b-869e-86ed06b39188 |
| Detector | int | Detector number | 1 |
| Total | int | Actuation count | 150 |
| anomaly | int | Anomaly indicator (1=yes, 0=no) | 0 |
| prediction | int | Predicted actuation count | 145 |

**Sample:**
```python
detector_health = pd.DataFrame({
    'TimeStamp': pd.to_datetime(['2024-01-15 08:00:00', '2024-01-15 08:00:00']),
    'DeviceId': [7115, 7115],
    'Detector': [1, 2],
    'Total': [150, 5],
    'anomaly': [0, 1],
    'prediction': [145, 150]
})
```
</details>

<details>
<summary><strong>has_data</strong> (Optional)</summary>

Records of data availability (presence of any record indicates data exists for that timestamp).

| Column | Type | Description | Example |
|--------|------|-------------|---------|---|
| TimeStamp | datetime | Event timestamp | 2024-01-15 00:00:00 |
| DeviceId | int | Signal identifier | 7115 |

**Sample:**
```python
has_data = pd.DataFrame({
    'TimeStamp': pd.to_datetime(['2024-01-15 00:00:00', '2024-01-15 00:15:00', '2024-01-15 00:30:00']),
    'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 3
})
# Missing timestamps indicate missing data
```
</details>

<details>
<summary><strong>pedestrian</strong> (Optional)</summary>

Pedestrian button press and service data.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| TimeStamp | datetime | Event timestamp | 2024-01-15 12:30:00 |
| DeviceId | str | Signal identifier (UUID) | 06ab8bb5-c909-4c5b-869e-86ed06b39188 |
| Phase | int | Pedestrian phase number | 2 |
| PedActuation | int | Button press count | 5 |
| PedServices | int | Service events (walk signal) | 1 |

**Sample:**
```python
pedestrian = pd.DataFrame({
    'TimeStamp': pd.to_datetime(['2024-01-15 12:30:00', '2024-01-15 12:30:00']),
    'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188', '3cb7be3e-123d-4f8f-a0d4-4d56c7fab684'],
    'Phase': [2, 4],
    'PedActuation': [5, 10],
    'PedServices': [1, 2]
})
```
</details>

<details>
<summary><strong>phase_skip_events</strong> (Optional)</summary>

Raw controller events for phase skip analysis.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| deviceid | str | Signal identifier (UUID) | 06ab8bb5-c909-4c5b-869e-86ed06b39188 |
| timestamp | datetime | Event timestamp | 2024-01-15 14:22:30 |
| eventid | int | NEMA event code | 104 |
| parameter | int | Event parameter (phase # or wait time) | 200 |

**Sample:**
```python
phase_skip_events = pd.DataFrame({
    'deviceid': ['06ab8bb5-c909-4c5b-869e-86ed06b39188'] * 3,
    'timestamp': pd.to_datetime(['2024-01-15 14:22:30', '2024-01-15 14:22:31', '2024-01-15 14:22:35']),
    'eventid': [612, 612, 132],  # 612=phase wait, 132=max cycle
    'parameter': [200, 200, 120]  # wait times or cycle length
})
```
</details>

<details>
<summary><strong>past_alerts</strong> (Optional)</summary>

Dictionary of past alerts by type for suppression logic.

**Structure:**
```python
past_alerts = {
    'maxout': pd.DataFrame,        # Past max-out alerts
    'actuations': pd.DataFrame,     # Past actuation alerts
    'missing_data': pd.DataFrame,   # Past missing data alerts
    'pedestrian': pd.DataFrame,     # Past pedestrian alerts
    'phase_skips': pd.DataFrame,    # Past phase skip alerts
    'system_outages': pd.DataFrame  # Past system outage alerts
}
```

Each DataFrame should contain historical alerts with columns matching the alert type's output schema (see Output DataFrames below). If a type is not provided, an empty DataFrame will be used.

**Sample:**
```python
past_alerts = {
    'maxout': pd.DataFrame({
        'DeviceId': ['06ab8bb5-c909-4c5b-869e-86ed06b39188', '3cb7be3e-123d-4f8f-a0d4-4d56c7fab684'],
        'Phase': [2, 4],
        'Date': pd.to_datetime(['2024-01-14', '2024-01-14'])
    }),
    'actuations': pd.DataFrame(),  # Empty if no past actuation alerts
    # ... other types
}
```
</details>

### Output Dictionary

<details>
<summary><strong>reports</strong></summary>

Dictionary of PDF reports keyed by region name.

**Type:** `Dict[str, BytesIO]`

Each key is a region name (e.g., "Region 2") and each value is a BytesIO object containing the PDF bytes.

**Sample Usage:**
```python
result = generator.generate(signals=signals_df)

for region, pdf_bytes in result['reports'].items():
    # Save to file
    with open(f'report_{region}.pdf', 'wb') as f:
        pdf_bytes.seek(0)
        f.write(pdf_bytes.read())
    
    # Or send via email
    send_email(attachment=pdf_bytes.getvalue(), filename=f'{region}.pdf')
```
</details>

<details>
<summary><strong>alerts</strong></summary>

Dictionary of current alert DataFrames by type.

**Type:** `Dict[str, pd.DataFrame]`

**Keys:** `maxout`, `actuations`, `missing_data`, `pedestrian`, `phase_skips`, `system_outages`

Each DataFrame contains alerts detected in the current run (after suppression).

**Sample Alert Schemas:**

**maxout:**
| Column | Type | Description |
|--------|------|-------------|
| DeviceId | str | Signal identifier (UUID) |
| Phase | int | Affected phase |
| Date | datetime | Max-out event date |
| Percent MaxOut | float | Percentage of max-outs (0-1) |
| Services | int | Number of service events |
| Alert | int | Alert flag (1=alert, 0=no alert) |

**actuations:**
| Column | Type | Description |
|--------|------|-------------|
| DeviceId | str | Signal identifier (UUID) |
| Detector | int | Affected detector |
| Date | datetime | Actuation event date |
| Total | int | Actuation count |
| PercentAnomalous | float | Percentage anomalous (0-1) |
| Alert | int | Alert flag (1=alert, 0=no alert) |

**missing_data:**
| Column | Type | Description |
|--------|------|-------------|
| DeviceId | str | Signal identifier (UUID) |
| Date | datetime | Date with missing data |
| MissingData | float | Proportion missing (0-1) |
| Alert | int | Alert flag (1=alert, 0=no alert) |

**pedestrian:**
| Column | Type | Description |
|--------|------|-------------|
| DeviceId | str | Signal identifier (UUID) |
| Phase | int | Pedestrian phase |
| Date | datetime | Service date |

**phase_skips:**
| Column | Type | Description |
|--------|------|-------------|
| DeviceId | str | Signal identifier (UUID) |
| Name | str | Signal location |
| Region | str | Geographic region |
| alert_start_date | datetime | First occurrence |
| last_alert_date | datetime | Most recent occurrence |
| date | datetime | Skip event date |
| phase | int | Affected phase |
| skips | int | Number of skips |

**Phase | int | Affected phase |
| Date | datetime | Skip event date |
| AggregatedSkips | int | Total n

<details>
<summary><strong>updated_past_alerts</strong></summary>

Dictionary of updated alert history for persistence.

**Type:** `Dict[str, pd.DataFrame]`

Same structure as `alerts` but includes historical alerts merged with current alerts. This should be persisted (e.g., to parquet files) and passed back as `past_alerts` in the next run to enable proper suppression logic.

**Sample Usage:**
```python
result = generator.generate(signals=signals_df, past_alerts=past_alerts)

# Save updated history for next run
for alert_type, df in result['updated_past_alerts'].items():
    df.to_parquet(f'past_{alert_type}_alerts.parquet', index=False)

# Next run: load and pass back
past_alerts = {
    'maxout': pd.read_parquet('past_maxout_alerts.parquet'),
    'actuations': pd.read_parquet('past_actuations_alerts.parquet'),
    # ... etc
}
result = generator.generate(signals=signals_df, past_alerts=past_alerts)
```
</details>

## Complete Example

Here's a complete working example using the test data included with this package:

```python
import pandas as pd
from pathlib import Path
from atspm_report import ReportGenerator

# ============== CONFIGURATION ==============

config = {
    'custom_logo_path': None,  # Use default ODOT logo (or specify path to your logo)
    'verbosity': 1,
    'alert_suppression_days': 14,  # Suppress alerts for 2 weeks
    'alert_retention_weeks': 3,    # Keep alert history for 3 weeks
}

# ============== LOAD INPUT DATA ==============

# Using test data (you would load from your own data source)
test_data_dir = Path('tests/data')  # Adjust path as needed

signals = pd.read_parquet(test_data_dir / 'signals.parquet')
terminations = pd.read_parquet(test_data_dir / 'terminations.parquet')
detector_health = pd.read_parquet(test_data_dir / 'detector_health.parquet')
has_data = pd.read_parquet(test_data_dir / 'has_data.parquet')
pedestrian = pd.read_parquet(test_data_dir / 'full_ped.parquet')

# Phase skip events (optional - create if you have raw event data)
phase_skip_events = None  # Or load your phase skip event data

# Load past alerts for suppression
past_alerts = {}
alert_types = ['maxout', 'actuations', 'missing_data', 'pedestrian', 'phase_skips', 'system_outages']
for alert_type in alert_types:
    file_path = Path(f'past_{alert_type}_alerts.parquet')
    if file_path.exists():
        past_alerts[alert_type] = pd.read_parquet(file_path)
    else:
        past_alerts[alert_type] = pd.DataFrame()

# ============== GENERATE REPORTS ==============

generator = ReportGenerator(config)
result = generator.generate(
    signals=signals,
    terminations=terminations,
    detector_health=detector_health,
    has_data=has_data,
    pedestrian=pedestrian,
    phase_skip_events=phase_skip_events,
    past_alerts=past_alerts,
)

# ============== PROCESS OUTPUTS ==============

# Save PDF reports
for region, pdf_bytes in result['reports'].items():
    output_path = Path(f'reports/{region.replace(" ", "_")}.pdf')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'wb') as f:
        pdf_bytes.seek(0)
        f.write(pdf_bytes.read())
    print(f"Saved: {output_path}")

# Save updated alert history
for alert_type, df in result['updated_past_alerts'].items():
    if not df.empty:
        df.to_parquet(f'past_{alert_type}_alerts.parquet', index=False)

# Export current alerts for analysis
for alert_type, df in result['alerts'].items():
    if not df.empty:
        df.to_csv(f'current_{alert_type}_alerts.csv', index=False)
        print(f"{alert_type}: {len(df)} alerts")

print(f"\nGenerated {len(result['reports'])} reports")
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `custom_logo_path` | str or None | None | Path to custom logo image (PNG/JPG). If None, uses default ODOT logo |
| `verbosity` | int | 1 | Output verbosity: 0=silent, 1=info, 2=debug |
| `alert_suppression_days` | int | 21 | Days to suppress repeat alerts for same signal/issue |
| `alert_retention_weeks` | int | 104 | Weeks to retain past alerts before cleanup |
| `historical_window_days` | int | 21 | Days of historical data to analyze |
| `alert_flagging_days` | int | 7 | Maximum age (days) for new alerts to be flagged |
| `suppress_repeated_alerts` | bool | True | Enable alert suppression logic |
| `figures_per_device` | int | 3 | Number of plots per device in reports |
| `phase_skip_alert_threshold` | int | 1 | Minimum skips to trigger phase skip alert |
| `phase_skip_retention_days` | int | 14 | Days to retain phase skip data |
| `joke_index` | int or None | None | Specific joke index (0-based). If None, auto-cycles by date |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

Contributions welcome, open an issue for problems or comment for help.
