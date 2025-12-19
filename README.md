# ATSPM Report Package

[![PyPI version](https://img.shields.io/pypi/v/atspm-report.svg)](https://pypi.org/project/atspm-report/)
[![Python versions](https://img.shields.io/pypi/pyversions/atspm-report.svg)](https://pypi.org/project/atspm-report/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Downloads](https://static.pepy.tech/badge/atspm-report)](https://pepy.tech/project/atspm-report)

A Python package for generating Automated Traffic Signal Performance Measures (ATSPM) reports. This package analyzes traffic signal data to identify issues like max-outs, actuation problems, pedestrian service issues, phase skipping, and system outages.

![Example Report](images/example_report.png)

## Alert Types

The package identifies 6 key types of traffic signal performance issues:

### 1. Max-Out Alerts
Detects when a phase terminates via max-out (rather than gap-out) too frequently, indicating potential capacity issues.

![Example Max-Out Alert](images/example_phase_termination.png)

### 2. Actuation Alerts
Identifies detectors with zero or abnormally low actuations, suggesting detector malfunction.

![Example Detector Alert](images/example_detector.png)

### 3. Pedestrian Alerts
Detects when pedestrian buttons are pressed but no walk signal is provided, indicating service failures.

![Example Pedestrian Alert](images/example_ped.png)

### 4. Missing Data Alerts
Flags signals that are not reporting data to the system, indicating communication or hardware issues.

### 5. Phase Skip Alerts
Identifies phases that are being skipped in coordination mode when they shouldn't be, indicating timing issues.

![Example Phase Skip Alert](images/example_phase_skip.png)

### 6. System Outage Alerts
Detects prolonged periods when signals are completely offline.

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
from atspm_report import ReportGenerator

# Configure the generator
config = {
    'logo_path': None,  # Use default ODOT logo
    'verbosity': 2,
    'suppression_days': 14,
    'retention_days': 21,
}

# Create generator instance
generator = ReportGenerator(config)

# Generate reports (minimal example with required DataFrame)
result = generator.generate(signals=signals_df)

# Access outputs
for region, pdf_bytes in result['reports'].items():
    with open(f'report_{region}.pdf', 'wb') as f:
        f.write(pdf_bytes.getvalue())
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
  - `logo_path` (str, optional): Path to custom logo image. If None, uses default ODOT logo
  - `verbosity` (int, optional): Output verbosity level (0=silent, 1=errors only, 2=info, 3=debug). Default: 2
  - `suppression_days` (int, optional): Days to suppress repeat alerts. Default: 14
  - `retention_days` (int, optional): Days to retain past alerts for suppression. Default: 21

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
| DeviceId | int | Unique signal identifier | 7115 |
| Name | str | Signal location name | 01-001-Main St & 1st Ave |
| Region | str | Geographic region assignment | Region 2 |

**Sample:**
```python
signals = pd.DataFrame({
    'DeviceId': [7115, 7116, 7117],
    'Name': ['01-001-Main St & 1st Ave', '01-002-Oak St & 2nd Ave', '06-001-Pine St & 3rd Ave'],
    'Region': ['Region 2', 'Region 2', 'Region 3']
})
```
</details>

<details>
<summary><strong>terminations</strong> (Optional)</summary>

Phase termination data for detecting max-out conditions.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| TimeStamp | datetime | Event timestamp | 2024-01-15 08:30:00 |
| DeviceId | int | Signal identifier | 7115 |
| phase | int | Phase number (1-8) | 2 |
| maxout | int | Max-out indicator (1=yes, 0=no) | 1 |

**Sample:**
```python
terminations = pd.DataFrame({
    'TimeStamp': pd.to_datetime(['2024-01-15 08:30:00', '2024-01-15 08:35:00']),
    'DeviceId': [7115, 7115],
    'phase': [2, 4],
    'maxout': [1, 0]
})
```
</details>

<details>
<summary><strong>detector_health</strong> (Optional)</summary>

Detector actuation counts for health monitoring.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| TimeStamp | datetime | Event timestamp | 2024-01-15 08:00:00 |
| DeviceId | int | Signal identifier | 7115 |
| detector | int | Detector number | 1 |
| actuations | int | Actuation count | 150 |

**Sample:**
```python
detector_health = pd.DataFrame({
    'TimeStamp': pd.to_datetime(['2024-01-15 08:00:00', '2024-01-15 08:00:00']),
    'DeviceId': [7115, 7115],
    'detector': [1, 2],
    'actuations': [150, 0]
})
```
</details>

<details>
<summary><strong>has_data</strong> (Optional)</summary>

Records indicating whether signal data is available.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| TimeStamp | datetime | Event timestamp | 2024-01-15 00:00:00 |
| DeviceId | int | Signal identifier | 7115 |
| has_data | int | Data availability (1=yes, 0=no) | 1 |

**Sample:**
```python
has_data = pd.DataFrame({
    'TimeStamp': pd.to_datetime(['2024-01-15', '2024-01-16', '2024-01-17']),
    'DeviceId': [7115, 7115, 7115],
    'has_data': [1, 0, 1]
})
```
</details>

<details>
<summary><strong>pedestrian</strong> (Optional)</summary>

Pedestrian button press and service data.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| TimeStamp | datetime | Event timestamp | 2024-01-15 12:30:00 |
| DeviceId | int | Signal identifier | 7115 |
| phase | int | Pedestrian phase number | 2 |
| actuations | int | Button press count | 5 |
| service | int | Service events (walk signal) | 1 |

**Sample:**
```python
pedestrian = pd.DataFrame({
    'TimeStamp': pd.to_datetime(['2024-01-15 12:30:00', '2024-01-15 12:30:00']),
    'DeviceId': [7115, 7116],
    'phase': [2, 4],
    'actuations': [5, 10],
    'service': [1, 0]
})
```
</details>

<details>
<summary><strong>phase_skip_events</strong> (Optional)</summary>

Raw controller events for phase skip analysis.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| deviceid | int | Signal identifier | 7115 |
| timestamp | datetime | Event timestamp | 2024-01-15 14:22:30 |
| eventid | int | NEMA event code | 104 |
| parameter | int | Event parameter (phase #) | 2 |

**Sample:**
```python
phase_skip_events = pd.DataFrame({
    'deviceid': [7115, 7115, 7115],
    'timestamp': pd.to_datetime(['2024-01-15 14:22:30', '2024-01-15 14:22:31', '2024-01-15 14:22:35']),
    'eventid': [104, 612, 627],
    'parameter': [2, 2, 2]
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
        'DeviceId': [7115, 7116],
        'Name': ['01-001-Main St & 1st Ave', '01-002-Oak St & 2nd Ave'],
        'Region': ['Region 2', 'Region 2'],
        'alert_start_date': pd.to_datetime(['2024-01-10', '2024-01-12']),
        'last_alert_date': pd.to_datetime(['2024-01-14', '2024-01-14']),
        'maxout_date': pd.to_datetime(['2024-01-14', '2024-01-14']),
        'phase': [2, 4],
        'maxout_pct': [75.5, 82.3]
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
| DeviceId | int | Signal identifier |
| Name | str | Signal location |
| Region | str | Geographic region |
| alert_start_date | datetime | First occurrence |
| last_alert_date | datetime | Most recent occurrence |
| maxout_date | datetime | Max-out event date |
| phase | int | Affected phase |
| maxout_pct | float | Percentage of max-outs |

**actuations:**
| Column | Type | Description |
|--------|------|-------------|
| DeviceId | int | Signal identifier |
| Name | str | Signal location |
| Region | str | Geographic region |
| alert_start_date | datetime | First occurrence |
| last_alert_date | datetime | Most recent occurrence |
| date | datetime | Actuation issue date |
| detector | int | Affected detector |
| actuations | int | Actuation count |

**missing_data:**
| Column | Type | Description |
|--------|------|-------------|
| DeviceId | int | Signal identifier |
| Name | str | Signal location |
| Region | str | Geographic region |
| alert_start_date | datetime | First occurrence |
| last_alert_date | datetime | Most recent occurrence |
| missing_date | datetime | Date with missing data |

**pedestrian:**
| Column | Type | Description |
|--------|------|-------------|
| DeviceId | int | Signal identifier |
| Name | str | Signal location |
| Region | str | Geographic region |
| alert_start_date | datetime | First occurrence |
| last_alert_date | datetime | Most recent occurrence |
| date | datetime | Service issue date |
| phase | int | Pedestrian phase |
| actuations | int | Button presses |
| service | int | Service events |

**phase_skips:**
| Column | Type | Description |
|--------|------|-------------|
| DeviceId | int | Signal identifier |
| Name | str | Signal location |
| Region | str | Geographic region |
| alert_start_date | datetime | First occurrence |
| last_alert_date | datetime | Most recent occurrence |
| date | datetime | Skip event date |
| phase | int | Affected phase |
| skips | int | Number of skips |

**system_outages:**
| Column | Type | Description |
|--------|------|-------------|
| DeviceId | int | Signal identifier |
| Name | str | Signal location |
| Region | str | Geographic region |
| alert_start_date | datetime | First occurrence |
| last_alert_date | datetime | Most recent occurrence |
| outage_date | datetime | Outage date |
| hours_offline | float | Duration offline |
</details>

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

```python
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from atspm_report import ReportGenerator

# ============== CONFIGURATION ==============

config = {
    'logo_path': 'my_agency_logo.png',  # Custom logo
    'verbosity': 2,
    'suppression_days': 14,  # Suppress alerts for 2 weeks
    'retention_days': 21,    # Keep alert history for 3 weeks
}

# ============== LOAD INPUT DATA ==============

# Load data from your source (database, files, etc.)
signals = pd.read_parquet('signals.parquet')
terminations = pd.read_parquet('terminations.parquet')
detector_health = pd.read_parquet('detector_health.parquet')
has_data = pd.read_parquet('has_data.parquet')
pedestrian = pd.read_parquet('pedestrian.parquet')
phase_skip_events = pd.read_parquet('phase_skip_events.parquet')

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
| `logo_path` | str or None | None | Path to custom logo image (PNG/JPG). If None, uses default ODOT logo |
| `verbosity` | int | 2 | Output verbosity: 0=silent, 1=errors, 2=info, 3=debug |
| `suppression_days` | int | 14 | Days to suppress repeat alerts for same signal/issue |
| `retention_days` | int | 21 | Days to retain past alerts before cleanup |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

Contributions welcome, open a issue for issues or coment for help.
