# ATSPM_Report
Generate automated reports to flag new issues using aggregate traffic signal performance measure data from the Python atspm package.

## Configuration

The script can be configured using a JSON configuration file (default: `config.json` in the working directory):

```json
{
  "use_parquet": true,
  "connection_params": {
    "server": "your_server_name",
    "database": "your_database_name",
    "username": "your_username"
  },
  "signals_query": "SELECT DeviceId, Name, Region FROM your_signals_table",
  "num_figures": 1,
  "email_reports_flag": false
}
```

### Configuration Options

- `use_parquet`: If true, read data from local parquet files. If false, connect to database.
- `connection_params`: Database connection parameters (required when `use_parquet` is false)
  - `server`: Database server name/address
  - `database`: Database name
  - `username`: Username (uses ActiveDirectoryInteractive authentication)
- `num_figures`: Number of figures to generate per device
- `email_reports_flag`: If true, email reports instead of saving to disk

### Signals Query Requirements

The `signals_query` configuration is required when using a database connection (`use_parquet: false`). Your query must return exactly these columns:

- `DeviceId`: The unique identifier for each signal
- `Name`: A human-readable name for the signal
- `Region`: The region/area the signal belongs to

Example query structure:
```sql
SELECT 
    device_id_column as DeviceId,
    CONCAT(name_column, '-', location_column) as Name,
    CASE 
        WHEN your_region_logic THEN 'Region 1'
        WHEN other_logic THEN 'Region 2'
        ELSE 'Other'
    END as Region
FROM your_intersections_table
WHERE Region <> 'Other'  -- Optional filtering
```

Customize the query based on your agency's database schema and regional organization.

## Usage

### Command Line

Run with default configuration (reads `config.json` from working directory):
```
python ATSPMReport.py
```

Specify a different configuration file:
```
python ATSPMReport.py --config path/to/config.json
```

Override configuration settings from command line:
```
python ATSPMReport.py --use-database --email --figures 2
```

### Import as Module

The script can also be imported and used in another Python script:

```python
from ATSPMReport import main

# Option 1: Use a config file
result = main(config_path="path/to/config.json")

# Option 2: Pass a config dictionary directly
config = {
    "use_parquet": False,
    "connection_params": {
        "server": "your_server",
        "database": "your_db",
        "username": "your_user"
    },
    "num_figures": 2,
    "email_reports_flag": True
}
result = main(config_dict=config)

# Option 3: Pass individual parameters
result = main(
    use_parquet=False,
    connection_params={"server": "your_server", "database": "your_db", "username": "your_user"},
    num_figures=2,
    email_reports_flag=True
)
```

The `main()` function returns different values based on the configuration:
- When `email_reports_flag` is True: `(success, report_buffers, region_names)`
- When `email_reports_flag` is False: `pdf_paths`
- Returns `None` if data loading fails

## Code Structure and Data Flow

This section outlines the purpose of each Python script and the general flow of data through the application.

1.  **`config.json`**: Stores configuration settings like database credentials, file paths, report options (e.g., email vs. save), and analysis parameters (e.g., number of days to look back, alert suppression duration).

2.  **`ATSPMReport.py` (Main Script)**:
    *   Orchestrates the entire process.
    *   Loads configuration from `config.json` or command-line arguments.
    *   Calls `data_access.py` to fetch raw data (either from parquet files or a database).
    *   Calls `data_processing.py` to perform calculations (daily aggregates, CUSUM, alert generation).
    *   Manages past alert history: loads previous alerts, suppresses new alerts if they occurred recently, and saves updated alert history using helper functions within the script and data from `Past_Alerts/`.
    *   Calls `visualization.py` to generate plots for identified alerts.
    *   Calls `report_generation.py` to create the PDF reports (regional and consolidated).
    *   Optionally calls `email_module.py` to send the generated reports via email.

3.  **`data_access.py`**:
    *   Handles all data retrieval.
    *   Connects to the SQL Server database using specified credentials (if `use_parquet` is false).
    *   Executes SQL queries to fetch data for terminations (`dbo.terminations`), detector health (`dbo.detector_health`), signal information (custom query from config), and data availability (`dbo.has_data`).
    *   Alternatively, reads data directly from parquet files located in the `raw_data/` directory (if `use_parquet` is true).
    *   Returns the raw data as pandas DataFrames.

4.  **`data_processing.py`**:
    *   Takes raw DataFrames from `data_access.py`.
    *   Uses DuckDB and Ibis to perform SQL-like transformations and aggregations on the DataFrames in memory.
    *   Calculates daily summaries for max-outs, detector actuations/anomalies, and missing data percentages.
    *   Applies the CUSUM (Cumulative Sum) algorithm to detect statistically significant deviations from the norm in the processed data.
    *   Generates alert flags based on CUSUM results and predefined thresholds.

5.  **`visualization.py`**:
    *   Takes the processed and alerted DataFrames from `ATSPMReport.py`.
    *   Uses Matplotlib to create time-series plots for each signal/phase or signal/detector combination that triggered an alert.
    *   Highlights alert periods on the plots.
    *   Returns a list of Matplotlib figure objects grouped by region.

6.  **`table_generation.py`**:
    *   Takes alerted DataFrames and signal information.
    *   Prepares formatted pandas DataFrames specifically for display in the PDF report tables.
    *   Sorts alerts by severity and selects the top N rows based on configuration.
    *   Generates sparkline plots (small inline charts) showing the trend for each alert.
    *   Uses ReportLab to convert these DataFrames and sparklines into styled table objects suitable for the PDF report.

7.  **`report_generation.py`**:
    *   Takes the alerted DataFrames, Matplotlib figures, and signal information.
    *   Uses ReportLab to construct the final PDF reports.
    *   Creates separate reports for each region and a consolidated "All Regions" report.
    *   Arranges titles, introductory text, alert tables (generated by `table_generation.py`), and plots (from `visualization.py`) within the PDF structure.
    *   Adds headers, footers (with page numbers and region names), and styling.
    *   Includes a "Joke of the Week" section read from `jokes.csv`.
    *   Saves the PDFs to disk or returns them as in-memory byte streams for emailing.

8.  **`email_module.py`**:
    *   Takes the generated PDF reports (as byte streams or file paths) and region information.
    *   Reads recipient email addresses from `emails.csv`, matching them to specific regions or the "All" report.
    *   Connects to an SMTP server (details likely configured within the script or environment variables) to send emails.
    *   Attaches the appropriate PDF report(s) to each email.

9.  **`utils.py`**:
    *   Contains utility functions, currently just `log_message` for conditional printing based on the configured verbosity level.

**Data Flow Summary:**

Configuration -> Fetch Raw Data (`data_access`) -> Process & Analyze (`data_processing`) -> Generate Alerts (`data_processing`) -> Load Past Alerts & Suppress (`ATSPMReport`) -> Create Visualizations (`visualization`) -> Prepare Tables (`table_generation`) -> Assemble PDF Report (`report_generation`) -> Save/Email Report (`report_generation`/`email_module`) -> Update Past Alerts (`ATSPMReport`).

## Adding a New Performance Measure

To add a new performance measure (e.g., Pedestrian Delay, Split Failures) to the report, you would typically need to modify the following files:

1.  **`data_access.py`**: If the new measure requires data not already being fetched, update this file to query the necessary database table or read the relevant parquet file (likely placed in `raw_data/`).
2.  **`data_processing.py`**: Add a new function to process the raw data for the new measure. Update or add logic within the `cusum` and `alert` functions to handle the specific calculations and thresholds for this measure.
3.  **`ATSPMReport.py`**: 
    *   Integrate the new processing steps into the main workflow.
    *   Add a configuration entry for the new alert type in `ALERT_CONFIG`.
    *   Update the alert loading, suppression, and saving logic to handle the new alert type.
    *   Pass the processed data and alerts for the new measure to the visualization and report generation steps.
4.  **`visualization.py`**: Update `create_device_plots` to recognize the new measure's data structure and generate appropriate plots (e.g., labels, value columns, ranking logic).
5.  **`table_generation.py`**: Add a new `prepare_*_alerts_table` function to format the new alerts for display in a PDF table, including generating relevant sparklines.
6.  **`report_generation.py`**: Add a new section to the `generate_pdf_report` function to include the explanation, table, and plots for the new performance measure in both the regional and consolidated reports.
7.  **`config.json`** (Optional): Add any specific configuration parameters needed for the new measure (e.g., CUSUM parameters, alert thresholds).
