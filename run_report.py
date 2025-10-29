##### THIS FILE IS ODOT SPECIFIC, FOR RUNNING THE REPORT ON A SCHEDULED BASIS #####

input('Ready to log in? Press Enter to continue...')

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL
import os
from datetime import datetime as dt
import schedule
import time
import subprocess
import sys

# Connection details
server = 'kinsigsynapseprod-ondemand.sql.azuresynapse.net'
database = 'PerformanceMetrics'
driver = 'ODBC Driver 17 for SQL Server'
username = "Shawn.STRASSER@ODOT.oregon.gov"

# Build the connection URL for SQLAlchemy
connection_url = URL.create(
    "mssql+pyodbc",
    username=username,
    password=os.getenv("OLOG"),  # Leave as None for Active Directory Interactive
    host=server,
    database=database,
    query={"driver": driver, "Authentication": "ActiveDirectoryInteractive"}
)

# Create the SQLAlchemy engine
engine = sa.create_engine(connection_url)
# Dummy query to test connection
query = "SELECT 1 as test"
test = pd.read_sql(query, engine)
if test['test'][0] == 1:
    print("Connection successful!")
else:
    print("Connection failed.")
    input('Press Enter to exit...')
    exit()



def get_data():
    # get the current date
    today = dt.today().date()
    # get three weeks ago
    three_weeks_ago = today - pd.DateOffset(weeks=3)

    print("Current date:", today)
    #print("Three weeks ago:", three_weeks_ago)

    sql = f"""
    SELECT distinct * FROM
    (SELECT 
        DeviceId,
        CONCAT(name, '-', intersection_name) as Name,
        CASE 
            WHEN LEFT(name, 2) IN ('01', '02', '03', '04', '05') THEN 'Region 2'
            WHEN LEFT(name, 2) IN ('2B', '2C') THEN 'Region 1'
            WHEN LEFT(name, 2) IN ('06', '07', '08') THEN 'Region 3'
            WHEN LEFT(name, 2) IN ('09', '10', '11') THEN 'Region 4'
            WHEN LEFT(name, 2) IN ('12', '13', '14') THEN 'Region 5'
            ELSE 'Other'
        END as Region,
        group_name
    FROM intersections) t
    WHERE Region <> 'Other'
    """

    intersections = pd.read_sql(sql, engine)
    intersections.to_parquet('raw_data/signals.parquet', index=False)

    #print("has_data")
    sql = f"""
    SELECT distinct * FROM has_data
    WHERE TimeStamp >= '{three_weeks_ago}'
    -- and less than today
    and TimeStamp < '{today}'
    """

    pd.read_sql(sql, engine).to_parquet('raw_data/has_data.parquet', index=False)

    #print("ped")
    sql = f"""
    SELECT distinct * FROM full_ped
    WHERE TimeStamp >= '{three_weeks_ago}'
    and TimeStamp < '{today}'
    """

    pd.read_sql(sql, engine).to_parquet('raw_data/full_ped.parquet', index=False)


    #print("maxouts")
    sql = f"""
    SELECT distinct * FROM terminations
    WHERE TimeStamp >= '{three_weeks_ago}'
    and TimeStamp < '{today}'
    """

    pd.read_sql(sql, engine).to_parquet('raw_data/terminations.parquet', index=False)


    #print("detector_health")

    sql = f"""
    SELECT distinct * FROM detector_health
    WHERE TimeStamp >= '{three_weeks_ago}'
    and TimeStamp < '{today}'
    """

    actuations = pd.read_sql(sql, engine)
    actuations.to_parquet('raw_data/detector_health.parquet', index=False)



def run_daily_tasks():
    """Run get_data() then ATSPMReport.py"""
    print("Running get_data()...")

    # Up to three trys to get data
    for try_count in range(3):
        print(f"Attempt {try_count + 1} to get data...")
        try:
            get_data()
            break  # Exit loop if successful
        except Exception as e:
            print(f"Attempt {try_count + 1} failed: {e}")
            try_count += 1
            time.sleep(30)  # Wait before retrying
    
    print("Running ATSPMReport.py...")
    subprocess.run([sys.executable, "ATSPMReport.py"])

# Schedule for 6 AM weekdays
schedule.every().monday.at("06:00").do(run_daily_tasks)
schedule.every().tuesday.at("06:00").do(run_daily_tasks)
schedule.every().wednesday.at("06:00").do(run_daily_tasks)
schedule.every().thursday.at("06:00").do(run_daily_tasks)
schedule.every().friday.at("06:00").do(run_daily_tasks)

run_now = input("Run now? (y/n): ").strip().lower()
if run_now == 'y':
    run_daily_tasks()

print("Scheduler started. Waiting for scheduled tasks...")
while True:
    schedule.run_pending()
    time.sleep(300)  # Check every 5 minutes