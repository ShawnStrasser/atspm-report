import os
import pandas as pd
import win32com.client
from typing import List, Optional, Dict, Union
from io import BytesIO
from datetime import datetime
from utils import log_message  # Import the utility function

def load_email_recipients(csv_path: str) -> Dict[str, str]:
    """
    Load email recipients from CSV file.
    
    Args:
        csv_path: Path to CSV file with Region and Email columns
        
    Returns:
        Dictionary mapping region names to email addresses
    """
    try:
        print(f"Loading email recipients from {csv_path}")
        emails_df = pd.read_csv(csv_path)
        # Convert to dictionary mapping each region to a list of emails
        email_dict = emails_df.groupby('Region')['Email'].apply(list).to_dict()
        return email_dict
    except Exception as e:
        print(f"Error loading email recipients: {e}")
        return {}

def email_reports(
        region_reports: List[Union[str, BytesIO]],
        regions: List[str],
        report_in_memory: bool = False,
        email_csv: str = "emails.csv",
        verbosity: int = 1
) -> bool:
    """
    Email reports to recipients using MS Outlook.
    
    Args:
        region_reports: List of file paths or BytesIO objects containing report PDFs
        regions: List of region names corresponding to the reports
        report_in_memory: If True, region_reports contains BytesIO objects, otherwise file paths
        email_csv: Path to CSV file containing email recipients
        verbosity: Verbosity level (0=silent, 1=info, 2=debug)
        
    Returns:
        Success status
    """
    # Load email recipients
    try:
        emails_df = pd.read_csv(email_csv)
    except FileNotFoundError:
        log_message("Error: emails.csv not found. Cannot send emails.", 1, verbosity)
        return False
    except Exception as e:
        log_message(f"Error reading emails.csv: {e}", 1, verbosity)
        return False
    
    recipients = load_email_recipients(email_csv)
    
    if not recipients:
        log_message("No email recipients found. Check the emails.csv file.", 1, verbosity)
        return False
    
    # Get today's date for the email subject
    today = datetime.today().strftime("%B %d, %Y")
    
    try:
        # Create Outlook application object
        outlook = win32com.client.Dispatch("Outlook.Application")
        
        # Track success
        all_success = True
        
        # Send emails for each region
        for i, region in enumerate(regions):
            if region not in recipients:
                log_message(f"No email recipient found for {region}. Skipping.", 1, verbosity)
                all_success = False
                continue
                
            # Create a new email
            mail = outlook.CreateItem(0)  # 0 corresponds to olMailItem
            mail.Subject = f"ATSPM Report - {region} - {today}"
            # Join all email addresses for the region into a single TO field
            mail.To = ';'.join(recipients[region])
            
            # Email body
            mail.HTMLBody = f"""
            <p>Hello,</p>
            <p>Sorry this is late! Ped detector health monitoring is now included (but still under development).</p>
            <p>Attached is the Automated Traffic Signal Performance Measures report for {region} dated {today}.</p>
            <p>Please review the findings and address any issues identified in the report.</p>
            <p><br><i>Note: This is an automated email generated by the open source ATSPM Report, source code available on <a href="https://github.com/ShawnStrasser/atspm-report">GitHub</a>.</i></p>
            <p>Please review the list of recipents including for you region and reply to this email to add or remove anyone. Thanks!</p>
            """
            
            # Attachment handling depends on whether report is in memory or on disk
            if report_in_memory:
                # Reports are in memory as BytesIO objects
                buffer = region_reports[i]
                buffer.seek(0)  # Ensure we're at the beginning of the buffer
                
                # Create a temporary file to attach
                temp_path = f"ATSPM_report_{region.replace(' ', '_')}.pdf"
                with open(temp_path, 'wb') as f:
                    f.write(buffer.getvalue())
                
                # Add the attachment
                mail.Attachments.Add(os.path.abspath(temp_path))
                
                # Send the email
                mail.Send()
                
                # Clean up temporary file
                try:
                    os.remove(temp_path)
                except Exception as e:
                    log_message(f"Warning: Could not remove temporary file {temp_path}: {e}", 1, verbosity)
            else:
                # Reports are on disk
                report_path = region_reports[i]
                
                # Add the attachment if file exists
                if os.path.exists(report_path):
                    mail.Attachments.Add(os.path.abspath(report_path))
                    mail.Send()
                else:
                    log_message(f"Error: Report file not found at {report_path}", 1, verbosity)
                    all_success = False
            
            log_message(f"Report for {region} sent to {recipients[region]}", 1, verbosity)
            
        return all_success
    
    except Exception as e:
        log_message(f"Error sending emails: {e}", 1, verbosity)
        return False