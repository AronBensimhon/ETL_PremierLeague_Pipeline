from google.cloud import bigquery
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import os


class BigQueryLoader:
    """Loads data to BigQuery"""

    def __init__(self, config, errors):
        """
        Initialize BigQuery loader.

        Args:
            config: ETLConfig instance
            errors: Error tracking dictionary
        """
        self.config = config
        self.errors = errors
        self.logger = logging.getLogger(__name__)
        self.client = bigquery.Client(project=config.gcp_project_id)
        self.logger.info("BigQuery client initialized")

    def load(self, dataframe, dataset_name):
        """
        Load DataFrame to BigQuery table.

        Args:
            dataframe: Data to load
            dataset_name: BigQuery dataset name
        """
        table_id = f"{self.config.gcp_project_id}.{dataset_name}.teams_standings"
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",  # change this to 'WRITE_APPEND' to avoid erasing previous data !
                autodetect=True
            )
            self.logger.info(f"Loading {len(dataframe)} rows to {table_id}")
            print(f"  Loading {len(dataframe)} rows to {table_id}...")
            job = self.client.load_table_from_dataframe(
                dataframe, table_id, job_config=job_config
            )
            job.result()
            self.logger.info(f"Successfully loaded to {table_id}")
            print(f"  Successfully loaded to BigQuery")

        except Exception as e:
            error_msg = f"Failed to load data to BigQuery: {e}"
            self.logger.error(error_msg)
            self.errors["load"].append(error_msg)
            raise


class EmailAlerter:
    """Sends email alerts for pipeline status"""

    def __init__(self, config):
        """
        Initialize email alerter.

        Args:
            config: ETLConfig instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.email_password = os.getenv("EMAIL_PASSWORD")

    def send_pipeline_status(self, api_sports_success, api_football_success, error_tracker):
        """
        Send pipeline completion email based on results.

        Args:
            api_sports_success: Whether API-Sports succeeded
            api_football_success: Whether API-Football succeeded
            error_tracker: Error tracker instance
        """
        if not self._is_configured():
            self.logger.warning("Email alerting not configured")
            return

        if not api_sports_success and not api_football_success:
            self._send_critical_failure(error_tracker)
        elif not api_sports_success or not api_football_success:
            self._send_partial_failure(api_sports_success, api_football_success, error_tracker)
        else:
            self._send_success(error_tracker)

    def _is_configured(self):
        """Check if email credentials are configured."""
        return bool(self.email_password and self.config.alert_email)

    def _send_critical_failure(self, error_tracker):
        """Send critical failure email when both APIs failed."""
        subject = "CRITICAL: ETL Pipeline Complete Failure"
        body = self._build_email_body(
            status="CRITICAL FAILURE",
            message="Both API sources failed to process.",
            api_sports_status="Failed",
            api_football_status="Failed",
            error_tracker=error_tracker
        )
        self._send_email(subject, body)
        self.logger.critical("Critical failure email sent")

    def _send_partial_failure(self, api_sports_success, api_football_success, error_tracker):
        """Send partial failure email when one API failed."""
        subject = "WARNING: ETL Pipeline Partial Failure"
        body = self._build_email_body(
            status="PARTIAL FAILURE",
            message="One API source failed, but data was partially processed.",
            api_sports_status="Success" if api_sports_success else "Failed",
            api_football_status="Success" if api_football_success else "Failed",
            error_tracker=error_tracker
        )
        self._send_email(subject, body)
        self.logger.warning("Partial failure email sent")

    def _send_success(self, error_tracker):
        """Send success email when both APIs succeeded."""
        subject = "SUCCESS: ETL Pipeline Completed Successfully"
        body = self._build_email_body(
            status="SUCCESS",
            message="Both API sources processed successfully.",
            api_sports_status="Success",
            api_football_status="Success",
            error_tracker=error_tracker
        )
        self._send_email(subject, body)
        self.logger.info("Success notification email sent")

    def _build_email_body(self, status, message, api_sports_status, api_football_status, error_tracker):
        """Build formatted email body with pipeline results."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_errors = error_tracker.get_total_errors()

        body = f"""Premier League ETL Pipeline - Status Report
                   {'=' * 50}

                    Status: {status}
                    Timestamp: {timestamp}
                    
                    {message}
                    
                    Pipeline Results:
                      - API-Sports: {api_sports_status}
                      - API-Football: {api_football_status}
                    
                    Error Summary:
                      - Total Errors: {total_errors}
                    """

        if total_errors > 0:
            body += "\nError Breakdown:\n"
            for category, error_list in error_tracker.errors.items():
                if error_list:
                    body += f"  - {category.upper()}: {len(error_list)} errors\n"
        body += f"\n{'=' * 50}\n"
        body += "For detailed logs, check: etl_pipeline.log\n"
        return body

    def _send_email(self, subject, body):
        """
        Send email via Gmail SMTP.

        Args:
            subject: Email subject line
            body: Email body content
        """
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config.alert_email
            msg['To'] = self.config.alert_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(self.config.alert_email, self.email_password)
                smtp.send_message(msg)

            self.logger.info(f"Email sent: {subject}")
            print(f"  Email notification sent: {subject}")

        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")
            print(f"  Failed to send email: {e}")


class ErrorTracker:
    """Tracks and reports errors during pipeline execution"""

    def __init__(self):
        """Initialize error tracking"""
        self.errors = {
            "api_sports": [],
            "api_football": [],
            "validation": [],
            "transformation": [],
            "load": []
        }
        self.logger = logging.getLogger(__name__)

    def has_errors(self):
        """Check if any errors occurred during pipeline execution."""
        return self.get_total_errors() > 0

    def get_total_errors(self):
        """Get total count of all errors across categories."""
        return sum(len(errors) for errors in self.errors.values())

    def print_summary(self):
        """Print error summary report to console"""
        print("\n" + "=" * 50)
        print("ERROR SUMMARY")
        print("=" * 50)

        total_errors = self.get_total_errors()

        if total_errors == 0:
            print("No errors encountered!")
            self.logger.info("Pipeline completed with no errors")
        else:
            print(f"Total errors: {total_errors}\n")
            self.logger.warning(f"Pipeline completed with {total_errors} total errors")

            for category, error_list in self.errors.items():
                if error_list:
                    print(f"{category.upper()}: {len(error_list)} errors")
                    for error in error_list[:5]:
                        print(f"  - {error}")
                    if len(error_list) > 5:
                        print(f"  ... and {len(error_list) - 5} more")
                    print()
        print("=" * 50)