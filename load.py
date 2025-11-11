from google.cloud import bigquery
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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
            dataframe (pd.DataFrame): Data to load
            dataset_name (str): BigQuery dataset name
        """
        table_id = f"{self.config.gcp_project_id}.{dataset_name}.teams_standings"

        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",  # todo: APPEND when prod ready
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
    """Sends email alerts on pipeline failures"""

    def __init__(self, config):
        """
        Initialize email alerter.

        Args:
            config: ETLConfig instance
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

    def send_alert(self, subject, body):
        """
        Send alert email via Gmail SMTP.

        Args:
            subject (str): Email subject
            body (str): Email body
        """
        email_password = os.getenv("EMAIL_PASSWORD")

        if not email_password or not self.config.alert_email:
            self.logger.warning("Email alerting not configured (missing password or email)")
            return

        try:
            msg = MIMEMultipart()
            msg['From'] = self.config.alert_email
            msg['To'] = self.config.alert_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(self.config.alert_email, email_password)
                smtp.send_message(msg)

            self.logger.info("Alert email sent successfully via Gmail")
            print("  Alert email sent successfully")

        except Exception as e:
            self.logger.error(f"Failed to send alert email: {e}")
            print(f"  Failed to send alert email: {e}")


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

    def print_summary(self):
        """Print error summary report"""
        print("\n" + "=" * 50)
        print("ERROR SUMMARY")
        print("=" * 50)

        total_errors = sum(len(errors) for errors in self.errors.values())

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

    def get_error_report(self):
        """
        Generate error report for email alerts.

        Returns:
            str: Formatted error report
        """
        report = "Error Summary:\n"
        for category, error_list in self.errors.items():
            report += f"- {category}: {len(error_list)} errors\n"
        report += f"\nTotal errors: {sum(len(e) for e in self.errors.values())}\n"
        report += "\nCheck etl_pipeline.log for detailed error information."
        return report
