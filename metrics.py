import time
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
import logging


class MetricsTracker:
    """Tracks pipeline execution metrics for monitoring dashboard"""

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.client = bigquery.Client(project=config.gcp_project_id)

        self.metrics = {
            "pipeline_start_time": None,
            "pipeline_end_time": None,
            "api_sports_calls": 0,
            "api_football_calls": 0,
            "api_sports_latency_seconds": 0,
            "api_football_latency_seconds": 0,
            "api_sports_errors": 0,
            "api_football_errors": 0,
            "teams_processed_api_sports": 0,
            "teams_processed_api_football": 0,
            "total_errors": 0
        }

    def start_pipeline(self):
        """Mark pipeline start time"""
        self.metrics["pipeline_start_time"] = datetime.now()

    def end_pipeline(self):
        """Mark pipeline end time"""
        self.metrics["pipeline_end_time"] = datetime.now()

    def record_api_call(self, api_name, duration_seconds):
        """Record an API call with its duration"""
        if api_name == "api_sports":
            self.metrics["api_sports_calls"] += 1
            self.metrics["api_sports_latency_seconds"] += duration_seconds
        elif api_name == "api_football":
            self.metrics["api_football_calls"] += 1
            self.metrics["api_football_latency_seconds"] += duration_seconds

    def record_error(self, api_name):
        """Record an error for specific API"""
        if api_name == "api_sports":
            self.metrics["api_sports_errors"] += 1
        elif api_name == "api_football":
            self.metrics["api_football_errors"] += 1
        self.metrics["total_errors"] += 1

    def record_teams_processed(self, api_name, count):
        """Record number of teams successfully processed"""
        if api_name == "api_sports":
            self.metrics["teams_processed_api_sports"] = count
        elif api_name == "api_football":
            self.metrics["teams_processed_api_football"] = count

    def save_to_bigquery(self):
        """Save metrics to BigQuery for dashboard visualization"""
        table_id = f"{self.config.gcp_project_id}.pipeline_monitoring.execution_metrics"

        # Calculate total processing time
        if self.metrics["pipeline_start_time"] and self.metrics["pipeline_end_time"]:
            duration = (self.metrics["pipeline_end_time"] - self.metrics["pipeline_start_time"]).total_seconds()
        else:
            duration = 0

        # Calculate average latencies
        avg_api_sports_latency = (
            self.metrics["api_sports_latency_seconds"] / self.metrics["api_sports_calls"]
            if self.metrics["api_sports_calls"] > 0 else 0
        )
        avg_api_football_latency = (
            self.metrics["api_football_latency_seconds"] / self.metrics["api_football_calls"]
            if self.metrics["api_football_calls"] > 0 else 0
        )

        # Prepare DataFrame with proper timestamp handling
        df = pd.DataFrame([{
            "timestamp": pd.Timestamp(self.metrics["pipeline_start_time"]),
            "pipeline_duration_seconds": duration,
            "api_sports_call_count": self.metrics["api_sports_calls"],
            "api_football_call_count": self.metrics["api_football_calls"],
            "api_sports_avg_latency_seconds": avg_api_sports_latency,
            "api_football_avg_latency_seconds": avg_api_football_latency,
            "api_sports_error_count": self.metrics["api_sports_errors"],
            "api_football_error_count": self.metrics["api_football_errors"],
            "total_error_count": self.metrics["total_errors"],
            "teams_processed_api_sports": self.metrics["teams_processed_api_sports"],
            "teams_processed_api_football": self.metrics["teams_processed_api_football"],
            "pipeline_status": "success" if self.metrics["total_errors"] == 0 else "partial" if (
                        self.metrics["teams_processed_api_sports"] > 0 or self.metrics[
                    "teams_processed_api_football"] > 0) else "failed"
        }])

        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField("timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("pipeline_duration_seconds", "FLOAT64"),
                    bigquery.SchemaField("api_sports_call_count", "INT64"),
                    bigquery.SchemaField("api_football_call_count", "INT64"),
                    bigquery.SchemaField("api_sports_avg_latency_seconds", "FLOAT64"),
                    bigquery.SchemaField("api_football_avg_latency_seconds", "FLOAT64"),
                    bigquery.SchemaField("api_sports_error_count", "INT64"),
                    bigquery.SchemaField("api_football_error_count", "INT64"),
                    bigquery.SchemaField("total_error_count", "INT64"),
                    bigquery.SchemaField("teams_processed_api_sports", "INT64"),
                    bigquery.SchemaField("teams_processed_api_football", "INT64"),
                    bigquery.SchemaField("pipeline_status", "STRING"),
                ]
            )

            job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()

            self.logger.info("Metrics saved to BigQuery successfully")
            print("  Metrics logged to monitoring dashboard")

        except Exception as e:
            self.logger.error(f"Error saving metrics: {e}")
            print(f"  Failed to save metrics: {e}")