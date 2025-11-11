import os
from dotenv import load_dotenv
import logging


class ETLConfig:
    """Configuration manager for ETL Pipeline"""

    def __init__(self):
        """Load all configuration from environment variables"""
        load_dotenv()

        # API Configuration
        self.api_sports_key = os.getenv("API_SPORTS_KEY")
        self.api_football_key = os.getenv("API_FOOTBALL_KEY")
        self.season = os.getenv("SEASON")
        self.api_sports_league = os.getenv("API_SPORTS_LEAGUE")
        self.api_football_league = os.getenv("API_FOOTBALL_LEAGUE")

        # GCP Configuration
        self.gcp_project_id = os.getenv("GCP_PROJECT_ID")
        self.bigquery_dataset_api_sports = os.getenv("BIGQUERY_DATASET_API_SPORTS")
        self.bigquery_dataset_api_football = os.getenv("BIGQUERY_DATASET_API_FOOTBALL")
        self.google_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        # Email Configuration
        self.alert_email = os.getenv("ALERT_EMAIL")

        # Setup logging
        self._setup_logging()

        # Create logger
        self.logger = logging.getLogger(__name__)
        self.logger.info("ETL Pipeline configuration loaded")

    def _setup_logging(self):
        """Configure logging for the entire pipeline"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('etl_pipeline.log'),
                logging.StreamHandler()
            ]
        )