import requests
import json
import time
import logging


class BaseAPIExtractor:
    """
    Base class providing retry logic and error handling for API requests.

    Design Pattern: Template Method
    - Child classes define fetch() with API-specific parameters
    - Base class handles retry logic, error tracking, and logging

    Retry Strategy:
    - 3 attempts maximum
    - Exponential backoff: 5s, 10s, 15s
    - Only retries transient errors (timeout, connection)
    - HTTP errors (4xx, 5xx) fail immediately
    """

    def __init__(self, config, errors, api_name):
        """
        Initialize base API extractor.

        Args:
            config: ETLConfig instance (from config.py)
            errors: Error tracking dictionary (e.g. {"api_sports": [], "api_football": []})
            api_name (str): Name of the API (for logging and error tracking)
        """
        self.config = config
        self.errors = errors
        self.api_name = api_name
        self.logger = logging.getLogger(__name__)

    def _make_request(self, url, params=None, headers=None, save_file=None):
        """
        Make an HTTP GET request with retry logic.

        Args:
            url (str): Full API URL
            params (dict): Query parameters
            headers (dict): Request headers
            save_file (str): Optional path to save JSON response

        Returns:
            dict | list: Parsed JSON response
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.logger.info(f"{self.api_name}: Attempt {attempt + 1}/{max_retries} for {url}")
                print(f"  Attempt {attempt + 1}/{max_retries} for {self.api_name}...")
                response = requests.get(url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                data_json = response.json()
                if save_file:
                    with open(save_file, "w", encoding="utf-8") as f:
                        json.dump(data_json, f, indent=4)
                    self.logger.info(f"{self.api_name}: Data saved to {save_file}")
                    print(f"  Data saved to {save_file}")
                return data_json

            except requests.exceptions.Timeout:
                error_msg = f"{self.api_name}: Timeout on attempt {attempt + 1} for {url}"
                self.logger.error(error_msg)
                self.errors[self.api_name.lower().replace('-', '_')].append(f"Timeout attempt {attempt + 1}: {url}")

            except (requests.exceptions.ConnectionError, ConnectionResetError) as e:
                error_msg = f"{self.api_name}: Connection error on attempt {attempt + 1} - {type(e).__name__}"
                self.logger.error(error_msg)
                self.errors[self.api_name.lower().replace('-', '_')].append(f"Connection error attempt {attempt + 1}: {url}")
                print(f"  Connection error: {type(e).__name__}")

            except requests.exceptions.HTTPError as e:
                error_msg = f"{self.api_name}: HTTP error {e.response.status_code} for {url}"
                self.logger.error(error_msg)
                self.errors[self.api_name.lower().replace('-', '_')].append(f"HTTP {e.response.status_code}: {url}")
                raise

            except Exception as e:
                error_msg = f"{self.api_name}: Unexpected error fetching {url} - {e}"
                self.logger.error(error_msg)
                self.errors[self.api_name.lower().replace('-', '_')].append(f"Unexpected: {str(e)}")
                raise

            # Retry delay
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)
                self.logger.info(f"{self.api_name}: Waiting {wait_time} seconds before retry...")
                print(f"  Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print(f"  Failed after {max_retries} attempts")
                raise


class APISportsExtractor(BaseAPIExtractor):
    """Extractor for API-Sports data (Football API v3)."""

    def __init__(self, config, errors):
        super().__init__(config, errors, api_name="API-Sports")

    def fetch(self, endpoint, params=None, save_file=None):
        """
        Fetch data from API-Sports (v3).

        Args:
            endpoint (str): API endpoint (e.g., "teams", "standings")
            params (dict): Query parameters
            save_file (str): Optional path to save response

        Returns:
            dict: JSON response data
        """
        base_url = "https://v3.football.api-sports.io/"
        url = f"{base_url}{endpoint}"
        headers = {"x-apisports-key": self.config.api_sports_key}
        return self._make_request(url, params=params, headers=headers, save_file=save_file)


class APIFootballExtractor(BaseAPIExtractor):
    """Extractor for API-Football data."""

    def __init__(self, config, errors):
        super().__init__(config, errors, api_name="API-Football")

    def fetch(self, action, league_id, save_file=None):
        """
        Fetch data from API-Football.

        Args:
            action (str): API action (e.g., "get_teams", "get_standings")
            league_id (int/str): League ID
            save_file (str): Optional file path to save response

        Returns:
            list: JSON response data
        """
        base_url = "https://apiv3.apifootball.com/"
        params = {
            "action": action,
            "league_id": league_id,
            "APIkey": self.config.api_football_key
        }
        data_json = self._make_request(base_url, params=params, headers=None, save_file=save_file)

        # Players data irrelevant, comment this section if you want to keep it
        if action == "get_teams" and isinstance(data_json, list):
            for team in data_json:
                team.pop("players", None)
        return data_json
