import requests
import json
import time
import logging


class APISportsExtractor:
    """Extractor for API-Sports data"""

    def __init__(self, config, errors):
        """
        Initialize API-Sports extractor.

        Args:
            config: ETLConfig instance
            errors: Error tracking dictionary
        """
        self.config = config
        self.errors = errors
        self.logger = logging.getLogger(__name__)

    def fetch(self, endpoint, params=None, save_file=None):
        """
        Fetch data from API-Sports with error handling.

        Args:
            endpoint (str): API endpoint (e.g., "teams", "standings")
            params (dict): Query parameters
            save_file (str): Optional file path to save response

        Returns:
            dict: JSON response data
        """
        base_url = "https://v3.football.api-sports.io/"
        url = f"{base_url}{endpoint}"

        headers = {
            "x-apisports-key": self.config.api_sports_key
        }

        try:
            self.logger.info(f"API-Sports: Fetching {endpoint} with params {params}")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()

            data_json = response.json()

            if save_file:
                with open(save_file, "w", encoding="utf-8") as f:
                    json.dump(data_json, f, indent=4)
                self.logger.info(f"API-Sports: Data saved to {save_file}")
                print(f"  Data saved to {save_file}")

            return data_json

        except requests.exceptions.Timeout:
            error_msg = f"API-Sports: Timeout after 30 seconds for {endpoint}"
            self.logger.error(error_msg)
            self.errors["api_sports"].append(f"Timeout: {endpoint}")
            raise
        except requests.exceptions.HTTPError as e:
            error_msg = f"API-Sports: HTTP error {e.response.status_code} for {endpoint}"
            self.logger.error(error_msg)
            self.errors["api_sports"].append(f"HTTP {e.response.status_code}: {endpoint}")
            raise
        except requests.exceptions.ConnectionError:
            error_msg = f"API-Sports: Connection error for {endpoint}"
            self.logger.error(error_msg)
            self.errors["api_sports"].append(f"Connection error: {endpoint}")
            raise
        except Exception as e:
            error_msg = f"API-Sports: Unexpected error fetching {endpoint} - {e}"
            self.logger.error(error_msg)
            self.errors["api_sports"].append(f"Unexpected: {str(e)}")
            raise


class APIFootballExtractor:
    """Extractor for API-Football data with retry logic"""

    def __init__(self, config, errors):
        """
        Initialize API-Football extractor.

        Args:
            config: ETLConfig instance
            errors: Error tracking dictionary
        """
        self.config = config
        self.errors = errors
        self.logger = logging.getLogger(__name__)

    def fetch(self, action, league_id, save_file=None):
        """
        Fetch data from API-Football with retry logic.

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

        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.logger.info(f"API-Football: Attempt {attempt + 1}/{max_retries} for {action}")
                print(f"  Attempt {attempt + 1}/{max_retries} for API-Football {action}...")

                response = requests.get(base_url, params=params, timeout=30)
                response.raise_for_status()

                data_json = response.json()

                # Filter out players for get_teams
                if action == "get_teams":
                    for team in data_json:
                        if "players" in team:
                            team.pop("players")

                if save_file:
                    with open(save_file, "w", encoding="utf-8") as f:
                        json.dump(data_json, f, indent=4)
                    self.logger.info(f"API-Football: Data saved to {save_file}")
                    print(f"  Data saved to {save_file}")

                return data_json

            except requests.exceptions.Timeout:
                error_msg = f"API-Football: Timeout on attempt {attempt + 1} for {action}"
                self.logger.error(error_msg)
                self.errors["api_football"].append(f"Timeout attempt {attempt + 1}: {action}")

                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    self.logger.info(f"Waiting {wait_time} seconds before retry...")
                    print(f"  Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    print(f"  Failed after {max_retries} attempts")
                    raise

            except (requests.exceptions.ConnectionError, ConnectionResetError) as e:
                error_msg = f"API-Football: Connection error on attempt {attempt + 1} - {type(e).__name__}"
                self.logger.error(error_msg)
                self.errors["api_football"].append(f"Connection error attempt {attempt + 1}: {action}")
                print(f"  Connection error: {type(e).__name__}")

                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    self.logger.info(f"Waiting {wait_time} seconds before retry...")
                    print(f"  Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    print(f"  Failed after {max_retries} attempts")
                    raise

            except Exception as e:
                error_msg = f"API-Football: Unexpected error - {e}"
                self.logger.error(error_msg)
                self.errors["api_football"].append(f"Unexpected: {str(e)}")
                raise