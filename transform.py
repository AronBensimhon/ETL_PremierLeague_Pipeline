import json
import pandas as pd
import logging


class DataValidator:
    """Validates API responses and team records"""

    def __init__(self, errors):
        """
        Initialize validator.

        Args:
            errors: Error tracking dictionary
        """
        self.errors = errors
        self.logger = logging.getLogger(__name__)

    def validate_api_sports_response(self, data, data_type):
        """
        Validate API-Sports response structure.

        Args:
            data (dict): API response
            data_type (str): Type of data ('teams' or 'standings')

        Returns:
            bool: True if valid, False otherwise
        """
        try:
            if not data:
                self.logger.error(f"API-Sports {data_type}: Response is None or empty")
                self.errors["validation"].append(f"API-Sports {data_type}: Empty response")
                return False

            if "response" not in data:
                self.logger.error(f"API-Sports {data_type}: Missing 'response' key")
                self.errors["validation"].append(f"API-Sports {data_type}: Missing 'response' key")
                return False

            if not data["response"]:
                self.logger.error(f"API-Sports {data_type}: Empty response array")
                self.errors["validation"].append(f"API-Sports {data_type}: Empty response array")
                return False

            self.logger.info(f"API-Sports {data_type}: Validation passed")
            return True

        except Exception as e:
            self.logger.error(f"API-Sports {data_type}: Validation error - {e}")
            self.errors["validation"].append(f"API-Sports {data_type}: {str(e)}")
            return False

    def validate_api_football_response(self, data, data_type):
        """
        Validate API-Football response structure.

        Args:
            data (list): API response
            data_type (str): Type of data ('teams' or 'standings')

        Returns:
            bool: True if valid, False otherwise
        """
        try:
            if not data:
                self.logger.error(f"API-Football {data_type}: Response is None or empty")
                self.errors["validation"].append(f"API-Football {data_type}: Empty response")
                return False

            if not isinstance(data, list):
                self.logger.error(f"API-Football {data_type}: Response is not a list")
                self.errors["validation"].append(f"API-Football {data_type}: Invalid response type")
                return False

            self.logger.info(f"API-Football {data_type}: Validation passed")
            return True

        except Exception as e:
            self.logger.error(f"API-Football {data_type}: Validation error - {e}")
            self.errors["validation"].append(f"API-Football {data_type}: {str(e)}")
            return False

    def validate_api_sports_schema(self, teams_data, standings_data):
        """
        Validate API-Sports data has expected critical structure.
        Checks nested objects and key identifier fields only.

        Args:
            teams_data (dict): Raw API teams response
            standings_data (dict): Raw API standings response

        Returns:
            bool: True if schema valid, False otherwise
        """
        try:
            # Validate teams structure
            teams = teams_data["response"]
            if teams:
                sample_team = teams[0]

                if "team" not in sample_team or "venue" not in sample_team:
                    self.logger.error("API-Sports: Missing critical nested structure (team/venue)")
                    self.errors["validation"].append("API-Sports: Schema change - missing team or venue structure")
                    return False

                if "id" not in sample_team["team"]:
                    self.logger.error("API-Sports: Missing team.id field")
                    self.errors["validation"].append("API-Sports: Schema change - missing team.id")
                    return False

            standings = standings_data["response"][0]["league"]["standings"][0]
            if standings:
                sample_standing = standings[0]

                if "team" not in sample_standing or "all" not in sample_standing:
                    self.logger.error("API-Sports: Missing critical nested structure in standings")
                    self.errors["validation"].append("API-Sports: Schema change - missing team or all structure")
                    return False

                if "goals" not in sample_standing["all"]:
                    self.logger.error("API-Sports: Missing goals structure in standings")
                    self.errors["validation"].append("API-Sports: Schema change - missing goals structure")
                    return False

            self.logger.info("API-Sports: Schema validation passed")
            return True

        except (KeyError, IndexError, TypeError) as e:
            self.logger.error(f"API-Sports: Schema structure error - {e}")
            self.errors["validation"].append(f"API-Sports: Schema changed - {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"API-Sports: Schema validation error - {e}")
            self.errors["validation"].append(f"API-Sports: Schema validation failed - {str(e)}")
            return False

    def validate_api_football_schema(self, teams_data, standings_data):
        """
        Validate API-Football data has expected critical structure.
        Checks key identifier fields only.

        Args:
            teams_data (list): Raw API teams response
            standings_data (list): Raw API standings response

        Returns:
            bool: True if schema valid, False otherwise
        """
        try:
            if teams_data:
                sample_team = teams_data[0]

                if "team_key" not in sample_team:
                    self.logger.error("API-Football: Missing team_key field")
                    self.errors["validation"].append("API-Football: Schema change - missing team_key")
                    return False

                if "venue" not in sample_team or not isinstance(sample_team["venue"], dict):
                    self.logger.error("API-Football: Missing or invalid venue structure")
                    self.errors["validation"].append("API-Football: Schema change - venue structure changed")
                    return False

            if standings_data:
                sample_standing = standings_data[0]

                if "team_id" not in sample_standing:
                    self.logger.error("API-Football: Missing team_id in standings")
                    self.errors["validation"].append("API-Football: Schema change - missing team_id")
                    return False

                critical_fields = ["overall_league_position", "overall_league_PTS"]
                missing = [f for f in critical_fields if f not in sample_standing]

                if missing:
                    self.logger.error(f"API-Football: Missing critical fields: {missing}")
                    self.errors["validation"].append(f"API-Football: Schema change - missing {missing}")
                    return False

            self.logger.info("API-Football: Schema validation passed")
            return True

        except (KeyError, IndexError, TypeError) as e:
            self.logger.error(f"API-Football: Schema structure error - {e}")
            self.errors["validation"].append(f"API-Football: Schema changed - {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"API-Football: Schema validation error - {e}")
            self.errors["validation"].append(f"API-Football: Schema validation failed - {str(e)}")
            return False

    def validate_team_record(self, team_data, source):
        """
        Validate team record has required fields.

        Args:
            team_data (dict): Team record
            source (str): Data source name

        Returns:
            tuple: (is_valid, missing_fields)
        """
        required_fields = ["team_id", "name", "rank", "points"]
        missing = [field for field in required_fields if field not in team_data or team_data[field] is None]

        if missing:
            self.logger.warning(f"{source}: Team record missing required fields: {missing}")
            self.errors["validation"].append(f"{source}: Missing fields {missing}")
            return False, missing

        return True, []


class APISportsTransformer:
    """Transforms API-Sports data to standard schema"""

    def __init__(self, validator, errors):
        """
        Initialize transformer.

        Args:
            validator: DataValidator instance
            errors: Error tracking dictionary
        """
        self.validator = validator
        self.errors = errors
        self.logger = logging.getLogger(__name__)

    def transform(self, teams_data, standings_data, output_file):
        """
        Transform API-Sports data to standard 15-field schema.

        Args:
            teams_data (dict): Raw teams data
            standings_data (dict): Raw standings data
            output_file (str): Path to save transformed data

        Returns:
            pd.DataFrame: Transformed data
        """
        self.logger.info("Starting API-Sports transformation")

        if not self.validator.validate_api_sports_response(teams_data, "teams"):
            raise ValueError("Invalid API-Sports teams data structure")
        if not self.validator.validate_api_sports_response(standings_data, "standings"):
            raise ValueError("Invalid API-Sports standings data structure")

        if not self.validator.validate_api_sports_schema(teams_data, standings_data):
            raise ValueError("API-Sports schema validation failed - possible API change")

        teams = teams_data["response"]
        standings = standings_data["response"][0]["league"]["standings"][0]

        standings_lookup = {team["team"]["id"]: team for team in standings}
        transformed = []
        skipped_count = 0

        for team_entry in teams:
            try:
                team = team_entry.get("team", {})
                venue = team_entry.get("venue", {})
                team_id = team.get("id")

                if not team_id:
                    self.logger.warning("API-Sports: Team entry missing ID, skipping")
                    self.errors["transformation"].append("API-Sports: Team missing ID")
                    skipped_count += 1
                    continue

                if team_id not in standings_lookup:
                    team_name = team.get("name", "Unknown")
                    self.logger.warning(f"API-Sports: No standings for team_id {team_id} ({team_name})")
                    self.errors["transformation"].append(f"API-Sports: No standings for team {team_id}")
                    skipped_count += 1
                    continue

                standing = standings_lookup[team_id]
                all_stats = standing.get("all", {})
                goals = all_stats.get("goals", {})

                team_record = {
                    "team_id": team_id,
                    "name": team.get("name"),
                    "country": team.get("country"),
                    "founded": team.get("founded"),
                    "stadium": venue.get("name"),
                    "city": venue.get("city"),
                    "capacity": venue.get("capacity"),
                    "rank": standing.get("rank"),
                    "points": standing.get("points"),
                    "goal_diff": standing.get("goalsDiff"),
                    "goals_for": goals.get("for"),
                    "goals_against": goals.get("against"),
                    "win": all_stats.get("win"),
                    "draw": all_stats.get("draw"),
                    "lose": all_stats.get("lose")
                }

                is_valid, missing = self.validator.validate_team_record(team_record, "API-Sports")
                if not is_valid:
                    skipped_count += 1
                    continue

                transformed.append(team_record)

            except Exception as e:
                self.logger.error(f"API-Sports: Error transforming team - {e}")
                self.errors["transformation"].append(f"API-Sports: {str(e)}")
                skipped_count += 1
                continue

        self.logger.info(f"API-Sports: Transformed {len(transformed)} teams, skipped {skipped_count}")
        print(f"  Transformed {len(transformed)} teams, skipped {skipped_count}")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(transformed, f, indent=4)
        self.logger.info(f"API-Sports: Transformed data saved to {output_file}")

        return pd.DataFrame(transformed)


class APIFootballTransformer:
    """Transforms API-Football data to standard schema"""

    def __init__(self, validator, errors):
        """
        Initialize transformer.

        Args:
            validator: DataValidator instance
            errors: Error tracking dictionary
        """
        self.validator = validator
        self.errors = errors
        self.logger = logging.getLogger(__name__)

    def transform(self, teams_data, standings_data, output_file):
        """
        Transform API-Football data to standard 15-field schema.

        Args:
            teams_data (list): Raw teams data
            standings_data (list): Raw standings data
            output_file (str): Path to save transformed data

        Returns:
            pd.DataFrame: Transformed data
        """
        self.logger.info("Starting API-Football transformation")

        if not self.validator.validate_api_football_response(teams_data, "teams"):
            raise ValueError("Invalid API-Football teams data structure")
        if not self.validator.validate_api_football_response(standings_data, "standings"):
            raise ValueError("Invalid API-Football standings data structure")
        if not self.validator.validate_api_football_schema(teams_data, standings_data):
            raise ValueError("API-Football schema validation failed - possible API change")

        standings_lookup = {}
        for team in standings_data:
            try:
                team_id = int(team.get("team_id", 0))
                standings_lookup[team_id] = team
            except (ValueError, TypeError) as e:
                self.logger.warning(f"API-Football: Invalid team_id in standings - {e}")
                continue

        transformed = []
        skipped_count = 0

        for team_entry in teams_data:
            try:
                team_key = team_entry.get("team_key")
                if not team_key:
                    self.logger.warning("API-Football: Team entry missing team_key, skipping")
                    self.errors["transformation"].append("API-Football: Team missing team_key")
                    skipped_count += 1
                    continue
                team_id = int(team_key)
                if team_id not in standings_lookup:
                    team_name = team_entry.get("team_name", "Unknown")
                    self.logger.warning(f"API-Football: No standings for team_id {team_id} ({team_name})")
                    self.errors["transformation"].append(f"API-Football: No standings for team {team_id}")
                    skipped_count += 1
                    continue
                standing = standings_lookup[team_id]

                try:
                    goals_for = int(standing.get("overall_league_GF", 0))
                    goals_against = int(standing.get("overall_league_GA", 0))
                    goal_diff = goals_for - goals_against
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"API-Football: Error calculating goal_diff for team {team_id} - {e}")
                    goals_for = 0
                    goals_against = 0
                    goal_diff = 0

                venue = team_entry.get("venue", {})

                team_record = {
                    "team_id": team_id,
                    "name": team_entry.get("team_name"),
                    "country": team_entry.get("team_country"),
                    "founded": team_entry.get("team_founded"),
                    "stadium": venue.get("venue_name") if venue else None,
                    "city": venue.get("venue_city") if venue else None,
                    "capacity": venue.get("venue_capacity") if venue else None,
                    "rank": int(standing.get("overall_league_position", 0)),
                    "points": int(standing.get("overall_league_PTS", 0)),
                    "goal_diff": goal_diff,
                    "goals_for": goals_for,
                    "goals_against": goals_against,
                    "win": int(standing.get("overall_league_W", 0)),
                    "draw": int(standing.get("overall_league_D", 0)),
                    "lose": int(standing.get("overall_league_L", 0))
                }

                is_valid, missing = self.validator.validate_team_record(team_record, "API-Football")
                if not is_valid:
                    skipped_count += 1
                    continue

                transformed.append(team_record)

            except Exception as e:
                self.logger.error(f"API-Football: Error transforming team - {e}")
                self.errors["transformation"].append(f"API-Football: {str(e)}")
                skipped_count += 1
                continue

        self.logger.info(f"API-Football: Transformed {len(transformed)} teams, skipped {skipped_count}")
        print(f"  Transformed {len(transformed)} teams, skipped {skipped_count}")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(transformed, f, indent=4)
        self.logger.info(f"API-Football: Transformed data saved to {output_file}")

        return pd.DataFrame(transformed)