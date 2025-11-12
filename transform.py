import json
import pandas as pd
import logging


class DataValidator:
    """Validates API responses and team records"""

    def __init__(self, errors):
        self.errors = errors
        self.logger = logging.getLogger(__name__)

    def _log_and_record(self, category, message, level="error"):
        """Helper to log and record validation messages."""
        getattr(self.logger, level)(message)
        self.errors["validation"].append(message)

    def validate_response(self, data, api_name, data_type, expected_type, required_keys=None):
        """
        Generic response validation for any API.

        Args:
            data: Response data to validate
            api_name: Name of the API (for logging)
            data_type: Type of data ('teams' or 'standings')
            expected_type: Expected Python type (dict, list, etc.)
            required_keys: List of required keys for dict responses

        Returns:
            bool: True if valid, False otherwise
        """
        try:
            if not data:
                self._log_and_record("validation", f"{api_name} {data_type}: Empty response")
                return False
            if not isinstance(data, expected_type):
                self._log_and_record("validation", f"{api_name} {data_type}: Invalid response type")
                return False
            if required_keys and isinstance(data, dict):
                for key in required_keys:
                    if key not in data:
                        self._log_and_record("validation", f"{api_name} {data_type}: Missing required key '{key}'")
                        return False
                    if not data[key]:
                        self._log_and_record("validation", f"{api_name} {data_type}: Empty '{key}' array")
                        return False
            self.logger.info(f"{api_name} {data_type}: Validation passed")
            return True

        except Exception as e:
            self._log_and_record("validation", f"{api_name} {data_type}: Validation error - {e}")
            return False

    def validate_api_sports_schema(self, teams_data, standings_data):
        """Validate API-Sports nested structure."""
        try:
            teams = teams_data["response"]
            standings = standings_data["response"][0]["league"]["standings"][0]
            if not teams or "team" not in teams[0] or "venue" not in teams[0]:
                raise KeyError("Missing team/venue structure in teams")

            if "id" not in teams[0]["team"]:
                raise KeyError("Missing team.id field")

            if not standings or "team" not in standings[0] or "all" not in standings[0]:
                raise KeyError("Missing team/all structure in standings")

            if "goals" not in standings[0]["all"]:
                raise KeyError("Missing goals structure in standings")

            self.logger.info("API-Sports: Schema validation passed")
            return True

        except (KeyError, IndexError, TypeError) as e:
            self._log_and_record("validation", f"API-Sports: Schema changed - {e}")
            return False
        except Exception as e:
            self._log_and_record("validation", f"API-Sports: Schema validation failed - {e}")
            return False

    def validate_api_football_schema(self, teams_data, standings_data):
        """Validate API-Football structure."""
        try:
            sample_team = teams_data[0] if teams_data else {}
            sample_standing = standings_data[0] if standings_data else {}

            if "team_key" not in sample_team:
                raise KeyError("Missing team_key in teams")
            if not isinstance(sample_team.get("venue"), dict):
                raise KeyError("Missing or invalid venue in teams")

            critical = ["team_id", "overall_league_position", "overall_league_PTS"]
            missing = [f for f in critical if f not in sample_standing]
            if missing:
                raise KeyError(f"Missing critical fields: {missing}")

            self.logger.info("API-Football: Schema validation passed")
            return True

        except (KeyError, IndexError, TypeError) as e:
            self._log_and_record("validation", f"API-Football: Schema changed - {e}")
            return False
        except Exception as e:
            self._log_and_record("validation", f"API-Football: Schema validation failed - {e}")
            return False

    def validate_team_record(self, team_data, source):
        """Ensure a team record has required fields."""
        required = ["team_id", "name", "rank", "points"]
        missing = [f for f in required if not team_data.get(f)]

        if missing:
            self.logger.warning(f"{source}: Missing fields {missing}")
            self.errors["validation"].append(f"{source}: Missing fields {missing}")
            return False, missing
        return True, []


# ---------------------------------------------------------------


class BaseTransformer:
    """Base transformer with shared helpers for saving and validation."""

    def __init__(self, validator, errors, api_name):
        """
        Initialize base transformer.

        Args:
            validator: DataValidator instance
            errors: Error tracking dictionary
            api_name: Name of the API (for logging)
        """
        self.validator = validator
        self.errors = errors
        self.api_name = api_name
        self.logger = logging.getLogger(__name__)

    def save_json(self, data, output_file):
        """Save transformed data to JSON file."""
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        self.logger.info(f"{self.api_name}: Transformed data saved to {output_file}")


class APISportsTransformer(BaseTransformer):
    """Transforms API-Sports data to standard schema."""

    def __init__(self, validator, errors):
        super().__init__(validator, errors, api_name="API-Sports")

    def transform(self, teams_data, standings_data, output_file):
        """
        Transform API-Sports data to standard 15-field schema.

        Args:
            teams_data: Raw teams data from API
            standings_data: Raw standings data from API
            output_file: Path to save transformed JSON

        Returns:
            pd.DataFrame: Transformed data
        """
        self.logger.info("Starting API-Sports transformation")

        if not self.validator.validate_response(  # validate response structure
                teams_data, self.api_name, "teams", dict, required_keys=["response"]
        ):
            raise ValueError("Invalid API-Sports teams data")

        if not self.validator.validate_response(
                standings_data, self.api_name, "standings", dict, required_keys=["response"]
        ):
            raise ValueError("Invalid API-Sports standings data")

        if not self.validator.validate_api_sports_schema(teams_data, standings_data):  # validate schema
            raise ValueError("API-Sports schema validation failed")

        teams = teams_data["response"]
        standings = standings_data["response"][0]["league"]["standings"][0]
        standings_lookup = {t["team"]["id"]: t for t in standings}

        transformed, skipped = [], 0
        for t in teams:
            try:
                team, venue = t.get("team", {}), t.get("venue", {})
                tid = team.get("id")
                if not tid or tid not in standings_lookup:
                    skipped += 1
                    continue
                s = standings_lookup[tid]
                stats = s.get("all", {})
                goals = stats.get("goals", {})
                record = {
                    "team_id": tid,
                    "name": team.get("name"),
                    "country": team.get("country"),
                    "founded": team.get("founded"),
                    "stadium": venue.get("name"),
                    "city": venue.get("city"),
                    "capacity": venue.get("capacity"),
                    "rank": s.get("rank"),
                    "points": s.get("points"),
                    "goal_diff": s.get("goalsDiff"),
                    "goals_for": goals.get("for"),
                    "goals_against": goals.get("against"),
                    "win": stats.get("win"),
                    "draw": stats.get("draw"),
                    "lose": stats.get("lose")
                }
                valid, _ = self.validator.validate_team_record(record, self.api_name)
                if valid:
                    transformed.append(record)
                else:
                    skipped += 1

            except Exception as e:
                self.logger.error(f"{self.api_name}: Error transforming team - {e}")
                self.errors["transformation"].append(f"{self.api_name}: {e}")
                skipped += 1

        self.logger.info(f"{self.api_name}: Transformed {len(transformed)} teams, skipped {skipped}")
        print(f" Transformed {len(transformed)} teams, skipped {skipped}")

        self.save_json(transformed, output_file)
        return pd.DataFrame(transformed)


class APIFootballTransformer(BaseTransformer):
    """Transforms API-Football data to standard schema."""

    def __init__(self, validator, errors):
        super().__init__(validator, errors, api_name="API-Football")

    def transform(self, teams_data, standings_data, output_file):
        """
        Transform API-Football data to standard 15-field schema.

        Args:
            teams_data: Raw teams data from API
            standings_data: Raw standings data from API
            output_file: Path to save transformed JSON

        Returns:
            pd.DataFrame: Transformed data
        """
        self.logger.info("Starting API-Football transformation")

        if not self.validator.validate_response(
                teams_data, self.api_name, "teams", list
        ):
            raise ValueError("Invalid API-Football teams data")

        if not self.validator.validate_response(
                standings_data, self.api_name, "standings", list
        ):
            raise ValueError("Invalid API-Football standings data")

        if not self.validator.validate_api_football_schema(teams_data, standings_data):
            raise ValueError("API-Football schema validation failed")

        standings_lookup = {
            int(t.get("team_id", 0)): t
            for t in standings_data
            if str(t.get("team_id", "")).isdigit()
        }
        transformed, skipped = [], 0
        for t in teams_data:
            try:
                tid = int(t.get("team_key", 0))
                if not tid or tid not in standings_lookup:
                    skipped += 1
                    continue
                s = standings_lookup[tid]
                try:  # calculate goals difference since not procured by api-football
                    gf = int(s.get("overall_league_GF", 0))
                    ga = int(s.get("overall_league_GA", 0))
                    goal_diff = gf - ga
                except (ValueError, TypeError):
                    gf, ga, goal_diff = 0, 0, 0
                venue = t.get("venue", {})
                record = {
                    "team_id": tid,
                    "name": t.get("team_name"),
                    "country": t.get("team_country"),
                    "founded": t.get("team_founded"),
                    "stadium": venue.get("venue_name") if venue else None,
                    "city": venue.get("venue_city") if venue else None,
                    "capacity": venue.get("venue_capacity") if venue else None,
                    "rank": int(s.get("overall_league_position", 0)),
                    "points": int(s.get("overall_league_PTS", 0)),
                    "goal_diff": goal_diff,
                    "goals_for": gf,
                    "goals_against": ga,
                    "win": int(s.get("overall_league_W", 0)),
                    "draw": int(s.get("overall_league_D", 0)),
                    "lose": int(s.get("overall_league_L", 0))
                }
                valid, _ = self.validator.validate_team_record(record, self.api_name)
                if valid:
                    transformed.append(record)
                else:
                    skipped += 1

            except Exception as e:
                self.logger.error(f"{self.api_name}: Error transforming team - {e}")
                self.errors["transformation"].append(f"{self.api_name}: {e}")
                skipped += 1

        self.logger.info(f"{self.api_name}: Transformed {len(transformed)} teams, skipped {skipped}")
        print(f"  Transformed {len(transformed)} teams, skipped {skipped}")
        self.save_json(transformed, output_file)
        return pd.DataFrame(transformed)
