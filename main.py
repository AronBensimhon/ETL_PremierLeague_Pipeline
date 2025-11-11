from config import ETLConfig
from extract import APISportsExtractor, APIFootballExtractor
from transform import DataValidator, APISportsTransformer, APIFootballTransformer
from load import BigQueryLoader, EmailAlerter, ErrorTracker


def main():
    """Main ETL pipeline orchestration"""

    # Initialize configuration and error tracking
    config = ETLConfig()
    error_tracker = ErrorTracker()

    print("Starting ETL Pipeline...")
    print("*" * 50)

    # Initialize components
    api_sports_extractor = APISportsExtractor(config, error_tracker.errors)
    api_football_extractor = APIFootballExtractor(config, error_tracker.errors)
    validator = DataValidator(error_tracker.errors)
    api_sports_transformer = APISportsTransformer(validator, error_tracker.errors)
    api_football_transformer = APIFootballTransformer(validator, error_tracker.errors)
    bigquery_loader = BigQueryLoader(config, error_tracker.errors)
    email_alerter = EmailAlerter(config)

    # Track pipeline success
    api_sports_success = False
    api_football_success = False

    # Process API-Sports
    print("\n[1/2] Processing API-Sports data...")
    try:
        # Extract
        teams_data = api_sports_extractor.fetch(
            endpoint="teams",
            params={"league": config.api_sports_league, "season": config.season},
            save_file="api_sports_teams.json"
        )

        standings_data = api_sports_extractor.fetch(
            endpoint="standings",
            params={"league": config.api_sports_league, "season": config.season},
            save_file="api_sports_standings.json"
        )

        # Transform
        api_sports_df = api_sports_transformer.transform(
            teams_data=teams_data,
            standings_data=standings_data,
            output_file="api_sports_transformed.json"
        )

        # Export to CSV
        csv_file = "api_sports_teams_standings.csv"
        api_sports_df.to_csv(csv_file, index=False)
        print(f"\n  Exported to CSV: {csv_file}")
        config.logger.info(f"Exported API-Sports data to {csv_file}")

        # Load to BigQuery
        print("\n  Loading API-Sports data to BigQuery...")
        bigquery_loader.load(api_sports_df, config.bigquery_dataset_api_sports)
        print("API-Sports pipeline completed successfully!")
        config.logger.info("API-Sports pipeline completed successfully")
        api_sports_success = True

    except Exception as e:
        error_msg = f"API-Sports pipeline failed: {e}"
        print(f"API-Sports pipeline failed: {e}")
        config.logger.error(error_msg)

    # Process API-Football
    print("\n[2/2] Processing API-Football data...")
    try:
        # Extract
        teams_data = api_football_extractor.fetch(
            action="get_teams",
            league_id=config.api_football_league,
            save_file="api_football_teams.json"
        )

        standings_data = api_football_extractor.fetch(
            action="get_standings",
            league_id=config.api_football_league,
            save_file="api_football_standings.json"
        )

        # Transform
        api_football_df = api_football_transformer.transform(
            teams_data=teams_data,
            standings_data=standings_data,
            output_file="api_football_transformed.json"
        )

        # Export to CSV
        csv_file = "api_football_teams_standings.csv"
        api_football_df.to_csv(csv_file, index=False)
        print(f"\n  Exported to CSV: {csv_file}")
        config.logger.info(f"Exported API-Football data to {csv_file}")

        # Load to BigQuery
        print("\n  Loading API-Football data to BigQuery...")
        bigquery_loader.load(api_football_df, config.bigquery_dataset_api_football)
        print("API-Football pipeline completed successfully!")
        config.logger.info("API-Football pipeline completed successfully")
        api_football_success = True

    except Exception as e:
        error_msg = f"API-Football pipeline failed: {e}"
        print(f"API-Football pipeline failed: {e}")
        config.logger.error(error_msg)

        if api_sports_success:
            print("\nNote: API-Football may be temporarily unavailable.")
            print("The API-Sports data was still loaded successfully to BigQuery.")

    # Print results
    print("\n" + "*" * 50)
    print("ETL Pipeline finished!")

    # Show generated files
    if api_sports_success or api_football_success:
        print("\nGenerated files:")
        if api_sports_success:
            print("  - api_sports_teams_standings.csv")
        if api_football_success:
            print("  - api_football_teams_standings.csv")

    # Print error summary
    error_tracker.print_summary()

    # Send alert email if critical failure occurred
    if not api_sports_success and not api_football_success:
        alert_subject = "CRITICAL: ETL Pipeline Complete Failure"
        alert_body = f"""ETL Pipeline Critical Failure Report

Both API sources failed to process.

{error_tracker.get_error_report()}"""
        email_alerter.send_alert(alert_subject, alert_body)
        config.logger.critical("Critical failure: Both API sources failed")

    elif not api_sports_success or not api_football_success:
        alert_subject = "WARNING: ETL Pipeline Partial Failure"
        alert_body = f"""ETL Pipeline Partial Failure Report

API-Sports success: {api_sports_success}
API-Football success: {api_football_success}

{error_tracker.get_error_report()}"""
        email_alerter.send_alert(alert_subject, alert_body)
        config.logger.warning("Partial failure: One API source failed")

    config.logger.info("ETL Pipeline execution completed")


if __name__ == "__main__":
    main()

# todo : I wanna focus on that part now :

"""I wanna focus on that part now : 
"(Optional) Schedules the pipeline or demonstrates how it would run periodically."

I only want to demonstrate how it would run periodically. Im thinking of using docker,
 kubernetes and cron jobs running at night for this pipeline. What do you think ? 
 Im not saying that i want to implement the periodic deployment of the pipeline but just demonstrate 
 how it would be done by like creating yaml files with the deployement params and config etc. 
 """

