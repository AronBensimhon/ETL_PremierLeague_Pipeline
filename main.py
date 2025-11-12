from config import ETLConfig
from extract import APISportsExtractor, APIFootballExtractor
from transform import DataValidator, APISportsTransformer, APIFootballTransformer
from load import BigQueryLoader, EmailAlerter, ErrorTracker
from metrics import MetricsTracker


def main():
    """Main ETL pipeline orchestration"""

    config = ETLConfig()
    error_tracker = ErrorTracker()
    metrics_tracker = MetricsTracker(config)

    metrics_tracker.start_pipeline()

    print("Starting ETL Pipeline...")
    print("*" * 50)

    api_sports_extractor = APISportsExtractor(config, error_tracker.errors, metrics_tracker)
    api_football_extractor = APIFootballExtractor(config, error_tracker.errors, metrics_tracker)
    validator = DataValidator(error_tracker.errors)
    api_sports_transformer = APISportsTransformer(validator, error_tracker.errors)
    api_football_transformer = APIFootballTransformer(validator, error_tracker.errors)
    bigquery_loader = BigQueryLoader(config, error_tracker.errors)
    email_alerter = EmailAlerter(config)

    api_sports_success = False
    api_football_success = False

    print("\n[1/2] Processing API-Sports data...")
    try:
        teams_data = api_sports_extractor.fetch(
            endpoint="teams",
            params={"league": config.api_sports_league, "season": config.season},
            save_file="jsons_and_csvs/api_sports_teams.json"
        )

        standings_data = api_sports_extractor.fetch(
            endpoint="standings",
            params={"league": config.api_sports_league, "season": config.season},
            save_file="jsons_and_csvs/api_sports_standings.json"
        )

        api_sports_df = api_sports_transformer.transform(
            teams_data=teams_data,
            standings_data=standings_data,
            output_file="jsons_and_csvs/api_sports_transformed.json"
        )

        metrics_tracker.record_teams_processed("api_sports", len(api_sports_df))

        csv_file = "jsons_and_csvs/api_sports_teams_standings.csv"
        api_sports_df.to_csv(csv_file, index=False)
        print(f"\n  Exported to CSV: {csv_file}")

        config.logger.info(f"Exported API-Sports data to {csv_file}")
        print("\n  Loading API-Sports data to BigQuery...")

        bigquery_loader.load(api_sports_df, config.bigquery_dataset_api_sports)
        print("API-Sports pipeline completed successfully!")

        config.logger.info("API-Sports pipeline completed successfully")
        api_sports_success = True

    except Exception as e:
        error_msg = f"API-Sports pipeline failed: {e}"
        print(f"API-Sports pipeline failed: {e}")
        config.logger.error(error_msg)

    print("\n[2/2] Processing API-Football data...")
    try:
        teams_data = api_football_extractor.fetch(
            action="get_teams",
            league_id=config.api_football_league,
            save_file="jsons_and_csvs/api_football_teams.json"
        )

        standings_data = api_football_extractor.fetch(
            action="get_standings",
            league_id=config.api_football_league,
            save_file="jsons_and_csvs/api_football_standings.json"
        )

        api_football_df = api_football_transformer.transform(
            teams_data=teams_data,
            standings_data=standings_data,
            output_file="jsons_and_csvs/api_football_transformed.json"
        )

        metrics_tracker.record_teams_processed("api_football", len(api_football_df))

        csv_file = "jsons_and_csvs/api_football_teams_standings.csv"
        api_football_df.to_csv(csv_file, index=False)
        print(f"\n  Exported to CSV: {csv_file}")

        config.logger.info(f"Exported API-Football data to {csv_file}")
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

    metrics_tracker.end_pipeline()

    print("\n" + "*" * 50)
    print("ETL Pipeline finished!")

    if api_sports_success or api_football_success:
        print("\nGenerated files:")

        if api_sports_success:
            print("  - api_sports_teams_standings.csv")

        if api_football_success:
            print("  - api_football_teams_standings.csv")

    error_tracker.print_summary()

    email_alerter.send_pipeline_status(api_sports_success, api_football_success, error_tracker)

    print("\nSaving metrics to monitoring dashboard...")
    metrics_tracker.save_to_bigquery()

    config.logger.info("ETL Pipeline execution completed")


if __name__ == "__main__":
    main()