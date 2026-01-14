"""
FPL Historical Data Ingestion DAG

This DAG orchestrates the ingestion of Fantasy Premier League (FPL)
historical and current-season data into an S3-backed data lake.

Data domains covered:
- Fixtures history
- Player gameweek statistics
- Teams history

The pipeline is designed to be:
- Idempotent (safe to re-run)
- Scalable across multiple seasons
- Fault-tolerant with retries where necessary
"""

from airflow.sdk import task, dag, Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import json

# Project-specific ingestion utilities
from include.fetch_players_url_ids import fetch_players_list
from include.fetch_history_data import fetch_fpl_historical_data
from include.utils.infer_season import infer_season, SEASONS


# Infer the current FPL season dynamically
# Used when only current-season ingestion is required
CURRENT_SEASON = infer_season()


@dag(
    dag_id="fpl_historical_data_dag",
    schedule="@weekly",               # Weekly ingestion cadence
    start_date=datetime(2026, 1, 1),   # DAG becomes active from this date
    catchup=False,                    # Prevents backfilling missed runs
    tags=["fpl", "historical_data", "data_ingestion"],
)
def fpl_historical_data_dag():
    """
    Main DAG definition function.

    Tasks inside this function define the end-to-end ingestion
    workflow for FPL historical datasets.
    """

    # 1. API Availability Check

    # Ensures the upstream data source is reachable before ingestion begins.
    # Prevents unnecessary failures in downstream tasks.
    check_api_status = HttpSensor(
        task_id="check_api_status",
        endpoint=f"{CURRENT_SEASON}/{Variable.get('fixtures_endpoint')}.csv",
        http_conn_id="fpl_api_conn",
        method="GET",
    )

    # Fixtures Data Ingestion Task
    @task
    def fetch_and_upload_fixtures(season: str):
        """
        Fetches fixture data for a given season and uploads it to S3.

        Parameters
        ----------
        season : str
            Season identifier (e.g. '2023-24')
        """

        api_url = Variable.get("historical_data_base_url")
        endpoint = Variable.get("fixtures_endpoint")
        bucket = Variable.get("fpl_bucket")

        # Fetch fixture data from endpoint
        data = fetch_fpl_historical_data(
            api_url=api_url,
            season=season,
            file_endpoint=endpoint,
        )

        # Upload directly to S3 (no local persistence)
        s3 = S3Hook(aws_conn_id="fpl_aws_conn")
        key = f"fpl_fixtures_history_data/{season}_fixtures.json"

        s3.load_string(
            string_data=json.dumps(data),
            bucket_name=bucket,
            key=key,
            replace=True,   # Idempotent overwrite
        )

    # Fetch Players Identifiers Task
    @task
    def fetch_players_ids(api_url: str):
        """
        Retrieves all player identifiers across supported seasons.

        These identifiers are later used to dynamically build
        player-specific endpoints for gameweek statistics.
        """

        api_url = Variable.get("historical_data_base_url")
        return fetch_players_list(api_url=api_url)

    # Player Gameweek Statistics Ingestion Task
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def fetch_and_upload_player_gw_stats(season: str):
        """
        Fetches gameweek-by-gameweek statistics for all players
        in a given season and uploads the aggregated result to S3.

        Retries are enabled to handle transient network/API failures.
        """

        from include.fetch_players_gw_stats import fetch_players_gw_stats

        api_url = Variable.get("historical_data_base_url")
        bucket = Variable.get("fpl_bucket")

        # Fetch player identifiers (used to construct per-player endpoints)
        players_ids = fetch_players_list(api_url=api_url)

        # Fetch aggregated player GW stats
        data = fetch_players_gw_stats(
            api_url=api_url,
            season=season,
            player_ids=players_ids,
        )

        # load to S3
        s3 = S3Hook(aws_conn_id="fpl_aws_conn")
        key = f"fpl_players_gw_stats/{season}_players_gw_stats.json"

        s3.load_string(
            string_data=json.dumps(data),
            bucket_name=bucket,
            key=key,
            replace=True,
        )

    # Teams History Ingestion Task
    @task
    def fetch_and_upload_teams_history(season: str):
        """
        Fetches historical team-level data for a given season
        and uploads it to S3.
        """

        api_url = Variable.get("historical_data_base_url")
        endpoint = Variable.get("teams_endpoint")
        bucket = Variable.get("fpl_bucket")

        data = fetch_fpl_historical_data(
            api_url=api_url,
            season=season,
            file_endpoint=endpoint,
        )

        s3 = S3Hook(aws_conn_id="fpl_aws_conn")
        key = f"fpl_teams_history_data/{season}_teams.json"

        s3.load_string(
            string_data=json.dumps(data),
            bucket_name=bucket,
            key=key,
            replace=True,
        )

    # DAG Dependency
    """
    NOTE:
    - Use `.expand(season=SEASONS)` for full historical backfills
    - Use `season=CURRENT_SEASON` for incremental, current-season ingestion

    In this setup:
    - Historical data has already been ingested
    - The DAG focuses on maintaining up-to-date data
    """

    historical_fixtures = fetch_and_upload_fixtures.expand(season=SEASONS)

    players_ids = fetch_players_ids(
        api_url=Variable.get("historical_data_base_url")
    )

    teams_history = fetch_and_upload_teams_history.expand(season=SEASONS)

    historical_players_stats = fetch_and_upload_player_gw_stats.expand(
        season=SEASONS
    )

    # execution order
    check_api_status >> historical_fixtures >> [players_ids, teams_history] >> historical_players_stats


# DAG instantiation
fpl_historical_data_dag()
