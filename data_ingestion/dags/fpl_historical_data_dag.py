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
from airflow.models.baseoperator import chain
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import json
from include.utils.ingest_to_s3 import upload_data_to_s3

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


    # Fixtures and Teams Data Ingestion Task
    @task(task_id='fetch_and_upload_teams_fixtures', retries=5, retry_delay=timedelta(minutes=2))
    def fetch_and_upload_teams_fixtures(season, endpoint, output_key):
        """
            Fetches historical Fantasy Premier League (FPL) team and fixtures data for a given season
            and uploads the retrieved data to Amazon S3.

            This task calls the FPL historical data API using a base URL stored in Airflow
            Variables, appends the provided endpoint, and fetches the data for the specified
            season. The resulting dataset is then persisted to S3 under the given output key.

            Retries are enabled to handle transient API or network failures.

            Args:
                season (str): The FPL season identifier (e.g., "2022-23") for which
                    fixtures data should be retrieved.
                endpoint (str): API endpoint path used to fetch the team fixtures data.
                output_key (str): S3 object key or prefix where the fetched data
                    will be uploaded.

            Returns:
                None: This task performs side effects only (data retrieval and upload to S3).
        """
        
        data = fetch_fpl_historical_data(
            api_url=Variable.get("historical_data_base_url"),
            season=season,
            file_endpoint=endpoint
        )
        
        upload_data_to_s3(
            data=data,
            season=season,
            output_key=output_key,
            file_name=None
        )
   
    # Player Gameweek Statistics Ingestion Task
    @task(retries=20, retry_delay=timedelta(minutes=5))
    def fetch_and_upload_player_gw_stats(season: str, output_key: str):
        """
        Fetches gameweek-by-gameweek statistics for all players
        in a given season and uploads the aggregated result to S3.

        Retries are enabled to handle transient network/API failures.
        """

        from include.fetch_players_gw_stats import fetch_players_gw_stats

        api_url = Variable.get("historical_data_base_url")
        player_ids = fetch_players_list(api_url=api_url)
        # Fetch aggregated player GW stats
        data = fetch_players_gw_stats(
            api_url=api_url,
            season=season,
            player_ids=player_ids,
        )
        
        upload_data_to_s3(
            data=data,
            season=season,
            output_key=output_key,
            file_name=None
        )
        


    # """
    # NOTE:
    # - Use `.expand(season=SEASONS)` for full historical backfills
    # - Use `season=CURRENT_SEASON` for incremental, current-season ingestion

    # In this setup:
    # - Historical data has already been ingested
    # - The DAG focuses on maintaining up-to-date data
    # """
    
    # fixtures
    fetch_and_load_historical_fixtures = fetch_and_upload_teams_fixtures.partial(
        endpoint=Variable.get("fixtures_endpoint"),
        output_key=Variable.get("fixture_s3_folder")).expand(season=SEASONS)
    
    # teams
    fetch_and_load_teams_history = fetch_and_upload_teams_fixtures.partial(
        endpoint=Variable.get("teams_endpoint"),
        output_key=Variable.get("teams_history_s3_folder")).expand(season=SEASONS)
    
    fetch_and_load_players_gameweek_stats = fetch_and_upload_player_gw_stats.partial(
        output_key=Variable.get("players_gw_stats_s3_folder")
    ).expand(
        season=SEASONS[0:2]
    )
    fetch_and_load_players_gameweek_stats = fetch_and_upload_player_gw_stats(
        season = "2020-21",
        output_key=Variable.get("players_gw_stats_s3_folder")
    )
        
    # # DAG Dependency

    chain(
        check_api_status,
        fetch_and_load_players_gameweek_stats,
        fetch_and_load_teams_history,
        fetch_and_load_historical_fixtures)


# DAG instantiation
fpl_historical_data_dag()