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


    # Fixtures Data Ingestion Task
    
    @task(retries=5, retry_delay=timedelta(minutes=2))
    def fetch_historical_data(season, endpoint, api_url: str=Variable.get("historical_data_base_url")):
        # Fetch fixture data from endpoint
        data = fetch_fpl_historical_data(
            api_url=api_url,
            season=season,
            file_endpoint=endpoint,
        )

        return {'season' : season, 'data': data}

    # @task
    # def save_data_to_path_task(data, output_key):
    #     return save_data_to_path(data=data['data'], 
    #                              season=data['season'], 
    #                              output_key=output_key)
    
    @task
    def load_data_to_s3(historical_data, prefix: str, output_key: str=Variable.get('fixture_s3_folder')):
        """
        Fetches fixture data for a given season and uploads it to S3.

        Parameters
        ----------
        season : str
            Season identifier (e.g. '2023-24')
        """
        upload_data_to_s3(
            data=historical_data['data'],
            season=historical_data['season'],
            output_key=output_key,
            prefix=prefix
        )


    # Fetch Players Identifiers Task
    @task
    def fetch_players_ids(api_url: str = Variable.get("historical_data_base_url")):
        """
        Retrieves all player identifiers across supported seasons.

        These identifiers are later used to dynamically build
        player-specific endpoints for gameweek statistics.
        """
        return fetch_players_list(api_url=api_url)


    # Player Gameweek Statistics Ingestion Task
    @task(retries=5, retry_delay=timedelta(minutes=2), pools='player_gamweeks')
    def fetch_player_gw_stats(player_ids, season: str):
        """
        Fetches gameweek-by-gameweek statistics for all players
        in a given season and uploads the aggregated result to S3.

        Retries are enabled to handle transient network/API failures.
        """

        from include.fetch_players_gw_stats import fetch_players_gw_stats

        api_url = Variable.get("historical_data_base_url")
        # Fetch aggregated player GW stats
        data = fetch_players_gw_stats(
            api_url=api_url,
            season=season,
            player_ids=player_ids,
        )
        
        return {
            'season': season,
            'data': data
        }
        

    # # DAG Dependency
    # """
    # NOTE:
    # - Use `.expand(season=SEASONS)` for full historical backfills
    # - Use `season=CURRENT_SEASON` for incremental, current-season ingestion

    # In this setup:
    # - Historical data has already been ingested
    # - The DAG focuses on maintaining up-to-date data
    # """

    # historical_fixtures = fetch_and_upload_fixtures.expand(season=SEASONS)



    # teams_history = fetch_and_upload_teams_history.expand(season=SEASONS)

    # historical_players_stats = fetch_and_upload_player_gw_stats.expand(
    #     season=SEASONS
    # )

    # # execution order
    # check_api_status >> historical_fixtures >> [players_ids, teams_history] >> historical_players_stats
    
    
    
    historical_fixtures = fetch_historical_data.partial(
        endpoint=Variable.get("fixtures_endpoint"),
        ).expand(
            season=SEASONS
        )


    load_fixtures_to_s3 = load_data_to_s3.expand(
        historical_data=historical_fixtures,
    )
    
    players_ids = fetch_players_ids()
    
    fetch_players_gameweek_stats = fetch_player_gw_stats.partial(
        player_ids=players_ids).expand(
        season=SEASONS[0:1],
    )
        
    load_players_gw_stats_to_s3 = load_data_to_s3.partial(output_key=Variable.get('players_gw_stats_s3_folder')).expand(
        historical_data=fetch_players_gameweek_stats,
    )
    
    teams_history = fetch_historical_data.partial(
        endpoint=Variable.get("teams_endpoint"),
        ).expand(
            season=SEASONS
        )
        
    load_teams_history_to_s3 = load_data_to_s3.partial(output_key=Variable.get('teams_history_s3_folder')).expand(
        historical_data=teams_history,
    )

    chain(
        check_api_status,
        historical_fixtures,
        [load_fixtures_to_s3, players_ids],
        [fetch_players_gameweek_stats, load_players_gw_stats_to_s3],
        teams_history,
        load_teams_history_to_s3
    )
# DAG instantiation
fpl_historical_data_dag()


#Fetch data for fixtures parameters
# api_url = Variable.get("historical_data_base_url")
# endpoint = Variable.get("fixtures_endpoint")
# bucket = Variable.get("fpl_bucket")