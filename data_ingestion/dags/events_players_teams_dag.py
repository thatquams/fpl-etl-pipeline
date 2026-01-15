
from airflow.sdk import task, dag, Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import json

# Project-specific ingestion utilities
from include.fetch_players_url_ids import fetch_players_list
from include.fetch_history_data import fetch_fpl_historical_data
from include.utils.infer_season import infer_season, SEASONS
from fpl_historical_data_dag import fpl_historical_data_dag

# Infer the current FPL season dynamically
# Used when only current-season ingestion is required
CURRENT_SEASON = infer_season()


@dag(
    dag_id="events_players_teams_dag",
    schedule="@daily",               # Daily ingestion cadence
    start_date=datetime(2026, 1, 1),   # DAG becomes active from this date
    catchup=False,                    # Prevents backfilling missed runs
    tags=["fpl", "gameweek (events)", "data_ingestion"],
)
def events_players_teams_dag():
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
        endpoint=f"{Variable.get('fpl_api_base_url')}",
        http_conn_id="fpl_api_conn",
        method="GET",
    )

    # Fixtures Data Ingestion Task
    @task
    def fetch_gameweeks_data(endpoint: str, api_url: str=Variable.get("fpl_api_base_url")):
        from include.fetch_teams_players_gws import fetch_teams_players_gws_data

        # Fetch gameweeks data from FPL API
        events = fetch_teams_players_gws_data(
            api_url=api_url,
            endpoint=endpoint
        )

        return events
    
    players_data = fetch_gameweeks_data(
        endpoint=Variable.get("players_endpoint")
    )

    teams_data = fetch_gameweeks_data(
        endpoint=Variable.get("teams_endpoint")
    )

    gameweeks_data = fetch_gameweeks_data(
        endpoint=Variable.get("events_endpoint")
    )

    check_api_status >> players_data >> teams_data >> gameweeks_data

events_players_teams_dag()