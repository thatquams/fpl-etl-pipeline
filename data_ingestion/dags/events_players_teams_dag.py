
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
from include.utils.ingest_to_s3 import upload_data_to_s3

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

    # Fixtures Data Ingestion Task
    def create_fetch_task(task_name, endpoint, output_key):
        """
            Factory function that creates an Airflow task for fetching Fantasy Premier League (FPL)
            fixtures–related data and uploading it to Amazon S3.

            The returned task pulls data from the FPL API using the provided endpoint and a base URL
            stored in Airflow Variables. The fetched data is then uploaded to S3 under the specified
            output key. This pattern enables dynamic task creation for multiple FPL resources
            (e.g., teams, players, or gameweeks) while reusing a common ingestion logic.

            Args:
                task_name (str): Unique Airflow task ID to assign to the generated task.
                endpoint (str): API endpoint used to fetch data from the FPL API.
                output_key (str): S3 object key or prefix where the fetched data
                    will be stored.

            Returns:
                function: An Airflow task function that, when executed, fetches data from the
                FPL API and uploads it to S3.
        """
        
        @task(task_id=task_name)    
        def fetch_data():
            from include.fetch_teams_players_gws import fetch_teams_players_gws_data

            # Fetch data from FPL API
            data = fetch_teams_players_gws_data(
                api_url=Variable.get("fpl_api_base_url"),
                endpoint=endpoint
            )
            upload_data_to_s3(
                data=data,
                season=None,
                output_key=output_key,
                file_name=endpoint
            )        
        return fetch_data
    
    @task
    def element_types(endpoint: str, output_key: str):
        """
            Fetches Fantasy Premier League (FPL) element types data from the FPL API
            and uploads the retrieved dataset to Amazon S3.

            This task retrieves data using a specified API endpoint and a base URL
            stored in Airflow Variables. The fetched data is persisted to S3 under the
            provided output key for downstream processing or analysis.

            Args:
                endpoint (str): API endpoint used to fetch element types data
                    from the FPL API.
                output_key (str): S3 object key or prefix where the fetched data
                    will be uploaded.

            Returns:
                Any: The raw data returned from the FPL API, enabling downstream
                Airflow tasks to consume it via XCom if required.
        """
        
        from include.fetch_teams_players_gws import fetch_teams_players_gws_data

        # Fetch data from FPL API
        data = fetch_teams_players_gws_data(
            api_url=Variable.get("fpl_api_base_url"),
            endpoint=endpoint
        )
        
        upload_data_to_s3(
            data=data,
            season=None,
            output_key=output_key,
            file_name=endpoint
        )
        
        return data

    # Create and execute task to fetch FPL element types data
    # (e.g., Goalkeepers, Defenders, Midfielders, Forwards) 
    element_types_task = element_types(
        endpoint=Variable.get("element_types_endpoint"),
        output_key=f"fpl_element_types"
    )

    # Create an Airflow task factory for fetching players data from the FPL API
    players_data_task = create_fetch_task(
                        task_name="fetch_players_data",  # Unique Airflow task ID
                        endpoint=Variable.get("players_endpoint"),      # API endpoint for players data
                        output_key=f"fpl_players"   # S3 key/prefix for storing players data
                        )
    # Execute the players data fetch task
    players_data = players_data_task()

    
    # Create an Airflow task factory for fetching teams data from the FPL API
    teams_data_task = create_fetch_task(
        task_name="fetch_teams_data",                   # Unique Airflow task ID
        endpoint=Variable.get("teams_endpoint"),        # API endpoint for teams data
        output_key="fpl_teams"                           # S3 key/prefix for storing teams data
    )
    
    # Execute the teams data fetch task
    teams_data = teams_data_task()

    # Create an Airflow task factory for fetching gameweeks (events) data
    gameweeks_data = create_fetch_task(
        task_name="fetch_gameweeks_data",                # Unique Airflow task ID
        endpoint=Variable.get("events_endpoint"),      # API endpoint for gameweeks data
        output_key=f"fpl_gameweeks"                         # S3 key/prefix for storing gameweeks data
    )
    gameweeks_data = gameweeks_data()

    # GAD DEPENDENCY
    teams_data >> element_types_task >> players_data >> gameweeks_data

events_players_teams_dag()