
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
    tags=["fpl", "gameweek (events)", "data_ingestion"],
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
