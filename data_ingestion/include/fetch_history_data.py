import logging

# Configure global logging

# Logs will include timestamp, log level, and message.
# INFO level is suitable for pipeline execution visibility.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create a module-level logger
logger = logging.getLogger(__name__)


def fetch_fpl_historical_data(season: str, api_url: str, file_endpoint: str):
    import requests
    import csv
    from io import StringIO
        
    """
        Fetches season-specific CSV data from the GitHub-hosted (or static) data source.

        This function is primarily used to retrieve non-JSON resources such as:
        - Fixtures history
        - Team historical data
        - Other season-based datasets exposed as CSV files

        Unlike API endpoints that return JSON payloads, this function handles
        raw CSV files and returns their contents as plain text for downstream
        parsing and transformation.

        Parameters
        ----------
        api_url : str
            The base URL of the data source hosting season-level CSV files.
            Example: "https://raw.githubusercontent.com/.../data/"

        season : str
            The football season identifier used to locate the correct directory.
            Example: "2023-24"

        file_endpoint : str
            The name of the CSV file (without the `.csv` extension) to retrieve.
            Example: "fixtures", "teams", "results"

        Returns
        -------
        str
            Raw CSV content as a text string, which can later be parsed using
            pandas, csv module, or other ETL tools.

        list
            Returns an empty list if the request fails.
    """
    
    try:
        # Construct URL for season-specific CSV file (fixtures and teams history)
        url_path = f"{api_url}{season}/{file_endpoint}.csv"
        
        # Make an HTTP GET request to retrieve the CSV file.
        # A timeout is enforced to avoid hanging requests in production pipelines.
        response = requests.get(url_path, timeout=10)
    
        # Since the response is a CSV file, the content is returned as raw text.
        payload = response.text
        
        # csv.DictWriter
        result = list(csv.DictReader(StringIO((payload)), delimiter=','))

        logger.info(
            f"Successfully retrieved CSV data for season '{season}' from '{file_endpoint}.csv'"
        )
        return result
    
    except requests.RequestException as e:
        # Log any request-related errors and fail gracefully
        logger.error(f"Error accessing API at {api_url}: {e}")
    
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        
    except requests.Timeout:
        logger.error(f"Request timed out while accessing {api_url}")
        
    except requests.HTTPError as e:
        logger.error(f"HTTP error occurred while accessing {api_url}: {e}")
            

# USAGE


# players_identifiers = fetch_api_endpoint_data("https://fantasy.premierleague.com/api/bootstrap-static/", "elements", get_players_infor=True)
# print([player for player in players_identifiers if player.startswith('P')])

# for player in players_identifiers:
#     print(player if player.startswith("Petr") else "")

# players_gw_stats = fetch_players_gw_stats(
#                                         "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/", 
#                                         season="2018-19", player_ids= [player for player in players_identifiers if player.startswith('P')])
# print(players_gw_stats)

# print(players_gw_stats)
# players = fetch_api_endpoint_data("https://fantasy.premierleague.com/api/bootstrap-static/", "elements", get_players_infor=False) # players data
# events = fetch_api_endpoint_data("https://fantasy.premierleague.com/api/bootstrap-static/", "events", get_players_infor=False) # gameweeks data
# print(events)
# teams = fetch_api_endpoint_data("https://fantasy.premierleague.com/api/bootstrap-static/", "teams", get_players_infor=False) # teams data

# players_identifiers = fetch_api_endpoint_data("https://fantasy.premierleague.com/api/bootstrap-static/", "elements", get_players_infor=True) # players folder names for player-specific endpoints



# fixtures = fetch_gt_contents_endpoint_data("https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/", season="2023-24", file_endpoint="fixtures")
# teams_history = fetch_teams_and_players_data("https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/", season="2023-24", file_endpoint="teams")
