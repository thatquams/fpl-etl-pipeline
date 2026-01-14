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


def fetch_players_list(api_url: str):
    from include.utils.infer_season import SEASONS
    seasons = SEASONS
    """
        Fetches and prepares a list of players for a given season from a CSV source.

        This function retrieves the raw players dataset (`players_raw.csv`) for a
        specific season, parses the CSV content into structured records, and then
        constructs a standardized player identifier format:

            <first_name>_<second_name>_<player_id>

        The resulting identifiers are typically used to dynamically build
        player-specific endpoints (e.g., gameweek-by-gameweek performance data).

        Parameters
        ----------
        api_url : str
            Base URL hosting season-level CSV datasets.
            Example: "https://raw.githubusercontent.com/.../data/"

        season : str
            Football season identifier used to locate the correct dataset.
            Example: "2023-24"

        Returns
        -------
        list
            A list containing:
            - Parsed player records (as dictionaries), and
            - Constructed player identifier strings used for downstream API access

    """
    import requests
    import csv
    from io import StringIO

    try:
        # Initialize an empty container to store parsed player data and constructed player identifiers.
        players_ids = []
        
        # Construct the full URL to the season-specific players CSV file.
        for season in seasons:
            updated_url = f"{api_url}{season}/players_raw.csv"
            
            # Make an HTTP GET request to retrieve the CSV file.
            # A timeout is enforced to avoid blocking ingestion pipelines.
            response = requests.get(updated_url, timeout=10)

            # Validate HTTP response status.
            # Non-200 responses indicate failure to retrieve the dataset.
            if response.status_code != 200:
                logger.error(
                    f"Failed to retrieve players list for season '{season}'. Status code: {response.status_code}"
                )
            
            # Parse the CSV text content into a list of dictionaries using DictReader.
            # StringIO is used to treat raw text as a file-like object.
            players = list(csv.DictReader(StringIO((response.text)), delimiter=','))
            logger.debug(f"Raw players data for season '{season}': {players}")
            
            # Each row represents a player with attributes such as:
            # - id
            # - first_name
            # - second_name
            for player in players:
                player['id'] = str(player['id'])  # Ensure the 'id' field is a string
                player['first_name'] = player['first_name'].split()[0].strip()
                player['second_name'] = player['second_name'].split()[-1].strip()

                players_ids.append(f"{player['first_name']}_{player['second_name']}_{player['id']}")
                
            logger.info(
                f"Successfully retrieved {len(players_ids)} players."
            )
        
        return players_ids

    except requests.RequestException as e:
        logger.error(f"Error accessing API at {api_url}: {e}")
    
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    
    except requests.Timeout:
        logger.error(f"Request timed out while accessing {api_url}")
    
    except requests.HTTPError as e:
        logger.error(f"HTTP error occurred while accessing {api_url}: {e}")
    
    
# players_list = fetch_players_list("https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/", season="2018-19")
# print(players_list)