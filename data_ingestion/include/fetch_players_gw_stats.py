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


def fetch_players_gw_stats(api_url: str, season, player_ids: list):
    import requests
    import csv
    from io import StringIO
    
    """
        Fetches gameweek-level performance statistics for multiple players.

        This function iterates through a list of player-specific identifiers and
        retrieves each player's gameweek-by-gameweek performance data from their
        dedicated CSV endpoint. These endpoints typically expose granular match
        statistics such as minutes played, goals, assists, expected metrics,
        and other Fantasy Premier League scoring components.

        The function is commonly used after:
            - Fetching the list of players for a given season
            - Constructing player identifiers in the format:
        <first_name>_<second_name>_<player_id>

        Each identifier maps directly to a player folder containing a `gw.csv`
        file that holds all gameweek records for that player.

        Parameters
        ----------
        api_url : str
            Base URL of the Fantasy Premier League data repository.
            Example:
            "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/"

        season : str
            Season identifier used to locate season-specific player folders.
            Example:
            "2023-24"

        player_ids : list
            A list of player folder identifiers used to access player-level
            gameweek statistics.
            Example:
            ["Mohamed_Salah_123", "Erling_Haaland_355"]

        Returns
        -------
        None
            This function does not return data directly.
            It retrieves and logs successful responses, allowing downstream
            processes to persist, transform, or aggregate the CSV payloads.
    """
    try:
        player_gw_data = {}
        for player_id in player_ids:
            response = requests.get(f"{api_url}{season}/players/{player_id}/gw.csv", timeout=10)
            
            if response.status_code == 200:
            
                payload = response.text
                result = list(csv.DictReader(StringIO((payload)), delimiter=','))

                logger.info(
                    f"Successfully retrieved gameweek stats for player endpoint '{player_id}', url: {api_url}{season}/players/{player_id}/gw.csv"
                )

                # player_numbers = player_id.split('_')[-1]
                player_gw_data[player_id] = result
                
            else:
                logger.warning(
                    f"Failed to retrieve gameweek stats for player endpoint '{player_id}'. Status code: {response.status_code}"
                )
                
        return player_gw_data
    
    except requests.RequestException as e:
        logger.error(f"Error accessing API at {api_url}: {e}")
    
    except requests.exceptions.Timeout:
        logger.error(f"Request timed out while accessing {api_url}")
    
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error occurred while accessing {api_url}: {e}")
        

# players_gw_stats = fetch_players_gw_stats(
#                                         "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/", 
#                                         season="2018-19", player_ids=players_list)
# print(players_gw_stats)
