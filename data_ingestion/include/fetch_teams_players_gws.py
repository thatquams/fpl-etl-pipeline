
def fetch_teams_players_gws_data(api_url: str, endpoint: str):
    
    
    from utils.loggings import logger
    import requests

    """
        Retrieve entity-level data from the Fantasy Premier League (FPL) public API.

        This function performs a JSON-based API request and extracts a specific
        top-level entity from the response payload. It is designed to support
        ingestion of core FPL entities that are common across the entire season.

        Parameters
        ----------
        api_url : str
            The base URL of the Fantasy Premier League API.
            Example:
            "https://fantasy.premierleague.com/api/bootstrap-static/"

        endpoint : str
            The top-level JSON key to extract from the API response.
            Expected values:
            - 'elements' → player metadata
            - 'events'   → gameweeks
            - 'teams'    → club-level metadata

        Returns
        -------
        list
            A list of dictionaries representing the requested entity records.
            Returns an empty list if the request fails or the endpoint is missing.
    """
    try:
        
        # JSON API request with a specific endpoint key (players, teams, events)
        response = requests.get(api_url, timeout=10)
            
        # Extract the data for (players, teams and events/gameweeks) from the JSON response
        payload = response.json().get(endpoint, [])
        
        logger.info(
            f"Successfully retrieved {len(payload)} records "
            f"from API endpoint '{endpoint}'"
        )

        return payload


    except requests.RequestException as e:
        # Log any request-related errors and fail gracefully
        logger.error(f"Error accessing API at {api_url}: {e}")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

    except requests.Timeout:
        logger.error(f"Request timed out while accessing {api_url}")

    except requests.HTTPError as e:
        logger.error(f"HTTP error occurred while accessing {api_url}: {e}")
        

# players = fetch_teams_players_gws_data("https://fantasy.premierleague.com/api/bootstrap-static/", "elements") # players data
# events = fetch_teams_players_gws_data("https://fantasy.premierleague.com/api/bootstrap-static/", "events") # gameweeks data
# teams = fetch_teams_players_gws_data("https://fantasy.premierleague.com/api/bootstrap-static/", "teams") # teams data