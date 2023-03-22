import requests
import json

class BarbAPI:
    """
    A class for connecting to the Barb API and making queries.

    Attributes:
        api_key (str): The API key for accessing the Barb API.
        connected (bool): Whether the API is currently connected.

    Methods:
        connect: Connect to the Barb API.
        query: Make a query to the Barb API.
    """

    def __init__(self, api_key: str):
        """
        Initializes a new instance of the BarbAPI class.

        Args:
            api_key (str): The API key for accessing the Barb API.
        """
        self.api_key = api_key
        self.api_root = "https://barb-api.co.uk/api/v1/"
        self.connected = False
        self.headers = None

    def connect(self):
        """
        Connects to the Barb API.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        # Code to connect to the Barb API
        self.connected = True

        # Code to get an access token from the Barb API
        token_request_url = self.api_root + "auth/token/"
        response = requests.post(token_request_url, data = self.api_key)
        access_token = json.loads(response.text)['access']
        self.headers = {'Authorization': 'Bearer {}'.format(access_token)}

    def query(self, query_string: str):
        """
        Makes a query to the Barb API.

        Args:
            query_string (str): The query string to send to the API.

        Returns:
            str: The response from the API.
        """
        if not self.connected:
            raise Exception("Not connected to the Barb API.")

        # Code to send the query to the Barb API and get the response
        api_url = self.api_root + "panels/"
        response = requests.get(url = api_url,headers=self.headers)
        return response
