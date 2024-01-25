"""modules to import"""
import re
import os
import time
import tempfile
import json
import requests
import pandas as pd
from helpers.secret_manager import GcpSecretManager
from google.cloud import storage

storage_client= storage.Client(project=os.getenv("PROJECT"))

class BarbAPI:
    """
    Represents the Barb API.

    Attributes:
        api_key (str): The API key for accessing the Barb API.
        api_root (str): The root URL of the Barb API.
        connected (bool): Whether the Barb API is connected.
        headers (dict): The headers for the Barb API.
        current_job_id (str): The current job id for the Barb API.

    Methods:
        connect: Connects to the Barb API.
        get_station_code: Gets the station code for a given station name.
        get_viewing_station_code: Gets the station code for a given station name.
        get_panel_code: Gets the panel code for a given panel region.
        programme_ratings: Gets the programme ratings for a given date range.
        advertising_spots: Gets the advertising spots for a given date range.
        spot_audience: gets the spot audience report data by day and panel
        audiences_by_time: Gets the audiences by time for a given date range.
        list_stations: Lists the stations available in the API.
        list_viewing_stations: Lists the stations available in the API.
        list_panels: Lists the panels available in the API.
        list_buyers: Lists the buyers available in the API.
        query_asynch_endpoint: Queries the asynch endpoint.
        get_asynch_file_urls: Gets the asynch file urls.
        get_asynch_files: Gets the asynch files.
        ping_job_status: Pings the job status.
    """

    def __init__(self, api_root:str="https://barb-api.co.uk/api/v1/"):
        """
        Initializes a new instance of the BarbAPI class.

        Args:
            api_key (dict): contains "email" and "password".
            api_root (str): The root URL of the Barb API.

        """
        self.api_root = api_root
        self.connected = False
        self.headers = {}
        self.headers["Authorization"]=self.connect()
        self.current_job_id = None

    def connect(self):
        """
        Connects to the Barb API.
        """

        try:
            # Code to connect to the Barb API
            self.connected = True
            #get secrets
            if os.getenv("ENV"):
                email = os.getenv("EMAIL")
                password = os.getenv("PASSWORD")
            else:
                secret_manager = GcpSecretManager()
                email = secret_manager.get_secret("barb_email")
                password = secret_manager.get_secret("barb_password")

            # Code to get an access token from the Barb API
            token_request_url = self.api_root + "auth/token/"
            response = requests.post(
                token_request_url
                , data={
                    "email":email,
                    "password":password
                }
                ,timeout = 300
            )
            print(response.text)
            access_token = json.loads(response.text)["access"]
            return f"Bearer {access_token}"

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            self.connected = False
        except json.JSONDecodeError:
            print("Failed to decode the response from the Barb API.")
            self.connected = False
        except KeyError:
            print("Failed to get access token from the response.")
            self.connected = False

    def get_viewing_station_code(self, viewing_station_name:str) ->dict:
        """
        Gets the viewing_station code for a given viewing_station name.

        Args:
            viewing_station_name (str): The name of the viewing_station to query.

        Returns:
            str: The viewing_station code.
        """

        api_url = f"{self.api_root}viewing_stations/"
        r = requests.get(
            url=api_url
            , headers=self.headers
            ,timeout=300
        )
        r.raise_for_status()
        api_data = r.json()
        viewing_station_code = [
            s["viewing_station_code"]
            for s in api_data
            if viewing_station_name.lower() == s["viewing_station_name"].lower()
        ]
        if len(viewing_station_code) == 1:
            viewing_station_code = viewing_station_code[0]
        else:
            raise KeyError(f"Viewing station name {viewing_station_name} not found.")
        return viewing_station_code

    def programme_ratings(
        self,
        min_date,
        max_date,
        station_code:str,
        panel_code=None,
        consolidated=True,
        last_updated_greater_than=None,
        use_reporting_days=True,
        limit=5000,
    ):
        """
        Gets the programme ratings for a given date range.

            Args:
                min_transmission_date (str): The minimum transmission date to query.
                max_transmission_date (str): The maximum transmission date to query.
                station (str): The name of the station to query.
                panel (str): The name of the panel to query.
                consolidated (bool): Whether to return consolidated data.
                last_updated_greater_than (str): The last updated date to query.
                use_reporting_days (bool): Whether to use reporting days.
                limit (int): The maximum number of results to return.

            Returns:
                ProgrammeRatingsResultSet: The programme ratings result set.
        """

        # The query parameters
        params = {
            "min_transmission_date": min_date,
            "max_transmission_date": max_date,
            "station_code": station_code,
            "panel_code": panel_code,
            "consolidated": consolidated,
            "last_updated_greater_than": last_updated_greater_than,
            "use_reporting_days": use_reporting_days,
            "limit": limit,
        }

        api_response_data = self.query_event_endpoint("programme_ratings", params)

        return ProgrammeRatingsResultSet(api_response_data)

    def programme_schedule(
        self,
        min_date:str,
        max_date:str,
        station_code:str,
        last_updated_greater_than:str=None,
    ):
        """
        Gets the programme ratings for a given date range.

            Args:
                min_scheduled_date (str): The minimum scheduled date to query.
                max_scheduled_date (str): The maximum scheduled date to query.
                station (str): The name of the station to query.
                last_updated_greater_than (str): The last updated date to query.

            Returns:
                ProgrammeScheduleResultSet: The programme ratings result set.
        """

        # The query parameters
        params = {
            "min_scheduled_date": min_date,
            "max_scheduled_date": max_date,
            "station_code": station_code,
            "last_updated_greater_than": last_updated_greater_than,
        }

        api_response_data = self.query_bulk_endpoint("programme_schedule", params)

        return ProgrammeScheduleResultSet(api_response_data)

    def programme_audience(
        self,
        min_date,
        max_date,
        panel_code:int,
    ):
        """
        Gets the programme audience for a given date range.

            Args:
                min_sesssion_date (str): The minimum transmission date to query.
                max_sesssion_date (str): The maximum transmission date to query.
                panel_code (str): The panel code to query.

            Returns:
                ProgrammeAudienceResultSet: The programme audience result set.
        """

        # The query parameters
        params = {
            "min_session_date": min_date,
            "max_session_date": max_date,
            "panel_code": panel_code,
        }

        api_response_data = self.query_bulk_endpoint(
            endpoint="bulk/programme_audience",
            parameters=params,
            method="GET"
        )
        if api_response_data is None:
            string=f"no data recieved for {panel_code} for {min_date} and {max_date}"
            print(string)
            # raise Warning(string)
            return None

        return GoogleBucketResultSet(api_response_data, endpoint="programme_audience")

    def advertising_spots(
        self,
        min_transmission_date:str,
        max_transmission_date:str,
        station_code=None,
        panel_code=None,
        advertiser=None,
        buyer=None,
        consolidated=True,
        standardise_audiences=None,
        use_reporting_days=True,
        last_updated_greater_than=None,
        limit=5000,
    ):
        """
        Gets the advertising spots for a given date range.

            Args:
                min_transmission_date (str): The minimum transmission date to query.
                max_transmission_date (str): The maximum transmission date to query.
                station_code (str): The code of the station to query.
                panel_code (str): The code of the panel to query.
                advertiser (str): The name of the advertiser to query.
                buyer (str): The name of the buyer to query.
                consolidated (bool): Whether to return consolidated data.
                standardise_audiences (bool): Whether to standardise audiences.
                use_reporting_days (bool): Whether to use reporting days.
                last_updated_greater_than (str): The last updated date to query.
                limit (int): The maximum number of results to return.

            Returns:
                AdvertisingSpotsResultSet: The advertising spots result set.
        """

        # The query parameters
        params = {
            "min_transmission_date": min_transmission_date,
            "max_transmission_date": max_transmission_date,
            "station_code": station_code,
            "panel_code":panel_code,
            "advertiser_name": advertiser,
            "buyer_name": buyer,
            "consolidated": consolidated,
            "standardise_audiences": standardise_audiences,
            "use_reporting_days": use_reporting_days,
            "last_updated_greater_than": last_updated_greater_than,
            "limit": limit,
        }
        api_response_data = self.query_event_endpoint("advertising_spots", params)

        return AdvertisingSpotsResultSet(api_response_data)

    def spot_audience(self,
        min_date:str,
        max_date:str,
        panel_code):
        """
        Gets the advertising spots for a given date range.

            Args:
                min_transmission_date (str): The minimum transmission date to query.
                max_transmission_date (str): The maximum transmission date to query.
                panel_code (str): The code of the panel to query.

            Returns:
                SpotAudienceResultSet: The advertising spots result set.
        """

        # The query parameters
        params = {
            "min_session_date": min_date,
            "max_session_date": max_date,
            "panel_code":panel_code,
        }

        api_response_data = self.query_bulk_endpoint(
            endpoint="bulk/spot_audience",
            parameters=params,
            method="GET"
        )
        print("api_response_data", api_response_data)

        if api_response_data is None:
            string=f"no data recieved for {panel_code} for {min_date} and {max_date}"
            #raise Warning(string)
            return None
        return GoogleBucketResultSet(api_response_data,endpoint="spot_audience")

    def spot_schedule(self
                      ,min_date:str
                      ,max_date:str
                      ,station_code:int
                      ,last_updated_gt:str=None):
        """
        request the spot schedule endpoint
        """
        # The query parameters
        params = {
            "min_scheduled_date": min_date,
            "max_scheduled_date": max_date,
            "station_code": station_code,
        }
        if last_updated_gt:
            params["last_updated_greater_than"]= last_updated_gt
        api_response_data = self.query_bulk_endpoint("spot_schedule", params)
        return SpotScheduleResultSet(api_response_data)

    def audiences_by_time(
        self,
        min_transmission_date,
        max_transmission_date,
        time_period_length,
        viewing_status,
        station_code=None,
        panel_code=None,
        use_polling_days=True,
        last_updated_greater_than=None,
        limit=5000,
    ):
        """
        Gets the audiences by time for a given date range.

            Args:
                min_transmission_date (str): The minimum transmission date to query.
                max_transmission_date (str): The maximum transmission date to query.
                time_period_length (str): The time period length to query.
                viewing_status (str): The viewing status to query.

                station (str): The name of the station to query.
                panel (str): The name of the panel to query.
                use_polling_days (bool): Whether to use polling days.
                last_updated_greater_than (str): The last updated date to query.
                limit (int): The maximum number of results to return.

            Returns:
                AudiencesByTimeResultSet: The audiences by time result set.
        """

        # The query parameters
        params = {
            "min_transmission_date": min_transmission_date,
            "max_transmission_date": max_transmission_date,
            "station_code": station_code,
            "panel_code": panel_code,
            "time_period_length": time_period_length,
            "viewing_status": viewing_status,
            "use_polling_days": use_polling_days,
            "last_updated_greater_than": last_updated_greater_than,
            "limit": limit,
        }

        api_response_data = self.query_event_endpoint("audiences_by_time", params)

        return AudiencesByTimeResultSet(api_response_data)

    def viewing(
        self,
        min_date:str,
        max_date:str,
        panel_code:str,
        #viewing_station=None,
        #activity_type=None,
        #last_updated_greater_than=None,
        #output_format="parquet",
        #limit=5000,
    ):
        """
        Gets the viewing for a given date range.

            Args:
                min_date (str): The minimum session date to query.
                max_date (str): The maximum session date to query.
                viewing_station (str): The name of the viewing_station to query.
                panel (str): The name of the panel to query.
                activity_type (str): The activity type to query.
                last_updated_greater_than (str): The last updated date to query.
                output_format (str): The output format to query.
                limit (int): The maximum number of results to return.

            Returns:
                ViewingResultSet: The viewing result set.
        """

        # The query parameters
        params = {
            "min_session_date": min_date,
            "max_session_date": max_date,
            "panel_code": panel_code,
            # "viewing_station_code": None
            # if viewing_station is None
            # else self.get_viewing_station_code(viewing_station),
            # "output_format": output_format,
            # "limit": limit,
        }

        # if activity_type is not None:
        #     params["activity_type"] = activity_type

        # if last_updated_greater_than is not None:
        #     params["last_updated_greater_than"] = last_updated_greater_than

        api_response_data = self.query_bulk_endpoint(
            endpoint="bulk/viewing/",
            parameters=params,
            method="GET"
        )
        if api_response_data is None:
            string=f"no data recieved for {panel_code} for {min_date} and {max_date}"
            if os.getenv("ENV"):
                print(string)
            else:
                raise Warning(string)
            return None
        return GoogleBucketResultSet(api_response_data,endpoint="viewing")

    def query_event_endpoint(self, endpoint, parameters,method="GET"):
        """
        Queries the event endpoint.
            Args:
                endpoint (str): The endpoint to query.
                parameters (dict): The query parameters.

            Returns:
                dict: The API response data.
        """
        api_response_data = {"endpoint": endpoint, "events": []}
        try:
            api_url = f"{self.api_root}{endpoint}"
            r = requests.request(
                url=api_url
                , params=parameters
                , headers=self.headers
                , timeout=300
                , method=method
            )
            r.raise_for_status()

            # If the response is not 200 then raise an exception
            if r.status_code != 200:
                raise requests.HTTPError(f"Error: {r.status_code} - {r.text}")

            r_json = r.json()
            #print(r_json)

            # If events is not in the response then raise an exception
            if "events" not in r_json.keys():
                raise KeyError(f"Error: {r_json['message']}")

            # If events is empty then raise an exception
            if len(r_json["events"]) == 0:
                return api_response_data

            api_response_data = {"endpoint": endpoint, "events": r_json["events"]}
            count=0
            while "X-Next" in r.headers:
                x_next_url = r.headers["X-Next"]
                r = requests.get(
                    url=x_next_url
                    , headers=self.headers
                    ,timeout=300
                )
                r.raise_for_status()
                r_json = r.json()
                api_response_data["events"] = (
                    api_response_data["events"] + r_json["events"]
                )
                print(count)
                count+=1

            return api_response_data

        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(f"An error occurred: {e}")
        except json.JSONDecodeError:
            print("Failed to decode the response.")
        return api_response_data

    def list_stations(self, regex_filter=None):
        """
        Lists the stations available in the API.

            Returns:
                list: The stations result set.
        """

        api_url = f"{self.api_root}stations"
        try:
            api_response_data = requests.get(
                url=api_url
                , headers=self.headers
                ,timeout=300
            )
            api_response_data.raise_for_status()
            list_of_stations = []
            for x in api_response_data.json():
                list_of_stations.append(x)

            if len(list_of_stations) == 0:
                raise requests.RequestException("Error: No stations returned.")

            if regex_filter is not None:
                regex = re.compile(regex_filter, flags=re.IGNORECASE)
                list_of_stations = list(filter(regex.search, list_of_stations))

            return list_of_stations
        except requests.RequestException as req_error:
            print(api_response_data.content)
            raise requests.RequestException("error") from req_error

    def list_viewing_stations(self, regex_filter=None):
        """
        Lists the stations available in the API.

            Returns:
                list: The stations result set.
        """

        api_url = f"{self.api_root}viewing_stations"

        api_response_data = requests.get(
            url=api_url
            , headers=self.headers
            ,timeout=300
        )

        list_of_stations = list(api_response_data.json())

        if len(list_of_stations) == 0:
            raise requests.RequestException("Error: No stations returned.")

        if regex_filter is not None:
            regex = re.compile(regex_filter, flags=re.IGNORECASE)
            list_of_stations = list(filter(regex.search, list_of_stations))

        return list_of_stations

    def list_panels(self, regex_filter=None) ->[list,None]:
        """
        Lists the panels available in the API.

            Returns:
                list: The panels result set.
        """

        api_url = f"{self.api_root}panels"
        api_response_data = requests.get(
            url=api_url
            , headers=self.headers
            ,timeout=300
        )

        list_of_panels = list(api_response_data.json())

        if len(list_of_panels) == 0:
            raise requests.RequestException("Error: No panels returned.")

        if regex_filter is not None:
            regex = re.compile(regex_filter, flags=re.IGNORECASE)
            list_of_panels = list(filter(regex.search, list_of_panels))

        return list_of_panels

    def list_buyers(self, regex_filter=None):
        """
        Lists the buyers available in the API.

            Returns:
                list: The buyers result set.
        """

        api_url = f"{self.api_root}buyers"
        api_response_data = requests.get(
            url=api_url
            , headers=self.headers
            ,timeout=300
        )

        list_of_buyers = api_response_data.json()

        if len(list_of_buyers) == 0:
            raise requests.RequestException("Error: No buyers returned.")

        if regex_filter is not None:
            regex = re.compile(regex_filter, flags=re.IGNORECASE)
            list_of_buyers = list(filter(regex.search, list_of_buyers))

        return list_of_buyers

    def list_advertisers(self, regex_filter=None):
        """
        Lists the advertisers available in the API.

            Returns:
                list: The advertisers result set.
        """

        api_url = f"{self.api_root}advertisers"
        api_response_data = requests.get(
            url=api_url
            , headers=self.headers
            ,timeout=300
        )

        list_of_advertisers = [a["advertiser_name"] for a in api_response_data.json()]

        if len(list_of_advertisers) == 0:
            raise requests.RequestException("Error: No advertisers returned.")

        if regex_filter is not None:
            regex = re.compile(regex_filter, flags=re.IGNORECASE)
            list_of_advertisers = list(filter(regex.search, list_of_advertisers))

        return list_of_advertisers

    def query_asynch_endpoint(self, endpoint, parameters,method="POST"):
        """
        Queries the asynch endpoint.

            Args:
                endpoint (str): The endpoint to query.
                parameters (dict): The query parameters.

            Returns:
                dict: The API response data.
        """

        api_url = f"{self.api_root}{endpoint}"

        # Query the API and turn the response into json
        try:
            r = requests.request(
                url=api_url
                , json=parameters
                , headers=self.headers
                ,timeout=300
                ,method=method
            )
            r.raise_for_status()
            r_json = r.json()
            print(r_json)
            self.current_job_id = r_json["job_id"]
            return r_json
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
        except json.JSONDecodeError:
            print("Failed to decode the response.")
        return None

    def get_asynch_file_urls(self, job_id=None):
        """
        Gets the asynch file urls.

            Args:
                job_id (str): The job id to query.

            Returns:
                list: The asynch file urls.
        """

        if job_id is None:
            job_id = self.current_job_id

        try:
            api_url = f"{self.api_root}async-batch/results/{job_id}"
            r = requests.get(
                url=api_url
                , headers=self.headers
                ,timeout=300
            )
            r.raise_for_status()
            r_json = r.json()
            if r_json["status"] == "started":
                return False
            urls = [x["data"] for x in r_json["result"]]
            if len(urls) == 0:
                raise IndexError("Error: No urls returned.")
            return urls
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
        except json.JSONDecodeError:
            print("Failed to decode the response.")
        return None

    def get_asynch_files(self):
        """
        Gets the asynch files.

            Returns:
                ViewingResultSet: The viewing result set.
        """

        try:
            results = pd.DataFrame()
            for file in self.current_file_urls:
                df = pd.read_parquet(file)
                results = pd.concat([results, df])
            return ViewingResultSet(results)
        except pd.errors.DataError as error:
            print("Failed to get the asynch files.")
            print(error)
        return None

    def ping_job_status(self, job_id=None):
        """
        Pings the job status.

            Args:
                job_id (str): The job id to query.

            Returns:
                list: The asynch file urls.
        """

        if job_id is None:
            job_id = self.current_job_id

        while self.get_asynch_file_urls(job_id) is False:
            print("Job not ready yet. Sleeping for 60 seconds.")
            time.sleep(60)

        self.current_file_urls = self.get_asynch_file_urls(job_id)

        print(
            f"Job complete. {len(self.current_file_urls)} files are ready for download."
        )

    def query_bulk_endpoint(self, endpoint:str, parameters:dict,method="GET")->list[dict]:
        """
        Queries the asynch endpoint.

            Args:
                endpoint (str): The endpoint to query.
                parameters (dict): The query parameters.

            Returns:
                dict: The API response data.
        """

        api_url = f"{self.api_root}{endpoint}"
        data_list=[]
        next_token=True
        # Query the API and turn the response into json
        try:
            while next_token:
                r = requests.request(
                    url = api_url
                    , params = parameters
                    , headers = self.headers
                    ,timeout = 300
                    ,method = method
                )
                r.raise_for_status()
                r_json = r.json()
                #print("json output:",r_json)
                #append data
                if isinstance(r_json, list):
                    for item in r_json:
                        data_list.append(item)
                elif isinstance(r_json,dict):
                    data_list.append(item)
                else:
                    raise TypeError("Wrong data type for",r_json)
                #continue?
                if "X-Next" in list(r.headers.keys()):
                    api_url = r.headers["X-Next"]
                else:
                    next_token=False
            return data_list
        except requests.exceptions.RequestException as e:
            print(r.content)
            print(r.url)
            raise Warning(f"An error occurred: {e}") from e
        except json.JSONDecodeError:
            print("Failed to decode the response.")
        return None

class APIResultSet:
    """
    Represents a result set from the Barb API.
    """

    def __init__(self, api_response_data: dict):
        """
        Initialises a new instance of the APIResultSet class.

        Args:
            api_response_data (dict): The API response data.
        """

        self.api_response_data = api_response_data

class BulkResultSet:
    """
    Respresents the bulk result set from the Barb API
    """
    def __init__(self, api_response_data: list[dict]):
        """
        Initialises a new instance of the BulkResultSet class.

        Args:
            api_response_data (list[dict]): The API response data.
        """

        self.api_response_data = api_response_data

class ProgrammeRatingsResultSet(APIResultSet):
    """
    Represents a programme ratings result set from the Barb API.
    """

    def to_dataframe(self) ->pd.DataFrame:
        """
        Converts the API response data into a pandas dataframe.

        Returns:
            pandas.DataFrame: A dataframe containing the API response data.

        """

        # if len(self.api_response_data["events"]) == 0:
        #     raise Warning("Error: No events returned.")

        # Loop through the events and then the audiences within the events
        df_data = []
        for e in self.api_response_data["events"]:
            try:
                e:dict
                df_data.append(
                    {
                        "panel_code": e.get("panel",{}).get("panel_code"),
                        "panel_region": e.get("panel",{}).get("panel_region"),
                        "is_macro_region": e.get("panel",{}).get("is_macro_region"),
                        "station_code": e.get("station",{}).get("station_code"),
                        "station_name": e.get("station",{}).get("station_name"),
                        "prog_name": e.get("transmission_log_programme_name"),
                        "programme_type": e.get("programme_type"),
                        "programme_start_datetime": e.get("programme_start_datetime",{}).get(
                            "standard_datetime"
                        ),
                        "programme_duration_minutes": e.get("programme_duration"),
                        "spans_normal_day": e.get("spans_normal_day"),
                        "sponsor_code": e.get("sponsor",{}).get("sponsor_code"),
                        "bumpers_included": e.get("sponsor",{}).get("bumpers_included"),
                        "broadcaster_transmission_code": e.get("broadcaster_transmission_code"),
                        "live_status": e.get("live_status"),
                        "uk_premiere": e.get("uk_premier"),
                        "broadcaster_premiere": e.get("broadcaster_premier"),
                        "programme_repeat": e.get("repeat"),
                        "episode_name": None if not(isinstance(e.get(
                            "programme_content"),dict)) else e.get(
                            "programme_content",{}).get("episode",{}).get(
                            "episode_name"
                        ),
                        "episode_number": -1 if not(isinstance(e.get(
                            "programme_content"),dict)) else e.get(
                                "programme_content",{}).get("episode",{}).get(
                            "episode_number"
                        ),
                        "series_number": -1 if not(isinstance(e.get(
                            "programme_content"),dict)) else e.get(
                                "programme_content",{}).get("series",{}).get(
                            "series_number"
                        ),
                        "number_of_episodes": -1 if not(isinstance(e.get(
                            "programme_content"),dict)) else e.get(
                                "programme_content",{}).get("series",{}).get(
                            "number_of_episodes"
                        ),
                        "broadcaster_series_id": -1 if not(isinstance(e.get(
                            "programme_content"),dict)) else e.get(
                                "programme_content",{}).get("series",{}).get(
                            "broadcaster_series_id"
                        ),
                        "genre": None if not(isinstance(e.get(
                            "programme_content"),dict)) else e.get(
                                "programme_content",{}).get("genre"),
                        "platforms": e.get("platforms",[]),
                        "audience_views": e.get("audience_views",[{}]),
                    }
                )
            except AttributeError as error:
                print(e)
                raise AttributeError(error) from error
            except KeyError as error:
                print(e)
                raise KeyError(error) from error
        # Convert the result into a data frame
        columns_dict={
            "panel_code": "string",
            "panel_region": "string",
            "is_macro_region": "bool",
            "station_code": "string",
            "station_name": "string",
            "prog_name": "string",
            "programme_type": "string",
            "programme_start_datetime": "datetime64[ns]",
            "programme_duration_minutes": "int64",
            "spans_normal_day": "bool",
            "sponsor_code": "string",
            "bumpers_included": "bool",
            "broadcaster_transmission_code": "string",
            "live_status": "string",
            "uk_premiere": "bool",
            "broadcaster_premiere": "bool",
            "programme_repeat": "bool",
            "episode_name": "string",
            "episode_number": "string",
            "series_number": "string",
            "number_of_episodes": "string",
            "broadcaster_series_id": "string",
            "genre": "string",
            "platforms": "object",
            "audience_views": "object",
            #"date_of_transmission": "datetime64[ns]"
        }
        #verify dtypes
        if len(df_data)>0:
            verifry_class = VerifyDtypes()
            df_data = verifry_class.verify_dtypes(
                data = df_data
                ,column_dtypes = columns_dict
            )
        df = pd.DataFrame(df_data, columns=list(columns_dict.keys()))

        if not df.empty:
            # Format the transmission_time_period as a pandas datetime
            df["programme_start_datetime"] = pd.to_datetime(df["programme_start_datetime"])
            df["date_of_transmission"] = df["programme_start_datetime"].dt.date
            #dtypes for each column
            df=df.astype(dtype=columns_dict)

        return df

class ProgrammeScheduleResultSet(BulkResultSet):
    """
    Represents a programme ratings result set from the Barb API.
    """

    def to_dataframe(self) ->pd.DataFrame:
        """
        Converts the API response data into a pandas dataframe.

        Returns:
            pandas.DataFrame: A dataframe containing the API response data.

        """

        # if len(self.api_response_data["events"]) == 0:
        #     raise Warning("Error: No events returned.")

        # Loop through the events and then the audiences within the events
        df_data = []

        try:
            for item in self.api_response_data:
                item:dict
                for e in item.get("station_schedule",{}):
                    e:dict
                    df_data.append(
                    {
                        "scheduled_date": item.get("scheduled_date"),
                        "station_code": item.get("station",{}).get(
                            "station_code"
                        ),
                        "station_name": item.get("station",{}).get(
                            "station_name"
                        ),
                        "panel_code": item.get("panel",{}).get("panel_code"),
                        "panel_region": item.get("panel",{}).get("panel_region"),
                        "is_macro_region": item.get("panel",{}).get(
                            "is_macro_region"
                        ),
                        "broadcaster_premier": e.get("broadcaster_premier"),
                        "broadcaster_transmission_code": e.get("broadcaster_transmission_code"),
                        "live_status": e.get("live_status"),
                        "platforms": e.get("platforms",[]),
                        "content_name": e.get("programme_content").get("content_name"),
                        "barb_content_id": e.get("programme_content").get("barb_content_id"),
                        "broadcaster_content_id": e.get("programme_content").get(
                            "broadcaster_content_id"),
                        "metabroadcast_content_id": e.get("programme_content").get(
                            "metabroadcast_information").get("metabroadcast_content_id"),
                        "episode_number": e.get("programme_content").get(
                            "episode").get("episode_number"),
                        "episode_name": e.get("programme_content").get(
                            "episode").get("episode_name"),
                        "series_number": e.get("programme_content").get(
                            "series").get("series_number"),
                        "number_of_episodes": e.get("programme_content").get(
                            "series").get("number_of_episodes"),
                        "broadcaster_series_id": e.get("programme_content").get(
                            "series").get("broadcaster_series_id"),
                        "genre": e.get("programme_content").get("genre"),
                        "programme_duration": e.get("programme_duration"),
                        "barb_reporting_datetime": e.get("programme_start_datetime").get(
                            "barb_reporting_datetime"),
                        "barb_polling_datetime": e.get("programme_start_datetime").get(
                            "barb_polling_datetime"),
                        "standard_datetime": e.get("programme_start_datetime").get(
                            "standard_datetime"),
                        "programme_type": e.get("programme_type"),
                        "repeat": e.get("repeat"),
                        "spans_normal_day": e.get("spans_normal_day"),
                        "sponsor_code": e.get("sponsor").get("sponsor_code"),
                        "bumpers_included": e.get("sponsor").get("bumpers_included"),
                        "transmission_log_programme_name": e.get("transmission_log_programme_name"),
                        "uk_premier": e.get("uk_premier")
                    }
                )
        except KeyError as error:
            print(error)
        # Convert the result into a data frame
        columns_dict={
            "scheduled_date": "datetime64[ns]",
            "station_code": "string",
            "station_name": "string",
            "panel_code": "string",
            "panel_region": "string",
            "is_macro_region": "bool",
            "broadcaster_premier": "bool",
            "broadcaster_transmission_code": "string",
            "live_status": "string",
            "platforms": "object",
            "content_name": "string",
            "barb_content_id": "string",
            "broadcaster_content_id": "string",
            "metabroadcast_content_id": "string",
            "episode_number": "int64",
            "episode_name": "string",
            "series_number": "string",
            "number_of_episodes": "int64",
            "broadcaster_series_id": "string",
            "genre": "string",
            "programme_duration": "int64",
            "barb_reporting_datetime": "string",
            "barb_polling_datetime": "string",
            "standard_datetime": "datetime64[ns]",
            "programme_type": "string",
            "repeat": "bool",
            "spans_normal_day": "bool",
            "sponsor_code": "string",
            "bumpers_included": "bool",
            "transmission_log_programme_name": "string",
            "uk_premier":"bool"
        }
        #verify dtypes
        if len(df_data)>0:
            verifry_class = VerifyDtypes()
            verifry_class.verify_dtypes(
                data=df_data
                ,column_dtypes=columns_dict
            )
        df = pd.DataFrame(df_data, columns=list(columns_dict.keys()))

        if not df.empty:
            #dtypes for each column
            df = df.astype(dtype=columns_dict)

        return df

class AdvertisingSpotsResultSet(APIResultSet):
    """
    Represents an advertising spots result set from the Barb API.
    """

    def to_dataframe(self) ->pd.DataFrame:
        """
        Converts the API response data into a pandas dataframe.

        Returns:
            pandas.DataFrame: A dataframe containing the API response data.

        """

        #if len(self.api_response_data["events"]) == 0:
        #    raise Exception("Error: No events returned.")

        try:
            # Loop through the events and then the audiences within the events
            spot_data = []
            for e in self.api_response_data.get("events",[{}]):
                dict_event = dict(e)
                spot_data.append(
                    {
                        "panel_region": e["panel"]["panel_region"],
                        "panel_code": dict_event["panel"]["panel_code"],
                        "station_name": dict_event["station"]["station_name"],
                        "station_code": dict_event["station"]["station_code"],
                        "spot_type": dict_event["spot_type"],
                        "spot_start_datetime": dict_event.get("spot_start_datetime",{}).get(
                            "standard_datetime"),
                        "spot_duration": dict_event["spot_duration"],
                        "preceding_programme_name": dict_event["preceding_programme_name"],
                        "succeeding_programme_name": dict_event["succeeding_programme_name"],
                        "break_type": dict_event["break_type"],
                        "position_in_break": dict_event["position_in_break"],
                        "broadcaster_spot_number": dict_event["broadcaster_spot_number"],
                        "commercial_number": dict_event["commercial_number"],
                        "clearcast_commercial_title": dict_event.get(
                            "clearcast_information",{}).get(
                            "clearcast_commercial_title",None),
                        "clearcast_match_group_code": dict_event.get(
                            "clearcast_information",{}).get(
                            "match_group_code",None),
                        "clearcast_match_group_name": dict_event.get(
                            "clearcast_information",{}).get(
                            "match_group_name",None),
                        "clearcast_buyer_code": dict_event.get("clearcast_information",{}).get(
                            "buyer_code",None),
                        "clearcast_buyer_name":  dict_event.get("clearcast_information",{}).get(
                            "buyer_name",None),
                        "clearcast_advertiser_code": dict_event.get("clearcast_information",{}).get(
                            "advertiser_code",None),
                        "clearcast_advertiser_name": dict_event.get("clearcast_information",{}).get(
                            "advertiser_name",None),
                        "campaign_approval_id": dict_event["campaign_approval_id"],
                        "sales_house_name": dict_event.get("sales_house",{}).get(
                            "sales_house_name"),
                        "audience_views": dict_event.get("audience_views",[{}]),
                    }
                )
            # Convert the result into a data frame
            columns=[
                "panel_region",
                "panel_code",
                "station_name",
                "station_code",
                "spot_type",
                "spot_start_datetime",
                "spot_duration",
                "preceding_programme_name",
                "succeeding_programme_name",
                "break_type",
                "position_in_break",
                "broadcaster_spot_number",
                "commercial_number",
                "clearcast_commercial_title",
                "clearcast_match_group_code",
                "clearcast_match_group_name",
                "clearcast_buyer_code",
                "clearcast_buyer_name",
                "clearcast_advertiser_code",
                "clearcast_advertiser_name",
                "campaign_approval_id",
                "sales_house_name",
                "audience_views"
            ]
            spot_data = pd.DataFrame(data=spot_data, columns=columns)

            # Format the transmission_time_period as a pandas datetime
            spot_data["spot_start_datetime"] = pd.to_datetime(
                spot_data["spot_start_datetime"]
            )
            spot_data["date_of_transmission"] = spot_data["spot_start_datetime"].dt.date

            #set dtypes
            if not spot_data.empty:
                spot_data=self.dataframe_set_dtypes(spot_data)
            return spot_data
        except pd.errors.DataError as error:
            print(error)
        return None

    def dataframe_set_dtypes(self,dataframe:pd.DataFrame):
        """sets the dtypes for the columns"""
        dtypes_dict={
            "panel_region":"string",
            "panel_code":"int64",
            "station_name":"string",
            "station_code":"int64",
            "spot_type":"string",
            #"spot_start_datetime":"datetime64[ns]",
            "spot_duration":"int64",
            "preceding_programme_name":"string",
            "succeeding_programme_name":"string",
            "break_type":"string",
            "position_in_break":"string",
            "broadcaster_spot_number":"string",
            "commercial_number":"string",
            "clearcast_commercial_title":"string",
            "clearcast_match_group_code":"string",
            "clearcast_match_group_name":"string",
            "clearcast_buyer_code":"string",
            "clearcast_buyer_name":"string",
            "clearcast_advertiser_code":"string",
            "clearcast_advertiser_name":"string",
            "campaign_approval_id":"string",
            "sales_house_name":"string",
            #"audience_views":"object",
            #"date_of_transmission":"datetime64[ns]"
        }
        dataframe=dataframe.astype(dtype=dtypes_dict)
        return dataframe

class AudiencesByTimeResultSet(APIResultSet):
    """
    Represents an audiences by time result set from the Barb API.
    """

    def to_dataframe(self) -> pd.DataFrame:
        """
        Converts the API response data into a pandas dataframe.

        Returns:
            pandas.DataFrame: A dataframe containing the API response data.

        """

        # if len(self.api_response_data.get("events",[]) == 0:
        #     raise Exception("Error: No events returned.")

        try:
            # Loop through the events and then the audiences within the events
            audience_data = []
            for e in self.api_response_data.get("events",[]):
                e:dict
                for v in e.get("audience_views",[]):
                    v:dict
                    audience_data.append(
                        {
                            "date_of_transmission": e.get("date_of_transmission"),
                            "panel_code": e.get("panel",{}).get("panel_code"),
                            "panel_region": e.get("panel",{}).get("panel_region"),
                            "is_macro_region": e.get("panel",{}).get("is_macro_region"),
                            "station_code": e.get("station",{}).get("station_code"),
                            "station_name": e.get("station",{}).get("station_name"),
                            "activity": e.get("activity"),
                            "transmission_time_period_duration_mins": e.get(
                                "transmission_time_period_duration_mins"),
                            "transmission_time_period_start": e.get(
                                "transmission_time_period_start",{}
                            ).get("standard_datetime"),
                            "platforms": e.get("platforms"),
                            "audience_code": v.get("audience_code"),
                            "audience_size_hundreds": v.get("audience_size_hundreds"),
                            "category_id": v.get("category_id"),
                            "audience_name": v.get("description"),
                            "audience_target_size_hundreds": v.get(
                                "target_size_in_hundreds"
                            ),
                        }
                    )
            # Convert the result into a data frame
            columns_dict={
                "date_of_transmission": "datetime64[ns]",
                "panel_code": "string",
                "panel_region": "string",
                "is_macro_region": "bool",
                "station_code": "string",
                "station_name": "string",
                "activity": "string",
                "transmission_time_period_duration_mins": "int64",
                "transmission_time_period_start": "datetime64[ns]",
                "platforms": "object",
                "audience_code": "string",
                "audience_size_hundreds": "int64",
                "category_id": "string",
                "audience_name": "string",
                "audience_target_size_hundreds": "int64",
            }
            #verify dtypes
            if len(audience_data)>0:
                verifry_class = VerifyDtypes()
                audience_data = verifry_class.verify_dtypes(
                    data = audience_data
                    ,column_dtypes = columns_dict
                )
            audience_data = pd.DataFrame(audience_data, columns=list(columns_dict.keys()))

            # Format the transmission_time_period as a pandas datetime
            if not audience_data.empty:
                audience_data["transmission_time_period_start"] = pd.to_datetime(
                    audience_data["transmission_time_period_start"]
                )
                audience_data = audience_data.astype(dtype=columns_dict)

            return audience_data
        except pd.errors.DataError as error:
            print(error)
        return None

class GoogleBucketResultSet(BulkResultSet):
    """Represents spot Audience result set from the Barb API"""

    def __init__(self, api_response_data: list[dict], endpoint:str):
        """initalise class"""
        super().__init__(api_response_data)
        self.endpoint =endpoint

    def download_parquet(self) -> None:
        """download the parquet"""
        print("response_data",self.api_response_data)
        for results_item in self.api_response_data:
            print("results_item",results_item)
            date = results_item.get("session_date")
            panel = results_item.get("panel_code")
            for i,link in enumerate(results_item.get("results",[])):
                print(link)
                with tempfile.TemporaryDirectory() as td:
                    r=requests.get(link,timeout=300)
                    r.raise_for_status()
                    print(r.headers)
                    with open(f"{td}/demo_{i}.parquet",mode="wb") as f:
                        f.write(r.content)
                    print(f.name)
                    self.parquet_to_google_bucket(
                        file=f"{f.name}"
                        ,date=date
                        ,panel_code=panel
                    )
        return "ok"

    def parquet_to_google_bucket(self, file:str, date:str, panel_code:int) -> None:
        """upload the parquet to the google bucket"""
        bucket_name = os.getenv("BUCKET")
        bucket = storage_client.get_bucket(bucket_name)
        bucket = storage_client.bucket(bucket_name)
        result = re.search("(demo_)[0-9]*.parquet$",file)
        file_name= result[0]
        blob = bucket.blob(f"{self.endpoint}/date={date}/panel_code={panel_code}/{file_name}")
        blob.upload_from_filename(filename=file)
        print("file uploaded")
        return "ok"

class SpotScheduleResultSet(BulkResultSet):
    """
    return dataframe
    """
    def to_dataframe(self) ->pd.DataFrame:
        """
        Converts the API response data into a pandas dataframe.

        Returns:
            pandas.DataFrame: A dataframe containing the API response data.

        """

        #if len(self.api_response_data["events"]) == 0:
        #    raise Exception("Error: No events returned.")

        try:
            # Loop through the events and then the audiences within the events
            spot_data = []
            #print(self.api_response_data)
            for e in self.api_response_data:
                for item in e.get("spot_schedule",[{}]):
                    item:dict
                    platforms=[]
                    #print("platform type:",type(item.get("platforms",[])))
                    for platfrom in item.get("platforms",[]):
                        platforms.append(str(platfrom))
                    spot_data.append(
                        {
                            "scheduled_date":e.get("scheduled_date"),
                            "station_code":str(e.get("station",{}).get("station_code",{})),
                            "station_name":str(e.get("station",{}).get("station_name",{})),
                            "panel_code":str(e.get("panel",{}).get("panel_code",{})),
                            "panel_region":str(e.get("panel",{}).get("panel_region",{})),
                            "is_macro_region":str(e.get("panel",{}).get("is_macro_region",{})),
                            "break_type": str(item.get("break_type")),
                            "broadcaster_spot_number": str(item.get("broadcaster_spot_number")),
                            "campaign_approval_id": str(item.get("campaign_approval_id")),
                            "match_group_code": str(item["clearcast_information"].get(
                                "match_group_code")),
                            "match_group_name": str(item["clearcast_information"].get(
                                "match_group_name")),
                            "buyer_code": str(item["clearcast_information"].get("buyer_code")),
                            "buyer_name": str(item["clearcast_information"].get("buyer_name")),
                            "advertiser_code": str(item["clearcast_information"].get(
                                "advertiser_code")),
                            "advertiser_name": str(item["clearcast_information"].get(
                                "advertiser_name")),
                            "holding_company_code": str(item["clearcast_information"].get(
                                "holding_company_code")),
                            "holding_company_name": str(item["clearcast_information"].get(
                                "holding_company_name")),
                            "product_code": str(item["clearcast_information"].get(
                                "product_code")),
                            "product_name": str(item["clearcast_information"].get(
                                "product_name")),
                            "clearcast_commercial_title": str(item["clearcast_information"].get(
                                "clearcast_commercial_title")),
                            "commercial_spot_length": str(item["clearcast_information"].get(
                                "commercial_spot_length")),
                            "clearcast_web_address": str(item["clearcast_information"].get(
                                "clearcast_web_address")),
                            "commercial_number": str(item.get("commercial_number")),
                            "platforms": json.dumps(platforms),
                            "position_in_break": str(item.get("position_in_break")),
                            "preceeding_programme_name": str(item.get("preceeding_programme_name")),
                            # "sales_house":{
                                "sales_house_name": str(item["sales_house"].get(
                                    "sales_house_name")),
                                "sales_house_brand_description": str(item["sales_house"].get(
                                    "sales_house_brand_description")),
                            # },
                            "spot_duration": str(item.get("spot_duration")),
                            # "spot_start_datetime": {
                                "barb_reporting_datetime": item["spot_start_datetime"].get(
                                    "barb_reporting_datetime"),
                                "barb_polling_datetime": item["spot_start_datetime"].get(
                                    "barb_polling_datetime"),
                                "standard_datetime": item["spot_start_datetime"].get(
                                    "standard_datetime"),
                            # },
                            "spot_type": str(item.get("spot_type")),
                            "succeeding_programme_name": str(item.get("succeeding_programme_name"))
                        }
                    )
            # Convert the result into a data frame
            columns_dict={
                "scheduled_date":"datetime64[ns]",
                # "station",
                # "panel",
                # "spot_schedule"
                "station_code":"string",
                "station_name":"string",
                "panel_code":"string",
                "panel_region":"string",
                "is_macro_region":"string",
                "break_type":"string",
                "broadcaster_spot_number":"string",
                "campaign_approval_id":"string",
                    "match_group_code":"string",
                    "match_group_name":"string",
                    "buyer_code":"string",
                    "buyer_name":"string",
                    "advertiser_code":"string",
                    "advertiser_name":"string",
                    "holding_company_code":"string",
                    "holding_company_name":"string",
                    "product_code":"string",
                    "product_name":"string",
                    "clearcast_commercial_title":"string",
                    "commercial_spot_length":"string",
                    "clearcast_web_address":"string",
                "commercial_number":"string",
                "platforms":"string",
                "position_in_break":"string",
                "preceeding_programme_name":"string",
                # "sales_house":{
                    "sales_house_name":"string",
                    "sales_house_brand_description":"string",
                # },
                "spot_duration":"string",
                # "spot_start_datetime": {
                    "barb_reporting_datetime":"string",
                    "barb_polling_datetime":"string",
                    "standard_datetime":"datetime64[ns]",
                # },
                "spot_type":"string",
                "succeeding_programme_name":"string"
            }
            #verify dtypes
            if len(spot_data)>0:
                verifry_class = VerifyDtypes()
                verifry_class.verify_dtypes(
                    data=spot_data
                    ,column_dtypes=columns_dict
                )
            spot_data_frame = pd.DataFrame(data=spot_data, columns=list(columns_dict.keys()))
            print("data frame:",spot_data_frame)

            if not spot_data_frame.empty:
                # Format the transmission_time_period as a pandas datetime
                spot_data_frame["scheduled_date"] = pd.to_datetime(
                    spot_data_frame["scheduled_date"]
                )
                #set dtypes
                spot_data_frame=spot_data_frame.astype(dtype=columns_dict)
            return spot_data_frame
        except pd.errors.DataError as error:
            print(error)
            raise pd.errors.DataError(
                error
            ) from error

class VerifyDtypes:
    """schema verification"""

    def verify_dtypes(self,data:list[dict],column_dtypes:dict) -> list[dict]:
        """process of verifiying and changing data"""
        for item,value in enumerate(data):
            value:dict
            for column, dtype in column_dtypes.items():
                column:str
                dtype:str
                series = pd.Series(data={"column":value.get(column)},index=["column"])
                if dtype.lower()=="datetime64[ns]":
                    pd.to_datetime(series)
                elif (dtype.lower()=="string" and not(
                    isinstance(type(value.get(column)),str))):
                    data[item][column] = None
                elif dtype.lower()=="int64" and not(
                    isinstance(type(value.get(column)),int)):
                    data[item][column] = 0
                elif dtype.lower()=="bool" and not(
                    isinstance(type(value.get(column)),bool)):
                    data[item][column] = False
                elif dtype.lower()=="object" and not(
                    isinstance(type(value.get(column)),list)):
                    data[item][column]=[]
                else:
                    raise TypeError(f"unknown type {dtype} for '{column}'")
        return data
