"""module used for environment variables"""
import os
import json
import datetime as DT
import flask
import functions_framework
import pandas as pd
import yaml
from google.cloud import tasks_v2
from google.api_core.exceptions import BadRequest
from helpers.big_query_class import BigQueryClass
from helpers.pybarb import BarbAPI

DATASET = "Barb_Data"

def default(request:flask.Request= None):
    """defaults for all the functions functions"""
    end_date = DT.date.today()
    end_date -= DT.timedelta(days=10)
    start_date = end_date - DT.timedelta(days=17)

    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    query_list=[
        "advertising_spots",
        #"spot_audience",
        "spot_schedule",
        "programme_ratings",
        "programme_audience",
        "programme_schedule",
        "viewing",
        "audience_by_time",
        #"panel_members",
        #"households"
    ]
    if request:
        if request.is_json:
            request_json = request.get_json(silent=True)
            request_json:dict
            start_date = request_json.get("start_date",start_date)
            end_date = request_json.get("end_date",end_date)
            query_list = request_json.get("query_list",query_list)
    print(start_date,end_date,query_list)
    return end_date, start_date, query_list

def send_to_tasks(
        message_json:str
        , task_name:str
        , queueurl:str
        , queue_name:str = "Barb-queue"
        ,prefix="barb"):
    """send the next section to tasks"""
    task_client = tasks_v2.CloudTasksClient()
    queue = task_client.queue_path(os.getenv('PROJECT_ID'), "europe-west2", queue_name)
    task ={
        "http_request":{
            "http_method": tasks_v2.HttpMethod.POST
            ,"headers":{
                "content-type": 'application/json'
            }
            ,"oidc_token":{
                "service_account_email" : os.getenv('SERVICE_ACCOUNT')
            }
        }
    }
    message_bytes = message_json.encode("utf-8")
    task["http_request"]["body"]= message_bytes
    #publish message
    task_name=f"{str(DT.datetime.now())}_{task_name}"
    name = task_client.task_path(os.getenv("PROJECT_ID"), "europe-west2", queue_name, task_name)
    name = name.replace(":","-")
    name = name.replace(".","_")
    name = name.replace(" ","__")
    task["name"]= name
    task_url = f"https://europe-west2-{os.getenv('PROJECT_ID')}"
    task_url+= f".cloudfunctions.net/{prefix}-{os.getenv('STAGE')}-{queueurl}"
    task["http_request"]["url"] = task_url
    task["http_request"]["oidc_token"]["audience"]= task["http_request"]["url"]
    print(task)
    request = tasks_v2.CreateTaskRequest(
        parent = queue
        ,task = task
    )
    if os.getenv("ENV"):
        return request
    else:
        response = task_client.create_task(
            request = request
            ,timeout = 20
        )
    return response

def daterange(date1:str, date2:str, n:int=6) -> list[dict]:
    """give a list of start date and end dates in interval 6"""
    format_date = "%Y-%m-%d"
    start_date = DT.datetime.strptime(date1, format_date)
    end_date = DT.datetime.strptime(date2, format_date)
    date_array = []
    sudo_start = start_date
    while sudo_start <= end_date:
        if (sudo_start + DT.timedelta(days=n))>end_date:
            sudo_end = end_date
        else:
            sudo_end = sudo_start + DT.timedelta(days=n)
        date_array.append({
            "start_date":sudo_start.strftime('%Y-%m-%d')
            ,"end_date": sudo_end.strftime('%Y-%m-%d')
        })
        sudo_start+= DT.timedelta(n+1)
    return date_array

@functions_framework.http
def barb_default(request:flask.Request= None):
    """main code starts here"""
    end_date, start_date, query_list= default(request)
    date_range = daterange(start_date,end_date,2)
    print(date_range)

    barb_api = BarbAPI()
    #list stations
    stations = barb_api.list_stations()
    print(f"total stations:{len(stations)}")
    panels = barb_api.list_panels()
    print(f"total panels:{len(panels)}")
    #print(panels)
    #loop to send to tasks
    for dates in date_range:
        for item in query_list:
            if item in [
                "advertising_spots",
                "spot_schedule",
                "programme_ratings",
                "programme_schedule",
                "audience_by_time",
            ]:
                for station in stations:
                    message_dict = {
                        'item': item
                        ,"station": station
                        ,"start_date": dates["start_date"]
                        ,"end_date": dates["end_date"]
                    }
                    message_json = json.dumps(message_dict)
                    print(message_json)
                    task_name = f"{item}-request-station_code-{station['station_code']}"
                    task_name+= f"-{dates['start_date']}--{dates['end_date']}"
                    if os.getenv("ENV"):
                        if item=="advertising_spots":
                            barb_2_request_advertising_spots(message_dict)
                        elif item=="spot_schedule":
                            barb_2_request_spot_schedule(message_dict)
                        elif item=="programme_ratings":
                            barb_2_request_programme_ratings(message_dict)
                        elif item=="programme_schedule":
                            barb_2_request_programme_schedule(message_dict)
                        elif item=="audience_by_time":
                            barb_2_request_audience_by_time(message_dict)
                    else:
                        send_to_tasks(
                            message_json = message_json
                            , queueurl= f"2_request_{item}"
                            , task_name = task_name
                        )
            elif item in [
                "spot_audience",
                "programme_audience",
                "viewing",
                "panel_members",
                "households"
            ]:
                for panel in panels:
                    message_dict = {
                        'item': item
                        ,"panel": panel
                        ,"start_date": dates["start_date"]
                        ,"end_date": dates["end_date"]
                    }
                    message_json = json.dumps(message_dict)
                    print(message_json)
                    task_name = f"{item}-request-panel_code-{panel['panel_code']}"
                    task_name+= f"-{dates['start_date']}--{dates['end_date']}"
                    if os.getenv("ENV"):
                        if item=="spot_audience":
                            barb_2_request_spot_audience(message_dict)
                        elif item=="programme_audience":
                            barb_2_request_programme_audience(message_dict)
                        elif item=="programme_schedule":
                            barb_2_request_programme_schedule(message_dict)
                        elif item=="viewing":
                            barb_2_request_viewing(message_dict)
                        #elif item=="panel_members":
                        #   barb_2_request_advertising_spots(message_dict)
                        #elif item=="households":
                        #   barb_2_request_advertising_spots(message_dict)
                    else:
                        send_to_tasks(
                            message_json = message_json
                            , queueurl= f"2_request_{item}"
                            , task_name = task_name
                        )
    return "sent to tasks"

@functions_framework.http
def barb_2_request_advertising_spots(request:flask.Request=None):
    """
    A request for advertising spots by station
    """
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    print(message_json)
    message_json:dict
    start_date = message_json.get('start_date')
    end_date = message_json.get('end_date')
    item = message_json.get('item')
    station = message_json.get('station')

    barb_api = BarbAPI()
    data_frame = barb_api.advertising_spots(
        min_transmission_date=start_date
        ,max_transmission_date=end_date
        ,station_code=station["station_code"]
        ,buyer="phd_media_limited"
    ).to_dataframe()

    if data_frame.empty:
        return "No Data"
    #test_dataframe(data_frame=data_frame)
    #return "done"
    message_json = bigquery_load_table(
        message_json=message_json
        , data_frame=data_frame
    )
    message_json["columns"]=["date_of_transmission","station_name"]
    task_name = f"{item}-delete-{start_date}--{end_date}"
    if os.getenv("ENV"):
        barb_3_delete_data(message_json)
    else:
        send_to_tasks(
            message_json = json.dumps(message_json)
            , queueurl= "3_delete_data"
            , task_name = task_name
        )
    return "ok"

@functions_framework.http
def barb_2_request_spot_audience(request:flask.Request=None):
    """
    Get a full viewer level details of a specified spot
    """
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    message_json:dict
    print(message_json)
    start_date = message_json.get('start_date')
    end_date = message_json.get('end_date')
    #item = message_json.get('item')
    panel = message_json.get('panel')

    barb_api = BarbAPI()
    data_frame = barb_api.spot_audience(
        min_date=start_date
        ,max_date=end_date
        ,panel_code=panel["panel_code"]
    )

    if not data_frame:
        print("no data finish")
        return "No data"
    print("there is data so download file")
    data_frame.download_parquet()

    # if data_frame.empty:
    #     return "No Data"
    # test_dataframe(data_frame=data_frame)
    # #return "done"
    # message_json = bigquery_load_table(
    #     message_json=message_json
    #     , data_frame=data_frame
    # )
    # message_json["columns"]=["","panel"]
    # task_name = f"{item}-delete-{start_date}--{end_date}"
    # if os.getenv("ENV"):
    #     barb_3_delete_data(message_json)
    # else:
    #     send_to_tasks(
    #         message_json = json.dumps(message_json)
    #         , queueurl= "3_delete_data"
    #         , task_name = task_name
    #     )
    return "ok"

@functions_framework.http
def barb_2_request_spot_schedule(request:flask.Request=None):
    """
    Get a full viewer level details of a specified spot
    """
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    message_json:dict
    print(message_json)
    start_date = message_json.get('start_date')
    end_date = message_json.get('end_date')
    item = message_json.get('item')
    station = message_json.get('station')

    barb_api = BarbAPI()
    data_frame = barb_api.spot_schedule(
        min_date=start_date
        ,max_date=end_date
        ,station_code=station["station_code"]
    ).to_dataframe()
    if data_frame.empty:
        return "No Data"
    #test_dataframe(data_frame=data_frame)
    #return "done"
    message_json = bigquery_load_table(
        message_json=message_json
        , data_frame=data_frame
    )
    message_json["columns"]=["scheduled_date","station_code"]
    task_name = f"{item}-delete-{start_date}--{end_date}"
    if os.getenv("ENV"):
        barb_3_delete_data(message_json)
    else:
        send_to_tasks(
            message_json = json.dumps(message_json)
            , queueurl= "3_delete_data"
            , task_name = task_name
        )
    return "ok"

@functions_framework.http
def barb_2_request_programme_ratings(request:flask.Request=None):
    """
    Get a full viewer level details of a specified spot
    """
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    message_json:dict
    print(message_json)
    start_date = message_json.get('start_date')
    end_date = message_json.get('end_date')
    item = message_json.get('item')
    station = message_json.get('station')

    barb_api = BarbAPI()
    data_frame = barb_api.programme_ratings(
        min_date=start_date
        ,max_date=end_date
        ,station_code=station["station_code"]
    ).to_dataframe()
    if data_frame.empty:
        return "No Data"
    #test_dataframe(data_frame=data_frame)
    #return "done"
    message_json = bigquery_load_table(
        message_json=message_json
        , data_frame=data_frame
    )
    message_json["columns"]=["date_of_transmission","station_code"]
    task_name = f"{item}-delete-{start_date}--{end_date}"
    if os.getenv("ENV"):
        barb_3_delete_data(message_json)
    else:
        send_to_tasks(
            message_json = json.dumps(message_json)
            , queueurl= "3_delete_data"
            , task_name = task_name
        )
    return "ok"

@functions_framework.http
def barb_2_request_programme_audience(request:flask.Request=None):
    """
    programme_audience
    """
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    message_json:dict
    print(message_json)
    start_date = message_json.get('start_date')
    end_date = message_json.get('end_date')
    #item = message_json.get('item')
    panel = message_json.get('panel')

    barb_api = BarbAPI()
    data_frame = barb_api.programme_audience(
        min_date=start_date
        ,max_date=end_date
        ,panel_code=panel["panel_code"]
    )
    print(data_frame)
    if not data_frame:
        print("no data finish")
        return "No data"
    print("there is data so download file")
    data_frame.download_parquet()
    return "ok"

@functions_framework.http
def barb_2_request_programme_schedule(request:flask.Request=None):
    """
    programme_schedule
    """
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    message_json:dict
    print(message_json)
    start_date = message_json.get('start_date')
    end_date = message_json.get('end_date')
    item = message_json.get('item')
    station = message_json.get('station')

    barb_api = BarbAPI()
    data_frame = barb_api.programme_schedule(
        min_date=start_date
        ,max_date=end_date
        ,station_code=station["station_code"]
    ).to_dataframe()
    print(data_frame)
    if data_frame.empty:
        print("no data finish")
        return "No data"
    #test_dataframe(data_frame=data_frame)
    #return "done"
    message_json = bigquery_load_table(
        message_json=message_json
        , data_frame=data_frame
    )
    message_json["columns"]=["scheduled_date","station_code"]
    task_name = f"{item}-delete-{start_date}--{end_date}"
    if os.getenv("ENV"):
        barb_3_delete_data(message_json)
    else:
        send_to_tasks(
            message_json = json.dumps(message_json)
            , queueurl= "3_delete_data"
            , task_name = task_name
        )
    return "ok"

@functions_framework.http
def barb_2_request_viewing(request:flask.Request=None):
    """
    Get the viewing report data by day and panel.
    """
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    message_json:dict
    print(message_json)
    start_date = message_json.get('start_date')
    end_date = message_json.get('end_date')
    #item = message_json.get('item')
    panel = message_json.get('panel')

    barb_api = BarbAPI()
    data_frame = barb_api.viewing(
        min_date=start_date
        ,max_date=end_date
        ,panel_code=panel["panel_code"]
    )

    if not data_frame:
        print("no data finish")
        return "No data"
    print("there is data so download file")
    data_frame.download_parquet()

    # if data_frame.empty:
    #     return "No Data"
    # test_dataframe(data_frame=data_frame)
    # #return "done"
    # message_json = bigquery_load_table(
    #     message_json=message_json
    #     , data_frame=data_frame
    # )
    # message_json["columns"]=["","panel"]
    # task_name = f"{item}-delete-{start_date}--{end_date}"
    # if os.getenv("ENV"):
    #     barb_3_delete_data(message_json)
    # else:
    #     send_to_tasks(
    #         message_json = json.dumps(message_json)
    #         , queueurl= "3_delete_data"
    #         , task_name = task_name
    #     )
    return "ok"

@functions_framework.http
def barb_2_request_audience_by_time(request:flask.Request=None):
    """
    Get the audience sizes for various_time periods_by day, station and panel
    """
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    print(message_json)
    message_json:dict
    start_date = message_json.get('start_date')
    end_date = message_json.get('end_date')
    item = message_json.get('item')
    station = message_json.get('station')

    barb_api = BarbAPI()
    # time_period_options=[1,5,15]
    # viewing_status_options=["live", "vosdal", "consolidated"]
    # for time in time_period_options:
    #     for status in viewing_status_options:
    data_frame = barb_api.audiences_by_time(
        min_transmission_date=start_date
        ,max_transmission_date=end_date
        ,time_period_length=15
        ,viewing_status="consolidated"
        ,station_code=station["station_code"]
    ).to_dataframe()

    if data_frame.empty:
        return "No Data"
    #test_dataframe(data_frame=data_frame)
    #return "done"
    message_json = bigquery_load_table(
        message_json=message_json
        , data_frame=data_frame
    )
    message_json["columns"]=["date_of_transmission","station_code"]
    task_name = f"{item}-delete-{start_date}--{end_date}"
    if os.getenv("ENV"):
        barb_3_delete_data(message_json)
    else:
        send_to_tasks(
            message_json = json.dumps(message_json)
            , queueurl= "3_delete_data"
            , task_name = task_name
        )
    return "ok"

def test_dataframe(data_frame:pd.DataFrame):
    """for testing purposes"""
    print(data_frame.dtypes)
    for column in data_frame.columns:
        print(data_frame[column])
    return "test done"

def bigquery_load_table(message_json:dict, data_frame:pd.DataFrame) -> None:
    """bigquery load data_frame into table"""
    start_date = message_json.get('start_date')
    end_date = message_json.get('end_date')
    item = message_json.get('item')
    station = message_json.get('station')
    panel = message_json.get('panel')
    job_config = message_json.get('job_config')
    #bigquery starts now
    bigquery_class = BigQueryClass(dataset=DATASET)
    if station:
        table_name = f"{item}-{station['station_code']}"
    elif panel:
        table_name = f"{item}-{panel['panel_code']}"
    temp_table = bigquery_class.temp_table(
        table_name=f"{table_name}-{start_date}--{end_date}"
    )
    raw_data_table = bigquery_class.raw_table(
        table_name=item
    )
    result = bigquery_class.create_table(
        raw_data= raw_data_table
        , temp_data= temp_table
    )
    try:
        result = bigquery_class.load_dataframe_into_table(
            dataframe= data_frame
            , temp_data= temp_table
            , job_config= job_config
        )
    except Exception as error:
        test_dataframe(data_frame)
        raise BadRequest(error) from error
    message_json["temp_table"]= temp_table
    message_json["raw_table"]= raw_data_table
    return message_json

@functions_framework.http
def barb_3_delete_data(request:flask.Request=None):
    """get keyword data between start date and end date"""
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    message_json:dict
    print(message_json)
    big_query_class = BigQueryClass(dataset=DATASET)

    result = big_query_class.delete_from_table(
        temp_data = message_json.get("temp_table")
        ,raw_data = message_json.get("raw_table")
        ,columns=message_json.get("columns",[])
    )
    task_name = f"INSERT-FROM_{message_json.get('temp_table')}"
    task_name+= f"--{DT.datetime.now()}"
    if os.getenv("ENV"):
        barb_4_insert_data(message_json)
    else:
        send_to_tasks(
            message_json = json.dumps(message_json)
            , queueurl= "4_insert_data"
            , task_name = task_name
            , queue_name= "Barb-bq-queue"
        )
    return "ok"

@functions_framework.http
def barb_4_insert_data(request:flask.Request=None):
    """get keyword data between start date and end date"""
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    print(message_json)
    message_json:dict
    big_query_class = BigQueryClass(dataset=DATASET)
    columns = big_query_class.get_columns_from_table(
        table = message_json.get("raw_table")
    )
    result = big_query_class.insert_from_table(
        temp_data= message_json.get("temp_table")
        ,raw_data= message_json.get("raw_table")
        ,columns=columns
    )
    task_name = f"DROP-TABLE_{message_json.get('temp_table')}"
    task_name+= f"--{DT.datetime.now()}"
    if os.getenv("ENV"):
        barb_5_drop_table(message_json)
    else:
        send_to_tasks(
            message_json = json.dumps(message_json)
            , queueurl= "5_drop_table"
            , task_name = task_name
            , queue_name= "Barb-bq-queue"
        )
    return "ok"

@functions_framework.http
def barb_5_drop_table(request:flask.Request=None):
    """get keyword data between start date and end date"""
    if os.getenv("ENV"):
        message_json=request
    else:
        message_json = request.get_json(silent=True)
    print(message_json)
    message_json:dict
    big_query_class = BigQueryClass(dataset=DATASET)
    result = big_query_class.drop_temp_table(
        temp_table= message_json.get("temp_table")
    )
    return "ok"

if __name__ == '__main__':
    with open("dev.yaml",'r',encoding="UTF-8") as file:
        variables = yaml.safe_load(file)
    os.environ["PROJECT_ID"] = variables["PROJECT_ID"]
    os.environ["VERSION"] = variables["VERSION"]
    os.environ["STAGE"] = variables["STAGE"]
    os.environ["BUCKET"] = variables["BUCKET"]
    os.environ["ENV"] = 'local'
    os.environ["EMAIL"] = variables["EMAIL"]
    os.environ["PASSWORD"] = variables["PASSWORD"]
    class TestRequest:
        """test request for local testing"""
        def __init__(self):
            self.is_json=True

        def get_json(self,force=False, silent=False, cache=True) ->dict:
            """json object to test"""
            json_object={
                "start_date":"2023-12-12"
                ,"end_date": "2023-12-15"
                ,"query_list":[
                    # "advertising_spots",   #done
                    # "spot_audience",       #done
                    # "spot_schedule",       #done
                    # "programme_ratings",   #done
                    # "programme_audience",  #done
                    "programme_schedule",  #done
                    # "viewing",             #done
                    # "audience_by_time",    #done
                    # "panel_members",       #
                    # "households"           #
                ]
            }
            return json_object

        def useless_f(self):
            """to pad class"""
    test_request=TestRequest()
    barb_default(test_request)
    #functions-framework --target=adgroup_request_function --host=localhost --port=8080
