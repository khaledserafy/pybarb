"""modules to import"""
import os
import re
import datetime as DT
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

class BigQueryClass:
    """bigquery class to insert data"""
    def __init__(self, dataset):
        self.project_id = os.getenv("PROJECT_ID")
        self.big_query_client = bigquery.Client(project=self.project_id)
        self.dataset = dataset

    def raw_table(self, table_name:str) -> str:
        """define the raw bigquery table"""
        temp_data = f"{self.project_id}.{self.dataset}.{table_name}"
        return temp_data

    def temp_table(self, table_name:str) -> str:
        """return tempoary table name"""
        #uuid_id = uuid.uuid4()
        timestamp = str(DT.datetime.now())
        timestamp = timestamp.replace(":","-")
        timestamp = timestamp.replace(".","_")
        timestamp = timestamp.replace(" ","__")
        temp_data = f"{self.project_id}.{self.dataset}.{table_name}_{timestamp}"
        return temp_data

    def create_table(self
                     , raw_data: str
                     , temp_data: str
                    ) -> None:
        """create table"""
        query = f"""
        CREATE TABLE `{temp_data}`
        LIKE `{raw_data}`
        OPTIONS(
            expiration_timestamp = TIMESTAMP_ADD(current_timestamp(), INTERVAL 7 DAY)
        );"""
        print(query)
        query_job = self.big_query_client.query(query)
        try:
            return query_job.result()
        except BadRequest as error:
            print(error)
        return None

    def load_dataframe_into_table(
            self
            , dataframe:pd.DataFrame
            , temp_data:str
            , job_config:bigquery.LoadJobConfig=None
    ):
        """load dataframe into table"""
        print("load df into temp table")
        job = self.big_query_client.load_table_from_dataframe(
            dataframe
            , temp_data
            , job_config=job_config
        )
        return job.result()  # Waits for the job to complete.

    def get_table_schema(self,temp_data:str):
        """when all goes wrong print the schema"""
        table = self.big_query_client.get_table(
            table=temp_data
        )
        schema = table.schema
        print(schema)

    def truncate_table(self, raw_data:str) ->None:
        """truncates table"""
        query = f"""TRUNCATE TABLE `{raw_data}`"""
        print(query)
        query_job = self.big_query_client.query(query)
        return query_job.result()  # Waits for the job to complete.

    def delete_from_table(self,temp_data:str, raw_data:str, columns:list) ->None:
        """deletes data from raw_data table using temp_data"""
        delete_query = f"""DELETE FROM `{raw_data}`
        WHERE """
        for column in columns:
            if columns.index(column)>0:
                delete_query +=" AND "
            delete_query +=" {column} in (select distinct {column} from `{temp_data}`)\n".format(
                column = column
                ,temp_data = temp_data
            )
        print(delete_query)
        query_job = self.big_query_client.query(delete_query)
        return query_job.result()  # Waits for the job to complete.

    def get_columns_from_table(self, table:str) ->list:
        """using information schema to get column names from table"""
        split_text = re.split(
            pattern="\."
            ,string=table
        )
        dataset = split_text[1]
        table_name = split_text[2]
        #query
        query = f"""SELECT COLUMN_NAME
        FROM `{self.project_id}`.{dataset}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{table_name}';"""
        print(query)
        query_job = self.big_query_client.query(query)
        response = query_job.result()
        columns=[]
        for row in response:
            columns.append(row.COLUMN_NAME)
        return columns

    def insert_from_table(self, temp_data:str, raw_data:str, columns:list) -> None:
        """insert data from temp_data into raw_data table"""
        print("insert data from temporary_data into raw_data")
        insert_query = f"INSERT `{raw_data}` ("
        for column in columns:
            if columns.index(column)>0:
                insert_query +=","
            insert_query+=f"{column}\n"
        insert_query +=")\nSELECT \n"
        for column in columns:
            if columns.index(column)>0:
                insert_query +=","
            insert_query+=f"{column}\n"
        insert_query += f"FROM `{temp_data}`; "

        print(insert_query)
        query_job = self.big_query_client.query(insert_query)
        return query_job.result()  # Waits for the job to complete.

    def drop_temp_table(self,temp_table:str):
        """drop tempoary table"""
        query = f"DROP TABLE IF EXISTS `{temp_table}`;"
        print(query)
        query_job = self.big_query_client.query(query)
        return query_job.result()

if __name__ == '__main__':
    bigquery_class = BigQueryClass("Barb_Data")
    bigquery_class.get_columns_from_table(
        'phd-solutions-platform-dev.Barb_Data.advertising_spots'
    )
