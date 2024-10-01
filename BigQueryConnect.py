import os
import uuid
import csv
import logging

from google.cloud import bigquery, storage
from datetime import datetime, timedelta
from dotenv import load_dotenv
from cachetools import LRUCache, cachedmethod

from GoogleCloudStorageManager import GoogleCloudStorageManager


class BigQueryManager:
    """
    BigQueryManager handles interactions with Google BigQuery. It provides
    methods to run queries and log their results, and to check for existing
    entries in a logging table.

    Attributes:
        project_id (str): The Google Cloud project ID.
        dataset_id (str): The dataset ID within BigQuery.
        client (Client): The BigQuery client.
    """

    def __init__(self, project_id, dataset_id, user_dataset):
        """
        Initializes the BigQueryManager with the provided credentials file,
        project ID, and dataset ID.

        Args:
            project_id (str): The Google Cloud project ID.
            dataset_id (str): The BigQuery dataset ID.
        """
        load_dotenv()
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.user_dataset = user_dataset

        self.cache = LRUCache(maxsize=1000000)

    
    @cachedmethod(cache=lambda self: self.cache, key=lambda self, myVal: "myVal")
    def buffer_check(self, input_text):
        """
        Checks if the provided input text already exists in the 'de_genai_logs' table
        to prevent redundant entries.

        Args:
            input_text (str): The user input text to check for in the table.

        Returns:
            bool: True if the input text is found in the table, False otherwise.
        """
        try:
           

            query = f"SELECT input_text, generated_query FROM `{self.project_id}.{self.dataset_id}.de_genai_logs` WHERE status = 1 and user_dataset = '{self.user_dataset}'"

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("entry_value", "STRING", input_text)
                ]
            )
            query_job = self.client.query(query, job_config=job_config)
            data_objects = []
            for row in query_job:
                row_dict = dict(row.items())
                data_objects.append(row_dict)

            # print("DATA OBJECTS : ",data_objects)
            return data_objects
        except Exception as e:
            logging.error(f"An error occurred during buffer check: {e}")
            return []

    def fetch_query(self, input_text):
        # Check if the result is already in the cache
        if "myVal" in self.cache and len(self.cache["myVal"]) > 0:
            print("CACHE_DATA <<<<<<<<<<<<<<<<< :")
            
            return self.cache["myVal"]
        else:
            # If not in cache, perform the query and store the result
            print("BQ DATA-BASE >>>>>>>>>>>>>>>>>>>>>>>>>>>>> : ")
            
            result = self.buffer_check(input_text)
            self.cache["myVal"] = result
            
            return self.cache["myVal"]

    

    def run_query(self, _query, input_text, status_id):
        """
        Executes a BigQuery SQL query and logs the query along with the input text
        that generated it to the 'tw_memory_logs' table.

        Args:
            _query (str): The SQL query to execute.
            input_text (str): The user input text related to the query.

        Prints a message indicating whether the data was inserted successfully or if there were errors.
        """
        try:
            table_ref = self.client.dataset(self.dataset_id).table("de_genai_logs")
            x = datetime.now()
            data_to_insert = [
                {
                    "input_text": input_text,
                    "generated_query": _query,
                    "created_at": x,
                    "status": status_id,
                    "user_dataset": self.user_dataset,
                }
            ]
            schema = [
                bigquery.SchemaField("input_text", "STRING"),
                bigquery.SchemaField("generated_query", "STRING"),
                bigquery.SchemaField("created_at", "DATETIME"),
                bigquery.SchemaField("status", "INT64"),
                bigquery.SchemaField("user_dataset", "STRING"),
                
            ]
            errors = self.client.insert_rows(
                table_ref, data_to_insert, selected_fields=schema
            )
            if errors:
                logging.error(f"Error inserting data: {errors}")
                return []
            else:
                print("Data inserted successfully.")
                query = f"SELECT created_at FROM `{self.project_id}.{self.dataset_id}.de_genai_logs` ORDER BY created_at DESC LIMIT 1;"

                # Run the query
                query_job = self.client.query(query)

                # Fetch the result
                results = query_job.result()

                latest_created_at = None

                # Iterate through the result rows
                for row in results:
                    # Extract the 'created_at' value
                    latest_created_at = (
                        row.created_at
                    )  # Assuming 'created_at' is the column name

                return [latest_created_at]

        except Exception as e:
            logging.error(
                f"An error occurred during query execution or data insertion: {e}"
            )
    
    def run_generated_query(self, gen_query):
        try:
            query_job = self.client.query(gen_query)

            # Fetch the results
            results = query_job.result()
            # print(results)

            response_data = []
            for row in results:
                # Convert each row to a dictionary
                row_dict = dict(row.items())
                response_data.append(row_dict)

            # Print or use the list_of_dicts as needed

            unique_identifier = (
                f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
            )

            csv_filename = f"genarated_output{unique_identifier}.csv"
            self.write_response_to_csv(response_data, csv_filename)

            bucket_name = os.getenv("BUCKET_NAME")
            storage_filename = f"genarated_output{unique_identifier}.csv"
            storage = GoogleCloudStorageManager(bucket_name, project_id=self.project_id)
            download_url = storage.upload_to_google_storage(
                csv_filename, storage_filename
            )

            print("DOWNLOAD URL", download_url)
            return download_url

        except Exception as e:
            print(f"An error occurred during generated query execution: {e}")
            
            return []

    def write_response_to_csv(self, response_data, csv_filename):
        with open(csv_filename, "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)

            # Assuming response_data is a list of dictionaries
            header = response_data[0].keys()
            csv_writer.writerow(header)

            for row in response_data:
                csv_writer.writerow(row.values())
