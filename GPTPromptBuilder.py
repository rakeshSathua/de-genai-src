import logging
from google.cloud import bigquery, storage 
from google.cloud.bigquery import LoadJobConfig
from dotenv import load_dotenv
import os
from pathlib import Path
import pandas as pd
import datetime
import re

 
class GPTPromptBuilder:
    def __init__(self, project_id, input_str, user_dataset_id, project_dataset_id):
        # Initialize the GPTPromptBuilder with project ID, dataset ID, and the input string.
        self.user_dataset_name = user_dataset_id
        self.project_dataset_name = project_dataset_id
        self.project_name = project_id
        self.input_str = input_str

    def construct_prompt(self, upload_file_path):
        # Constructs the complete prompt for GPT.
        try:
            # Fetch the template prompt string from BigQuery.
            my_prompt_string = self.get_prompt(upload_file_path)
        except Exception as e:
            # Log any errors that occur during prompt construction.
            logging.error(f"Error in constructing prompt: {e}")
            return None

        # Format the final prompt with the template and the user's input.
        prompt = [
            {"role": "system", "content": f"{my_prompt_string}"},
            {
                "role": "user",
                "content": f"Translate the following English instruction to SQL Query: {self.input_str}",
            },
        ]


        # print("MY PROMPT",prompt)
        return prompt
    
    
    def extract_file_name(self, file_path):
        return os.path.basename(file_path)
    
    def file_upload_ser(self, abs_upload_file_path):
        upload_file_path = abs_upload_file_path.split('//')[1]
        dir = upload_file_path.split('/')
        if len(dir) >= 2:
            inp_bucket = dir[0]
            source_blob_name = '/'.join(map(str, dir[1:]))
            ROOT_DIR = Path(__file__).parents[0]
            #it should be coming out directory
            filename = self.extract_file_name(upload_file_path)


            destination_file_path = ROOT_DIR /f'{filename}'

            
            
            print("File Downloaded Start...")
            csv_file_path = self.download_csv_from_gcs(inp_bucket, source_blob_name, destination_file_path)
            print("File Downloaded Finished...", csv_file_path)

            
            
            user_out_dataset = os.getenv('USER_OUT_DATASET')

            new_table_name = f"{filename.split('.')[0]}_{datetime.date.today()}"

            self.csv_to_bigquery(csv_file_path, self.project_name, user_out_dataset, new_table_name)
            val = self.get_table_schema( self.project_name, user_out_dataset, new_table_name)
            print("VALUE OF SCHEMA COL VAL: ",val, type(val))
        else :
            print("ENTER A VALID PATH *")


    def get_prompt(self, abs_upload_file_path):
        # Retrieves a prompt template from BigQuery.
        try:
            #self.download_csv_from_gcs(bucket_name, source_blob_name, destination_file_path)
            # Initialize the BigQuery client.
            
            if abs_upload_file_path is not None:
                self.file_upload_ser(abs_upload_file_path)

            client = bigquery.Client()
            # Create and execute the SQL query to fetch prompts.
            sql_query = f"SELECT * FROM `{self.project_name}.{self.project_dataset_name}`.de_prompt_store ORDER BY id ASC"
            query_job = client.query(sql_query)

            results = query_job.result()
        except Exception as e:
            # Log any errors that occur during the querying process.
            logging.error(f"Error in querying BigQuery: {e}")
            raise

        data_objects = []
        for row in results:
            # Convert each row of the result into a dictionary.
            row_dict = dict(row.items())
            data_objects.append(row_dict)

        prompt_content = ""
        for data_object in data_objects:
            # Append each prompt message to the prompt content string.
            prompt_content += data_object["prompt_message"] + "\n"

        str_to_be_modified = "MY_DATASET_NAME"
        try:
            # Replace the placeholder string with the actual dataset name.
            modified_prompt_query = self.replace_dataset_name(
                prompt_content, str_to_be_modified
            ) 
        except Exception as e:
            # Log any errors that occur during string replacement.
            logging.error(f"Error in replacing dataset name: {e}")
            raise

        return modified_prompt_query

    def replace_dataset_name(self, prompt_str, str_val_to_change):
        # Replaces a specified substring in the prompt with the dataset name.
        try:
            # Perform the string replacement.
            modified_prompt = prompt_str.replace(
                str_val_to_change, self.user_dataset_name
            )
            return modified_prompt
        except Exception as e:
            # Log any errors that occur during the replacement process.
            logging.error(f"Error in replace_dataset_name method: {e}")
            raise

    def download_csv_from_gcs(self, bucket_name, source_blob_name, destination_file_path):
        """Download a CSV file from a Google Cloud Storage bucket."""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        # Download the file
        blob.download_to_filename(destination_file_path)

        print(f"File {source_blob_name} downloaded to {destination_file_path}")
        '''
        # Replace these values with your own
        bucket_name = 'your-gcs-bucket-name'
        source_blob_name = 'path/to/your/file.csv'
        
         
        '''
        return destination_file_path
        
    
        
    def csv_to_bigquery(self, csv_file_path, project_id, dataset_name, table_name):
    # Read CSV file into a Pandas DataFrame
        try :
            # Initialize a BigQuery client
             # Create a BigQuery client
            client = bigquery.Client(project=project_id)

            # Construct a reference to the dataset
            dataset_ref = client.dataset(dataset_name)

            # Check if the dataset exists, create it if not
            try:
                client.get_dataset(dataset_ref)
            except NotFound:
                dataset = bigquery.Dataset(dataset_ref)
                client.create_dataset(dataset)

            # Construct a reference to the table
            table_ref = dataset_ref.table(table_name)

            # Configure the job to append data if the table exists, create it if not
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip the header row
                autodetect=True,      # Automatically detect schema
            )

            # Load data from a CSV file into the table
            with open(csv_file_path, "rb") as source_file:
                load_job = client.load_table_from_file(
                    source_file, table_ref, job_config=job_config
                )

            # Wait for the job to complete
            load_job.result()
            print(f"Loaded {load_job.output_rows} rows into {table_ref.path}")

            os.remove(csv_file_path)
            print(f"CSV file {csv_file_path} deleted after uploading to BigQuery")

        except Exception as e:
            print("Not able to convert ....,",e)
         

    def get_table_schema(self, project_id, dataset_id, table_id):
        # # Create a BigQuery client
        # client = bigquery.Client(project=project_id)

        # # Get the dataset reference
        # dataset_ref = client.dataset(dataset_id)

        # # Get the table reference
        # table_ref = dataset_ref.table(table_id)

        # # Get the table schema
        # table = client.get_table(table_ref)
        # schema = table.schema

        # # Extract column names
        # column_names = [field.name for field in schema]
        client = bigquery.Client(project=project_id)

        # Get the dataset reference
        dataset_ref = client.dataset(dataset_id)

        # Get the table reference
        table_ref = dataset_ref.table(table_id)

        # Get the table schema
        table = client.get_table(table_ref)
        schema = table.schema

        # Extract column names and data types
        columns_info = [(field.name, field.field_type) for field in schema]

        return columns_info
    

# Example usage:
# builder = GPTPromptBuilder("your_project_id", "your_dataset_id", "your_input_str")
# prompt = builder.construct_prompt()
# print(prompt)
