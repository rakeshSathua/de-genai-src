from GPTBigQueryInterface import GPTBigQueryInterface
from dotenv import load_dotenv
import os

# project_id = "masterdb-317014"
# dataset_id = "AlleviateTax"

project_id = "ds-webdev"
user_dataset = "AlleviateTax"

interface = GPTBigQueryInterface(project_id, user_dataset)
input_text = "get me all incoming calls for last week"

# upload_file_path = 'gs://de_genai_csv_responses/genarated_output20240112071813_19669574.csv'
upload_file_path = None
url = interface.run(input_text, upload_file_path)

    
