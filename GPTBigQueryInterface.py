import re
import openai
import logging
from BigQueryConnect import BigQueryManager
from dotenv import load_dotenv
import os

# Import the `tw_table` from your `promt` module. Make sure this module exists and is accessible.
from GPTPromptBuilder import GPTPromptBuilder
from util import LogUtil

# Set up logging at the beginning of your application
LogUtil.setup_logging(log_file="interface.log", level=logging.INFO)


class GPTBigQueryInterface:
    """
    This class interfaces GPT with BigQuery to extract SQL queries from natural language
    input using GPT and log them to BigQuery.
    """

    def __init__(self, project_id, user_dataset):
        """
        Initialize the GPTBigQueryInterface.

        Args:
            bq_credentials_path (str): Path to the BigQuery service account credentials file.
            project_id (str): Google Cloud project ID.
            user_dataset (str): BigQuery dataset ID.
        """
        load_dotenv()

        openai.api_key = os.getenv("OPENAI_API_KEY")
        # Check if the OPENAI_API_KEY is loaded properly
        if not openai.api_key:
            logging.error("OPENAI_API_KEY not found in environment variables.")
            raise ValueError(
                "OPENAI_API_KEY is required to authenticate GPT API requests."
            )
        self.project_dataset = os.getenv("PROJECT_DATASET")
        self.project_id = project_id
        self.user_dataset = user_dataset
        self.bq_manager = BigQueryManager(self.project_id, self.project_dataset, self.user_dataset)
        self.prompt = None

        logging.basicConfig(level=logging.INFO)

    def extract_sql_query(self, response_text):
        """
        Extract SQL queries from the response text.

        Args:
            response_text (str): Text from which to extract SQL queries.

        Returns:
            list: A list of extracted SQL queries.
        """
        
        sql_keywords = ["WITH", "SELECT", "UPDATE", "INSERT", "DELETE"]
        if any(keyword in response_text.upper() for keyword in sql_keywords):
            return [response_text]

        return []

    def get_sql_query_from_response(self, messages, input_text):
        """
        Get SQL query from GPT response based on the input messages.

        Args:
            messages (dict): A dictionary structure representing the conversation.

        Returns:
            list: A list of SQL queries extracted from the response.
        """
        try:
            response = openai.chat.completions.create(
                model="gpt-4",
                messages=messages,
                temperature=0.1,
                max_tokens=1500,
            )
            respStr = response.choices[0].message.content
            sql_queries = self.extract_sql_query(respStr)

            if len(sql_queries) == 0:
                logging.info(f"GPT response: {respStr}")
                val = self.bq_manager.run_query(
                    "I am afraid I am not able to answer this", input_text, 0
                )
                print("TIMESTAMP ", val)
            return sql_queries

        except Exception as e:
            logging.exception(f"Error getting SQL query from response: {e}")
            return []

    def run(self, input_text, upload_file_path):
        """
        Main method to run the interface. Checks if the input is already logged,
        gets the SQL query from GPT, and logs it to BigQuery.

        Args:
            input_text (str): The user's input text to process.
        """
        gs_link = None
        try:
            prompt = GPTPromptBuilder(
                self.project_id, input_text, self.user_dataset, self.project_dataset
            )
            check_res = self.bq_manager.fetch_query(input_text)
            print("CHECK RES : ", check_res)

            gen_query = self.query_exists_in_cache(check_res, input_text)



            if gen_query is not None:
                logging.info(
                    f"Input already exists in BigQuery : { gen_query}"
                )
                
                gs_link = self.bq_manager.run_generated_query(
                    gen_query
                )

                query_exec_status = 0
                if len(gs_link) > 0:
                    query_exec_status = 1

                time_val = self.bq_manager.run_query(
                    gen_query, input_text, query_exec_status
                )
                print("TIMESTAMP", time_val)

            else:
                if (self.prompt is None):
                    self.prompt = prompt.construct_prompt(upload_file_path)
                    print("PROMPT INITIALISED ........")

                
                sql_queries = self.get_sql_query_from_response(
                    self.prompt, input_text
                )
                
                
                if len(sql_queries) > 0:
                    query = sql_queries[0]
                    logging.info(f"SQL Query: {query}")
                    gs_link = self.bq_manager.run_generated_query(query)

                    query_exec_status = 0
                    if len(gs_link) > 0:
                        query_exec_status = 1

                    time_val = self.bq_manager.run_query(
                        query, input_text, query_exec_status
                    )
                    print("TIMESTAMP", time_val)
                
                

            return gs_link

        except Exception as e:
            self.bq_manager.run_query(
                f"Exception in running the interface: {e}", input_text, 0
            )
            logging.exception(f"Exception in running the interface: {e}")
    
    
    def query_exists_in_cache(self, cached_data, input_text):
        print("INPUT TEXT : ", input_text)

        for entry in cached_data:
            if entry['input_text'] == input_text:
                generated_query = entry['generated_query']
                print("GEN QUERY INSIDE : ", entry['generated_query'])
                return generated_query
        
        return None


# project_id = "masterdb-317014"
# dataset_id = "AlleviateTax"

# interface = GPTBigQueryInterface(project_id, dataset_id)
# input_text = "how are you"  # Example input
# url = interface.run(input_text)
# if url == None:
#     print("I am afraid I am not able to answer this")

# How many dials were made in October
# How many leads have score greater than 60
# get me all incoming calls for last week
