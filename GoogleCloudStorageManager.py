import logging
from datetime import datetime, timedelta

import os
from dotenv import load_dotenv

from google.cloud import storage


class GoogleCloudStorageManager:
    def __init__(self, bucket_name, project_id):
        """
        Initialize the GoogleCloudStorageManager instance.
        """
        try:
            # Load environment variables from .env file
            load_dotenv()
            self.bucket_name = bucket_name
            self.client = storage.Client(project=project_id)
            logging.info("Google Cloud Storage client initialized successfully.")

        except Exception as e:
            logging.error(f"Error initializing Google Cloud Storage client: {e}")
            raise

    def upload_to_google_storage(self, local_filename, storage_filename):
        """
        Upload a file9 to Google Cloud Storage and return its gs:// URL.
        """
        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(storage_filename)

            blob.upload_from_filename(local_filename)
            os.remove(local_filename)

            gs_url = f"gs://{self.bucket_name}/{storage_filename}"
            logging.info(f"File uploaded successfully to {gs_url}")
            return gs_url

        except Exception as e:
            os.remove(local_filename) 
            logging.error(f"Error uploading file to Google Cloud Storage: {e}")

            return f"An error occurred: {str(e)}"

    def generate_signed_url(self, storage_filename, expiration=3600):
        """
        Generate a signed URL for a file in Google Cloud Storage.
        """
        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(storage_filename)

            expiration_time = datetime.utcnow() + timedelta(seconds=expiration)
            url = blob.generate_signed_url(expiration=expiration_time, method="GET")
            logging.info(f"Signed URL generated successfully: {url}")
            return url

        except Exception as e:
            logging.error(f"Error generating signed URL: {e}")
            return f"An error occurred: {str(e)}"
