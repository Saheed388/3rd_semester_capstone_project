from typing import Dict, Any, Union
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import requests
import io
import os
import gzip

# Import necessary libraries

class WebToGCSHKOperator(BaseOperator):
    def __init__(
        self,
        gcs_bucket_name: str,
        gcs_object_name: str,
        api_endpoint: str,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_object_name = gcs_object_name
        self.api_endpoint = api_endpoint

    def execute(self, context: Dict[str, Any]) -> None:
        # Make an authenticated GET request to the API

        response = requests.get(self.api_endpoint)

        if response.status_code == 200:
            try:
                # Get the content of the response (binary data)
                data = response.content
                
                # Log that data is being extracted
                self.log.info("Extracting data from API...")

                # Upload the binary data to GCS
                gcs_hook = GCSHook(google_cloud_storage_conn_id="google_cloud_default")
                gcs_hook.upload(
                    bucket_name=self.gcs_bucket_name,
                    object_name=self.gcs_object_name,
                    data=data,
                    mime_type='application/gzip',  # Set the correct MIME type for .csv.gz
                )

                self.log.info(f"Data uploaded to GCS: gs://{self.gcs_bucket_name}/{self.gcs_object_name}")
            except Exception as e:
                self.log.error(f"Error while uploading data to GCS: {str(e)}")
                raise
        else:
            self.log.error(f"Failed to retrieve data. Status code: {response.status_code}")
            raise ValueError(f"Failed to retrieve data. Status code: {response.status_code}")
