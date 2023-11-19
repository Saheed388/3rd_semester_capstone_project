from typing import Dict, Any
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

class YouTubeDataToGCSAndBigQueryOperator(BaseOperator):
    def __init__(
        self,
        gcs_bucket_name: str,
        gcs_object_name: str,
        max_results: int = 50,
        postgres_conn_id: str = 'postgres_conn',  # Provide a default value
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_object_name = gcs_object_name
        self.max_results = max_results
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context: Dict[str, Any]) -> None:
        try:
            # Retrieve data from PostgreSQL using a hook
            postgres_data = self.fetch_postgres_data()

            # Convert the DataFrame to JSON
            json_content = postgres_data.to_json(orient='records', lines=True, date_format='iso')

            # Upload the JSON content to GCS
            gcs_hook = GCSHook(google_cloud_storage_conn_id="google_cloud_default")
            gcs_hook.upload(
                bucket_name=self.gcs_bucket_name,
                object_name=self.gcs_object_name,
                data=json_content.encode('utf-8'),
                mime_type='application/json',
            )

            self.log.info(f"Data uploaded to GCS: gs://{self.gcs_bucket_name}/{self.gcs_object_name}")

            # You can add any additional tasks or logic here

        except Exception as e:
            self.log.error(f"An unexpected error occurred: {str(e)}")
            raise

    def fetch_postgres_data(self):
        # Replace the following with your PostgreSQL connection details and query
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        query = "SELECT * FROM youtube_data;"
        postgres_data = postgres_hook.get_pandas_df(sql=query)

        # Optionally, you can perform data transformations or filtering here

        return postgres_data
