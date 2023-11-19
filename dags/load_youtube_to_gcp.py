from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from youtubeapikey import api_key
from web.operators.youtube_to_gcp import YouTubeDataToGCSOperator  # Import the new operator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Define your default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Create your DAG
with DAG('youtube_gcp_video_dag', default_args=default_args, schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')

    # Fetch and store data in GCS using the new operator
    fetch_and_store_data = YouTubeDataToGCSOperator(
        task_id='fetch_and_store_data',
        gcs_bucket_name='alt_new_bucket',
        gcs_object_name='youtube_project_folder/youtube_data.csv',  # Adjust the object name
        api_key=api_key,
        max_results=50,  # Set the maximum number of results
    )

# Push data from GCS to BigQuery
    upload_to_bigquery = GCSToBigQueryOperator(
        task_id='upload_to_bigquery',
        source_objects=['youtube_project_folder/youtube_data.csv'],
        destination_project_dataset_table='poetic-now-399015.youtube_dataset.youtube_data_table',
        schema_fields=[],  # Define schema fields if needed
        skip_leading_rows=1,
        source_format='NEWLINE_DELIMITED_JSON',
        field_delimiter=',',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',  # Change to your desired write disposition
        autodetect=True, 
        bucket ="alt_new_bucket",
    )

    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> fetch_and_store_data >> upload_to_bigquery >> end
