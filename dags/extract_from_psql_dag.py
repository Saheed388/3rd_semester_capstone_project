from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from web.operators.extract_from_psql import YouTubeDataToGCSAndBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    'youtube_data_from_psql',
    default_args=default_args,
    description='Extract YouTube data from PostgreSQL and load it into GCS and BigQuery',
    schedule_interval=timedelta(days=1),
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Extract from PostgreSQL and Upload to GCS
extract_and_upload_task = YouTubeDataToGCSAndBigQueryOperator(
    task_id='extract_and_upload_to_gcs',
    gcs_bucket_name='alt_new_bucket',
    gcs_object_name='youtube_data.json',
    postgres_conn_id='postgres_conn',
    max_results=50,
    dag=dag,
)

# Upload to BigQuery
upload_to_bigquery_task = GCSToBigQueryOperator(
    task_id='upload_to_bigquery',
    source_objects=['youtube_data.json'],
    destination_project_dataset_table='poetic-now-399015.youtube_dataset.youtube_data_table',
    schema_fields=[],  # Define schema fields if needed
    skip_leading_rows=1,
    source_format='NEWLINE_DELIMITED_JSON',
    field_delimiter=',',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',  # Change to your desired write disposition
    autodetect=True, 
    bucket="alt_new_bucket",  # Add your GCS bucket
    dag=dag,  # Add the dag parameter
)


end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

start_task >> extract_and_upload_task >> upload_to_bigquery_task >> end_task


# 