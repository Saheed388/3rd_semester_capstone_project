from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from web.operators.youtube_to_psql import YouTubeDataToPostgresOperator
from airflow.operators.python_operator import PythonOperator

# Define your default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Define a Python function for creating the PostgreSQL table
def create_postgres_table():
    from airflow.hooks.postgres_hook import PostgresHook

    postgres_conn_id = 'postgres_conn'  # Replace with your PostgreSQL connection ID
    table_name = 'youtube_data'  # Replace with your PostgreSQL table name

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            "Video Title" VARCHAR,
            "Channel Name" VARCHAR,
            "Duration" VARCHAR,
            "Date Posted" TIMESTAMP,
            "Views" INTEGER,
            "Likes" INTEGER,
            "Comments" INTEGER
        )
    """
    pg_hook.run(create_table_sql)

# Create your DAG
with DAG('youtube_psql_video_dag', default_args=default_args, schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')

    # Create PostgreSQL table
    create_table_task = PythonOperator(
        task_id='create_postgres_table',
        python_callable=create_postgres_table,
    )

    # Fetch and store data in PostgreSQL using the new operator
    fetch_and_store_data = YouTubeDataToPostgresOperator(
        task_id='fetch_and_store_data',
        postgres_conn_id='postgres_conn',  # Replace with your PostgreSQL connection ID
        table_name='youtube_data',  # Replace with your PostgreSQL table name
        api_key="AIzaSyD3ZEYJcXZSQoU_MHFs8VJ0kXnrsS1Tu0I",
        max_results=50,  # Set the maximum number of results
    )

    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> create_table_task >> fetch_and_store_data >> end
