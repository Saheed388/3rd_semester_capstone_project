from typing import Dict, Any
from airflow.models import BaseOperator
import pandas as pd
import requests
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

class YouTubeDataToPostgresOperator(BaseOperator):
    def __init__(
        self,
        postgres_conn_id: str,
        table_name: str,
        api_key: str,
        max_results: int = 50,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.api_key = api_key
        self.max_results = max_results

    def create_postgres_table(self):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS {} (
                "Video Title" VARCHAR,
                "Channel Name" VARCHAR,
                "Duration" VARCHAR,
                "Date Posted" TIMESTAMP,
                "Views" INTEGER,
                "Likes" INTEGER,
                "Comments" INTEGER
            )
            """.format(self.table_name)
        pg_hook.run(create_table_sql)

    def execute(self, context: Dict[str, Any]) -> None:
        try:
            # Use the YouTube API code to fetch and process data
            video_df = self.fetch_video_data(self.api_key, self.max_results)

            # Upload the DataFrame to PostgreSQL
            self.upload_to_postgres(video_df)

            self.log.info(f"Data uploaded to PostgreSQL table: {self.table_name}")
        except Exception as e:
            self.log.error(f"An unexpected error occurred: {str(e)}")
            raise


    def fetch_video_data(self, api_key, max_results=50):

        search_url = "https://www.googleapis.com/youtube/v3/search"
        video_url = "https://www.googleapis.com/youtube/v3/videos"
        channels_url = "https://www.googleapis.com/youtube/v3/channels"

        video_data = []
        page_token = None
        total_videos = 0

        while True:
            search_params = {
                "key": api_key,
                "part": "snippet",
                "q": "Data",
                "type": "video",
                "maxResults": max_results,
                "pageToken": page_token
            }

            search_response = requests.get(search_url, params=search_params)

            if search_response.status_code == 200:
                search_results = search_response.json()
                video_ids = [item['id']['videoId'] for item in search_results['items']]
                channel_ids = [item['snippet']['channelId'] for item in search_results['items']]

                for i, (video_id, channel_id) in enumerate(zip(video_ids, channel_ids), total_videos):
                    print(f"Downloading video {i + 1} of {total_videos + len(video_ids)}")

                    video_params = {
                        "key": api_key,
                        "part": "snippet,statistics,contentDetails",
                        "id": video_id
                    }
                    video_response = requests.get(video_url, params=video_params)

                    channel_params = {
                        "key": api_key,
                        "part": "snippet",
                        "id": channel_id
                    }
                    channel_response = requests.get(channels_url, params=channel_params)

                    if video_response.status_code == 200 and channel_response.status_code == 200:
                        video_details = video_response.json()
                        channel_details = channel_response.json()
                        video_snippet = video_details['items'][0]['snippet']
                        video_statistics = video_details['items'][0]['statistics']
                        video_content_details = video_details['items'][0]['contentDetails']
                        channel_snippet = channel_details['items'][0]['snippet']
                        video_duration = video_content_details['duration']
                        video_date_posted = video_snippet['publishedAt']
                        video_title = video_snippet['title']
                        view_count = video_statistics.get('viewCount', 0)
                        like_count = video_statistics.get('likeCount', 0)
                        comment_count = video_statistics.get('commentCount', 0)
                        channel_name = channel_snippet['title']

                        video_data.append({
                            "Video Title": video_title,
                            "Channel Name": channel_name,
                            "Duration": video_duration,
                            "Date Posted": video_date_posted,
                            "Views": view_count,
                            "Likes": like_count,
                            "Comments": comment_count
                        })

                    else:
                        print(f"Error in video request for video {i + 1}: {video_response.status_code}")

                page_token = search_results.get('nextPageToken')
                if not page_token:
                    break

            else:
                print("Error in search request:", search_response.status_code)

        df = pd.DataFrame(video_data)
        csv_filename = "youtube_data.csv"
        df.to_csv(csv_filename, index=False)

        return df

    # Replace with your own API key
        api_key = "AIzaSyD3ZEYJcXZSQoU_MHFs8VJ0kXnrsS1Tu0I"
    # Call the function to retrieve and store the data in a DataFrame
        video_df = fetch_video_data(api_key, max_results)

    def upload_to_postgres(self, df):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # Create the table if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                "Video Title" VARCHAR,
                "Channel Name" VARCHAR,
                "Duration" VARCHAR,
                "Date Posted" TIMESTAMP,
                "Views" INTEGER,
                "Likes" INTEGER,
                "Comments" INTEGER
            )
        """
        hook.run(create_table_query)

        # Insert data into the table
        for index, row in df.iterrows():
            insert_query = f"""
                INSERT INTO {self.table_name} VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                row["Video Title"],
                row["Channel Name"],
                row["Duration"],
                pd.to_datetime(row["Date Posted"]),
                row["Views"],
                row["Likes"],
                row["Comments"]
            )
            hook.run(insert_query, parameters=values)

# Usage
# Replace 'your_postgres_conn_id' and 'your_table_name' with your PostgreSQL connection ID and table name




    