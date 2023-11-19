from typing import Dict, Any, Union
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import requests

class YouTubeDataToGCSOperator(BaseOperator):
    def __init__(
        self,
        gcs_bucket_name: str,
        gcs_object_name: str,
        api_key: str,
        max_results: int = 50,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_object_name = gcs_object_name
        self.api_key = api_key
        self.max_results = max_results

    def execute(self, context: Dict[str, Any]) -> None:
        try:
            # Use the YouTube API code to fetch and process data
            video_df = self.fetch_video_data(self.api_key, self.max_results)

            # Convert the DataFrame to JSON
            json_content = video_df.to_json(orient='records', lines=True, date_format='iso')

            # Upload the JSON content to GCS
            gcs_hook = GCSHook(google_cloud_storage_conn_id="google_cloud_conn")
            gcs_hook.upload(
                bucket_name=self.gcs_bucket_name,
                object_name=self.gcs_object_name,
                data=json_content.encode('utf-8'),
                mime_type='application/json',
            )

            self.log.info(f"Data uploaded to GCS: gs://{self.gcs_bucket_name}/{self.gcs_object_name}")
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

    




    