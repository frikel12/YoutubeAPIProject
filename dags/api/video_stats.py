import requests
import json
import os
import psycopg2

from airflow.decorators import task
from airflow.models import Variable


from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_data, update_rows, delete_rows

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50

# Youtube Database credentials
yt_db_host = os.getenv("POSTGRES_CONN_HOST")
yt_db_name = os.getenv("ELT_DATABASE_NAME")
yt_db_user = os.getenv("ELT_DATABASE_USERNAME")
yt_db_password = os.getenv("ELT_DATABASE_PASSWORD")


@task
def get_playlist_id():
    try:

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        channel_items = data["items"][0]

        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        print(channel_playlistId)
        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e
    
@task
def get_video_ids(playlistId):
    video_ids = []

    pageToken = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}"
    try:
        while True:
            url = base_url
            if pageToken:
                url += f"&pageToken={pageToken}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data["items"]:
                video_ids.append(item["contentDetails"]["videoId"])

            pageToken = data.get("nextPageToken")
            if not pageToken:
                break
        
        return video_ids            
    except requests.exceptions.RequestException as e:
        raise e
    
@task
def extract_video_stats(video_ids):
    extracted_data = []

    def batch_list(video_ids_list, batch_size=50):
        for i in range(0, len(video_ids_list), batch_size):
            yield video_ids_list[i:i+batch_size]

    try:
        for batch in batch_list(video_ids, maxResults):
            ids = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={ids}&key={API_KEY}"

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data["items"]:
                video_data = {
                    "videoId": item["id"],
                    "title": item["snippet"]["title"],
                    "publishedAt": item["snippet"]["publishedAt"],
                    "duration": item["contentDetails"]["duration"],
                    "viewCount": item["statistics"].get("viewCount", None),
                    "likeCount": item["statistics"].get("likeCount", None),
                    "commentCount": item["statistics"].get("commentCount", None),
                }
                extracted_data.append(video_data)
        return extracted_data
    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    
    return filename
