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


@task
def load_to_postgres(filename):
    connection, cursor = get_conn_cursor(yt_db_name, yt_db_user, yt_db_password, yt_db_host, port=5432)

    schema = "public"
    create_schema(schema, connection, cursor)
    create_table(schema, connection, cursor)

    data = load_data(filename)
    
    for row in data:
        insert_data(schema, connection, cursor, row)
    
    close_conn_cursor(connection, cursor)



def load_to_postgres2(filename):
    try:
        connection = psycopg2.connect(database=yt_db_name, user=yt_db_user, password=yt_db_password, host=yt_db_host, port=5432)
        cursor = connection.cursor()

        # Create table if it does not exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS youtube_video_stats (
            videoId TEXT PRIMARY KEY,
            title TEXT,
            publishedAt TIMESTAMP,
            duration TEXT,
            viewCount BIGINT,
            likeCount BIGINT,
            commentCount BIGINT
        )
        """
        cursor.execute(create_table_query)

        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)

        for item in data:
            cursor.execute(
                """
                INSERT INTO youtube_video_stats (videoId, title, publishedAt, duration, viewCount, likeCount, commentCount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    item.get("videoId"),
                    item.get("title"),
                    item.get("publishedAt"),
                    item.get("duration"),
                    int(item["viewCount"]) if item.get("viewCount") is not None else None,
                    int(item["likeCount"]) if item.get("likeCount") is not None else None,
                    int(item["commentCount"]) if item.get("commentCount") is not None else None
                )
            )

        connection.commit()
    except Exception as e:
        # Rollback on error and re-raise
        try:
            connection.rollback()
        except Exception:
            pass
        raise e
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            connection.close()
        except Exception:
            pass

