import requests
import json
import os
from datetime import date
from dotenv import load_dotenv


load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")

CHANNEL_HANDLE = "MrBeast"
maxResults = 50


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

def save_to_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)



if __name__ == "__main__":
    playlistId = get_playlist_id()
    video_ids = get_video_ids(playlistId)
    extracted_data = extract_video_stats(video_ids)

    file_name = f"data/video_stats_{date.today()}.json"
    save_to_json(extracted_data, file_name)