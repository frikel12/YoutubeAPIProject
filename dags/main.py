from airflow.decorators import dag
from api.video_stats import get_playlist_id, get_video_ids, extract_video_stats, save_to_json, load_to_postgres
from datetime import date, datetime




@dag("youtube_video_stats_etl", start_date=datetime(2024, 1, 1), schedule_interval="@once", catchup=False)
def youtube_video_stats_elt_dag():

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_stats = extract_video_stats(video_ids)
    filename = f"data/youtube_video_stats_{date.today()}.json"
    filename = save_to_json(video_stats, filename)
    load_to_postgres(filename)


youtube_video_stats_elt_dag()
