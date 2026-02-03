from airflow.decorators import dag
from airflow import DAG
from api.video_stats import get_playlist_id, get_video_ids, extract_video_stats, save_to_json
from datetime import date, datetime
from dataquality.soda import yt_elt_data_quality
from datawarehouse.dwh import staging_table, core_table



@dag(
        dag_id="youtube_video_stats_etl", 
        description="DAG to extract and load YouTube video statistics in json file", 
        start_date=datetime(2024, 1, 1), 
        schedule_interval="@once", 
        catchup=False,
        max_active_runs=1
        )
def youtube_video_stats_elt_dag():

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_stats = extract_video_stats(video_ids)
    filename = f"data/youtube_video_stats_{date.today()}.json"
    filename = save_to_json(video_stats, filename)


youtube_video_stats_elt_dag()


with DAG(
    dag_id="youtube_video_stats_dwh", 
    description="DAG to process json file and load data in datawarehouse",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once", 
    catchup=False,
    max_active_runs=1
) as dag:
    

    update_staging = staging_table()
    update_core = core_table()

    update_staging >> update_core


with DAG(
    dag_id="data_quality", 
    description="DAG to run data quality checks",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once", 
    catchup=False,
    max_active_runs=1
) as dag:
    

    soda_validate_staging = yt_elt_data_quality("staging")
    soda_validate_core = yt_elt_data_quality("core")

    soda_validate_staging >> soda_validate_core


